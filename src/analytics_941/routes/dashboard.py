"""
Dashboard routes for 941 Analytics.

Uses Jinja2 templates for rendering, supports HTMX partial loading.
"""

import asyncio
import hashlib
import logging
import secrets
import time
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from threading import Lock

from fastapi import APIRouter, Cookie, Form, HTTPException, Query, Request, Response

logger = logging.getLogger(__name__)
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from ..config import MIN_PASSKEY_LENGTH, AnalyticsConfig, verify_passkey
from ..core.client import AnalyticsClient
from ..core.models import DashboardFilters

# Auth constants
AUTH_COOKIE_NAME = "analytics_auth"
AUTH_COOKIE_MAX_AGE = 60 * 60 * 24 * 30  # 30 days

# Rate limiting constants (sec-2)
RATE_LIMIT_MAX_ATTEMPTS = 5
RATE_LIMIT_WINDOW_SEC = 15 * 60  # 15 minutes


class LoginRateLimiter:
    """In-memory rate limiter for login attempts.

    Uses hashed IP addresses for privacy. Thread-safe.
    """

    def __init__(self, max_attempts: int = RATE_LIMIT_MAX_ATTEMPTS, window_sec: int = RATE_LIMIT_WINDOW_SEC):
        self.max_attempts = max_attempts
        self.window_sec = window_sec
        self._attempts: dict[str, list[float]] = defaultdict(list)
        self._lock = Lock()

    def _hash_ip(self, ip: str, salt: str) -> str:
        """Hash IP with salt for privacy (no raw IPs stored)."""
        return hashlib.sha256(f"{salt}:{ip}".encode()).hexdigest()[:16]

    def _cleanup(self, key: str, now: float) -> None:
        """Remove expired attempts."""
        cutoff = now - self.window_sec
        self._attempts[key] = [t for t in self._attempts[key] if t > cutoff]

    def is_rate_limited(self, ip: str, salt: str) -> bool:
        """Check if IP is rate limited."""
        key = self._hash_ip(ip, salt)
        now = time.time()

        with self._lock:
            self._cleanup(key, now)
            return len(self._attempts[key]) >= self.max_attempts

    def record_attempt(self, ip: str, salt: str) -> None:
        """Record a login attempt."""
        key = self._hash_ip(ip, salt)
        now = time.time()

        with self._lock:
            self._cleanup(key, now)
            self._attempts[key].append(now)

    def clear(self, ip: str, salt: str) -> None:
        """Clear rate limit for IP (on successful login)."""
        key = self._hash_ip(ip, salt)

        with self._lock:
            self._attempts.pop(key, None)

    def get_remaining_attempts(self, ip: str, salt: str) -> int:
        """Get remaining attempts before rate limit."""
        key = self._hash_ip(ip, salt)
        now = time.time()

        with self._lock:
            self._cleanup(key, now)
            return max(0, self.max_attempts - len(self._attempts[key]))


# Global rate limiter instance
_login_rate_limiter = LoginRateLimiter()


def _hash_passkey(passkey: str, site_name: str) -> str:
    """Hash the passkey with the site name as salt."""
    return hashlib.sha256(f"{site_name}:{passkey}".encode()).hexdigest()


def _verify_auth(auth_cookie: str | None, expected_hash: str) -> bool:
    """Verify the auth cookie matches the expected hash."""
    if not auth_cookie:
        return False
    return secrets.compare_digest(auth_cookie, expected_hash)


def _parse_date_range(
    period: str,
    custom_start: str | None = None,
    custom_end: str | None = None,
) -> tuple[date, date, date | None, date | None]:
    """Parse period string or custom dates into date range with comparison period.

    Args:
        period: Preset period string (24h, 7d, 30d, 90d, year, all, custom)
        custom_start: Custom start date in YYYY-MM-DD format
        custom_end: Custom end date in YYYY-MM-DD format

    Returns:
        Tuple of (start_date, end_date, compare_start, compare_end)

    Raises:
        HTTPException: If custom dates are invalid
    """
    today = date.today()

    # Handle custom date range
    if period == "custom" or (custom_start and custom_end):
        if not custom_start or not custom_end:
            raise HTTPException(
                status_code=400,
                detail="Both start and end dates are required for custom date range"
            )

        try:
            start = date.fromisoformat(custom_start)
            end = date.fromisoformat(custom_end)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid date format. Use YYYY-MM-DD (e.g., 2024-01-15)"
            ) from None

        # Validate date range
        if end < start:
            raise HTTPException(
                status_code=400,
                detail="End date must be on or after start date"
            )

        if end > today:
            raise HTTPException(
                status_code=400,
                detail="End date cannot be in the future"
            )

        # Calculate comparison period (same duration, immediately prior)
        duration = (end - start).days + 1
        compare_end = start - timedelta(days=1)
        compare_start = compare_end - timedelta(days=duration - 1)

        return start, end, compare_start, compare_end

    # Handle preset periods
    if period == "24h":
        start = today - timedelta(days=1)
        end = today
        compare_start = start - timedelta(days=1)
        compare_end = start
    elif period == "7d":
        start = today - timedelta(days=7)
        end = today
        compare_start = start - timedelta(days=7)
        compare_end = start
    elif period == "30d":
        start = today - timedelta(days=30)
        end = today
        compare_start = start - timedelta(days=30)
        compare_end = start
    elif period == "90d":
        start = today - timedelta(days=90)
        end = today
        compare_start = start - timedelta(days=90)
        compare_end = start
    elif period == "year":
        start = today - timedelta(days=365)
        end = today
        compare_start = start - timedelta(days=365)
        compare_end = start
    elif period == "all":
        # No comparison for all-time
        start = date(2020, 1, 1)
        end = today
        compare_start = None
        compare_end = None
    else:
        # Default to 30 days
        start = today - timedelta(days=30)
        end = today
        compare_start = start - timedelta(days=30)
        compare_end = start

    return start, end, compare_start, compare_end


def _format_duration(seconds: int) -> str:
    """Format duration in seconds to human readable string."""
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        minutes = seconds // 60
        secs = seconds % 60
        return f"{minutes}m {secs}s"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{hours}h {minutes}m"


def _pydantic_json(value):
    """Convert Pydantic models to JSON-serializable dicts.

    Handles single models, lists of models, and nested structures.
    Uses mode='json' to convert datetime to ISO strings.
    """
    from pydantic import BaseModel

    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    elif isinstance(value, list):
        return [_pydantic_json(item) for item in value]
    elif isinstance(value, dict):
        return {k: _pydantic_json(v) for k, v in value.items()}
    return value


def _substr(value, start: int, end: int | None = None):
    """Get a substring from a string value.

    Args:
        value: The string to slice
        start: Start index
        end: Optional end index

    Returns:
        Substring from start to end (or end of string if end is None)
    """
    if not isinstance(value, str):
        value = str(value)
    if end is None:
        return value[start:]
    return value[start:end]


def create_dashboard_router(config: AnalyticsConfig) -> APIRouter:
    """Create dashboard router with Jinja2 templates.

    Args:
        config: Analytics configuration
    """
    router = APIRouter(tags=["analytics"])

    # Set up templates
    template_dir = Path(__file__).parent.parent / "templates"
    templates = Jinja2Templates(directory=str(template_dir))

    # Add custom filters
    templates.env.filters["format_duration"] = _format_duration
    templates.env.filters["pydantic_json"] = _pydantic_json
    templates.env.filters["substr"] = _substr

    # Static files directory
    static_dir = Path(__file__).parent.parent / "static"

    # Explicit routes for static files (mount() doesn't work with include_router prefix)
    @router.get("/static/css/{filename}")
    async def serve_css(filename: str):
        """Serve CSS files with caching."""
        file_path = static_dir / "css" / filename
        if not file_path.exists() or not file_path.is_file():
            raise HTTPException(status_code=404, detail="File not found")
        return FileResponse(
            file_path,
            media_type="text/css",
            headers={"Cache-Control": "public, max-age=31536000"},
        )

    @router.get("/static/js/{filename}")
    async def serve_js(filename: str):
        """Serve JavaScript files with caching."""
        file_path = static_dir / "js" / filename
        if not file_path.exists() or not file_path.is_file():
            raise HTTPException(status_code=404, detail="File not found")
        return FileResponse(
            file_path,
            media_type="application/javascript",
            headers={"Cache-Control": "public, max-age=31536000"},
        )

    # Pre-compute expected hash if passkey is set
    expected_hash = _hash_passkey(config.passkey, config.site_name) if config.passkey else None

    # Create client
    client = AnalyticsClient(
        d1_database_id=config.d1_database_id,
        cf_account_id=config.cf_account_id,
        cf_api_token=config.cf_api_token,
        site_name=config.site_name,
    )

    def _check_auth(auth_cookie: str | None) -> bool:
        """Check if authentication is valid."""
        if not config.has_auth:
            return True
        if not expected_hash:
            return True
        return _verify_auth(auth_cookie, expected_hash)

    def _get_common_context(request: Request, active_tab: str, period: str = "30d") -> dict:
        """Build common template context."""
        # Build current params string for filter chip removal
        current_params = str(request.query_params)
        return {
            "request": request,
            "site_name": config.effective_display_name,  # Use display name for UI
            "site_domain": config.site_name,  # Keep domain for API calls
            "site_timezone": config.timezone,  # Site timezone for JS display
            "config": config,
            "has_auth": config.has_auth,
            "active_tab": active_tab,
            "date_range_key": period,
            "format_duration": _format_duration,
            "current_params": current_params,
        }

    def _get_filters(
        country: str | None = None,
        region: str | None = None,
        device: str | None = None,
        browser: str | None = None,
        source: str | None = None,
        source_type: str | None = None,
        page: str | None = None,
        utm_source: str | None = None,
        utm_campaign: str | None = None,
    ) -> DashboardFilters:
        """Parse query parameters into DashboardFilters."""
        return DashboardFilters(
            country=country,
            region=region,
            device=device,
            browser=browser,
            source=source,
            source_type=source_type,
            page=page,
            utm_source=utm_source,
            utm_campaign=utm_campaign,
        )

    async def _parallel_queries(**queries: dict) -> dict:
        """Execute multiple async queries in parallel with error handling.

        Args:
            **queries: Named coroutines to execute (e.g., metrics=client.get_metrics(...))

        Returns:
            Dict with same keys, values are either results or None on failure.
            Failed queries are logged but don't fail the entire request.
        """
        names = list(queries.keys())
        coros = list(queries.values())

        results = await asyncio.gather(*coros, return_exceptions=True)

        output = {}
        for name, result in zip(names, results):
            if isinstance(result, Exception):
                logger.error(f"Query '{name}' failed: {result}")
                output[name] = None
            else:
                output[name] = result

        return output

    # -------------------------------------------------------------------------
    # Auth Routes
    # -------------------------------------------------------------------------

    @router.get("/login", response_class=HTMLResponse)
    async def login_page(request: Request, error: str = ""):
        """Render login page."""
        return templates.TemplateResponse(
            "pages/login.html",
            {
                "request": request,
                "site_name": config.effective_display_name,
                "error": error,
                "has_webauthn": config.has_webauthn,
            },
        )

    @router.post("/login")
    async def login_submit(
        request: Request,
        response: Response,
        passkey: str = Form(...),
    ):
        """Handle passkey login with rate limiting (sec-2)."""
        # Get client IP (use X-Forwarded-For if behind proxy)
        client_ip = request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        if not client_ip:
            client_ip = request.client.host if request.client else "unknown"

        # Check rate limit before processing
        if _login_rate_limiter.is_rate_limited(client_ip, config.site_name):
            raise HTTPException(
                status_code=429,
                detail="Too many login attempts. Please try again in 15 minutes."
            )

        # Record this attempt
        _login_rate_limiter.record_attempt(client_ip, config.site_name)

        # Validate passkey length (config-4: 16+ characters required)
        if len(passkey) < MIN_PASSKEY_LENGTH:
            return RedirectResponse(
                url=f"./login?error=Passkey+must+be+at+least+{MIN_PASSKEY_LENGTH}+characters",
                status_code=303
            )

        if config.passkey and verify_passkey(config.passkey, passkey):
            # Clear rate limit on successful login (sec-2)
            _login_rate_limiter.clear(client_ip, config.site_name)

            response = RedirectResponse(url="./", status_code=303)
            response.set_cookie(
                AUTH_COOKIE_NAME,
                _hash_passkey(passkey, config.site_name),
                max_age=AUTH_COOKIE_MAX_AGE,
                httponly=True,
                samesite="lax",
            )
            return response
        return RedirectResponse(url="./login?error=Invalid+passkey", status_code=303)

    @router.get("/logout")
    async def logout(response: Response):
        """Clear auth cookie and redirect to login."""
        response = RedirectResponse(url="./login", status_code=303)
        response.delete_cookie(AUTH_COOKIE_NAME)
        return response

    # -------------------------------------------------------------------------
    # Dashboard Routes
    # -------------------------------------------------------------------------

    @router.get("/", response_class=HTMLResponse)
    async def overview_page(
        request: Request,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        start: str | None = Query(None, alias="start", description="Custom start date (YYYY-MM-DD)"),
        end: str | None = Query(None, alias="end", description="Custom end date (YYYY-MM-DD)"),
        country: str | None = None,
        region: str | None = None,
        device: str | None = None,
        browser: str | None = None,
        source: str | None = None,
        page: str | None = None,
    ):
        """Render overview dashboard page."""
        if not _check_auth(auth):
            return RedirectResponse(url="./login", status_code=303)

        start_date, end_date, compare_start, compare_end = _parse_date_range(period, start, end)
        filters = _get_filters(
            country=country, region=region, device=device,
            browser=browser, source=source, page=page
        )

        # Fetch data in parallel - all queries are independent
        data = await _parallel_queries(
            metrics=client.get_core_metrics(start_date, end_date, compare_start, compare_end, filters),
            time_series=client.get_time_series(start_date, end_date, "day", filters),
            top_pages=client.get_top_pages(start_date, end_date, 10, filters),
            entry_pages=client.get_entry_pages(start_date, end_date, 10, filters),
            exit_pages=client.get_exit_pages(start_date, end_date, 10, filters),
            entry_exit_flow=client.get_entry_exit_flow(start_date, end_date, 10, filters),
            sources=client.get_sources(start_date, end_date, 10, filters),
            countries=client.get_countries(start_date, end_date, 10, filters),
            devices=client.get_devices(start_date, end_date, filters),
            browsers=client.get_browsers(start_date, end_date, 10, filters),
        )

        context = _get_common_context(request, "overview", period)
        context.update({
            "metrics": data["metrics"],
            "time_series": data["time_series"] or [],
            "chart_metric": "visitors",
            "granularity": "day",
            "top_pages": data["top_pages"] or [],
            "entry_pages": data["entry_pages"] or [],
            "exit_pages": data["exit_pages"] or [],
            "entry_exit_flow": data["entry_exit_flow"] or [],
            "sources": data["sources"] or [],
            "countries": data["countries"] or [],
            "devices": data["devices"] or {},
            "browsers": data["browsers"] or [],
            "filters": filters,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        })

        return templates.TemplateResponse("pages/overview.html", context)

    @router.get("/partials/overview", response_class=HTMLResponse)
    async def overview_partial(
        request: Request,
        response: Response,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        start: str | None = Query(None, alias="start", description="Custom start date (YYYY-MM-DD)"),
        end: str | None = Query(None, alias="end", description="Custom end date (YYYY-MM-DD)"),
        country: str | None = None,
        region: str | None = None,
        device: str | None = None,
        browser: str | None = None,
        source: str | None = None,
        page: str | None = None,
    ):
        """HTMX partial for overview tab."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        start_date, end_date, compare_start, compare_end = _parse_date_range(period, start, end)
        filters = _get_filters(
            country=country, region=region, device=device,
            browser=browser, source=source, page=page
        )

        # Fetch data in parallel - all queries are independent
        data = await _parallel_queries(
            metrics=client.get_core_metrics(start_date, end_date, compare_start, compare_end, filters),
            time_series=client.get_time_series(start_date, end_date, "day", filters),
            top_pages=client.get_top_pages(start_date, end_date, 10, filters),
            entry_pages=client.get_entry_pages(start_date, end_date, 10, filters),
            exit_pages=client.get_exit_pages(start_date, end_date, 10, filters),
            entry_exit_flow=client.get_entry_exit_flow(start_date, end_date, 10, filters),
            sources=client.get_sources(start_date, end_date, 10, filters),
            countries=client.get_countries(start_date, end_date, 10, filters),
            devices=client.get_devices(start_date, end_date, filters),
            browsers=client.get_browsers(start_date, end_date, 10, filters),
        )

        context = _get_common_context(request, "overview", period)
        context.update({
            "metrics": data["metrics"],
            "time_series": data["time_series"] or [],
            "chart_metric": "visitors",
            "granularity": "day",
            "top_pages": data["top_pages"] or [],
            "entry_pages": data["entry_pages"] or [],
            "exit_pages": data["exit_pages"] or [],
            "entry_exit_flow": data["entry_exit_flow"] or [],
            "sources": data["sources"] or [],
            "countries": data["countries"] or [],
            "devices": data["devices"] or {},
            "browsers": data["browsers"] or [],
            "filters": filters,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        })

        # Cache dashboard partials for 60 seconds (private to avoid shared caching)
        response.headers["Cache-Control"] = "private, max-age=60"
        return templates.TemplateResponse("partials/overview_content.html", context)

    @router.get("/partials/chart", response_class=HTMLResponse)
    async def chart_partial(
        request: Request,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
        metric: str = "visitors",
        period: str = "30d",
        start: str | None = Query(None, alias="start", description="Custom start date (YYYY-MM-DD)"),
        end: str | None = Query(None, alias="end", description="Custom end date (YYYY-MM-DD)"),
        country: str | None = None,
        region: str | None = None,
        device: str | None = None,
        browser: str | None = None,
        source: str | None = None,
        page: str | None = None,
    ):
        """HTMX partial for chart metric toggle (visitors, views, sessions)."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        # Validate metric
        valid_metrics = {"visitors", "views", "sessions"}
        if metric not in valid_metrics:
            metric = "visitors"

        start_date, end_date, _, _ = _parse_date_range(period, start, end)
        filters = _get_filters(
            country=country, region=region, device=device,
            browser=browser, source=source, page=page
        )

        time_series = await client.get_time_series(start_date, end_date, "day", filters)

        context = {
            "request": request,
            "time_series": time_series,
            "chart_metric": metric,
            "granularity": "day",
        }

        return templates.TemplateResponse("components/chart_area.html", context)

    @router.get("/sources", response_class=HTMLResponse)
    async def sources_page(
        request: Request,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        start: str | None = Query(None, alias="start", description="Custom start date (YYYY-MM-DD)"),
        end: str | None = Query(None, alias="end", description="Custom end date (YYYY-MM-DD)"),
        source: str | None = None,
        source_type: str | None = None,
        utm_source: str | None = None,
        utm_campaign: str | None = None,
    ):
        """Render sources page."""
        if not _check_auth(auth):
            return RedirectResponse(url="./login", status_code=303)

        start_date, end_date, compare_start, compare_end = _parse_date_range(period, start, end)
        filters = _get_filters(
            source=source, source_type=source_type,
            utm_source=utm_source, utm_campaign=utm_campaign
        )

        # Fetch data in parallel
        data = await _parallel_queries(
            metrics=client.get_core_metrics(start_date, end_date, compare_start, compare_end, filters),
            sources_list=client.get_sources(start_date, end_date, 50, filters),
            source_types=client.get_source_types(start_date, end_date, filters),
            utm_sources=client.get_utm_sources(start_date, end_date, 20, filters),
            utm_campaigns=client.get_utm_campaigns(start_date, end_date, 20, filters),
        )

        context = _get_common_context(request, "sources", period)
        context.update({
            "metrics": data["metrics"],
            "sources": data["sources_list"] or [],
            "source_types": data["source_types"] or [],
            "utm_sources": data["utm_sources"] or [],
            "utm_campaigns": data["utm_campaigns"] or [],
            "filters": filters,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        })

        return templates.TemplateResponse("pages/sources.html", context)

    @router.get("/partials/sources", response_class=HTMLResponse)
    async def sources_partial(
        request: Request,
        response: Response,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        start: str | None = Query(None, alias="start", description="Custom start date (YYYY-MM-DD)"),
        end: str | None = Query(None, alias="end", description="Custom end date (YYYY-MM-DD)"),
        source: str | None = None,
        source_type: str | None = None,
        utm_source: str | None = None,
        utm_campaign: str | None = None,
    ):
        """HTMX partial for sources tab."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        start_date, end_date, compare_start, compare_end = _parse_date_range(period, start, end)
        filters = _get_filters(
            source=source, source_type=source_type,
            utm_source=utm_source, utm_campaign=utm_campaign
        )

        # Fetch data in parallel
        data = await _parallel_queries(
            metrics=client.get_core_metrics(start_date, end_date, compare_start, compare_end, filters),
            sources_list=client.get_sources(start_date, end_date, 50, filters),
            source_types=client.get_source_types(start_date, end_date, filters),
            utm_sources=client.get_utm_sources(start_date, end_date, 20, filters),
            utm_campaigns=client.get_utm_campaigns(start_date, end_date, 20, filters),
        )

        context = _get_common_context(request, "sources", period)
        context.update({
            "metrics": data["metrics"],
            "sources": data["sources_list"] or [],
            "source_types": data["source_types"] or [],
            "utm_sources": data["utm_sources"] or [],
            "utm_campaigns": data["utm_campaigns"] or [],
            "filters": filters,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        })

        response.headers["Cache-Control"] = "private, max-age=60"
        return templates.TemplateResponse("partials/sources_content.html", context)

    @router.get("/geography", response_class=HTMLResponse)
    async def geography_page(
        request: Request,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        start: str | None = Query(None, alias="start", description="Custom start date (YYYY-MM-DD)"),
        end: str | None = Query(None, alias="end", description="Custom end date (YYYY-MM-DD)"),
        country: str | None = None,
        region: str | None = None,
    ):
        """Render geography page with lazy-loaded globe."""
        if not _check_auth(auth):
            return RedirectResponse(url="./login", status_code=303)

        start_date, end_date, compare_start, compare_end = _parse_date_range(period, start, end)
        filters = _get_filters(country=country, region=region)

        # Build base queries (always run)
        queries = {
            "metrics": client.get_core_metrics(start_date, end_date, compare_start, compare_end, filters),
            "countries": client.get_countries(start_date, end_date, 50, filters),
            "globe_data": client.get_globe_data(start_date, end_date, filters),
        }

        # Add conditional queries based on filters
        if country:
            queries["regions"] = client.get_regions(start_date, end_date, country, 30)
            if region:
                queries["cities"] = client.get_cities(start_date, end_date, country, region, 30)

        # Execute all queries in parallel
        data = await _parallel_queries(**queries)

        context = _get_common_context(request, "geography", period)
        context.update({
            "metrics": data["metrics"],
            "countries": data["countries"] or [],
            "regions": data.get("regions") or [],
            "cities": data.get("cities") or [],
            "globe_data": data["globe_data"].model_dump() if data["globe_data"] else {},
            "filters": filters,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        })

        return templates.TemplateResponse("pages/geography.html", context)

    @router.get("/partials/geography", response_class=HTMLResponse)
    async def geography_partial(
        request: Request,
        response: Response,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        start: str | None = Query(None, alias="start", description="Custom start date (YYYY-MM-DD)"),
        end: str | None = Query(None, alias="end", description="Custom end date (YYYY-MM-DD)"),
        country: str | None = None,
        region: str | None = None,
    ):
        """HTMX partial for geography tab."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        start_date, end_date, compare_start, compare_end = _parse_date_range(period, start, end)
        filters = _get_filters(country=country, region=region)

        # Build base queries (always run)
        queries = {
            "metrics": client.get_core_metrics(start_date, end_date, compare_start, compare_end, filters),
            "countries": client.get_countries(start_date, end_date, 50, filters),
            "globe_data": client.get_globe_data(start_date, end_date, filters),
        }

        # Add conditional queries based on filters
        if country:
            queries["regions"] = client.get_regions(start_date, end_date, country, 30)
            if region:
                queries["cities"] = client.get_cities(start_date, end_date, country, region, 30)

        # Execute all queries in parallel
        data = await _parallel_queries(**queries)

        context = _get_common_context(request, "geography", period)
        context.update({
            "metrics": data["metrics"],
            "countries": data["countries"] or [],
            "regions": data.get("regions") or [],
            "cities": data.get("cities") or [],
            "globe_data": data["globe_data"].model_dump() if data["globe_data"] else {},
            "filters": filters,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        })

        response.headers["Cache-Control"] = "private, max-age=60"
        return templates.TemplateResponse("partials/geography_content.html", context)

    @router.get("/technology", response_class=HTMLResponse)
    async def technology_page(
        request: Request,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        start: str | None = Query(None, alias="start", description="Custom start date (YYYY-MM-DD)"),
        end: str | None = Query(None, alias="end", description="Custom end date (YYYY-MM-DD)"),
        device: str | None = None,
        browser: str | None = None,
        os: str | None = None,
    ):
        """Render technology page."""
        if not _check_auth(auth):
            return RedirectResponse(url="./login", status_code=303)

        start_date, end_date, compare_start, compare_end = _parse_date_range(period, start, end)
        filters = _get_filters(device=device, browser=browser)

        # Fetch data in parallel
        data = await _parallel_queries(
            metrics=client.get_core_metrics(start_date, end_date, compare_start, compare_end, filters),
            devices=client.get_devices(start_date, end_date, filters),
            browsers_list=client.get_browsers(start_date, end_date, 20, filters),
            operating_systems=client.get_operating_systems(start_date, end_date, 20, filters),
            screen_sizes=client.get_screen_sizes(start_date, end_date, 20, filters),
            screen_breakpoints=client.get_screen_breakpoints(start_date, end_date, filters),
            languages=client.get_languages(start_date, end_date, 20, filters),
        )

        context = _get_common_context(request, "technology", period)
        context.update({
            "metrics": data["metrics"],
            "devices": data["devices"] or {},
            "browsers": data["browsers_list"] or [],
            "operating_systems": data["operating_systems"] or [],
            "screen_sizes": data["screen_sizes"] or [],
            "screen_breakpoints": data["screen_breakpoints"] or [],
            "languages": data["languages"] or [],
            "filters": filters,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        })

        return templates.TemplateResponse("pages/technology.html", context)

    @router.get("/partials/technology", response_class=HTMLResponse)
    async def technology_partial(
        request: Request,
        response: Response,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        start: str | None = Query(None, alias="start", description="Custom start date (YYYY-MM-DD)"),
        end: str | None = Query(None, alias="end", description="Custom end date (YYYY-MM-DD)"),
        device: str | None = None,
        browser: str | None = None,
        os: str | None = None,
    ):
        """HTMX partial for technology tab."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        start_date, end_date, compare_start, compare_end = _parse_date_range(period, start, end)
        filters = _get_filters(device=device, browser=browser)

        # Fetch data in parallel
        data = await _parallel_queries(
            metrics=client.get_core_metrics(start_date, end_date, compare_start, compare_end, filters),
            devices=client.get_devices(start_date, end_date, filters),
            browsers_list=client.get_browsers(start_date, end_date, 20, filters),
            operating_systems=client.get_operating_systems(start_date, end_date, 20, filters),
            screen_sizes=client.get_screen_sizes(start_date, end_date, 20, filters),
            screen_breakpoints=client.get_screen_breakpoints(start_date, end_date, filters),
            languages=client.get_languages(start_date, end_date, 20, filters),
        )

        context = _get_common_context(request, "technology", period)
        context.update({
            "metrics": data["metrics"],
            "devices": data["devices"] or {},
            "browsers": data["browsers_list"] or [],
            "operating_systems": data["operating_systems"] or [],
            "screen_sizes": data["screen_sizes"] or [],
            "screen_breakpoints": data["screen_breakpoints"] or [],
            "languages": data["languages"] or [],
            "filters": filters,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        })

        response.headers["Cache-Control"] = "private, max-age=60"
        return templates.TemplateResponse("partials/technology_content.html", context)

    @router.get("/events", response_class=HTMLResponse)
    async def events_page(
        request: Request,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        start: str | None = Query(None, alias="start", description="Custom start date (YYYY-MM-DD)"),
        end: str | None = Query(None, alias="end", description="Custom end date (YYYY-MM-DD)"),
        event: str | None = None,
        event_type: str | None = None,
    ):
        """Render events page."""
        if not _check_auth(auth):
            return RedirectResponse(url="./login", status_code=303)

        start_date, end_date, compare_start, compare_end = _parse_date_range(period, start, end)
        filters = _get_filters()

        # Build queries dict for parallel execution
        queries = {
            "metrics": client.get_core_metrics(start_date, end_date, compare_start, compare_end, filters),
            "events": client.get_events_with_trend(
                start_date, end_date, compare_start, compare_end, 50, event_type, filters
            ),
            "events_time_series": client.get_events_time_series(start_date, end_date, event_type, filters),
            "scroll_depth": client.get_scroll_depth(start_date, end_date),
            "scroll_depth_by_page": client.get_scroll_depth_by_page(start_date, end_date, 10, filters),
            "outbound_clicks": client.get_outbound_clicks(start_date, end_date, 20, filters),
            "file_downloads": client.get_file_downloads(start_date, end_date, 20, filters),
            "form_submissions": client.get_form_submissions(start_date, end_date, 20, filters),
            "js_errors": client.get_js_errors(start_date, end_date, 20, filters),
            "event_types_list": client.get_event_types(start_date, end_date),
        }

        # Add conditional query for event properties
        if event:
            queries["event_properties"] = client.get_event_properties(event, start_date, end_date, 100, filters)

        # Execute all queries in parallel
        data = await _parallel_queries(**queries)

        context = _get_common_context(request, "events", period)
        context.update({
            "metrics": data["metrics"],
            "events": data["events"] or [],
            "events_time_series": data["events_time_series"] or [],
            "scroll_depth": data["scroll_depth"],
            "scroll_depth_by_page": data["scroll_depth_by_page"] or [],
            "outbound_clicks": data["outbound_clicks"] or [],
            "file_downloads": data["file_downloads"] or [],
            "form_submissions": data["form_submissions"] or [],
            "js_errors": data["js_errors"] or [],
            "event_types": data["event_types_list"] or [],
            "event_properties": data.get("event_properties") or [],
            "selected_event": event,
            "selected_event_type": event_type,
            "filters": filters,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        })

        return templates.TemplateResponse("pages/events.html", context)

    @router.get("/partials/events", response_class=HTMLResponse)
    async def events_partial(
        request: Request,
        response: Response,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        start: str | None = Query(None, alias="start", description="Custom start date (YYYY-MM-DD)"),
        end: str | None = Query(None, alias="end", description="Custom end date (YYYY-MM-DD)"),
        event: str | None = None,
        event_type: str | None = None,
    ):
        """HTMX partial for events tab."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        start_date, end_date, compare_start, compare_end = _parse_date_range(period, start, end)
        filters = _get_filters()

        # Build queries dict for parallel execution
        queries = {
            "metrics": client.get_core_metrics(start_date, end_date, compare_start, compare_end, filters),
            "events": client.get_events_with_trend(
                start_date, end_date, compare_start, compare_end, 50, event_type, filters
            ),
            "events_time_series": client.get_events_time_series(start_date, end_date, event_type, filters),
            "scroll_depth": client.get_scroll_depth(start_date, end_date),
            "scroll_depth_by_page": client.get_scroll_depth_by_page(start_date, end_date, 10, filters),
            "outbound_clicks": client.get_outbound_clicks(start_date, end_date, 20, filters),
            "file_downloads": client.get_file_downloads(start_date, end_date, 20, filters),
            "form_submissions": client.get_form_submissions(start_date, end_date, 20, filters),
            "js_errors": client.get_js_errors(start_date, end_date, 20, filters),
            "event_types_list": client.get_event_types(start_date, end_date),
        }

        # Add conditional query for event properties
        if event:
            queries["event_properties"] = client.get_event_properties(event, start_date, end_date, 100, filters)

        # Execute all queries in parallel
        data = await _parallel_queries(**queries)

        context = _get_common_context(request, "events", period)
        context.update({
            "metrics": data["metrics"],
            "events": data["events"] or [],
            "events_time_series": data["events_time_series"] or [],
            "scroll_depth": data["scroll_depth"],
            "scroll_depth_by_page": data["scroll_depth_by_page"] or [],
            "outbound_clicks": data["outbound_clicks"] or [],
            "file_downloads": data["file_downloads"] or [],
            "form_submissions": data["form_submissions"] or [],
            "js_errors": data["js_errors"] or [],
            "event_types": data["event_types_list"] or [],
            "event_properties": data.get("event_properties") or [],
            "selected_event": event,
            "selected_event_type": event_type,
            "filters": filters,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        })

        response.headers["Cache-Control"] = "private, max-age=60"
        return templates.TemplateResponse("partials/events_content.html", context)

    @router.get("/realtime", response_class=HTMLResponse)
    async def realtime_page(
        request: Request,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
    ):
        """Render realtime page."""
        if not _check_auth(auth):
            return RedirectResponse(url="./login", status_code=303)

        realtime = await client.get_realtime_data()

        context = _get_common_context(request, "realtime")
        context["realtime"] = realtime
        context["realtime_count"] = realtime.active_visitors

        return templates.TemplateResponse("pages/realtime.html", context)

    # -------------------------------------------------------------------------
    # Funnel Routes
    # -------------------------------------------------------------------------

    @router.get("/funnels", response_class=HTMLResponse)
    async def funnels_page(
        request: Request,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        start: str | None = Query(None, alias="start", description="Custom start date (YYYY-MM-DD)"),
        end: str | None = Query(None, alias="end", description="Custom end date (YYYY-MM-DD)"),
        funnel_id: int | None = Query(None, description="Specific funnel to analyze"),
    ):
        """Render funnels page."""
        if not _check_auth(auth):
            return RedirectResponse(url="./login", status_code=303)

        start_date, end_date, compare_start, compare_end = _parse_date_range(period, start, end)
        filters = _get_filters()

        # Ensure preset funnels exist
        await client.ensure_preset_funnels()

        # Get all funnels
        funnels = await client.get_funnels()

        # Analyze selected funnel or first available
        selected_funnel = None
        funnel_result = None
        if funnels:
            if funnel_id:
                selected_funnel = next((f for f in funnels if f.id == funnel_id), funnels[0])
            else:
                selected_funnel = funnels[0]

            funnel_result = await client.analyze_funnel(
                selected_funnel.id,
                start_date,
                end_date,
            )

        metrics = await client.get_core_metrics(start_date, end_date, compare_start, compare_end, filters)

        context = _get_common_context(request, "funnels", period)
        context.update({
            "metrics": metrics,
            "funnels": funnels,
            "selected_funnel": selected_funnel,
            "funnel_result": funnel_result,
            "filters": filters,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        })

        return templates.TemplateResponse("pages/funnels.html", context)

    @router.get("/partials/funnels", response_class=HTMLResponse)
    async def funnels_partial(
        request: Request,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        start: str | None = Query(None, alias="start", description="Custom start date (YYYY-MM-DD)"),
        end: str | None = Query(None, alias="end", description="Custom end date (YYYY-MM-DD)"),
        funnel_id: int | None = Query(None, description="Specific funnel to analyze"),
    ):
        """HTMX partial for funnels tab."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        start_date, end_date, compare_start, compare_end = _parse_date_range(period, start, end)
        filters = _get_filters()

        # Ensure preset funnels exist
        await client.ensure_preset_funnels()

        # Get all funnels
        funnels = await client.get_funnels()

        # Analyze selected funnel or first available
        selected_funnel = None
        funnel_result = None
        if funnels:
            if funnel_id:
                selected_funnel = next((f for f in funnels if f.id == funnel_id), funnels[0])
            else:
                selected_funnel = funnels[0]

            funnel_result = await client.analyze_funnel(
                selected_funnel.id,
                start_date,
                end_date,
            )

        metrics = await client.get_core_metrics(start_date, end_date, compare_start, compare_end, filters)

        context = _get_common_context(request, "funnels", period)
        context.update({
            "metrics": metrics,
            "funnels": funnels,
            "selected_funnel": selected_funnel,
            "funnel_result": funnel_result,
            "filters": filters,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        })

        return templates.TemplateResponse("partials/funnels_content.html", context)

    @router.post("/funnels/create", response_class=HTMLResponse)
    async def create_funnel(
        request: Request,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
        name: str = Form(...),
        description: str = Form(""),
        steps: str = Form(...),  # JSON string of steps
    ):
        """Create a new custom funnel."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        import json
        try:
            steps_data = json.loads(steps)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid steps JSON")

        from ..core.models import FunnelStep
        funnel_steps = [FunnelStep(**step) for step in steps_data]

        await client.create_funnel(name, description or None, funnel_steps)

        # Redirect back to funnels page
        return RedirectResponse(url="./funnels", status_code=303)

    @router.delete("/funnels/{funnel_id}")
    async def delete_funnel(
        request: Request,
        funnel_id: int,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
    ):
        """Delete a funnel."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        await client.delete_funnel(funnel_id)
        return {"status": "deleted"}

    # -------------------------------------------------------------------------
    # Goals Routes
    # -------------------------------------------------------------------------

    @router.get("/goals", response_class=HTMLResponse)
    async def goals_page(
        request: Request,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        start: str | None = Query(None, alias="start", description="Custom start date (YYYY-MM-DD)"),
        end: str | None = Query(None, alias="end", description="Custom end date (YYYY-MM-DD)"),
        goal_id: int | None = Query(None, description="Specific goal to highlight"),
    ):
        """Render goals page."""
        if not _check_auth(auth):
            return RedirectResponse(url="./login")

        start_date, end_date = _parse_date_range(period, start, end)

        # Ensure preset goals exist
        await client.ensure_preset_goals()

        # Get all goals
        goals = await client.get_goals(active_only=False)

        # Analyze all goals or just selected one
        goal_results = []
        selected_goal = None

        if goal_id and goals:
            selected_goal = next((g for g in goals if g.id == goal_id), None)
            if selected_goal:
                result = await client.analyze_goal(selected_goal, start_date, end_date)
                goal_results = [result]
        elif goals:
            # Analyze all active goals
            for goal in goals:
                if goal.is_active:
                    result = await client.analyze_goal(goal, start_date, end_date)
                    goal_results.append(result)
            # Select first as default
            selected_goal = goals[0] if goals else None

        context = _get_common_context(request, "goals")
        context["period"] = period
        context["start_date"] = start_date.isoformat()
        context["end_date"] = end_date.isoformat()
        context["goals"] = goals
        context["selected_goal"] = selected_goal
        context["goal_results"] = goal_results

        return templates.TemplateResponse("pages/goals.html", context)

    @router.get("/partials/goals", response_class=HTMLResponse)
    async def goals_partial(
        request: Request,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        start: str | None = Query(None),
        end: str | None = Query(None),
        goal_id: int | None = Query(None),
    ):
        """HTMX partial for goals tab."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        start_date, end_date = _parse_date_range(period, start, end)

        await client.ensure_preset_goals()
        goals = await client.get_goals(active_only=False)

        goal_results = []
        selected_goal = None

        if goal_id and goals:
            selected_goal = next((g for g in goals if g.id == goal_id), None)
            if selected_goal:
                result = await client.analyze_goal(selected_goal, start_date, end_date)
                goal_results = [result]
        elif goals:
            for goal in goals:
                if goal.is_active:
                    result = await client.analyze_goal(goal, start_date, end_date)
                    goal_results.append(result)
            selected_goal = goals[0] if goals else None

        context = _get_common_context(request, "goals")
        context["period"] = period
        context["start_date"] = start_date.isoformat()
        context["end_date"] = end_date.isoformat()
        context["goals"] = goals
        context["selected_goal"] = selected_goal
        context["goal_results"] = goal_results

        return templates.TemplateResponse("partials/goals_content.html", context)

    @router.post("/goals/create", response_class=HTMLResponse)
    async def create_goal(
        request: Request,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
        name: str = Form(...),
        description: str = Form(""),
        goal_type: str = Form(...),
        goal_value: str = Form(...),
        target_count: int | None = Form(None),
    ):
        """Create a new custom goal."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        from analytics_941.core.models import GoalDefinition

        goal = GoalDefinition(
            site=config.site_name,
            name=name,
            description=description if description else None,
            goal_type=goal_type,
            goal_value=goal_value,
            target_count=target_count,
            is_active=True,
        )

        await client.create_goal(goal)
        return RedirectResponse(url="./goals", status_code=303)

    @router.post("/goals/{goal_id}/toggle")
    async def toggle_goal(
        request: Request,
        goal_id: int,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
    ):
        """Toggle goal active status."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        # Toggle is_active status
        goals = await client.get_goals(active_only=False)
        goal = next((g for g in goals if g.id == goal_id), None)
        if goal:
            await client._query(
                "UPDATE goals SET is_active = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND site = ?",
                [int(not goal.is_active), goal_id, config.site_name],
            )
        return {"status": "toggled"}

    @router.delete("/goals/{goal_id}")
    async def delete_goal(
        request: Request,
        goal_id: int,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
    ):
        """Delete a goal."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        await client.delete_goal(goal_id)
        return {"status": "deleted"}

    # =========================================================================
    # Saved Views Routes
    # =========================================================================

    @router.get("/views", response_class=HTMLResponse)
    async def saved_views_list(
        request: Request,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
    ):
        """Get list of saved views as HTML."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        saved_views = await client.get_saved_views()

        context = _get_common_context(request, "overview")
        context["saved_views"] = saved_views

        return templates.TemplateResponse("partials/saved_views_dropdown.html", context)

    @router.post("/views/create", response_class=HTMLResponse)
    async def create_saved_view(
        request: Request,
        name: str = Form(...),
        description: str = Form(None),
        date_preset: str = Form(None),
        is_default: bool = Form(False),
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
    ):
        """Create a new saved view from current filters."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        from analytics_941.core.models import SavedView

        # Extract current filters from query params
        filters = {}
        filter_keys = ["country", "region", "city", "device", "browser", "os",
                       "source", "source_type", "page", "utm_source", "utm_medium", "utm_campaign"]
        for key in filter_keys:
            value = request.query_params.get(key)
            if value:
                filters[key] = value

        view = SavedView(
            site=config.site_name,
            name=name,
            description=description,
            filters=filters,
            date_preset=date_preset,
            is_default=is_default,
        )

        await client.create_saved_view(view)

        # Return updated dropdown
        saved_views = await client.get_saved_views()
        context = _get_common_context(request, "overview")
        context["saved_views"] = saved_views

        return templates.TemplateResponse("partials/saved_views_dropdown.html", context)

    @router.post("/views/{view_id}/default")
    async def set_view_default(
        request: Request,
        view_id: int,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
    ):
        """Set a view as the default."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        await client.set_default_view(view_id)
        return {"status": "set_default"}

    @router.delete("/views/{view_id}")
    async def delete_saved_view(
        request: Request,
        view_id: int,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
    ):
        """Delete a saved view."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        await client.delete_saved_view(view_id)
        return {"status": "deleted"}

    # =========================================================================
    # Export Routes
    # =========================================================================

    @router.get("/export/pages.csv")
    async def export_pages_csv(
        request: Request,
        period: str = "30d",
        start: str | None = None,
        end: str | None = None,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
    ):
        """Export top pages data as CSV."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        import csv
        from io import StringIO
        from fastapi.responses import StreamingResponse

        date_range = _parse_date_range(period, start, end)
        pages = await client.get_top_pages(date_range.start, date_range.end, limit=1000)

        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(["URL", "Views", "Visitors", "Bounce Rate %", "Avg Time (s)", "Entries", "Exits"])

        for page in pages:
            writer.writerow([
                page.url,
                page.views,
                page.visitors,
                f"{page.bounce_rate:.1f}" if page.bounce_rate else "",
                f"{page.avg_time:.0f}" if page.avg_time else "",
                page.entries,
                page.exits,
            ])

        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=pages_{date_range.start}_{date_range.end}.csv"}
        )

    @router.get("/export/sources.csv")
    async def export_sources_csv(
        request: Request,
        period: str = "30d",
        start: str | None = None,
        end: str | None = None,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
    ):
        """Export traffic sources data as CSV."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        import csv
        from io import StringIO
        from fastapi.responses import StreamingResponse

        date_range = _parse_date_range(period, start, end)
        sources = await client.get_sources(date_range.start, date_range.end)

        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(["Source", "Type", "Visits", "Visitors", "Bounce Rate %"])

        for source in sources:
            writer.writerow([
                source.source,
                source.source_type,
                source.visits,
                source.visitors,
                f"{source.bounce_rate:.1f}" if source.bounce_rate else "",
            ])

        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=sources_{date_range.start}_{date_range.end}.csv"}
        )

    @router.get("/export/geography.csv")
    async def export_geography_csv(
        request: Request,
        period: str = "30d",
        start: str | None = None,
        end: str | None = None,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
    ):
        """Export geography data as CSV."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        import csv
        from io import StringIO
        from fastapi.responses import StreamingResponse

        date_range = _parse_date_range(period, start, end)
        countries = await client.get_countries(date_range.start, date_range.end)

        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(["Country Code", "Country Name", "Visits", "Visitors"])

        for country in countries:
            writer.writerow([
                country.country_code,
                country.country_name,
                country.visits,
                country.visitors,
            ])

        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=geography_{date_range.start}_{date_range.end}.csv"}
        )

    @router.get("/export/events.csv")
    async def export_events_csv(
        request: Request,
        period: str = "30d",
        start: str | None = None,
        end: str | None = None,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
    ):
        """Export events data as CSV."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        import csv
        from io import StringIO
        from fastapi.responses import StreamingResponse

        date_range = _parse_date_range(period, start, end)
        events = await client.get_events(date_range.start, date_range.end)

        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(["Event Name", "Event Type", "Count", "Unique Sessions"])

        for event in events:
            writer.writerow([
                event.event_name,
                event.event_type,
                event.count,
                event.unique_sessions,
            ])

        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=events_{date_range.start}_{date_range.end}.csv"}
        )

    @router.get("/export/report", response_class=HTMLResponse)
    async def export_report(
        request: Request,
        period: str = "30d",
        start: str | None = None,
        end: str | None = None,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
    ):
        """Generate a printable/PDF-ready report."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        filters = DashboardFilters()
        date_range = _parse_date_range(period, start, end)

        # Get all report data
        metrics = await client.get_core_metrics(date_range.start, date_range.end, filters)
        pages = await client.get_top_pages(date_range.start, date_range.end, filters=filters, limit=20)
        sources = await client.get_sources(date_range.start, date_range.end, filters=filters)
        countries = await client.get_countries(date_range.start, date_range.end, filters=filters)
        devices = await client.get_device_breakdown(date_range.start, date_range.end, filters=filters)
        browsers = await client.get_browser_breakdown(date_range.start, date_range.end, filters=filters)

        context = {
            "request": request,
            "site_name": config.effective_display_name,
            "date_range": date_range,
            "metrics": metrics,
            "pages": pages,
            "sources": sources,
            "countries": countries,
            "devices": devices,
            "browsers": browsers,
            "generated_at": datetime.now().isoformat(),
        }

        return templates.TemplateResponse("pages/export_report.html", context)

    @router.get("/partials/realtime", response_class=HTMLResponse)
    async def realtime_partial(
        request: Request,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
    ):
        """HTMX partial for realtime tab (auto-refreshes)."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        realtime = await client.get_realtime_data()

        context = _get_common_context(request, "realtime")
        context["realtime"] = realtime

        return templates.TemplateResponse("partials/realtime_content.html", context)

    @router.get("/partials/activity-feed", response_class=HTMLResponse)
    async def activity_feed_partial(
        request: Request,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
        event_type: str | None = Query(None, description="Filter by event type"),
    ):
        """HTMX partial for activity feed (polled every 5s)."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        active_count, activity = await client.get_activity_feed(
            minutes=5, event_type=event_type
        )

        context = {
            "request": request,
            "active_count": active_count,
            "activity": activity,
            "event_type_filter": event_type or "all",
        }

        return templates.TemplateResponse(
            "components/activity_feed.html", context
        )

    # -------------------------------------------------------------------------
    # Export Routes
    # -------------------------------------------------------------------------

    @router.get("/export/pageviews.csv")
    async def export_pageviews(
        request: Request,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        start: str | None = Query(None, alias="start", description="Custom start date (YYYY-MM-DD)"),
        end: str | None = Query(None, alias="end", description="Custom end date (YYYY-MM-DD)"),
    ):
        """Export pageviews as CSV."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        start_date, end_date, _, _ = _parse_date_range(period, start, end)
        csv_data = await client.export_pageviews(start_date, end_date)

        return StreamingResponse(
            iter([csv_data]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=pageviews_{start_date}_{end_date}.csv"
            },
        )

    @router.get("/export/events.csv")
    async def export_events(
        request: Request,
        auth: str | None = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        start: str | None = Query(None, alias="start", description="Custom start date (YYYY-MM-DD)"),
        end: str | None = Query(None, alias="end", description="Custom end date (YYYY-MM-DD)"),
    ):
        """Export events as CSV."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        start_date, end_date, _, _ = _parse_date_range(period, start, end)
        csv_data = await client.export_events(start_date, end_date)

        return StreamingResponse(
            iter([csv_data]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=events_{start_date}_{end_date}.csv"
            },
        )

    return router
