"""
Dashboard routes for 941 Analytics.

Uses Jinja2 templates for rendering, supports HTMX partial loading.
"""

import hashlib
import secrets
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Optional

from fastapi import APIRouter, Request, Response, Cookie, Query, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from ..config import AnalyticsConfig, verify_passkey, MIN_PASSKEY_LENGTH
from ..core.client import AnalyticsClient
from ..core.models import DashboardFilters, DateRange


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


def _verify_auth(auth_cookie: Optional[str], expected_hash: str) -> bool:
    """Verify the auth cookie matches the expected hash."""
    if not auth_cookie:
        return False
    return secrets.compare_digest(auth_cookie, expected_hash)


def _parse_date_range(
    period: str,
    custom_start: Optional[str] = None,
    custom_end: Optional[str] = None,
) -> tuple[date, date, Optional[date], Optional[date]]:
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
            )

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

    # Mount static files with cache headers
    static_dir = Path(__file__).parent.parent / "static"
    if static_dir.exists():
        router.mount("/static", StaticFiles(directory=str(static_dir)), name="analytics_static")

    # Pre-compute expected hash if passkey is set
    expected_hash = _hash_passkey(config.passkey, config.site_name) if config.passkey else None

    # Create client
    client = AnalyticsClient(
        d1_database_id=config.d1_database_id,
        cf_account_id=config.cf_account_id,
        cf_api_token=config.cf_api_token,
        site_name=config.site_name,
    )

    def _check_auth(auth_cookie: Optional[str]) -> bool:
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
        country: Optional[str] = None,
        region: Optional[str] = None,
        device: Optional[str] = None,
        browser: Optional[str] = None,
        source: Optional[str] = None,
        source_type: Optional[str] = None,
        page: Optional[str] = None,
        utm_source: Optional[str] = None,
        utm_campaign: Optional[str] = None,
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
        auth: Optional[str] = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        start: Optional[str] = Query(None, alias="start", description="Custom start date (YYYY-MM-DD)"),
        end: Optional[str] = Query(None, alias="end", description="Custom end date (YYYY-MM-DD)"),
        country: Optional[str] = None,
        region: Optional[str] = None,
        device: Optional[str] = None,
        browser: Optional[str] = None,
        source: Optional[str] = None,
        page: Optional[str] = None,
    ):
        """Render overview dashboard page."""
        if not _check_auth(auth):
            return RedirectResponse(url="./login", status_code=303)

        start_date, end_date, compare_start, compare_end = _parse_date_range(period, start, end)
        filters = _get_filters(
            country=country, region=region, device=device,
            browser=browser, source=source, page=page
        )

        # Fetch data
        metrics = await client.get_core_metrics(start_date, end_date, compare_start, compare_end, filters)
        time_series = await client.get_time_series(start_date, end_date, "day", filters)
        top_pages = await client.get_top_pages(start_date, end_date, 10, filters)
        entry_pages = await client.get_entry_pages(start_date, end_date, 10, filters)
        exit_pages = await client.get_exit_pages(start_date, end_date, 10, filters)
        entry_exit_flow = await client.get_entry_exit_flow(start_date, end_date, 10, filters)
        sources = await client.get_sources(start_date, end_date, 10, filters)
        countries = await client.get_countries(start_date, end_date, 10, filters)
        devices = await client.get_devices(start_date, end_date, filters)
        browsers = await client.get_browsers(start_date, end_date, 10, filters)

        context = _get_common_context(request, "overview", period)
        context.update({
            "metrics": metrics,
            "time_series": time_series,
            "chart_metric": "visitors",
            "granularity": "day",
            "top_pages": top_pages,
            "entry_pages": entry_pages,
            "exit_pages": exit_pages,
            "entry_exit_flow": entry_exit_flow,
            "sources": sources,
            "countries": countries,
            "devices": devices,
            "browsers": browsers,
            "filters": filters,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        })

        return templates.TemplateResponse("pages/overview.html", context)

    @router.get("/partials/overview", response_class=HTMLResponse)
    async def overview_partial(
        request: Request,
        auth: Optional[str] = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        start: Optional[str] = Query(None, alias="start", description="Custom start date (YYYY-MM-DD)"),
        end: Optional[str] = Query(None, alias="end", description="Custom end date (YYYY-MM-DD)"),
        country: Optional[str] = None,
        region: Optional[str] = None,
        device: Optional[str] = None,
        browser: Optional[str] = None,
        source: Optional[str] = None,
        page: Optional[str] = None,
    ):
        """HTMX partial for overview tab."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        start_date, end_date, compare_start, compare_end = _parse_date_range(period, start, end)
        filters = _get_filters(
            country=country, region=region, device=device,
            browser=browser, source=source, page=page
        )

        metrics = await client.get_core_metrics(start_date, end_date, compare_start, compare_end, filters)
        time_series = await client.get_time_series(start_date, end_date, "day", filters)
        top_pages = await client.get_top_pages(start_date, end_date, 10, filters)
        entry_pages = await client.get_entry_pages(start_date, end_date, 10, filters)
        exit_pages = await client.get_exit_pages(start_date, end_date, 10, filters)
        entry_exit_flow = await client.get_entry_exit_flow(start_date, end_date, 10, filters)
        sources = await client.get_sources(start_date, end_date, 10, filters)
        countries = await client.get_countries(start_date, end_date, 10, filters)
        devices = await client.get_devices(start_date, end_date, filters)
        browsers = await client.get_browsers(start_date, end_date, 10, filters)

        context = _get_common_context(request, "overview", period)
        context.update({
            "metrics": metrics,
            "time_series": time_series,
            "chart_metric": "visitors",
            "granularity": "day",
            "top_pages": top_pages,
            "entry_pages": entry_pages,
            "exit_pages": exit_pages,
            "entry_exit_flow": entry_exit_flow,
            "sources": sources,
            "countries": countries,
            "devices": devices,
            "browsers": browsers,
            "filters": filters,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        })

        return templates.TemplateResponse("partials/overview_content.html", context)

    @router.get("/sources", response_class=HTMLResponse)
    async def sources_page(
        request: Request,
        auth: Optional[str] = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        start: Optional[str] = Query(None, alias="start", description="Custom start date (YYYY-MM-DD)"),
        end: Optional[str] = Query(None, alias="end", description="Custom end date (YYYY-MM-DD)"),
        source: Optional[str] = None,
        source_type: Optional[str] = None,
        utm_source: Optional[str] = None,
        utm_campaign: Optional[str] = None,
    ):
        """Render sources page."""
        if not _check_auth(auth):
            return RedirectResponse(url="./login", status_code=303)

        start_date, end_date, compare_start, compare_end = _parse_date_range(period, start, end)
        filters = _get_filters(
            source=source, source_type=source_type,
            utm_source=utm_source, utm_campaign=utm_campaign
        )

        metrics = await client.get_core_metrics(start_date, end_date, compare_start, compare_end, filters)
        sources_list = await client.get_sources(start_date, end_date, 50, filters)
        source_types = await client.get_source_types(start_date, end_date, filters)
        utm_sources = await client.get_utm_sources(start_date, end_date, 20, filters)
        utm_campaigns = await client.get_utm_campaigns(start_date, end_date, 20, filters)

        context = _get_common_context(request, "sources", period)
        context.update({
            "metrics": metrics,
            "sources": sources_list,
            "source_types": source_types,
            "utm_sources": utm_sources,
            "utm_campaigns": utm_campaigns,
            "filters": filters,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        })

        return templates.TemplateResponse("pages/sources.html", context)

    @router.get("/partials/sources", response_class=HTMLResponse)
    async def sources_partial(
        request: Request,
        auth: Optional[str] = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        start: Optional[str] = Query(None, alias="start", description="Custom start date (YYYY-MM-DD)"),
        end: Optional[str] = Query(None, alias="end", description="Custom end date (YYYY-MM-DD)"),
        source: Optional[str] = None,
        source_type: Optional[str] = None,
        utm_source: Optional[str] = None,
        utm_campaign: Optional[str] = None,
    ):
        """HTMX partial for sources tab."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        start_date, end_date, compare_start, compare_end = _parse_date_range(period, start, end)
        filters = _get_filters(
            source=source, source_type=source_type,
            utm_source=utm_source, utm_campaign=utm_campaign
        )

        metrics = await client.get_core_metrics(start_date, end_date, compare_start, compare_end, filters)
        sources_list = await client.get_sources(start_date, end_date, 50, filters)
        source_types = await client.get_source_types(start_date, end_date, filters)
        utm_sources = await client.get_utm_sources(start_date, end_date, 20, filters)
        utm_campaigns = await client.get_utm_campaigns(start_date, end_date, 20, filters)

        context = _get_common_context(request, "sources", period)
        context.update({
            "metrics": metrics,
            "sources": sources_list,
            "source_types": source_types,
            "utm_sources": utm_sources,
            "utm_campaigns": utm_campaigns,
            "filters": filters,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        })

        return templates.TemplateResponse("partials/sources_content.html", context)

    @router.get("/geography", response_class=HTMLResponse)
    async def geography_page(
        request: Request,
        auth: Optional[str] = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        start: Optional[str] = Query(None, alias="start", description="Custom start date (YYYY-MM-DD)"),
        end: Optional[str] = Query(None, alias="end", description="Custom end date (YYYY-MM-DD)"),
        country: Optional[str] = None,
        region: Optional[str] = None,
    ):
        """Render geography page with lazy-loaded globe."""
        if not _check_auth(auth):
            return RedirectResponse(url="./login", status_code=303)

        start_date, end_date, compare_start, compare_end = _parse_date_range(period, start, end)
        filters = _get_filters(country=country, region=region)

        metrics = await client.get_core_metrics(start_date, end_date, compare_start, compare_end, filters)
        countries = await client.get_countries(start_date, end_date, 50, filters)
        globe_data = await client.get_globe_data(start_date, end_date, filters)

        # Get regions if country is selected
        regions = []
        cities = []
        if country:
            regions = await client.get_regions(start_date, end_date, country, 30)
            if region:
                cities = await client.get_cities(start_date, end_date, country, region, 30)

        context = _get_common_context(request, "geography", period)
        context.update({
            "metrics": metrics,
            "countries": countries,
            "regions": regions,
            "cities": cities,
            "globe_data": globe_data.model_dump(),
            "filters": filters,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        })

        return templates.TemplateResponse("pages/geography.html", context)

    @router.get("/partials/geography", response_class=HTMLResponse)
    async def geography_partial(
        request: Request,
        auth: Optional[str] = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        start: Optional[str] = Query(None, alias="start", description="Custom start date (YYYY-MM-DD)"),
        end: Optional[str] = Query(None, alias="end", description="Custom end date (YYYY-MM-DD)"),
        country: Optional[str] = None,
        region: Optional[str] = None,
    ):
        """HTMX partial for geography tab."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        start_date, end_date, compare_start, compare_end = _parse_date_range(period, start, end)
        filters = _get_filters(country=country, region=region)

        metrics = await client.get_core_metrics(start_date, end_date, compare_start, compare_end, filters)
        countries = await client.get_countries(start_date, end_date, 50, filters)
        globe_data = await client.get_globe_data(start_date, end_date, filters)

        regions = []
        cities = []
        if country:
            regions = await client.get_regions(start_date, end_date, country, 30)
            if region:
                cities = await client.get_cities(start_date, end_date, country, region, 30)

        context = _get_common_context(request, "geography", period)
        context.update({
            "metrics": metrics,
            "countries": countries,
            "regions": regions,
            "cities": cities,
            "globe_data": globe_data.model_dump(),
            "filters": filters,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        })

        return templates.TemplateResponse("partials/geography_content.html", context)

    @router.get("/technology", response_class=HTMLResponse)
    async def technology_page(
        request: Request,
        auth: Optional[str] = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        start: Optional[str] = Query(None, alias="start", description="Custom start date (YYYY-MM-DD)"),
        end: Optional[str] = Query(None, alias="end", description="Custom end date (YYYY-MM-DD)"),
        device: Optional[str] = None,
        browser: Optional[str] = None,
        os: Optional[str] = None,
    ):
        """Render technology page."""
        if not _check_auth(auth):
            return RedirectResponse(url="./login", status_code=303)

        start_date, end_date, compare_start, compare_end = _parse_date_range(period, start, end)
        filters = _get_filters(device=device, browser=browser)

        metrics = await client.get_core_metrics(start_date, end_date, compare_start, compare_end, filters)
        devices = await client.get_devices(start_date, end_date, filters)
        browsers_list = await client.get_browsers(start_date, end_date, 20, filters)
        operating_systems = await client.get_operating_systems(start_date, end_date, 20, filters)
        screen_sizes = await client.get_screen_sizes(start_date, end_date, 20, filters)
        languages = await client.get_languages(start_date, end_date, 20, filters)

        context = _get_common_context(request, "technology", period)
        context.update({
            "metrics": metrics,
            "devices": devices,
            "browsers": browsers_list,
            "operating_systems": operating_systems,
            "screen_sizes": screen_sizes,
            "languages": languages,
            "filters": filters,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        })

        return templates.TemplateResponse("pages/technology.html", context)

    @router.get("/partials/technology", response_class=HTMLResponse)
    async def technology_partial(
        request: Request,
        auth: Optional[str] = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        start: Optional[str] = Query(None, alias="start", description="Custom start date (YYYY-MM-DD)"),
        end: Optional[str] = Query(None, alias="end", description="Custom end date (YYYY-MM-DD)"),
        device: Optional[str] = None,
        browser: Optional[str] = None,
        os: Optional[str] = None,
    ):
        """HTMX partial for technology tab."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        start_date, end_date, compare_start, compare_end = _parse_date_range(period, start, end)
        filters = _get_filters(device=device, browser=browser)

        metrics = await client.get_core_metrics(start_date, end_date, compare_start, compare_end, filters)
        devices = await client.get_devices(start_date, end_date, filters)
        browsers_list = await client.get_browsers(start_date, end_date, 20, filters)
        operating_systems = await client.get_operating_systems(start_date, end_date, 20, filters)
        screen_sizes = await client.get_screen_sizes(start_date, end_date, 20, filters)
        languages = await client.get_languages(start_date, end_date, 20, filters)

        context = _get_common_context(request, "technology", period)
        context.update({
            "metrics": metrics,
            "devices": devices,
            "browsers": browsers_list,
            "operating_systems": operating_systems,
            "screen_sizes": screen_sizes,
            "languages": languages,
            "filters": filters,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        })

        return templates.TemplateResponse("partials/technology_content.html", context)

    @router.get("/events", response_class=HTMLResponse)
    async def events_page(
        request: Request,
        auth: Optional[str] = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        start: Optional[str] = Query(None, alias="start", description="Custom start date (YYYY-MM-DD)"),
        end: Optional[str] = Query(None, alias="end", description="Custom end date (YYYY-MM-DD)"),
        event: Optional[str] = None,
        event_type: Optional[str] = None,
    ):
        """Render events page."""
        if not _check_auth(auth):
            return RedirectResponse(url="./login", status_code=303)

        start_date, end_date, compare_start, compare_end = _parse_date_range(period, start, end)
        filters = _get_filters()

        metrics = await client.get_core_metrics(start_date, end_date, compare_start, compare_end, filters)
        events = await client.get_events_with_trend(
            start_date, end_date, compare_start, compare_end, 50, event_type, filters
        )
        events_time_series = await client.get_events_time_series(start_date, end_date, event_type, filters)
        scroll_depth = await client.get_scroll_depth(start_date, end_date)
        scroll_depth_by_page = await client.get_scroll_depth_by_page(start_date, end_date, 10, filters)
        outbound_clicks = await client.get_outbound_clicks(start_date, end_date, 20, filters)
        file_downloads = await client.get_file_downloads(start_date, end_date, 20, filters)
        form_submissions = await client.get_form_submissions(start_date, end_date, 20, filters)
        js_errors = await client.get_js_errors(start_date, end_date, 20, filters)
        event_types_list = await client.get_event_types(start_date, end_date)

        # Get event properties if a specific event is selected
        event_properties = []
        if event:
            event_properties = await client.get_event_properties(event, start_date, end_date, 100, filters)

        context = _get_common_context(request, "events", period)
        context.update({
            "metrics": metrics,
            "events": events,
            "events_time_series": events_time_series,
            "scroll_depth": scroll_depth,
            "scroll_depth_by_page": scroll_depth_by_page,
            "outbound_clicks": outbound_clicks,
            "file_downloads": file_downloads,
            "form_submissions": form_submissions,
            "js_errors": js_errors,
            "event_types": event_types_list,
            "event_properties": event_properties,
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
        auth: Optional[str] = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        start: Optional[str] = Query(None, alias="start", description="Custom start date (YYYY-MM-DD)"),
        end: Optional[str] = Query(None, alias="end", description="Custom end date (YYYY-MM-DD)"),
        event: Optional[str] = None,
        event_type: Optional[str] = None,
    ):
        """HTMX partial for events tab."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        start_date, end_date, compare_start, compare_end = _parse_date_range(period, start, end)
        filters = _get_filters()

        metrics = await client.get_core_metrics(start_date, end_date, compare_start, compare_end, filters)
        events = await client.get_events_with_trend(
            start_date, end_date, compare_start, compare_end, 50, event_type, filters
        )
        events_time_series = await client.get_events_time_series(start_date, end_date, event_type, filters)
        scroll_depth = await client.get_scroll_depth(start_date, end_date)
        scroll_depth_by_page = await client.get_scroll_depth_by_page(start_date, end_date, 10, filters)
        outbound_clicks = await client.get_outbound_clicks(start_date, end_date, 20, filters)
        file_downloads = await client.get_file_downloads(start_date, end_date, 20, filters)
        form_submissions = await client.get_form_submissions(start_date, end_date, 20, filters)
        js_errors = await client.get_js_errors(start_date, end_date, 20, filters)
        event_types_list = await client.get_event_types(start_date, end_date)

        # Get event properties if a specific event is selected
        event_properties = []
        if event:
            event_properties = await client.get_event_properties(event, start_date, end_date, 100, filters)

        context = _get_common_context(request, "events", period)
        context.update({
            "metrics": metrics,
            "events": events,
            "events_time_series": events_time_series,
            "scroll_depth": scroll_depth,
            "scroll_depth_by_page": scroll_depth_by_page,
            "outbound_clicks": outbound_clicks,
            "file_downloads": file_downloads,
            "form_submissions": form_submissions,
            "js_errors": js_errors,
            "event_types": event_types_list,
            "event_properties": event_properties,
            "selected_event": event,
            "selected_event_type": event_type,
            "filters": filters,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        })

        return templates.TemplateResponse("partials/events_content.html", context)

    @router.get("/realtime", response_class=HTMLResponse)
    async def realtime_page(
        request: Request,
        auth: Optional[str] = Cookie(None, alias=AUTH_COOKIE_NAME),
    ):
        """Render realtime page."""
        if not _check_auth(auth):
            return RedirectResponse(url="./login", status_code=303)

        realtime = await client.get_realtime_data()

        context = _get_common_context(request, "realtime")
        context["realtime"] = realtime

        return templates.TemplateResponse("pages/realtime.html", context)

    @router.get("/partials/realtime", response_class=HTMLResponse)
    async def realtime_partial(
        request: Request,
        auth: Optional[str] = Cookie(None, alias=AUTH_COOKIE_NAME),
    ):
        """HTMX partial for realtime tab (auto-refreshes)."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        realtime = await client.get_realtime_data()

        context = _get_common_context(request, "realtime")
        context["realtime"] = realtime

        return templates.TemplateResponse("partials/realtime_content.html", context)

    # -------------------------------------------------------------------------
    # Export Routes
    # -------------------------------------------------------------------------

    @router.get("/export/pageviews.csv")
    async def export_pageviews(
        request: Request,
        auth: Optional[str] = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        start: Optional[str] = Query(None, alias="start", description="Custom start date (YYYY-MM-DD)"),
        end: Optional[str] = Query(None, alias="end", description="Custom end date (YYYY-MM-DD)"),
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
        auth: Optional[str] = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        start: Optional[str] = Query(None, alias="start", description="Custom start date (YYYY-MM-DD)"),
        end: Optional[str] = Query(None, alias="end", description="Custom end date (YYYY-MM-DD)"),
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
