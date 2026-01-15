"""
Dashboard routes for 941 Analytics.

Uses Jinja2 templates for rendering, supports HTMX partial loading.
"""

import hashlib
import secrets
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Request, Response, Cookie, Query, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from ..config import AnalyticsConfig
from ..core.client import AnalyticsClient
from ..core.models import DashboardFilters, DateRange


# Auth constants
AUTH_COOKIE_NAME = "analytics_auth"
AUTH_COOKIE_MAX_AGE = 60 * 60 * 24 * 30  # 30 days


def _hash_passkey(passkey: str, site_name: str) -> str:
    """Hash the passkey with the site name as salt."""
    return hashlib.sha256(f"{site_name}:{passkey}".encode()).hexdigest()


def _verify_auth(auth_cookie: Optional[str], expected_hash: str) -> bool:
    """Verify the auth cookie matches the expected hash."""
    if not auth_cookie:
        return False
    return secrets.compare_digest(auth_cookie, expected_hash)


def _parse_date_range(period: str) -> tuple[date, date, Optional[date], Optional[date]]:
    """Parse period string into date range with comparison period."""
    today = date.today()

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
        return {
            "request": request,
            "site_name": config.site_name,
            "config": config,
            "has_auth": config.has_auth,
            "active_tab": active_tab,
            "date_range_key": period,
            "format_duration": _format_duration,
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
                "site_name": config.site_name,
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
        """Handle passkey login."""
        if config.passkey and passkey == config.passkey:
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

        start, end, compare_start, compare_end = _parse_date_range(period)
        filters = _get_filters(
            country=country, region=region, device=device,
            browser=browser, source=source, page=page
        )

        # Fetch data
        metrics = await client.get_core_metrics(start, end, compare_start, compare_end, filters)
        time_series = await client.get_time_series(start, end, "day", filters)
        top_pages = await client.get_top_pages(start, end, 10, filters)
        sources = await client.get_sources(start, end, 10, filters)
        countries = await client.get_countries(start, end, 10, filters)
        devices = await client.get_devices(start, end, filters)
        browsers = await client.get_browsers(start, end, 10, filters)

        context = _get_common_context(request, "overview", period)
        context.update({
            "metrics": metrics,
            "time_series": time_series,
            "chart_metric": "visitors",
            "granularity": "day",
            "top_pages": top_pages,
            "sources": sources,
            "countries": countries,
            "devices": devices,
            "browsers": browsers,
            "filters": filters,
        })

        return templates.TemplateResponse("pages/overview.html", context)

    @router.get("/partials/overview", response_class=HTMLResponse)
    async def overview_partial(
        request: Request,
        auth: Optional[str] = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
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

        start, end, compare_start, compare_end = _parse_date_range(period)
        filters = _get_filters(
            country=country, region=region, device=device,
            browser=browser, source=source, page=page
        )

        metrics = await client.get_core_metrics(start, end, compare_start, compare_end, filters)
        time_series = await client.get_time_series(start, end, "day", filters)
        top_pages = await client.get_top_pages(start, end, 10, filters)
        sources = await client.get_sources(start, end, 10, filters)
        countries = await client.get_countries(start, end, 10, filters)
        devices = await client.get_devices(start, end, filters)
        browsers = await client.get_browsers(start, end, 10, filters)

        context = _get_common_context(request, "overview", period)
        context.update({
            "metrics": metrics,
            "time_series": time_series,
            "chart_metric": "visitors",
            "granularity": "day",
            "top_pages": top_pages,
            "sources": sources,
            "countries": countries,
            "devices": devices,
            "browsers": browsers,
            "filters": filters,
        })

        return templates.TemplateResponse("partials/overview_content.html", context)

    @router.get("/sources", response_class=HTMLResponse)
    async def sources_page(
        request: Request,
        auth: Optional[str] = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        source: Optional[str] = None,
        source_type: Optional[str] = None,
        utm_source: Optional[str] = None,
        utm_campaign: Optional[str] = None,
    ):
        """Render sources page."""
        if not _check_auth(auth):
            return RedirectResponse(url="./login", status_code=303)

        start, end, compare_start, compare_end = _parse_date_range(period)
        filters = _get_filters(
            source=source, source_type=source_type,
            utm_source=utm_source, utm_campaign=utm_campaign
        )

        metrics = await client.get_core_metrics(start, end, compare_start, compare_end, filters)
        sources = await client.get_sources(start, end, 50, filters)
        source_types = await client.get_source_types(start, end, filters)
        utm_sources = await client.get_utm_sources(start, end, 20, filters)
        utm_campaigns = await client.get_utm_campaigns(start, end, 20, filters)

        context = _get_common_context(request, "sources", period)
        context.update({
            "metrics": metrics,
            "sources": sources,
            "source_types": source_types,
            "utm_sources": utm_sources,
            "utm_campaigns": utm_campaigns,
            "filters": filters,
        })

        return templates.TemplateResponse("pages/sources.html", context)

    @router.get("/partials/sources", response_class=HTMLResponse)
    async def sources_partial(
        request: Request,
        auth: Optional[str] = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        source: Optional[str] = None,
        source_type: Optional[str] = None,
        utm_source: Optional[str] = None,
        utm_campaign: Optional[str] = None,
    ):
        """HTMX partial for sources tab."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        start, end, compare_start, compare_end = _parse_date_range(period)
        filters = _get_filters(
            source=source, source_type=source_type,
            utm_source=utm_source, utm_campaign=utm_campaign
        )

        metrics = await client.get_core_metrics(start, end, compare_start, compare_end, filters)
        sources = await client.get_sources(start, end, 50, filters)
        source_types = await client.get_source_types(start, end, filters)
        utm_sources = await client.get_utm_sources(start, end, 20, filters)
        utm_campaigns = await client.get_utm_campaigns(start, end, 20, filters)

        context = _get_common_context(request, "sources", period)
        context.update({
            "metrics": metrics,
            "sources": sources,
            "source_types": source_types,
            "utm_sources": utm_sources,
            "utm_campaigns": utm_campaigns,
            "filters": filters,
        })

        return templates.TemplateResponse("partials/sources_content.html", context)

    @router.get("/geography", response_class=HTMLResponse)
    async def geography_page(
        request: Request,
        auth: Optional[str] = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        country: Optional[str] = None,
        region: Optional[str] = None,
    ):
        """Render geography page with lazy-loaded globe."""
        if not _check_auth(auth):
            return RedirectResponse(url="./login", status_code=303)

        start, end, compare_start, compare_end = _parse_date_range(period)
        filters = _get_filters(country=country, region=region)

        metrics = await client.get_core_metrics(start, end, compare_start, compare_end, filters)
        countries = await client.get_countries(start, end, 50, filters)
        globe_data = await client.get_globe_data(start, end, filters)

        # Get regions if country is selected
        regions = []
        cities = []
        if country:
            regions = await client.get_regions(start, end, country, 30)
            if region:
                cities = await client.get_cities(start, end, country, region, 30)

        context = _get_common_context(request, "geography", period)
        context.update({
            "metrics": metrics,
            "countries": countries,
            "regions": regions,
            "cities": cities,
            "globe_data": globe_data.model_dump(),
            "filters": filters,
        })

        return templates.TemplateResponse("pages/geography.html", context)

    @router.get("/partials/geography", response_class=HTMLResponse)
    async def geography_partial(
        request: Request,
        auth: Optional[str] = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        country: Optional[str] = None,
        region: Optional[str] = None,
    ):
        """HTMX partial for geography tab."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        start, end, compare_start, compare_end = _parse_date_range(period)
        filters = _get_filters(country=country, region=region)

        metrics = await client.get_core_metrics(start, end, compare_start, compare_end, filters)
        countries = await client.get_countries(start, end, 50, filters)
        globe_data = await client.get_globe_data(start, end, filters)

        regions = []
        cities = []
        if country:
            regions = await client.get_regions(start, end, country, 30)
            if region:
                cities = await client.get_cities(start, end, country, region, 30)

        context = _get_common_context(request, "geography", period)
        context.update({
            "metrics": metrics,
            "countries": countries,
            "regions": regions,
            "cities": cities,
            "globe_data": globe_data.model_dump(),
            "filters": filters,
        })

        return templates.TemplateResponse("partials/geography_content.html", context)

    @router.get("/technology", response_class=HTMLResponse)
    async def technology_page(
        request: Request,
        auth: Optional[str] = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        device: Optional[str] = None,
        browser: Optional[str] = None,
        os: Optional[str] = None,
    ):
        """Render technology page."""
        if not _check_auth(auth):
            return RedirectResponse(url="./login", status_code=303)

        start, end, compare_start, compare_end = _parse_date_range(period)
        filters = _get_filters(device=device, browser=browser)

        metrics = await client.get_core_metrics(start, end, compare_start, compare_end, filters)
        devices = await client.get_devices(start, end, filters)
        browsers = await client.get_browsers(start, end, 20, filters)
        operating_systems = await client.get_operating_systems(start, end, 20, filters)
        screen_sizes = await client.get_screen_sizes(start, end, 20, filters)
        languages = await client.get_languages(start, end, 20, filters)

        context = _get_common_context(request, "technology", period)
        context.update({
            "metrics": metrics,
            "devices": devices,
            "browsers": browsers,
            "operating_systems": operating_systems,
            "screen_sizes": screen_sizes,
            "languages": languages,
            "filters": filters,
        })

        return templates.TemplateResponse("pages/technology.html", context)

    @router.get("/partials/technology", response_class=HTMLResponse)
    async def technology_partial(
        request: Request,
        auth: Optional[str] = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        device: Optional[str] = None,
        browser: Optional[str] = None,
        os: Optional[str] = None,
    ):
        """HTMX partial for technology tab."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        start, end, compare_start, compare_end = _parse_date_range(period)
        filters = _get_filters(device=device, browser=browser)

        metrics = await client.get_core_metrics(start, end, compare_start, compare_end, filters)
        devices = await client.get_devices(start, end, filters)
        browsers = await client.get_browsers(start, end, 20, filters)
        operating_systems = await client.get_operating_systems(start, end, 20, filters)
        screen_sizes = await client.get_screen_sizes(start, end, 20, filters)
        languages = await client.get_languages(start, end, 20, filters)

        context = _get_common_context(request, "technology", period)
        context.update({
            "metrics": metrics,
            "devices": devices,
            "browsers": browsers,
            "operating_systems": operating_systems,
            "screen_sizes": screen_sizes,
            "languages": languages,
            "filters": filters,
        })

        return templates.TemplateResponse("partials/technology_content.html", context)

    @router.get("/events", response_class=HTMLResponse)
    async def events_page(
        request: Request,
        auth: Optional[str] = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        event: Optional[str] = None,
        event_type: Optional[str] = None,
    ):
        """Render events page."""
        if not _check_auth(auth):
            return RedirectResponse(url="./login", status_code=303)

        start, end, compare_start, compare_end = _parse_date_range(period)
        filters = _get_filters()

        metrics = await client.get_core_metrics(start, end, compare_start, compare_end, filters)
        events = await client.get_events(start, end, 50, event_type)
        scroll_depth = await client.get_scroll_depth(start, end)
        event_types = await client.get_event_types(start, end)

        context = _get_common_context(request, "events", period)
        context.update({
            "metrics": metrics,
            "events": events,
            "scroll_depth": scroll_depth,
            "event_types": event_types,
            "filters": filters,
        })

        return templates.TemplateResponse("pages/events.html", context)

    @router.get("/partials/events", response_class=HTMLResponse)
    async def events_partial(
        request: Request,
        auth: Optional[str] = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
        event: Optional[str] = None,
        event_type: Optional[str] = None,
    ):
        """HTMX partial for events tab."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        start, end, compare_start, compare_end = _parse_date_range(period)
        filters = _get_filters()

        metrics = await client.get_core_metrics(start, end, compare_start, compare_end, filters)
        events = await client.get_events(start, end, 50, event_type)
        scroll_depth = await client.get_scroll_depth(start, end)
        event_types = await client.get_event_types(start, end)

        context = _get_common_context(request, "events", period)
        context.update({
            "metrics": metrics,
            "events": events,
            "scroll_depth": scroll_depth,
            "event_types": event_types,
            "filters": filters,
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
    ):
        """Export pageviews as CSV."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        start, end, _, _ = _parse_date_range(period)
        csv_data = await client.export_pageviews(start, end)

        return StreamingResponse(
            iter([csv_data]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=pageviews_{start}_{end}.csv"
            },
        )

    @router.get("/export/events.csv")
    async def export_events(
        request: Request,
        auth: Optional[str] = Cookie(None, alias=AUTH_COOKIE_NAME),
        period: str = "30d",
    ):
        """Export events as CSV."""
        if not _check_auth(auth):
            raise HTTPException(status_code=401, detail="Unauthorized")

        start, end, _, _ = _parse_date_range(period)
        csv_data = await client.export_events(start, end)

        return StreamingResponse(
            iter([csv_data]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=events_{start}_{end}.csv"
            },
        )

    return router
