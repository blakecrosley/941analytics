"""
Privacy-first analytics for 941 Apps projects.

Usage:
    from analytics_941 import setup_analytics

    analytics = setup_analytics(
        site_name="941return.com",
        worker_url="https://analytics.941apps.workers.dev",
        d1_database_id="your-d1-id",
        cf_account_id="your-account-id",
        cf_api_token="your-api-token",
    )

    # Include dashboard routes
    app.include_router(analytics.dashboard_router, prefix="/admin/analytics")

    # In templates: {{ analytics.tracking_script() }}
"""

from .client import AnalyticsClient
from .routes import create_dashboard_router
from .models import PageView, DailyStats

__version__ = "0.3.0"
__all__ = ["setup_analytics", "AnalyticsClient", "PageView", "DailyStats"]


class Analytics:
    """Main analytics interface for a site."""

    def __init__(
        self,
        site_name: str,
        worker_url: str,
        d1_database_id: str,
        cf_account_id: str,
        cf_api_token: str,
        passkey: str = None,
        rp_id: str = None,
        rp_origin: str = None,
    ):
        self.site_name = site_name
        self.worker_url = worker_url
        self.passkey = passkey
        self.rp_id = rp_id
        self.rp_origin = rp_origin
        self.client = AnalyticsClient(
            d1_database_id=d1_database_id,
            cf_account_id=cf_account_id,
            cf_api_token=cf_api_token,
            site_name=site_name,
        )
        self.dashboard_router = create_dashboard_router(
            self.client,
            site_name,
            passkey=passkey,
            rp_id=rp_id,
            rp_origin=rp_origin,
        )

    def tracking_script(self) -> str:
        """Generate the tracking script HTML for templates.

        Features (v2.0):
        - Session tracking with 30-minute timeout
        - Auto-events: scroll depth, outbound clicks, downloads, forms
        - JS error tracking and 404 detection
        - Heartbeat for accurate time-on-site
        - SPA navigation support
        - Privacy-first: no cookies, hashed visitor IDs
        - ~1.5KB minified
        """
        return f'<script defer src="{self.worker_url}/track.js" data-endpoint="{self.worker_url}/collect" data-site="{self.site_name}"></script>'


def setup_analytics(
    site_name: str,
    worker_url: str,
    d1_database_id: str,
    cf_account_id: str,
    cf_api_token: str,
    passkey: str = None,
    rp_id: str = None,
    rp_origin: str = None,
) -> Analytics:
    """
    Set up analytics for a site.

    Args:
        site_name: Identifier for this site (e.g., "941return.com")
        worker_url: URL of the Cloudflare Worker (e.g., "https://analytics.941apps.workers.dev")
        d1_database_id: Cloudflare D1 database ID
        cf_account_id: Cloudflare account ID
        cf_api_token: Cloudflare API token with D1 read access
        passkey: Optional passkey to protect the dashboard. If set, users must
                 enter this passkey to access analytics.
        rp_id: WebAuthn Relying Party ID (domain name, e.g., "example.com").
               Required for passkey/biometric authentication.
        rp_origin: WebAuthn Relying Party origin (full URL, e.g., "https://example.com").
                   Required for passkey/biometric authentication.

    Returns:
        Analytics instance with dashboard_router and tracking_script()
    """
    return Analytics(
        site_name=site_name,
        worker_url=worker_url,
        d1_database_id=d1_database_id,
        cf_account_id=cf_account_id,
        cf_api_token=cf_api_token,
        passkey=passkey,
        rp_id=rp_id,
        rp_origin=rp_origin,
    )
