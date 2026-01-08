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

__version__ = "0.1.0"
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
    ):
        self.site_name = site_name
        self.worker_url = worker_url
        self.passkey = passkey
        self.client = AnalyticsClient(
            d1_database_id=d1_database_id,
            cf_account_id=cf_account_id,
            cf_api_token=cf_api_token,
            site_name=site_name,
        )
        self.dashboard_router = create_dashboard_router(
            self.client, site_name, passkey=passkey
        )

    def tracking_script(self) -> str:
        """Generate the tracking script HTML for templates."""
        return f'''<script>
(function(){{
  var d=document,w=window,e=encodeURIComponent;
  var url="{self.worker_url}/collect";
  var data={{
    site:"{self.site_name}",
    url:d.location.pathname,
    title:d.title,
    ref:d.referrer?new URL(d.referrer).hostname:"",
    w:w.innerWidth
  }};
  var params=Object.keys(data).map(function(k){{return k+"="+e(data[k])}}).join("&");
  var img=new Image();img.src=url+"?"+params;
}})();
</script>'''


def setup_analytics(
    site_name: str,
    worker_url: str,
    d1_database_id: str,
    cf_account_id: str,
    cf_api_token: str,
    passkey: str = None,
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
    )
