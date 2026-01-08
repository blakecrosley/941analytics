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

__version__ = "0.2.0"
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
        """Generate the tracking script HTML for templates.

        Features:
        - Initial pageload tracking
        - SPA navigation support (pushState, replaceState, popstate)
        - UTM parameter extraction for attribution
        - Debounced to prevent duplicate tracks
        - ~700 bytes minified
        """
        return f'''<script>
(function(){{
  var d=document,w=window,h=history,l=location,e=encodeURIComponent;
  var url="{self.worker_url}/collect";
  var site="{self.site_name}";
  var lastPath="",timer;

  function getUtm(){{
    var p=new URLSearchParams(l.search);
    var u={{}};
    ["source","medium","campaign","term","content"].forEach(function(k){{
      var v=p.get("utm_"+k);if(v)u[k]=v;
    }});
    return u;
  }}

  function track(){{
    clearTimeout(timer);
    timer=setTimeout(function(){{
      var path=l.pathname;
      if(path===lastPath)return;
      lastPath=path;
      var data={{
        site:site,
        url:path,
        title:d.title,
        ref:d.referrer?new URL(d.referrer).hostname:"",
        w:w.innerWidth
      }};
      var utm=getUtm();
      Object.keys(utm).forEach(function(k){{data["utm_"+k]=utm[k]}});
      var params=Object.keys(data).map(function(k){{return k+"="+e(data[k])}}).join("&");
      new Image().src=url+"?"+params;
    }},50);
  }}

  track();

  var push=h.pushState;
  h.pushState=function(){{push.apply(h,arguments);track()}};
  var replace=h.replaceState;
  h.replaceState=function(){{replace.apply(h,arguments);track()}};
  w.addEventListener("popstate",track);
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
