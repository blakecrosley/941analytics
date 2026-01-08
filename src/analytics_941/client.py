"""HTTP client for querying Cloudflare D1 analytics database."""

from datetime import date, datetime, timedelta
from typing import Optional
import httpx

from .models import DashboardData


class AnalyticsClient:
    """Client for querying analytics data from Cloudflare D1."""

    def __init__(
        self,
        d1_database_id: str,
        cf_account_id: str,
        cf_api_token: str,
        site_name: str,
    ):
        self.database_id = d1_database_id
        self.account_id = cf_account_id
        self.api_token = cf_api_token
        self.site_name = site_name
        self.base_url = f"https://api.cloudflare.com/client/v4/accounts/{cf_account_id}/d1/database/{d1_database_id}"

    async def _query(self, sql: str, params: Optional[list] = None) -> list[dict]:
        """Execute a SQL query against D1."""
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/query",
                headers={
                    "Authorization": f"Bearer {self.api_token}",
                    "Content-Type": "application/json",
                },
                json={"sql": sql, "params": params or []},
            )
            response.raise_for_status()
            data = response.json()

            if not data.get("success"):
                raise Exception(f"D1 query failed: {data.get('errors')}")

            # D1 returns results in a specific format
            results = data.get("result", [])
            if results and len(results) > 0:
                return results[0].get("results", [])
            return []

    async def get_dashboard_data(
        self, period: str = "7d", include_bots: bool = False
    ) -> DashboardData:
        """
        Get dashboard data for the specified period.

        Args:
            period: Time period - 'today', '7d', or '30d'
            include_bots: If False (default), human traffic only. If True, all traffic.
        """
        # Calculate date range
        today = date.today()
        if period == "today":
            start_date = today
        elif period == "7d":
            start_date = today - timedelta(days=7)
        elif period == "30d":
            start_date = today - timedelta(days=30)
        else:
            start_date = today - timedelta(days=7)

        start_str = start_date.isoformat()
        bot_filter = "" if include_bots else "AND is_bot = 0"

        # Total views and unique visitors (humans only by default)
        totals = await self._query(
            f"""
            SELECT
                COUNT(*) as total_views,
                COUNT(DISTINCT visitor_hash) as unique_visitors
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? {bot_filter}
            """,
            [self.site_name, start_str],
        )

        total_views = totals[0]["total_views"] if totals else 0
        unique_visitors = totals[0]["unique_visitors"] if totals else 0

        # Bot traffic count (always separate)
        bot_totals = await self._query(
            """
            SELECT COUNT(*) as bot_views
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? AND is_bot = 1
            """,
            [self.site_name, start_str],
        )
        bot_views = bot_totals[0]["bot_views"] if bot_totals else 0

        # Views by day
        views_by_day = await self._query(
            f"""
            SELECT
                date(timestamp) as day,
                COUNT(*) as views,
                COUNT(DISTINCT visitor_hash) as visitors
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? {bot_filter}
            GROUP BY date(timestamp)
            ORDER BY day ASC
            """,
            [self.site_name, start_str],
        )

        # Top pages
        top_pages = await self._query(
            f"""
            SELECT
                url,
                COUNT(*) as views
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? {bot_filter}
            GROUP BY url
            ORDER BY views DESC
            LIMIT 10
            """,
            [self.site_name, start_str],
        )

        # Top referrers (by domain)
        top_referrers = await self._query(
            f"""
            SELECT
                referrer_domain as domain,
                referrer_type as type,
                COUNT(*) as views
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? AND referrer_domain != '' {bot_filter}
            GROUP BY referrer_domain
            ORDER BY views DESC
            LIMIT 10
            """,
            [self.site_name, start_str],
        )

        # Referrer types breakdown
        referrer_types_raw = await self._query(
            f"""
            SELECT
                referrer_type,
                COUNT(*) as views
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? {bot_filter}
            GROUP BY referrer_type
            """,
            [self.site_name, start_str],
        )
        referrer_types = {row["referrer_type"] or "direct": row["views"] for row in referrer_types_raw}

        # UTM sources
        utm_sources = await self._query(
            f"""
            SELECT
                utm_source as source,
                utm_medium as medium,
                COUNT(*) as views
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? AND utm_source != '' {bot_filter}
            GROUP BY utm_source, utm_medium
            ORDER BY views DESC
            LIMIT 10
            """,
            [self.site_name, start_str],
        )

        # UTM campaigns
        utm_campaigns = await self._query(
            f"""
            SELECT
                utm_campaign as campaign,
                utm_source as source,
                COUNT(*) as views
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? AND utm_campaign != '' {bot_filter}
            GROUP BY utm_campaign, utm_source
            ORDER BY views DESC
            LIMIT 10
            """,
            [self.site_name, start_str],
        )

        # Countries
        countries = await self._query(
            f"""
            SELECT
                country,
                COUNT(*) as views
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? AND country != '' {bot_filter}
            GROUP BY country
            ORDER BY views DESC
            LIMIT 10
            """,
            [self.site_name, start_str],
        )

        # Regions (states)
        regions = await self._query(
            f"""
            SELECT
                country,
                region,
                COUNT(*) as views
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? AND region != '' AND region IS NOT NULL {bot_filter}
            GROUP BY country, region
            ORDER BY views DESC
            LIMIT 20
            """,
            [self.site_name, start_str],
        )

        # Cities
        cities = await self._query(
            f"""
            SELECT
                country,
                region,
                city,
                COUNT(*) as views
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? AND city != '' AND city IS NOT NULL {bot_filter}
            GROUP BY country, region, city
            ORDER BY views DESC
            LIMIT 30
            """,
            [self.site_name, start_str],
        )

        # Devices
        devices_raw = await self._query(
            f"""
            SELECT
                device_type,
                COUNT(*) as views
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? {bot_filter}
            GROUP BY device_type
            """,
            [self.site_name, start_str],
        )
        devices = {row["device_type"] or "unknown": row["views"] for row in devices_raw}

        # Browsers
        browsers_raw = await self._query(
            f"""
            SELECT
                browser,
                COUNT(*) as views
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? AND browser != '' {bot_filter}
            GROUP BY browser
            ORDER BY views DESC
            LIMIT 10
            """,
            [self.site_name, start_str],
        )
        browsers = {row["browser"]: row["views"] for row in browsers_raw}

        # Operating systems
        os_raw = await self._query(
            f"""
            SELECT
                os,
                COUNT(*) as views
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? AND os != '' {bot_filter}
            GROUP BY os
            ORDER BY views DESC
            LIMIT 10
            """,
            [self.site_name, start_str],
        )
        operating_systems = {row["os"]: row["views"] for row in os_raw}

        # Bot breakdown (by category)
        bot_breakdown_raw = await self._query(
            """
            SELECT
                bot_category,
                COUNT(*) as views
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? AND is_bot = 1
            GROUP BY bot_category
            ORDER BY views DESC
            """,
            [self.site_name, start_str],
        )
        bot_breakdown = {row["bot_category"] or "unknown": row["views"] for row in bot_breakdown_raw}

        return DashboardData(
            site=self.site_name,
            period=period,
            total_views=total_views,
            unique_visitors=unique_visitors,
            bot_views=bot_views,
            views_by_day=views_by_day,
            top_pages=top_pages,
            top_referrers=top_referrers,
            referrer_types=referrer_types,
            utm_sources=utm_sources,
            utm_campaigns=utm_campaigns,
            countries=countries,
            regions=regions,
            cities=cities,
            devices=devices,
            browsers=browsers,
            operating_systems=operating_systems,
            bot_breakdown=bot_breakdown,
        )

    async def get_realtime_count(self) -> int:
        """Get count of human visitors in the last 5 minutes (excludes bots)."""
        five_min_ago = (datetime.utcnow() - timedelta(minutes=5)).isoformat()
        result = await self._query(
            """
            SELECT COUNT(DISTINCT visitor_hash) as count
            FROM page_views
            WHERE site = ? AND timestamp >= ? AND is_bot = 0
            """,
            [self.site_name, five_min_ago],
        )
        return result[0]["count"] if result else 0

    async def get_campaign_performance(
        self, campaign: str, start_date: Optional[date] = None, end_date: Optional[date] = None
    ) -> dict:
        """
        Get performance metrics for a specific UTM campaign.

        Returns views, unique visitors, and source breakdown for the campaign.
        """
        if start_date is None:
            start_date = date.today() - timedelta(days=30)
        if end_date is None:
            end_date = date.today()

        # Campaign totals
        totals = await self._query(
            """
            SELECT
                COUNT(*) as views,
                COUNT(DISTINCT visitor_hash) as visitors
            FROM page_views
            WHERE site = ? AND utm_campaign = ?
              AND date(timestamp) >= ? AND date(timestamp) <= ?
              AND is_bot = 0
            """,
            [self.site_name, campaign, start_date.isoformat(), end_date.isoformat()],
        )

        # Performance by day
        by_day = await self._query(
            """
            SELECT
                date(timestamp) as day,
                COUNT(*) as views,
                COUNT(DISTINCT visitor_hash) as visitors
            FROM page_views
            WHERE site = ? AND utm_campaign = ?
              AND date(timestamp) >= ? AND date(timestamp) <= ?
              AND is_bot = 0
            GROUP BY date(timestamp)
            ORDER BY day ASC
            """,
            [self.site_name, campaign, start_date.isoformat(), end_date.isoformat()],
        )

        # Top landing pages
        landing_pages = await self._query(
            """
            SELECT url, COUNT(*) as views
            FROM page_views
            WHERE site = ? AND utm_campaign = ?
              AND date(timestamp) >= ? AND date(timestamp) <= ?
              AND is_bot = 0
            GROUP BY url
            ORDER BY views DESC
            LIMIT 10
            """,
            [self.site_name, campaign, start_date.isoformat(), end_date.isoformat()],
        )

        return {
            "campaign": campaign,
            "views": totals[0]["views"] if totals else 0,
            "visitors": totals[0]["visitors"] if totals else 0,
            "by_day": by_day,
            "landing_pages": landing_pages,
        }
