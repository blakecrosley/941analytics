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
        self, period: str = "7d"
    ) -> DashboardData:
        """Get dashboard data for the specified period."""
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

        # Total views and unique visitors
        totals = await self._query(
            """
            SELECT
                COUNT(*) as total_views,
                COUNT(DISTINCT visitor_hash) as unique_visitors
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ?
            """,
            [self.site_name, start_str],
        )

        total_views = totals[0]["total_views"] if totals else 0
        unique_visitors = totals[0]["unique_visitors"] if totals else 0

        # Views by day
        views_by_day = await self._query(
            """
            SELECT
                date(timestamp) as day,
                COUNT(*) as views,
                COUNT(DISTINCT visitor_hash) as visitors
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ?
            GROUP BY date(timestamp)
            ORDER BY day ASC
            """,
            [self.site_name, start_str],
        )

        # Top pages
        top_pages = await self._query(
            """
            SELECT
                url,
                COUNT(*) as views
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ?
            GROUP BY url
            ORDER BY views DESC
            LIMIT 10
            """,
            [self.site_name, start_str],
        )

        # Top referrers
        top_referrers = await self._query(
            """
            SELECT
                referrer,
                COUNT(*) as views
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? AND referrer != ''
            GROUP BY referrer
            ORDER BY views DESC
            LIMIT 10
            """,
            [self.site_name, start_str],
        )

        # Countries
        countries = await self._query(
            """
            SELECT
                country,
                COUNT(*) as views
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? AND country != ''
            GROUP BY country
            ORDER BY views DESC
            LIMIT 10
            """,
            [self.site_name, start_str],
        )

        # Devices
        devices_raw = await self._query(
            """
            SELECT
                device_type,
                COUNT(*) as views
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ?
            GROUP BY device_type
            """,
            [self.site_name, start_str],
        )
        devices = {row["device_type"]: row["views"] for row in devices_raw}

        return DashboardData(
            site=self.site_name,
            period=period,
            total_views=total_views,
            unique_visitors=unique_visitors,
            views_by_day=views_by_day,
            top_pages=top_pages,
            top_referrers=top_referrers,
            countries=countries,
            devices=devices,
        )

    async def get_realtime_count(self) -> int:
        """Get count of visitors in the last 5 minutes."""
        five_min_ago = (datetime.utcnow() - timedelta(minutes=5)).isoformat()
        result = await self._query(
            """
            SELECT COUNT(DISTINCT visitor_hash) as count
            FROM page_views
            WHERE site = ? AND timestamp >= ?
            """,
            [self.site_name, five_min_ago],
        )
        return result[0]["count"] if result else 0
