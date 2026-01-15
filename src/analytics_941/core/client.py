"""
HTTP client for querying Cloudflare D1 analytics database.

Enhanced version with session tracking, events, and filtering support.
"""
import json
from datetime import date, datetime, timedelta
from typing import Optional, Dict, List, Any
import httpx

from .models import (
    DashboardData, DashboardFilters, DateRange,
    CoreMetrics, MetricChange, TimeSeriesPoint,
    PageStats, SourceStats, CountryStats, DeviceStats, BrowserStats, EventStats,
    RealtimeData, GlobeData, Session, Event
)


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
        async with httpx.AsyncClient(timeout=30.0) as client:
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

            results = data.get("result", [])
            if results and len(results) > 0:
                return results[0].get("results", [])
            return []

    async def _execute(self, sql: str, params: Optional[list] = None) -> None:
        """Execute a SQL statement without returning results."""
        await self._query(sql, params)

    # =========================================================================
    # CORE METRICS
    # =========================================================================

    async def get_core_metrics(
        self,
        start_date: date,
        end_date: date,
        compare_start: Optional[date] = None,
        compare_end: Optional[date] = None,
        filters: Optional[DashboardFilters] = None,
    ) -> CoreMetrics:
        """Get core dashboard metrics with optional comparison period."""

        filter_sql, filter_params = self._build_filter_sql(filters)

        # Current period
        current = await self._query(
            f"""
            SELECT
                COUNT(*) as views,
                COUNT(DISTINCT visitor_hash) as visitors,
                COUNT(DISTINCT session_id) as sessions,
                SUM(CASE WHEN is_bot = 1 THEN 1 ELSE 0 END) as bot_views
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
                AND is_bot = 0 {filter_sql}
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params,
        )

        # Session metrics (bounce rate, avg duration)
        session_filter_sql, session_filter_params = self._build_session_filter_sql(filters)
        session_stats = await self._query(
            f"""
            SELECT
                AVG(CASE WHEN is_bounce = 1 THEN 1 ELSE 0 END) * 100 as bounce_rate,
                AVG(duration_seconds) as avg_duration
            FROM sessions
            WHERE site = ? AND date(started_at) >= ? AND date(started_at) <= ?
                {session_filter_sql}
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + session_filter_params,
        )

        current_data = current[0] if current else {}
        session_data = session_stats[0] if session_stats else {}

        views = current_data.get("views") or 0
        visitors = current_data.get("visitors") or 0
        sessions = current_data.get("sessions") or 0
        bounce_rate = round(session_data.get("bounce_rate", 0) or 0, 1)
        avg_duration = round(session_data.get("avg_duration", 0) or 0)
        bot_views = current_data.get("bot_views") or 0

        # Comparison period
        prev_views = prev_visitors = prev_sessions = prev_bounce = prev_duration = None
        if compare_start and compare_end:
            prev = await self._query(
                f"""
                SELECT
                    COUNT(*) as views,
                    COUNT(DISTINCT visitor_hash) as visitors,
                    COUNT(DISTINCT session_id) as sessions
                FROM page_views
                WHERE site = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
                    AND is_bot = 0 {filter_sql}
                """,
                [self.site_name, compare_start.isoformat(), compare_end.isoformat()] + filter_params,
            )
            prev_sess = await self._query(
                f"""
                SELECT
                    AVG(CASE WHEN is_bounce = 1 THEN 1 ELSE 0 END) * 100 as bounce_rate,
                    AVG(duration_seconds) as avg_duration
                FROM sessions
                WHERE site = ? AND date(started_at) >= ? AND date(started_at) <= ?
                    {session_filter_sql}
                """,
                [self.site_name, compare_start.isoformat(), compare_end.isoformat()] + session_filter_params,
            )
            if prev:
                prev_views = prev[0].get("views") or 0
                prev_visitors = prev[0].get("visitors") or 0
                prev_sessions = prev[0].get("sessions") or 0
            if prev_sess:
                prev_bounce = round(prev_sess[0].get("bounce_rate", 0) or 0, 1)
                prev_duration = round(prev_sess[0].get("avg_duration", 0) or 0)

        return CoreMetrics(
            views=self._metric_with_change(views, prev_views),
            visitors=self._metric_with_change(visitors, prev_visitors),
            sessions=self._metric_with_change(sessions, prev_sessions),
            bounce_rate=self._metric_with_change(bounce_rate, prev_bounce),
            avg_duration=self._metric_with_change(avg_duration, prev_duration),
            bot_views=bot_views,
        )

    def _metric_with_change(self, current: int, previous: Optional[int]) -> MetricChange:
        """Create a MetricChange object with percentage change."""
        if previous is None:
            return MetricChange(value=current)

        if previous == 0:
            change = 100.0 if current > 0 else 0.0
        else:
            change = round(((current - previous) / previous) * 100, 1)

        direction = "up" if change > 0 else "down" if change < 0 else "same"
        return MetricChange(
            value=current,
            previous=previous,
            change_percent=abs(change),
            change_direction=direction,
        )

    def _build_filter_sql(self, filters: Optional[DashboardFilters]) -> tuple[str, list]:
        """Build SQL WHERE clauses from filters.

        Uses parameterized queries to prevent SQL injection.
        Returns (sql_string, params_list) tuple.
        """
        if not filters:
            return "", []

        clauses = []
        params = []

        # Geographic filters
        if filters.country:
            clauses.append("AND country = ?")
            params.append(filters.country)
        if filters.region:
            clauses.append("AND region = ?")
            params.append(filters.region)
        if filters.city:
            clauses.append("AND city = ?")
            params.append(filters.city)

        # Technology filters
        if filters.device:
            clauses.append("AND device_type = ?")
            params.append(filters.device)
        if filters.browser:
            clauses.append("AND browser = ?")
            params.append(filters.browser)
        if filters.os:
            clauses.append("AND os = ?")
            params.append(filters.os)

        # Source filters
        if filters.source_type:
            clauses.append("AND referrer_type = ?")
            params.append(filters.source_type)
        if filters.source:
            clauses.append("AND referrer_domain = ?")
            params.append(filters.source)

        # Page filter
        if filters.page:
            clauses.append("AND url = ?")
            params.append(filters.page)

        # UTM filters
        if filters.utm_source:
            clauses.append("AND utm_source = ?")
            params.append(filters.utm_source)
        if filters.utm_medium:
            clauses.append("AND utm_medium = ?")
            params.append(filters.utm_medium)
        if filters.utm_campaign:
            clauses.append("AND utm_campaign = ?")
            params.append(filters.utm_campaign)

        return " ".join(clauses), params

    def _build_session_filter_sql(self, filters: Optional[DashboardFilters]) -> tuple[str, list]:
        """Build session table filter SQL with parameterized queries.

        Sessions table has fewer columns than page_views, so only
        certain filters apply.
        """
        if not filters:
            return "", []

        clauses = []
        params = []

        # Geographic (sessions table has country, region)
        if filters.country:
            clauses.append("AND country = ?")
            params.append(filters.country)
        if filters.region:
            clauses.append("AND region = ?")
            params.append(filters.region)

        # Technology (sessions table has device_type, browser, os)
        if filters.device:
            clauses.append("AND device_type = ?")
            params.append(filters.device)
        if filters.browser:
            clauses.append("AND browser = ?")
            params.append(filters.browser)
        if filters.os:
            clauses.append("AND os = ?")
            params.append(filters.os)

        # Source (sessions table has referrer_type, referrer_domain)
        if filters.source_type:
            clauses.append("AND referrer_type = ?")
            params.append(filters.source_type)
        if filters.source:
            clauses.append("AND referrer_domain = ?")
            params.append(filters.source)

        # UTM (sessions table has utm_source, utm_campaign)
        if filters.utm_source:
            clauses.append("AND utm_source = ?")
            params.append(filters.utm_source)
        if filters.utm_campaign:
            clauses.append("AND utm_campaign = ?")
            params.append(filters.utm_campaign)

        return " ".join(clauses), params

    def _build_event_filter_sql(self, filters: Optional[DashboardFilters]) -> tuple[str, list]:
        """Build event table filter SQL with parameterized queries.

        Events table has limited columns: country, device_type, page_url.
        """
        if not filters:
            return "", []

        clauses = []
        params = []

        if filters.country:
            clauses.append("AND country = ?")
            params.append(filters.country)
        if filters.device:
            clauses.append("AND device_type = ?")
            params.append(filters.device)
        if filters.page:
            clauses.append("AND page_url = ?")
            params.append(filters.page)

        return " ".join(clauses), params

    # =========================================================================
    # TIME SERIES
    # =========================================================================

    async def get_time_series(
        self,
        start_date: date,
        end_date: date,
        granularity: str = "day",
        filters: Optional[DashboardFilters] = None,
    ) -> List[TimeSeriesPoint]:
        """Get views/visitors/sessions over time."""

        filter_sql, filter_params = self._build_filter_sql(filters)

        if granularity == "hour":
            group_by = "strftime('%Y-%m-%d %H:00', timestamp)"
        else:
            group_by = "date(timestamp)"

        results = await self._query(
            f"""
            SELECT
                {group_by} as ts,
                COUNT(*) as views,
                COUNT(DISTINCT visitor_hash) as visitors,
                COUNT(DISTINCT session_id) as sessions
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
                AND is_bot = 0 {filter_sql}
            GROUP BY {group_by}
            ORDER BY ts ASC
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params,
        )

        return [
            TimeSeriesPoint(
                timestamp=datetime.fromisoformat(r["ts"]) if " " in r["ts"] else datetime.strptime(r["ts"], "%Y-%m-%d"),
                views=r["views"],
                visitors=r["visitors"],
                sessions=r["sessions"],
            )
            for r in results
        ]

    # =========================================================================
    # PAGES
    # =========================================================================

    async def get_top_pages(
        self,
        start_date: date,
        end_date: date,
        limit: int = 10,
        filters: Optional[DashboardFilters] = None,
    ) -> List[PageStats]:
        """Get top pages by views."""

        filter_sql, filter_params = self._build_filter_sql(filters)

        results = await self._query(
            f"""
            SELECT
                url,
                COUNT(*) as views,
                COUNT(DISTINCT visitor_hash) as visitors
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
                AND is_bot = 0 {filter_sql}
            GROUP BY url
            ORDER BY views DESC
            LIMIT ?
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params + [limit],
        )

        return [
            PageStats(url=r["url"], views=r["views"], visitors=r["visitors"])
            for r in results
        ]

    async def get_entry_pages(
        self,
        start_date: date,
        end_date: date,
        limit: int = 10,
        filters: Optional[DashboardFilters] = None,
    ) -> List[PageStats]:
        """Get top entry pages (first page of sessions)."""

        filter_sql, filter_params = self._build_session_filter_sql(filters)

        results = await self._query(
            f"""
            SELECT
                entry_page as url,
                COUNT(*) as entries,
                COUNT(DISTINCT visitor_hash) as visitors
            FROM sessions
            WHERE site = ? AND date(started_at) >= ? AND date(started_at) <= ?
                {filter_sql}
            GROUP BY entry_page
            ORDER BY entries DESC
            LIMIT ?
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params + [limit],
        )

        return [
            PageStats(url=r["url"], views=r["entries"], visitors=r["visitors"], entries=r["entries"])
            for r in results
        ]

    async def get_exit_pages(
        self,
        start_date: date,
        end_date: date,
        limit: int = 10,
        filters: Optional[DashboardFilters] = None,
    ) -> List[PageStats]:
        """Get top exit pages (last page of sessions)."""

        filter_sql, filter_params = self._build_session_filter_sql(filters)

        results = await self._query(
            f"""
            SELECT
                exit_page as url,
                COUNT(*) as exits,
                COUNT(DISTINCT visitor_hash) as visitors
            FROM sessions
            WHERE site = ? AND date(started_at) >= ? AND date(started_at) <= ?
                AND exit_page IS NOT NULL {filter_sql}
            GROUP BY exit_page
            ORDER BY exits DESC
            LIMIT ?
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params + [limit],
        )

        return [
            PageStats(url=r["url"], views=r["exits"], visitors=r["visitors"], exits=r["exits"])
            for r in results
        ]

    # =========================================================================
    # SOURCES
    # =========================================================================

    async def get_sources(
        self,
        start_date: date,
        end_date: date,
        limit: int = 10,
        filters: Optional[DashboardFilters] = None,
    ) -> List[SourceStats]:
        """Get top traffic sources."""

        filter_sql, filter_params = self._build_filter_sql(filters)

        results = await self._query(
            f"""
            SELECT
                COALESCE(referrer_domain, 'Direct') as source,
                referrer_type as source_type,
                COUNT(*) as visits,
                COUNT(DISTINCT visitor_hash) as visitors
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
                AND is_bot = 0 {filter_sql}
            GROUP BY referrer_domain, referrer_type
            ORDER BY visits DESC
            LIMIT ?
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params + [limit],
        )

        return [
            SourceStats(
                source=r["source"] or "Direct",
                source_type=r["source_type"] or "direct",
                visits=r["visits"],
                visitors=r["visitors"],
            )
            for r in results
        ]

    async def get_source_types(
        self,
        start_date: date,
        end_date: date,
        filters: Optional[DashboardFilters] = None,
    ) -> List[Dict[str, Any]]:
        """Get traffic breakdown by source type."""

        filter_sql, filter_params = self._build_filter_sql(filters)

        results = await self._query(
            f"""
            SELECT
                COALESCE(referrer_type, 'direct') as source_type,
                COUNT(*) as visits
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
                AND is_bot = 0 {filter_sql}
            GROUP BY referrer_type
            ORDER BY visits DESC
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params,
        )

        return results

    # =========================================================================
    # GEOGRAPHY
    # =========================================================================

    async def get_countries(
        self,
        start_date: date,
        end_date: date,
        limit: int = 20,
        filters: Optional[DashboardFilters] = None,
    ) -> List[CountryStats]:
        """Get traffic by country."""

        filter_sql, filter_params = self._build_filter_sql(filters)

        results = await self._query(
            f"""
            SELECT
                country as country_code,
                COUNT(*) as visits,
                COUNT(DISTINCT visitor_hash) as visitors,
                AVG(latitude) as lat,
                AVG(longitude) as lon
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
                AND is_bot = 0 AND country != '' {filter_sql}
            GROUP BY country
            ORDER BY visits DESC
            LIMIT ?
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params + [limit],
        )

        # Country name lookup
        names = self._get_country_names()

        return [
            CountryStats(
                country_code=r["country_code"],
                country_name=names.get(r["country_code"], r["country_code"]),
                visits=r["visits"],
                visitors=r["visitors"],
            )
            for r in results
        ]

    async def get_regions(
        self,
        start_date: date,
        end_date: date,
        country: str,
        limit: int = 20,
        filters: Optional[DashboardFilters] = None,
    ) -> List[Dict[str, Any]]:
        """Get regions/states for a specific country."""

        filter_sql, filter_params = self._build_filter_sql(filters)

        results = await self._query(
            f"""
            SELECT
                region,
                COUNT(*) as visits,
                COUNT(DISTINCT visitor_hash) as visitors,
                AVG(latitude) as lat,
                AVG(longitude) as lon
            FROM page_views
            WHERE site = ? AND country = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
                AND is_bot = 0 AND region != '' AND region IS NOT NULL {filter_sql}
            GROUP BY region
            ORDER BY visits DESC
            LIMIT ?
            """,
            [self.site_name, country, start_date.isoformat(), end_date.isoformat()] + filter_params + [limit],
        )

        return results

    async def get_cities(
        self,
        start_date: date,
        end_date: date,
        country: str,
        region: Optional[str] = None,
        limit: int = 30,
        filters: Optional[DashboardFilters] = None,
    ) -> List[Dict[str, Any]]:
        """Get cities for a specific country/region."""

        filter_sql, filter_params = self._build_filter_sql(filters)

        if region:
            results = await self._query(
                f"""
                SELECT
                    city,
                    COUNT(*) as visits,
                    AVG(latitude) as lat,
                    AVG(longitude) as lon
                FROM page_views
                WHERE site = ? AND country = ? AND region = ?
                    AND date(timestamp) >= ? AND date(timestamp) <= ?
                    AND is_bot = 0 AND city != '' AND city IS NOT NULL {filter_sql}
                GROUP BY city
                ORDER BY visits DESC
                LIMIT ?
                """,
                [self.site_name, country, region, start_date.isoformat(), end_date.isoformat()] + filter_params + [limit],
            )
        else:
            results = await self._query(
                f"""
                SELECT
                    city,
                    region,
                    COUNT(*) as visits,
                    AVG(latitude) as lat,
                    AVG(longitude) as lon
                FROM page_views
                WHERE site = ? AND country = ?
                    AND date(timestamp) >= ? AND date(timestamp) <= ?
                    AND is_bot = 0 AND city != '' AND city IS NOT NULL {filter_sql}
                GROUP BY city, region
                ORDER BY visits DESC
                LIMIT ?
                """,
                [self.site_name, country, start_date.isoformat(), end_date.isoformat()] + filter_params + [limit],
            )

        return results

    async def get_globe_data(
        self,
        start_date: date,
        end_date: date,
        filters: Optional[DashboardFilters] = None,
    ) -> GlobeData:
        """Get data for the 3D globe visualization."""

        filter_sql, filter_params = self._build_filter_sql(filters)

        countries = await self._query(
            f"""
            SELECT
                country as code,
                COUNT(*) as visits,
                AVG(latitude) as lat,
                AVG(longitude) as lon
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
                AND is_bot = 0 AND country != '' {filter_sql}
            GROUP BY country
            ORDER BY visits DESC
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params,
        )

        names = self._get_country_names()

        return GlobeData(
            countries=[
                {
                    "code": c["code"],
                    "name": names.get(c["code"], c["code"]),
                    "visits": c["visits"],
                    "lat": c["lat"],
                    "lon": c["lon"],
                }
                for c in countries
            ]
        )

    def _get_country_names(self) -> Dict[str, str]:
        """Country code to name mapping."""
        return {
            'US': 'United States', 'CN': 'China', 'CA': 'Canada', 'SG': 'Singapore',
            'PT': 'Portugal', 'DE': 'Germany', 'VN': 'Vietnam', 'PK': 'Pakistan',
            'GB': 'United Kingdom', 'FR': 'France', 'JP': 'Japan', 'IN': 'India',
            'BR': 'Brazil', 'AU': 'Australia', 'KR': 'South Korea', 'NL': 'Netherlands',
            'IT': 'Italy', 'ES': 'Spain', 'CH': 'Switzerland', 'SE': 'Sweden',
            'NO': 'Norway', 'DK': 'Denmark', 'FI': 'Finland', 'IE': 'Ireland',
            'RU': 'Russia', 'MX': 'Mexico', 'AR': 'Argentina', 'CL': 'Chile',
            'CO': 'Colombia', 'PE': 'Peru', 'EG': 'Egypt', 'NG': 'Nigeria',
            'ZA': 'South Africa', 'SA': 'Saudi Arabia', 'AE': 'UAE', 'IL': 'Israel',
            'TR': 'Turkey', 'PL': 'Poland', 'UA': 'Ukraine', 'CZ': 'Czech Republic',
            'HK': 'Hong Kong', 'TW': 'Taiwan', 'MY': 'Malaysia', 'TH': 'Thailand',
            'ID': 'Indonesia', 'PH': 'Philippines', 'NZ': 'New Zealand', 'AT': 'Austria',
            'BE': 'Belgium', 'GR': 'Greece', 'HU': 'Hungary', 'RO': 'Romania',
            'BD': 'Bangladesh', 'KE': 'Kenya',
        }

    # =========================================================================
    # TECHNOLOGY
    # =========================================================================

    async def get_devices(
        self,
        start_date: date,
        end_date: date,
        filters: Optional[DashboardFilters] = None,
    ) -> List[DeviceStats]:
        """Get device type breakdown."""

        filter_sql, filter_params = self._build_filter_sql(filters)

        results = await self._query(
            f"""
            SELECT
                device_type,
                COUNT(*) as visits
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
                AND is_bot = 0 {filter_sql}
            GROUP BY device_type
            ORDER BY visits DESC
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params,
        )

        total = sum(r["visits"] for r in results)

        return [
            DeviceStats(
                device_type=r["device_type"] or "unknown",
                visits=r["visits"],
                percentage=round((r["visits"] / total) * 100, 1) if total > 0 else 0,
            )
            for r in results
        ]

    async def get_browsers(
        self,
        start_date: date,
        end_date: date,
        limit: int = 10,
        filters: Optional[DashboardFilters] = None,
    ) -> List[BrowserStats]:
        """Get browser breakdown."""

        filter_sql, filter_params = self._build_filter_sql(filters)

        results = await self._query(
            f"""
            SELECT
                browser,
                COUNT(*) as visits
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
                AND is_bot = 0 AND browser != '' {filter_sql}
            GROUP BY browser
            ORDER BY visits DESC
            LIMIT ?
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params + [limit],
        )

        total = sum(r["visits"] for r in results)

        return [
            BrowserStats(
                browser=r["browser"],
                visits=r["visits"],
                percentage=round((r["visits"] / total) * 100, 1) if total > 0 else 0,
            )
            for r in results
        ]

    async def get_operating_systems(
        self,
        start_date: date,
        end_date: date,
        limit: int = 10,
        filters: Optional[DashboardFilters] = None,
    ) -> List[Dict[str, Any]]:
        """Get OS breakdown."""

        filter_sql, filter_params = self._build_filter_sql(filters)

        results = await self._query(
            f"""
            SELECT
                os,
                COUNT(*) as visits
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
                AND is_bot = 0 AND os != '' {filter_sql}
            GROUP BY os
            ORDER BY visits DESC
            LIMIT ?
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params + [limit],
        )

        total = sum(r["visits"] for r in results)

        return [
            {
                "os": r["os"],
                "visits": r["visits"],
                "percentage": round((r["visits"] / total) * 100, 1) if total > 0 else 0,
            }
            for r in results
        ]

    async def get_languages(
        self,
        start_date: date,
        end_date: date,
        limit: int = 10,
        filters: Optional[DashboardFilters] = None,
    ) -> List[Dict[str, Any]]:
        """Get language breakdown."""

        filter_sql, filter_params = self._build_filter_sql(filters)

        results = await self._query(
            f"""
            SELECT
                language,
                COUNT(*) as visits
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
                AND is_bot = 0 AND language != '' AND language IS NOT NULL
                {filter_sql}
            GROUP BY language
            ORDER BY visits DESC
            LIMIT ?
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params + [limit],
        )

        return results

    async def get_screen_sizes(
        self,
        start_date: date,
        end_date: date,
        limit: int = 10,
        filters: Optional[DashboardFilters] = None,
    ) -> List[Dict[str, Any]]:
        """Get screen size breakdown."""

        filter_sql, filter_params = self._build_filter_sql(filters)

        results = await self._query(
            f"""
            SELECT
                (screen_width || 'x' || screen_height) as resolution,
                COUNT(*) as visits
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
                AND is_bot = 0
                AND screen_width IS NOT NULL AND screen_height IS NOT NULL
                {filter_sql}
            GROUP BY resolution
            ORDER BY visits DESC
            LIMIT ?
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params + [limit],
        )

        return results

    # =========================================================================
    # EVENTS
    # =========================================================================

    async def get_events(
        self,
        start_date: date,
        end_date: date,
        limit: int = 20,
        event_type: Optional[str] = None,
        filters: Optional[DashboardFilters] = None,
    ) -> List[EventStats]:
        """Get event statistics, optionally filtered by event type."""

        filter_sql, filter_params = self._build_event_filter_sql(filters)
        type_filter = "AND event_type = ?" if event_type else ""

        params = [self.site_name, start_date.isoformat(), end_date.isoformat()]
        if event_type:
            params.append(event_type)
        params.extend(filter_params)
        params.append(limit)

        results = await self._query(
            f"""
            SELECT
                event_name,
                event_type,
                COUNT(*) as count,
                COUNT(DISTINCT session_id) as unique_sessions
            FROM events
            WHERE site = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
                {type_filter} {filter_sql}
            GROUP BY event_name, event_type
            ORDER BY count DESC
            LIMIT ?
            """,
            params,
        )

        return [
            EventStats(
                event_name=r["event_name"],
                event_type=r["event_type"],
                count=r["count"],
                unique_sessions=r["unique_sessions"],
            )
            for r in results
        ]

    async def get_scroll_depth(
        self,
        start_date: date,
        end_date: date,
        filters: Optional[DashboardFilters] = None,
    ) -> Dict[str, int]:
        """Get scroll depth breakdown."""

        filter_sql, filter_params = self._build_event_filter_sql(filters)

        results = await self._query(
            f"""
            SELECT
                event_name,
                COUNT(*) as count
            FROM events
            WHERE site = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
                AND event_type = 'scroll' {filter_sql}
            GROUP BY event_name
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params,
        )

        depths = {"25": 0, "50": 0, "75": 0, "100": 0}
        for r in results:
            name = r["event_name"]  # e.g., "scroll_25", "scroll_50"
            depth = name.replace("scroll_", "")
            if depth in depths:
                depths[depth] = r["count"]

        return depths

    async def get_event_types(
        self,
        start_date: date,
        end_date: date,
        filters: Optional[DashboardFilters] = None,
    ) -> List[Dict[str, Any]]:
        """Get event type breakdown."""

        filter_sql, filter_params = self._build_event_filter_sql(filters)

        results = await self._query(
            f"""
            SELECT
                event_type,
                COUNT(*) as count
            FROM events
            WHERE site = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
                {filter_sql}
            GROUP BY event_type
            ORDER BY count DESC
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params,
        )

        return results

    # =========================================================================
    # REALTIME
    # =========================================================================

    async def get_realtime_data(self, minutes: int = 5) -> RealtimeData:
        """Get real-time visitor data."""

        cutoff = (datetime.utcnow() - timedelta(minutes=minutes)).isoformat()

        # Active visitors count
        visitors = await self._query(
            """
            SELECT COUNT(DISTINCT visitor_hash) as count
            FROM page_views
            WHERE site = ? AND timestamp >= ? AND is_bot = 0
            """,
            [self.site_name, cutoff],
        )

        # Active sessions with details
        sessions = await self._query(
            """
            SELECT
                session_id,
                url as page,
                country,
                device_type as device,
                MAX(timestamp) as last_seen
            FROM page_views
            WHERE site = ? AND timestamp >= ? AND is_bot = 0
            GROUP BY session_id
            ORDER BY last_seen DESC
            LIMIT 20
            """,
            [self.site_name, cutoff],
        )

        # Pages being viewed
        pages = await self._query(
            """
            SELECT url, COUNT(DISTINCT visitor_hash) as count
            FROM page_views
            WHERE site = ? AND timestamp >= ? AND is_bot = 0
            GROUP BY url
            ORDER BY count DESC
            LIMIT 10
            """,
            [self.site_name, cutoff],
        )

        # Countries
        countries = await self._query(
            """
            SELECT country as code, COUNT(DISTINCT visitor_hash) as count
            FROM page_views
            WHERE site = ? AND timestamp >= ? AND is_bot = 0 AND country != ''
            GROUP BY country
            ORDER BY count DESC
            LIMIT 10
            """,
            [self.site_name, cutoff],
        )

        # Sources
        sources = await self._query(
            """
            SELECT
                COALESCE(referrer_domain, 'Direct') as source,
                COUNT(DISTINCT visitor_hash) as count
            FROM page_views
            WHERE site = ? AND timestamp >= ? AND is_bot = 0
            GROUP BY referrer_domain
            ORDER BY count DESC
            LIMIT 10
            """,
            [self.site_name, cutoff],
        )

        return RealtimeData(
            active_visitors=visitors[0]["count"] if visitors else 0,
            active_sessions=sessions,
            pages=pages,
            countries=countries,
            sources=sources,
        )

    async def get_realtime_count(self) -> int:
        """Get count of human visitors in the last 5 minutes."""
        data = await self.get_realtime_data(minutes=5)
        return data.active_visitors

    # =========================================================================
    # EXPORT
    # =========================================================================

    async def export_pageviews(
        self,
        start_date: date,
        end_date: date,
        limit: int = 10000,
        filters: Optional[DashboardFilters] = None,
        include_bots: bool = False,
    ) -> List[Dict[str, Any]]:
        """Export raw pageview data for CSV.

        Args:
            start_date: Start of date range
            end_date: End of date range
            limit: Maximum rows to export (default 10000)
            filters: Optional filters to apply
            include_bots: If True, include bot traffic (default False)
        """
        filter_sql, filter_params = self._build_filter_sql(filters)
        bot_filter = "" if include_bots else "AND is_bot = 0"

        results = await self._query(
            f"""
            SELECT
                timestamp, url, page_title,
                referrer_type, referrer_domain,
                country, region, city,
                device_type, browser, os,
                utm_source, utm_medium, utm_campaign
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
                {bot_filter} {filter_sql}
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params + [limit],
        )

        return results

    async def export_events(
        self,
        start_date: date,
        end_date: date,
        limit: int = 10000,
        filters: Optional[DashboardFilters] = None,
    ) -> List[Dict[str, Any]]:
        """Export event data for CSV."""

        filter_sql, filter_params = self._build_event_filter_sql(filters)

        results = await self._query(
            f"""
            SELECT
                timestamp, event_type, event_name, event_data,
                page_url, country, device_type
            FROM events
            WHERE site = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
                {filter_sql}
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params + [limit],
        )

        return results

    # =========================================================================
    # UTM CAMPAIGNS
    # =========================================================================

    async def get_utm_sources(
        self,
        start_date: date,
        end_date: date,
        limit: int = 10,
        filters: Optional[DashboardFilters] = None,
    ) -> List[Dict[str, Any]]:
        """Get UTM source breakdown."""

        filter_sql, filter_params = self._build_filter_sql(filters)

        results = await self._query(
            f"""
            SELECT
                utm_source as source,
                utm_medium as medium,
                COUNT(*) as visits,
                COUNT(DISTINCT visitor_hash) as visitors
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
                AND is_bot = 0 AND utm_source != '' AND utm_source IS NOT NULL
                {filter_sql}
            GROUP BY utm_source, utm_medium
            ORDER BY visits DESC
            LIMIT ?
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params + [limit],
        )

        return results

    async def get_utm_campaigns(
        self,
        start_date: date,
        end_date: date,
        limit: int = 10,
        filters: Optional[DashboardFilters] = None,
    ) -> List[Dict[str, Any]]:
        """Get UTM campaign breakdown."""

        filter_sql, filter_params = self._build_filter_sql(filters)

        results = await self._query(
            f"""
            SELECT
                utm_campaign as campaign,
                utm_source as source,
                COUNT(*) as visits,
                COUNT(DISTINCT visitor_hash) as visitors
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
                AND is_bot = 0 AND utm_campaign != '' AND utm_campaign IS NOT NULL
                {filter_sql}
            GROUP BY utm_campaign, utm_source
            ORDER BY visits DESC
            LIMIT ?
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params + [limit],
        )

        return results

    # =========================================================================
    # AUTHENTICATION (imported from parent module for backwards compat)
    # =========================================================================

    async def has_passkeys(self) -> bool:
        """Check if any passkeys are registered for this site."""
        result = await self._query(
            "SELECT id FROM passkeys WHERE site = ? LIMIT 1",
            [self.site_name],
        )
        return len(result) > 0

    async def get_passkeys(self) -> list[dict]:
        """Get all passkeys for this site."""
        return await self._query(
            """
            SELECT id, credential_id, public_key, sign_count, device_name,
                   created_at, last_used_at
            FROM passkeys WHERE site = ? ORDER BY created_at DESC
            """,
            [self.site_name],
        )

    async def get_passkey_by_credential_id(self, credential_id: str) -> Optional[dict]:
        """Get a passkey by its credential ID."""
        result = await self._query(
            """
            SELECT id, credential_id, public_key, sign_count, device_name,
                   created_at, last_used_at
            FROM passkeys WHERE site = ? AND credential_id = ?
            """,
            [self.site_name, credential_id],
        )
        return result[0] if result else None

    async def create_passkey(
        self,
        credential_id: str,
        public_key: str,
        sign_count: int = 0,
        device_name: str = "Unknown Device",
    ) -> int:
        """Create a new passkey. Returns the passkey ID."""
        await self._query(
            """
            INSERT INTO passkeys (site, credential_id, public_key, sign_count, device_name)
            VALUES (?, ?, ?, ?, ?)
            """,
            [self.site_name, credential_id, public_key, sign_count, device_name],
        )
        result = await self._query(
            "SELECT id FROM passkeys WHERE site = ? AND credential_id = ?",
            [self.site_name, credential_id],
        )
        return result[0]["id"] if result else 0

    async def update_passkey_sign_count(self, passkey_id: int, sign_count: int) -> None:
        """Update passkey sign count and last_used_at timestamp."""
        await self._query(
            """
            UPDATE passkeys
            SET sign_count = ?, last_used_at = datetime('now')
            WHERE id = ? AND site = ?
            """,
            [sign_count, passkey_id, self.site_name],
        )

    async def delete_passkey(self, passkey_id: int) -> bool:
        """Delete a passkey by ID. Returns True if deleted."""
        count_result = await self._query(
            "SELECT COUNT(*) as count FROM passkeys WHERE site = ?",
            [self.site_name],
        )
        count = count_result[0]["count"] if count_result else 0

        if count <= 1:
            return False

        await self._query(
            "DELETE FROM passkeys WHERE id = ? AND site = ?",
            [passkey_id, self.site_name],
        )
        return True

    async def create_session(
        self,
        token_hash: str,
        passkey_id: Optional[int] = None,
        user_agent: str = "",
        ip_address: str = "",
        expires_hours: int = 168,
    ) -> None:
        """Create a new authenticated session."""
        await self._query(
            """
            INSERT INTO auth_sessions (site, token_hash, passkey_id, expires_at, user_agent, ip_address)
            VALUES (?, ?, ?, datetime('now', '+' || ? || ' hours'), ?, ?)
            """,
            [self.site_name, token_hash, passkey_id, expires_hours, user_agent, ip_address],
        )

    async def validate_session(self, token_hash: str) -> Optional[dict]:
        """Validate a session token. Returns session data if valid."""
        result = await self._query(
            """
            SELECT id, passkey_id, created_at, expires_at
            FROM auth_sessions
            WHERE site = ? AND token_hash = ? AND expires_at > datetime('now')
            """,
            [self.site_name, token_hash],
        )
        return result[0] if result else None

    async def delete_session(self, token_hash: str) -> None:
        """Delete a session (logout)."""
        await self._query(
            "DELETE FROM auth_sessions WHERE site = ? AND token_hash = ?",
            [self.site_name, token_hash],
        )

    async def store_challenge(self, challenge: str, challenge_type: str) -> None:
        """Store a WebAuthn challenge (expires in 5 minutes)."""
        await self._query(
            "DELETE FROM webauthn_challenges WHERE site = ? AND expires_at <= datetime('now')",
            [self.site_name],
        )
        await self._query(
            """
            INSERT INTO webauthn_challenges (site, challenge, challenge_type, expires_at)
            VALUES (?, ?, ?, datetime('now', '+5 minutes'))
            """,
            [self.site_name, challenge, challenge_type],
        )

    async def consume_challenge(self, challenge_type: str) -> Optional[str]:
        """Get and delete the most recent valid challenge."""
        result = await self._query(
            """
            SELECT id, challenge FROM webauthn_challenges
            WHERE site = ? AND challenge_type = ? AND expires_at > datetime('now')
            ORDER BY created_at DESC LIMIT 1
            """,
            [self.site_name, challenge_type],
        )
        if not result:
            return None

        challenge = result[0]["challenge"]
        challenge_id = result[0]["id"]

        await self._query(
            "DELETE FROM webauthn_challenges WHERE id = ?",
            [challenge_id],
        )
        return challenge
