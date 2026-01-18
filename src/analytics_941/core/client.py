"""
HTTP client for querying Cloudflare D1 analytics database.

Enhanced version with session tracking, events, and filtering support.
"""
import json
from datetime import date, datetime, timedelta
from typing import Any

import httpx

from .models import (
    ActivityEvent,
    BrowserStats,
    CoreMetrics,
    CountryStats,
    DashboardFilters,
    DateRange,
    DeviceStats,
    EventStats,
    FunnelDefinition,
    FunnelResult,
    FunnelStep,
    FunnelStepResult,
    GlobeData,
    GoalDefinition,
    GoalResult,
    MetricChange,
    PageStats,
    RealtimeData,
    SourceStats,
    TimeSeriesPoint,
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

    async def _query(self, sql: str, params: list | None = None) -> list[dict]:
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

    async def _execute(self, sql: str, params: list | None = None) -> None:
        """Execute a SQL statement without returning results."""
        await self._query(sql, params)

    # =========================================================================
    # CORE METRICS
    # =========================================================================

    async def get_core_metrics(
        self,
        start_date: date,
        end_date: date,
        compare_start: date | None = None,
        compare_end: date | None = None,
        filters: DashboardFilters | None = None,
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

        # Session metrics (bounce rate, avg duration, pages per session)
        session_filter_sql, session_filter_params = self._build_session_filter_sql(filters)
        session_stats = await self._query(
            f"""
            SELECT
                AVG(CASE WHEN is_bounce = 1 THEN 1 ELSE 0 END) * 100 as bounce_rate,
                AVG(duration_seconds) as avg_duration,
                AVG(pageview_count) as pages_per_session
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
        pages_per_session = round(session_data.get("pages_per_session", 0) or 0, 1)
        bot_views = current_data.get("bot_views") or 0

        # Comparison period
        prev_views = prev_visitors = prev_sessions = prev_bounce = prev_duration = prev_pps = None
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
                    AVG(duration_seconds) as avg_duration,
                    AVG(pageview_count) as pages_per_session
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
                prev_pps = round(prev_sess[0].get("pages_per_session", 0) or 0, 1)

        return CoreMetrics(
            views=self._metric_with_change(views, prev_views),
            visitors=self._metric_with_change(visitors, prev_visitors),
            sessions=self._metric_with_change(sessions, prev_sessions),
            bounce_rate=self._metric_with_change(bounce_rate, prev_bounce),
            avg_duration=self._metric_with_change(avg_duration, prev_duration),
            pages_per_session=self._metric_with_change(pages_per_session, prev_pps),
            bot_views=bot_views,
        )

    def _metric_with_change(self, current: int, previous: int | None) -> MetricChange:
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

    def _build_filter_sql(self, filters: DashboardFilters | None) -> tuple[str, list]:
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

    def _build_session_filter_sql(self, filters: DashboardFilters | None) -> tuple[str, list]:
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

    def _build_event_filter_sql(self, filters: DashboardFilters | None) -> tuple[str, list]:
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
    # SESSION METRICS (Standalone)
    # =========================================================================

    async def get_bounce_rate(
        self,
        start_date: date,
        end_date: date,
        filters: DashboardFilters | None = None,
        compare_start: date | None = None,
        compare_end: date | None = None,
    ) -> MetricChange:
        """Get bounce rate as percentage (0-100).

        Bounce rate = percentage of single-pageview sessions.
        Returns 0 if no session data exists (handles zero-data gracefully).
        """
        filter_sql, filter_params = self._build_session_filter_sql(filters)

        result = await self._query(
            f"""
            SELECT AVG(CASE WHEN is_bounce = 1 THEN 1.0 ELSE 0.0 END) * 100 as bounce_rate
            FROM sessions
            WHERE site = ? AND date(started_at) >= ? AND date(started_at) <= ?
                {filter_sql}
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params,
        )

        current = round(result[0].get("bounce_rate", 0) or 0, 1) if result else 0

        # Comparison period
        previous = None
        if compare_start and compare_end:
            prev_result = await self._query(
                f"""
                SELECT AVG(CASE WHEN is_bounce = 1 THEN 1.0 ELSE 0.0 END) * 100 as bounce_rate
                FROM sessions
                WHERE site = ? AND date(started_at) >= ? AND date(started_at) <= ?
                    {filter_sql}
                """,
                [self.site_name, compare_start.isoformat(), compare_end.isoformat()] + filter_params,
            )
            previous = round(prev_result[0].get("bounce_rate", 0) or 0, 1) if prev_result else 0

        return self._metric_with_change(current, previous)

    async def get_avg_session_duration(
        self,
        start_date: date,
        end_date: date,
        filters: DashboardFilters | None = None,
        compare_start: date | None = None,
        compare_end: date | None = None,
    ) -> MetricChange:
        """Get average session duration in seconds.

        Only includes sessions that have ended (duration_seconds IS NOT NULL).
        Returns 0 if no completed sessions exist (handles zero-data gracefully).
        """
        filter_sql, filter_params = self._build_session_filter_sql(filters)

        result = await self._query(
            f"""
            SELECT AVG(duration_seconds) as avg_duration
            FROM sessions
            WHERE site = ? AND date(started_at) >= ? AND date(started_at) <= ?
                AND duration_seconds IS NOT NULL
                {filter_sql}
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params,
        )

        current = round(result[0].get("avg_duration", 0) or 0) if result else 0

        # Comparison period
        previous = None
        if compare_start and compare_end:
            prev_result = await self._query(
                f"""
                SELECT AVG(duration_seconds) as avg_duration
                FROM sessions
                WHERE site = ? AND date(started_at) >= ? AND date(started_at) <= ?
                    AND duration_seconds IS NOT NULL
                    {filter_sql}
                """,
                [self.site_name, compare_start.isoformat(), compare_end.isoformat()] + filter_params,
            )
            previous = round(prev_result[0].get("avg_duration", 0) or 0) if prev_result else 0

        return self._metric_with_change(current, previous)

    async def get_sessions_count(
        self,
        start_date: date,
        end_date: date,
        filters: DashboardFilters | None = None,
        compare_start: date | None = None,
        compare_end: date | None = None,
    ) -> MetricChange:
        """Get total number of sessions.

        Returns 0 if no sessions exist (handles zero-data gracefully).
        """
        filter_sql, filter_params = self._build_session_filter_sql(filters)

        result = await self._query(
            f"""
            SELECT COUNT(*) as session_count
            FROM sessions
            WHERE site = ? AND date(started_at) >= ? AND date(started_at) <= ?
                {filter_sql}
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params,
        )

        current = int(result[0].get("session_count", 0) or 0) if result else 0

        # Comparison period
        previous = None
        if compare_start and compare_end:
            prev_result = await self._query(
                f"""
                SELECT COUNT(*) as session_count
                FROM sessions
                WHERE site = ? AND date(started_at) >= ? AND date(started_at) <= ?
                    {filter_sql}
                """,
                [self.site_name, compare_start.isoformat(), compare_end.isoformat()] + filter_params,
            )
            previous = int(prev_result[0].get("session_count", 0) or 0) if prev_result else 0

        return self._metric_with_change(current, previous)

    async def get_pages_per_session(
        self,
        start_date: date,
        end_date: date,
        filters: DashboardFilters | None = None,
        compare_start: date | None = None,
        compare_end: date | None = None,
    ) -> MetricChange:
        """Get average pages per session.

        Returns 0 if no sessions exist (handles zero-data gracefully).
        """
        filter_sql, filter_params = self._build_session_filter_sql(filters)

        result = await self._query(
            f"""
            SELECT AVG(pageview_count) as pages_per_session
            FROM sessions
            WHERE site = ? AND date(started_at) >= ? AND date(started_at) <= ?
                {filter_sql}
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params,
        )

        current = round(result[0].get("pages_per_session", 0) or 0, 1) if result else 0

        # Comparison period
        previous = None
        if compare_start and compare_end:
            prev_result = await self._query(
                f"""
                SELECT AVG(pageview_count) as pages_per_session
                FROM sessions
                WHERE site = ? AND date(started_at) >= ? AND date(started_at) <= ?
                    {filter_sql}
                """,
                [self.site_name, compare_start.isoformat(), compare_end.isoformat()] + filter_params,
            )
            previous = round(prev_result[0].get("pages_per_session", 0) or 0, 1) if prev_result else 0

        return self._metric_with_change(current, previous)

    # =========================================================================
    # TIME SERIES
    # =========================================================================

    async def get_time_series(
        self,
        start_date: date,
        end_date: date,
        granularity: str = "day",
        filters: DashboardFilters | None = None,
    ) -> list[TimeSeriesPoint]:
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
        filters: DashboardFilters | None = None,
    ) -> list[PageStats]:
        """Get top pages by views with bounce rate per page."""

        filter_sql, filter_params = self._build_filter_sql(filters)
        session_filter_sql, session_filter_params = self._build_session_filter_sql(filters)

        # Get page views
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

        # Get bounce rates per entry page
        bounce_results = await self._query(
            f"""
            SELECT
                entry_page as url,
                AVG(CASE WHEN is_bounce = 1 THEN 1.0 ELSE 0.0 END) * 100 as bounce_rate
            FROM sessions
            WHERE site = ? AND date(started_at) >= ? AND date(started_at) <= ?
                {session_filter_sql}
            GROUP BY entry_page
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + session_filter_params,
        )

        # Map bounce rates by URL
        bounce_map = {r["url"]: round(r["bounce_rate"] or 0, 1) for r in bounce_results}

        return [
            PageStats(
                url=r["url"],
                views=r["views"],
                visitors=r["visitors"],
                bounce_rate=bounce_map.get(r["url"]),
            )
            for r in results
        ]

    async def get_entry_pages(
        self,
        start_date: date,
        end_date: date,
        limit: int = 10,
        filters: DashboardFilters | None = None,
    ) -> list[PageStats]:
        """Get top entry pages (first page of sessions) with bounce rate."""

        filter_sql, filter_params = self._build_session_filter_sql(filters)

        results = await self._query(
            f"""
            SELECT
                entry_page as url,
                COUNT(*) as entries,
                COUNT(DISTINCT visitor_hash) as visitors,
                AVG(CASE WHEN is_bounce = 1 THEN 1.0 ELSE 0.0 END) * 100 as bounce_rate
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
            PageStats(
                url=r["url"],
                views=r["entries"],
                visitors=r["visitors"],
                entries=r["entries"],
                bounce_rate=round(r["bounce_rate"] or 0, 1),
            )
            for r in results
        ]

    async def get_exit_pages(
        self,
        start_date: date,
        end_date: date,
        limit: int = 10,
        filters: DashboardFilters | None = None,
    ) -> list[PageStats]:
        """Get top exit pages (last page of sessions) with exit rate.

        Exit rate = exits from this page / total pageviews of this page * 100
        """

        filter_sql, filter_params = self._build_session_filter_sql(filters)
        pv_filter_sql, pv_filter_params = self._build_filter_sql(filters)

        # Get exits per page from sessions
        exit_results = await self._query(
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

        if not exit_results:
            return []

        # Get pageview counts for exit rate calculation
        urls = [r["url"] for r in exit_results]
        placeholders = ", ".join(["?" for _ in urls])
        pv_results = await self._query(
            f"""
            SELECT
                url,
                COUNT(*) as views
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
                AND url IN ({placeholders}) AND is_bot = 0 {pv_filter_sql}
            GROUP BY url
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + urls + pv_filter_params,
        )

        # Map pageviews by URL
        pv_map = {r["url"]: r["views"] for r in pv_results}

        return [
            PageStats(
                url=r["url"],
                views=pv_map.get(r["url"], r["exits"]),  # Use pageviews if available
                visitors=r["visitors"],
                exits=r["exits"],
                exit_rate=round((r["exits"] / pv_map.get(r["url"], r["exits"])) * 100, 1) if pv_map.get(r["url"], r["exits"]) > 0 else 0,
            )
            for r in exit_results
        ]

    async def get_entry_exit_flow(
        self,
        start_date: date,
        end_date: date,
        limit: int = 10,
        filters: DashboardFilters | None = None,
    ) -> list[dict[str, Any]]:
        """Get entry→exit page flow data for visualization.

        Returns top entry→exit page combinations with session counts.
        """

        filter_sql, filter_params = self._build_session_filter_sql(filters)

        results = await self._query(
            f"""
            SELECT
                entry_page,
                exit_page,
                COUNT(*) as sessions
            FROM sessions
            WHERE site = ? AND date(started_at) >= ? AND date(started_at) <= ?
                AND entry_page IS NOT NULL AND exit_page IS NOT NULL
                {filter_sql}
            GROUP BY entry_page, exit_page
            ORDER BY sessions DESC
            LIMIT ?
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params + [limit],
        )

        return [
            {
                "entry_page": r["entry_page"],
                "exit_page": r["exit_page"],
                "sessions": r["sessions"],
            }
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
        filters: DashboardFilters | None = None,
    ) -> list[SourceStats]:
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
        filters: DashboardFilters | None = None,
    ) -> list[dict[str, Any]]:
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
        filters: DashboardFilters | None = None,
    ) -> list[CountryStats]:
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
        filters: DashboardFilters | None = None,
    ) -> list[dict[str, Any]]:
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
        region: str | None = None,
        limit: int = 30,
        filters: DashboardFilters | None = None,
    ) -> list[dict[str, Any]]:
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
        filters: DashboardFilters | None = None,
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

    def _get_country_names(self) -> dict[str, str]:
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
        filters: DashboardFilters | None = None,
    ) -> list[DeviceStats]:
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
        filters: DashboardFilters | None = None,
    ) -> list[BrowserStats]:
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
        filters: DashboardFilters | None = None,
    ) -> list[dict[str, Any]]:
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
        filters: DashboardFilters | None = None,
    ) -> list[dict[str, Any]]:
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
        filters: DashboardFilters | None = None,
    ) -> list[dict[str, Any]]:
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
        event_type: str | None = None,
        filters: DashboardFilters | None = None,
    ) -> list[EventStats]:
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
        filters: DashboardFilters | None = None,
    ) -> dict[str, int]:
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

    async def get_scroll_depth_by_page(
        self,
        start_date: date,
        end_date: date,
        limit: int = 10,
        filters: DashboardFilters | None = None,
    ) -> list[dict[str, Any]]:
        """Get average scroll depth per page.

        Returns pages with their average maximum scroll depth reached.
        Uses the highest scroll event (scroll_100 > scroll_75 > etc.) per session per page.
        """
        filter_sql, filter_params = self._build_event_filter_sql(filters)

        # Get max scroll depth per page per session, then average across sessions
        results = await self._query(
            f"""
            WITH max_scroll_per_session AS (
                SELECT
                    page_url,
                    session_id,
                    MAX(CAST(REPLACE(event_name, 'scroll_', '') AS INTEGER)) as max_depth
                FROM events
                WHERE site = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
                    AND event_type = 'scroll' {filter_sql}
                GROUP BY page_url, session_id
            )
            SELECT
                page_url,
                AVG(max_depth) as avg_depth,
                COUNT(*) as sessions
            FROM max_scroll_per_session
            GROUP BY page_url
            ORDER BY sessions DESC
            LIMIT ?
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params + [limit],
        )

        return [
            {
                "url": r["page_url"],
                "avg_depth": round(r["avg_depth"], 1) if r["avg_depth"] else 0,
                "sessions": r["sessions"],
            }
            for r in results
        ]

    async def get_event_types(
        self,
        start_date: date,
        end_date: date,
        filters: DashboardFilters | None = None,
    ) -> list[dict[str, Any]]:
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

    async def get_events_time_series(
        self,
        start_date: date,
        end_date: date,
        event_type: str | None = None,
        filters: DashboardFilters | None = None,
    ) -> list[dict[str, Any]]:
        """Get events count over time for charting.

        Returns daily event counts, optionally filtered by event type.
        """
        filter_sql, filter_params = self._build_event_filter_sql(filters)
        type_filter = "AND event_type = ?" if event_type else ""

        params = [self.site_name, start_date.isoformat(), end_date.isoformat()]
        if event_type:
            params.append(event_type)
        params.extend(filter_params)

        results = await self._query(
            f"""
            SELECT
                date(timestamp) as date,
                COUNT(*) as count
            FROM events
            WHERE site = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
                {type_filter} {filter_sql}
            GROUP BY date(timestamp)
            ORDER BY date
            """,
            params,
        )

        return [{"date": r["date"], "count": r["count"]} for r in results]

    async def get_events_with_trend(
        self,
        start_date: date,
        end_date: date,
        compare_start: date | None = None,
        compare_end: date | None = None,
        limit: int = 20,
        event_type: str | None = None,
        filters: DashboardFilters | None = None,
    ) -> list[dict[str, Any]]:
        """Get top events with trend comparison to previous period.

        Returns events with current count and percentage change vs comparison period.
        """
        filter_sql, filter_params = self._build_event_filter_sql(filters)
        type_filter = "AND event_type = ?" if event_type else ""

        # Build params for current period
        params = [self.site_name, start_date.isoformat(), end_date.isoformat()]
        if event_type:
            params.append(event_type)
        params.extend(filter_params)
        params.append(limit)

        # Get current period events
        current_results = await self._query(
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

        # If no comparison period, return without trends
        if not compare_start or not compare_end:
            return [
                {
                    "event_name": r["event_name"],
                    "event_type": r["event_type"],
                    "count": r["count"],
                    "unique_sessions": r["unique_sessions"],
                    "trend_percent": None,
                    "trend_direction": None,
                }
                for r in current_results
            ]

        # Build params for comparison period
        compare_params = [self.site_name, compare_start.isoformat(), compare_end.isoformat()]
        if event_type:
            compare_params.append(event_type)
        compare_params.extend(filter_params)

        # Get comparison period events
        prev_results = await self._query(
            f"""
            SELECT
                event_name,
                COUNT(*) as count
            FROM events
            WHERE site = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
                {type_filter} {filter_sql}
            GROUP BY event_name
            """,
            compare_params,
        )

        # Build lookup for previous counts
        prev_counts = {r["event_name"]: r["count"] for r in prev_results}

        # Calculate trends
        events_with_trend = []
        for r in current_results:
            current_count = r["count"]
            prev_count = prev_counts.get(r["event_name"], 0)

            if prev_count == 0:
                trend_percent = 100.0 if current_count > 0 else 0.0
            else:
                trend_percent = round(((current_count - prev_count) / prev_count) * 100, 1)

            trend_direction = "up" if trend_percent > 0 else "down" if trend_percent < 0 else "same"

            events_with_trend.append({
                "event_name": r["event_name"],
                "event_type": r["event_type"],
                "count": current_count,
                "unique_sessions": r["unique_sessions"],
                "previous_count": prev_count,
                "trend_percent": abs(trend_percent),
                "trend_direction": trend_direction,
            })

        return events_with_trend

    async def get_event_properties(
        self,
        event_name: str,
        start_date: date,
        end_date: date,
        limit: int = 100,
        filters: DashboardFilters | None = None,
    ) -> list[dict[str, Any]]:
        """Get property breakdowns for a specific event.

        Returns event_data properties with their frequency counts.
        """
        filter_sql, filter_params = self._build_event_filter_sql(filters)

        # Get recent event data samples to analyze properties
        results = await self._query(
            f"""
            SELECT
                event_data,
                COUNT(*) as count
            FROM events
            WHERE site = ? AND event_name = ?
                AND date(timestamp) >= ? AND date(timestamp) <= ?
                AND event_data IS NOT NULL AND event_data != 'null'
                {filter_sql}
            GROUP BY event_data
            ORDER BY count DESC
            LIMIT ?
            """,
            [self.site_name, event_name, start_date.isoformat(), end_date.isoformat()]
            + filter_params
            + [limit],
        )

        # Parse and aggregate properties
        property_counts: dict[str, dict[str, int]] = {}

        for r in results:
            try:
                data = json.loads(r["event_data"]) if r["event_data"] else {}
                count = r["count"]

                for key, value in data.items():
                    if key not in property_counts:
                        property_counts[key] = {}

                    # Convert value to string for grouping
                    str_value = str(value) if value is not None else "(empty)"
                    property_counts[key][str_value] = property_counts[key].get(str_value, 0) + count
            except (json.JSONDecodeError, TypeError):
                continue

        # Format output
        properties = []
        for prop_name, values in property_counts.items():
            top_values = sorted(values.items(), key=lambda x: x[1], reverse=True)[:10]
            properties.append({
                "property": prop_name,
                "values": [{"value": v, "count": c} for v, c in top_values],
                "total": sum(values.values()),
            })

        return sorted(properties, key=lambda x: x["total"], reverse=True)

    async def get_outbound_clicks(
        self,
        start_date: date,
        end_date: date,
        limit: int = 20,
        filters: DashboardFilters | None = None,
    ) -> list[dict[str, Any]]:
        """Get top outbound link destinations.

        Returns outbound click events with destination URL, click count, and link text.
        """
        filter_sql, filter_params = self._build_event_filter_sql(filters)

        results = await self._query(
            f"""
            SELECT
                json_extract(event_data, '$.destination') as destination,
                json_extract(event_data, '$.text') as link_text,
                COUNT(*) as clicks,
                COUNT(DISTINCT session_id) as sessions
            FROM events
            WHERE site = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
                AND event_name = 'outbound_click' {filter_sql}
            GROUP BY destination
            ORDER BY clicks DESC
            LIMIT ?
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params + [limit],
        )

        return [
            {
                "destination": r["destination"],
                "link_text": r["link_text"] or "",
                "clicks": r["clicks"],
                "sessions": r["sessions"],
            }
            for r in results
        ]

    async def get_file_downloads(
        self,
        start_date: date,
        end_date: date,
        limit: int = 20,
        filters: DashboardFilters | None = None,
    ) -> list[dict[str, Any]]:
        """Get top file downloads.

        Returns download events with filename, extension, click count.
        """
        filter_sql, filter_params = self._build_event_filter_sql(filters)

        results = await self._query(
            f"""
            SELECT
                json_extract(event_data, '$.filename') as filename,
                json_extract(event_data, '$.extension') as extension,
                COUNT(*) as downloads,
                COUNT(DISTINCT session_id) as sessions
            FROM events
            WHERE site = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
                AND event_name = 'file_download' {filter_sql}
            GROUP BY filename, extension
            ORDER BY downloads DESC
            LIMIT ?
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params + [limit],
        )

        return [
            {
                "filename": r["filename"],
                "extension": r["extension"],
                "downloads": r["downloads"],
                "sessions": r["sessions"],
            }
            for r in results
        ]

    async def get_form_submissions(
        self,
        start_date: date,
        end_date: date,
        limit: int = 20,
        filters: DashboardFilters | None = None,
    ) -> list[dict[str, Any]]:
        """Get form submission events.

        Returns form submissions with form id, name, action, and counts.
        """
        filter_sql, filter_params = self._build_event_filter_sql(filters)

        results = await self._query(
            f"""
            SELECT
                COALESCE(json_extract(event_data, '$.form_id'), json_extract(event_data, '$.form_name'), json_extract(event_data, '$.action')) as form_identifier,
                json_extract(event_data, '$.form_id') as form_id,
                json_extract(event_data, '$.form_name') as form_name,
                json_extract(event_data, '$.action') as action,
                json_extract(event_data, '$.method') as method,
                COUNT(*) as submissions,
                COUNT(DISTINCT session_id) as sessions
            FROM events
            WHERE site = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
                AND event_name = 'form_submit' {filter_sql}
            GROUP BY form_identifier
            ORDER BY submissions DESC
            LIMIT ?
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params + [limit],
        )

        return [
            {
                "form_id": r["form_id"] or "",
                "form_name": r["form_name"] or "",
                "action": r["action"] or "",
                "method": r["method"] or "GET",
                "submissions": r["submissions"],
                "sessions": r["sessions"],
            }
            for r in results
        ]

    async def get_js_errors(
        self,
        start_date: date,
        end_date: date,
        limit: int = 20,
        filters: DashboardFilters | None = None,
    ) -> list[dict[str, Any]]:
        """Get JavaScript errors grouped by normalized message.

        Returns error events with message, source, count, and unique sessions.
        Errors are grouped by normalized message for similarity grouping.
        """
        filter_sql, filter_params = self._build_event_filter_sql(filters)

        results = await self._query(
            f"""
            SELECT
                json_extract(event_data, '$.normalized') as normalized_message,
                json_extract(event_data, '$.message') as message,
                json_extract(event_data, '$.source') as source,
                COUNT(*) as error_count,
                COUNT(DISTINCT session_id) as sessions,
                MAX(timestamp) as last_seen
            FROM events
            WHERE site = ? AND date(timestamp) >= ? AND date(timestamp) <= ?
                AND event_name = 'js_error' {filter_sql}
            GROUP BY normalized_message
            ORDER BY error_count DESC
            LIMIT ?
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()] + filter_params + [limit],
        )

        return [
            {
                "message": r["message"] or "Unknown error",
                "normalized": r["normalized_message"] or "",
                "source": r["source"] or "",
                "error_count": r["error_count"],
                "sessions": r["sessions"],
                "last_seen": r["last_seen"],
            }
            for r in results
        ]

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

        # Recent individual activity events for activity feed
        activity_rows = await self._query(
            """
            SELECT
                id,
                url,
                country,
                device_type,
                browser,
                timestamp
            FROM page_views
            WHERE site = ? AND timestamp >= ? AND is_bot = 0
            ORDER BY timestamp DESC
            LIMIT 20
            """,
            [self.site_name, cutoff],
        )

        recent_activity = [
            ActivityEvent(
                id=str(row.get("id", "")),
                event_type="pageview",
                page=row.get("url", ""),
                country=row.get("country"),
                device=row.get("device_type"),
                browser=row.get("browser"),
                timestamp=row.get("timestamp", ""),
            )
            for row in activity_rows
        ]

        return RealtimeData(
            active_visitors=visitors[0]["count"] if visitors else 0,
            active_sessions=sessions,
            pages=pages,
            countries=countries,
            sources=sources,
            recent_activity=recent_activity,
        )

    async def get_realtime_count(self) -> int:
        """Get count of human visitors in the last 5 minutes."""
        data = await self.get_realtime_data(minutes=5)
        return data.active_visitors

    async def get_activity_feed(
        self, minutes: int = 5, event_type: str | None = None
    ) -> tuple[int, list[ActivityEvent]]:
        """
        Get recent activity events for live feed.

        Returns tuple of (active_visitor_count, activity_events).
        Optionally filter by event_type.
        """
        cutoff = (datetime.utcnow() - timedelta(minutes=minutes)).isoformat()

        # Get visitor count
        visitors = await self._query(
            """
            SELECT COUNT(DISTINCT visitor_hash) as count
            FROM page_views
            WHERE site = ? AND timestamp >= ? AND is_bot = 0
            """,
            [self.site_name, cutoff],
        )

        # Get recent activity
        activity_rows = await self._query(
            """
            SELECT
                id,
                url,
                country,
                device_type,
                browser,
                timestamp
            FROM page_views
            WHERE site = ? AND timestamp >= ? AND is_bot = 0
            ORDER BY timestamp DESC
            LIMIT 20
            """,
            [self.site_name, cutoff],
        )

        recent_activity = [
            ActivityEvent(
                id=str(row.get("id", "")),
                event_type="pageview",
                page=row.get("url", ""),
                country=row.get("country"),
                device=row.get("device_type"),
                browser=row.get("browser"),
                timestamp=row.get("timestamp", ""),
            )
            for row in activity_rows
        ]

        # Filter by event type if specified
        if event_type and event_type != "all":
            recent_activity = [e for e in recent_activity if e.event_type == event_type]

        return (visitors[0]["count"] if visitors else 0, recent_activity)

    # =========================================================================
    # EXPORT
    # =========================================================================

    async def export_pageviews(
        self,
        start_date: date,
        end_date: date,
        limit: int = 10000,
        filters: DashboardFilters | None = None,
        include_bots: bool = False,
    ) -> list[dict[str, Any]]:
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
        filters: DashboardFilters | None = None,
    ) -> list[dict[str, Any]]:
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
        filters: DashboardFilters | None = None,
    ) -> list[dict[str, Any]]:
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
        filters: DashboardFilters | None = None,
    ) -> list[dict[str, Any]]:
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

    async def get_passkey_by_credential_id(self, credential_id: str) -> dict | None:
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
        passkey_id: int | None = None,
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

    async def validate_session(self, token_hash: str) -> dict | None:
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

    async def consume_challenge(self, challenge_type: str) -> str | None:
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

    # =========================================================================
    # FUNNEL ANALYSIS
    # =========================================================================

    async def get_funnels(self) -> list[FunnelDefinition]:
        """Get all funnels for this site."""
        result = await self._query(
            """
            SELECT id, site, name, description, steps, is_preset, created_at, updated_at
            FROM funnels
            WHERE site = ?
            ORDER BY is_preset DESC, name ASC
            """,
            [self.site_name],
        )

        funnels = []
        for row in result:
            steps = json.loads(row["steps"]) if row["steps"] else []
            funnels.append(FunnelDefinition(
                id=row["id"],
                site=row["site"],
                name=row["name"],
                description=row.get("description"),
                steps=[FunnelStep(**s) for s in steps],
                is_preset=bool(row.get("is_preset", 0)),
                created_at=datetime.fromisoformat(row["created_at"]) if row.get("created_at") else None,
                updated_at=datetime.fromisoformat(row["updated_at"]) if row.get("updated_at") else None,
            ))

        return funnels

    async def get_funnel(self, funnel_id: int) -> FunnelDefinition | None:
        """Get a single funnel by ID."""
        result = await self._query(
            """
            SELECT id, site, name, description, steps, is_preset, created_at, updated_at
            FROM funnels
            WHERE site = ? AND id = ?
            """,
            [self.site_name, funnel_id],
        )

        if not result:
            return None

        row = result[0]
        steps = json.loads(row["steps"]) if row["steps"] else []
        return FunnelDefinition(
            id=row["id"],
            site=row["site"],
            name=row["name"],
            description=row.get("description"),
            steps=[FunnelStep(**s) for s in steps],
            is_preset=bool(row.get("is_preset", 0)),
            created_at=datetime.fromisoformat(row["created_at"]) if row.get("created_at") else None,
            updated_at=datetime.fromisoformat(row["updated_at"]) if row.get("updated_at") else None,
        )

    async def create_funnel(self, funnel: FunnelDefinition) -> int:
        """Create a new funnel. Returns the new funnel ID."""
        steps_json = json.dumps([s.model_dump() for s in funnel.steps])
        await self._query(
            """
            INSERT INTO funnels (site, name, description, steps, is_preset)
            VALUES (?, ?, ?, ?, ?)
            """,
            [self.site_name, funnel.name, funnel.description, steps_json, int(funnel.is_preset)],
        )

        # Get the inserted ID
        result = await self._query(
            "SELECT id FROM funnels WHERE site = ? AND name = ?",
            [self.site_name, funnel.name],
        )
        return result[0]["id"] if result else 0

    async def delete_funnel(self, funnel_id: int) -> bool:
        """Delete a funnel. Returns True if deleted."""
        await self._query(
            "DELETE FROM funnels WHERE site = ? AND id = ? AND is_preset = 0",
            [self.site_name, funnel_id],
        )
        return True

    async def analyze_funnel(
        self,
        funnel: FunnelDefinition,
        start_date: date,
        end_date: date,
    ) -> FunnelResult:
        """
        Analyze a funnel for the given date range.

        This uses a sequential approach: for each step, we find visitors
        who completed that step AND all previous steps in order.
        """
        date_range = DateRange(start=start_date, end=end_date)
        start_str = start_date.isoformat()
        end_str = (end_date + timedelta(days=1)).isoformat()

        step_results: list[FunnelStepResult] = []
        previous_visitors: set[str] | None = None

        for i, step in enumerate(funnel.steps):
            step_num = i + 1
            label = step.label or step.value

            # Build query based on step type
            if step.type == "page":
                query = """
                    SELECT DISTINCT visitor_hash
                    FROM page_views
                    WHERE site = ?
                      AND timestamp >= ? AND timestamp < ?
                      AND url LIKE ?
                      AND is_bot = 0
                """
                params = [self.site_name, start_str, end_str, f"%{step.value}%"]
            else:  # event
                query = """
                    SELECT DISTINCT visitor_hash
                    FROM events
                    WHERE site = ?
                      AND timestamp >= ? AND timestamp < ?
                      AND event_name = ?
                """
                params = [self.site_name, start_str, end_str, step.value]

            result = await self._query(query, params)
            current_visitors = {row["visitor_hash"] for row in result}

            # For steps after the first, only count visitors who completed all previous steps
            if previous_visitors is not None:
                current_visitors = current_visitors.intersection(previous_visitors)

            visitors_count = len(current_visitors)

            # Calculate conversion from previous step
            if i == 0:
                conversion_rate = 100.0
                drop_off_rate = 0.0
                drop_off_count = 0
            else:
                prev_count = step_results[i - 1].visitors
                conversion_rate = (visitors_count / prev_count * 100) if prev_count > 0 else 0
                drop_off_rate = 100 - conversion_rate
                drop_off_count = prev_count - visitors_count

            step_results.append(FunnelStepResult(
                step_number=step_num,
                label=label,
                type=step.type,
                value=step.value,
                visitors=visitors_count,
                sessions=visitors_count,  # Simplified: treat visitors as sessions for now
                conversion_rate=round(conversion_rate, 1),
                drop_off_rate=round(drop_off_rate, 1),
                drop_off_count=drop_off_count,
            ))

            previous_visitors = current_visitors

        # Calculate overall metrics
        total_entered = step_results[0].visitors if step_results else 0
        total_converted = step_results[-1].visitors if step_results else 0
        overall_rate = (total_converted / total_entered * 100) if total_entered > 0 else 0

        return FunnelResult(
            funnel=funnel,
            date_range=date_range,
            steps=step_results,
            total_entered=total_entered,
            total_converted=total_converted,
            overall_conversion_rate=round(overall_rate, 1),
            avg_time_to_convert=None,  # TODO: Calculate from timestamps
        )

    async def ensure_preset_funnels(self) -> None:
        """Create preset funnels if they don't exist."""
        existing = await self.get_funnels()
        existing_names = {f.name for f in existing}

        presets = [
            FunnelDefinition(
                site=self.site_name,
                name="Landing to Signup",
                description="Track visitors from landing page to signup completion",
                steps=[
                    FunnelStep(type="page", value="/", label="Landing Page"),
                    FunnelStep(type="page", value="/signup", label="Signup Page"),
                    FunnelStep(type="event", value="signup_complete", label="Signup Complete"),
                ],
                is_preset=True,
            ),
            FunnelDefinition(
                site=self.site_name,
                name="Blog to Conversion",
                description="Track blog readers who convert",
                steps=[
                    FunnelStep(type="page", value="/blog", label="Blog"),
                    FunnelStep(type="page", value="/pricing", label="Pricing"),
                    FunnelStep(type="event", value="checkout_start", label="Start Checkout"),
                ],
                is_preset=True,
            ),
            FunnelDefinition(
                site=self.site_name,
                name="Product Journey",
                description="Track the product page to purchase flow",
                steps=[
                    FunnelStep(type="page", value="/products", label="Products"),
                    FunnelStep(type="page", value="/cart", label="Cart"),
                    FunnelStep(type="page", value="/checkout", label="Checkout"),
                    FunnelStep(type="event", value="purchase", label="Purchase"),
                ],
                is_preset=True,
            ),
        ]

        for preset in presets:
            if preset.name not in existing_names:
                await self.create_funnel(preset)

    # =========================================================================
    # GOAL TRACKING
    # =========================================================================

    async def get_goals(self, active_only: bool = True) -> list[GoalDefinition]:
        """Get all goals for this site."""
        query = """
            SELECT id, site, name, description, goal_type, goal_value,
                   target_count, is_active, created_at, updated_at
            FROM goals
            WHERE site = ?
        """
        params = [self.site_name]

        if active_only:
            query += " AND is_active = 1"

        query += " ORDER BY name ASC"

        result = await self._query(query, params)

        return [
            GoalDefinition(
                id=row["id"],
                site=row["site"],
                name=row["name"],
                description=row.get("description"),
                goal_type=row["goal_type"],
                goal_value=row["goal_value"],
                target_count=row.get("target_count"),
                is_active=bool(row.get("is_active", 1)),
                created_at=datetime.fromisoformat(row["created_at"]) if row.get("created_at") else None,
                updated_at=datetime.fromisoformat(row["updated_at"]) if row.get("updated_at") else None,
            )
            for row in result
        ]

    async def create_goal(self, goal: GoalDefinition) -> int:
        """Create a new goal. Returns the new goal ID."""
        await self._query(
            """
            INSERT INTO goals (site, name, description, goal_type, goal_value, target_count, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                self.site_name,
                goal.name,
                goal.description,
                goal.goal_type,
                goal.goal_value,
                goal.target_count,
                int(goal.is_active),
            ],
        )

        result = await self._query(
            "SELECT id FROM goals WHERE site = ? AND name = ?",
            [self.site_name, goal.name],
        )
        return result[0]["id"] if result else 0

    async def delete_goal(self, goal_id: int) -> bool:
        """Delete a goal."""
        await self._query(
            "DELETE FROM goals WHERE site = ? AND id = ?",
            [self.site_name, goal_id],
        )
        return True

    async def analyze_goal(
        self,
        goal: GoalDefinition,
        start_date: date,
        end_date: date,
    ) -> GoalResult:
        """Analyze goal completions for the given date range."""
        date_range = DateRange(start=start_date, end=end_date)
        start_str = start_date.isoformat()
        end_str = (end_date + timedelta(days=1)).isoformat()

        # Get completions based on goal type
        if goal.goal_type == "page":
            query = """
                SELECT
                    COUNT(*) as completions,
                    COUNT(DISTINCT visitor_hash) as unique_visitors
                FROM page_views
                WHERE site = ?
                  AND timestamp >= ? AND timestamp < ?
                  AND url LIKE ?
                  AND is_bot = 0
            """
            params = [self.site_name, start_str, end_str, f"%{goal.goal_value}%"]
        else:  # event
            query = """
                SELECT
                    COUNT(*) as completions,
                    COUNT(DISTINCT visitor_hash) as unique_visitors
                FROM events
                WHERE site = ?
                  AND timestamp >= ? AND timestamp < ?
                  AND event_name = ?
            """
            params = [self.site_name, start_str, end_str, goal.goal_value]

        result = await self._query(query, params)
        completions = result[0]["completions"] if result else 0
        unique_visitors = result[0]["unique_visitors"] if result else 0

        # Get total visitors for conversion rate
        total_query = """
            SELECT COUNT(DISTINCT visitor_hash) as total
            FROM page_views
            WHERE site = ? AND timestamp >= ? AND timestamp < ? AND is_bot = 0
        """
        total_result = await self._query(total_query, [self.site_name, start_str, end_str])
        total_visitors = total_result[0]["total"] if total_result else 0

        conversion_rate = (unique_visitors / total_visitors * 100) if total_visitors > 0 else 0

        # Get daily trend
        if goal.goal_type == "page":
            trend_query = """
                SELECT date(timestamp) as day, COUNT(*) as completions
                FROM page_views
                WHERE site = ? AND timestamp >= ? AND timestamp < ? AND url LIKE ? AND is_bot = 0
                GROUP BY date(timestamp)
                ORDER BY day
            """
            trend_params = [self.site_name, start_str, end_str, f"%{goal.goal_value}%"]
        else:
            trend_query = """
                SELECT date(timestamp) as day, COUNT(*) as completions
                FROM events
                WHERE site = ? AND timestamp >= ? AND timestamp < ? AND event_name = ?
                GROUP BY date(timestamp)
                ORDER BY day
            """
            trend_params = [self.site_name, start_str, end_str, goal.goal_value]

        trend_result = await self._query(trend_query, trend_params)
        trend = [{"date": row["day"], "completions": row["completions"]} for row in trend_result]

        return GoalResult(
            goal=goal,
            date_range=date_range,
            completions=completions,
            unique_visitors=unique_visitors,
            conversion_rate=round(conversion_rate, 2),
            trend=trend,
        )

    async def ensure_preset_goals(self) -> None:
        """Create default preset goals if none exist."""
        existing = await self.get_goals(active_only=False)
        if existing:
            return  # Already has goals

        preset_goals = [
            GoalDefinition(
                site=self.site_name,
                name="Contact Form Submitted",
                description="Track visitors who submit the contact form",
                goal_type="event",
                goal_value="form_submit",
                is_active=True,
            ),
            GoalDefinition(
                site=self.site_name,
                name="Pricing Page Viewed",
                description="Track visitors who view the pricing page",
                goal_type="page",
                goal_value="/pricing",
                is_active=True,
            ),
            GoalDefinition(
                site=self.site_name,
                name="Signup Completed",
                description="Track visitors who complete signup",
                goal_type="event",
                goal_value="signup",
                is_active=True,
            ),
            GoalDefinition(
                site=self.site_name,
                name="Blog Post Read",
                description="Track visitors who read blog content",
                goal_type="page",
                goal_value="/blog/",
                is_active=True,
            ),
        ]

        for goal in preset_goals:
            try:
                await self.create_goal(goal)
            except Exception:
                # Goal may already exist (race condition)
                pass

    # =========================================================================
    # Saved Views
    # =========================================================================

    async def get_saved_views(self) -> list["SavedView"]:
        """Get all saved views for this site."""
        from analytics_941.core.models import SavedView

        result = await self._query(
            """
            SELECT id, site, name, description, filters, date_preset,
                   is_default, is_shared, created_at, updated_at
            FROM saved_views
            WHERE site = ?
            ORDER BY is_default DESC, name ASC
            """,
            [self.site_name],
        )

        views = []
        for row in result:
            import json
            filters = json.loads(row["filters"]) if row["filters"] else {}
            views.append(SavedView(
                id=row["id"],
                site=row["site"],
                name=row["name"],
                description=row["description"],
                filters=filters,
                date_preset=row["date_preset"],
                is_default=bool(row["is_default"]),
                is_shared=bool(row["is_shared"]),
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            ))
        return views

    async def get_saved_view(self, view_id: int) -> "SavedView | None":
        """Get a specific saved view by ID."""
        from analytics_941.core.models import SavedView

        result = await self._query(
            """
            SELECT id, site, name, description, filters, date_preset,
                   is_default, is_shared, created_at, updated_at
            FROM saved_views
            WHERE id = ? AND site = ?
            """,
            [view_id, self.site_name],
        )

        if not result:
            return None

        row = result[0]
        import json
        filters = json.loads(row["filters"]) if row["filters"] else {}
        return SavedView(
            id=row["id"],
            site=row["site"],
            name=row["name"],
            description=row["description"],
            filters=filters,
            date_preset=row["date_preset"],
            is_default=bool(row["is_default"]),
            is_shared=bool(row["is_shared"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    async def get_default_view(self) -> "SavedView | None":
        """Get the default saved view for this site."""
        from analytics_941.core.models import SavedView

        result = await self._query(
            """
            SELECT id, site, name, description, filters, date_preset,
                   is_default, is_shared, created_at, updated_at
            FROM saved_views
            WHERE site = ? AND is_default = 1
            LIMIT 1
            """,
            [self.site_name],
        )

        if not result:
            return None

        row = result[0]
        import json
        filters = json.loads(row["filters"]) if row["filters"] else {}
        return SavedView(
            id=row["id"],
            site=row["site"],
            name=row["name"],
            description=row["description"],
            filters=filters,
            date_preset=row["date_preset"],
            is_default=True,
            is_shared=bool(row["is_shared"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    async def create_saved_view(self, view: "SavedView") -> int:
        """Create a new saved view and return its ID."""
        import json

        # If setting as default, clear other defaults first
        if view.is_default:
            await self._execute(
                "UPDATE saved_views SET is_default = 0 WHERE site = ?",
                [self.site_name],
            )

        result = await self._execute(
            """
            INSERT INTO saved_views (site, name, description, filters, date_preset, is_default, is_shared)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                self.site_name,
                view.name,
                view.description,
                json.dumps(view.filters),
                view.date_preset,
                1 if view.is_default else 0,
                1 if view.is_shared else 0,
            ],
        )
        return result.get("meta", {}).get("last_row_id", 0)

    async def update_saved_view(self, view_id: int, view: "SavedView") -> bool:
        """Update an existing saved view."""
        import json

        # If setting as default, clear other defaults first
        if view.is_default:
            await self._execute(
                "UPDATE saved_views SET is_default = 0 WHERE site = ? AND id != ?",
                [self.site_name, view_id],
            )

        result = await self._execute(
            """
            UPDATE saved_views
            SET name = ?, description = ?, filters = ?, date_preset = ?,
                is_default = ?, is_shared = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND site = ?
            """,
            [
                view.name,
                view.description,
                json.dumps(view.filters),
                view.date_preset,
                1 if view.is_default else 0,
                1 if view.is_shared else 0,
                view_id,
                self.site_name,
            ],
        )
        return result.get("meta", {}).get("changes", 0) > 0

    async def delete_saved_view(self, view_id: int) -> bool:
        """Delete a saved view."""
        result = await self._execute(
            "DELETE FROM saved_views WHERE id = ? AND site = ?",
            [view_id, self.site_name],
        )
        return result.get("meta", {}).get("changes", 0) > 0

    async def set_default_view(self, view_id: int) -> bool:
        """Set a view as the default, clearing any existing default."""
        # Clear existing default
        await self._execute(
            "UPDATE saved_views SET is_default = 0 WHERE site = ?",
            [self.site_name],
        )

        # Set new default
        result = await self._execute(
            "UPDATE saved_views SET is_default = 1 WHERE id = ? AND site = ?",
            [view_id, self.site_name],
        )
        return result.get("meta", {}).get("changes", 0) > 0
