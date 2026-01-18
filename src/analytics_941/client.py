"""HTTP client for querying Cloudflare D1 analytics database."""

import json
from datetime import date, datetime, timedelta

import httpx

from .models import DailyStats, DashboardData


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

        # Regions (states) - include average lat/lon
        regions = await self._query(
            f"""
            SELECT
                country,
                region,
                COUNT(*) as views,
                AVG(latitude) as lat,
                AVG(longitude) as lon
            FROM page_views
            WHERE site = ? AND date(timestamp) >= ? AND region != '' AND region IS NOT NULL {bot_filter}
            GROUP BY country, region
            ORDER BY views DESC
            LIMIT 20
            """,
            [self.site_name, start_str],
        )

        # Cities - include average lat/lon from Cloudflare/MaxMind geolocation
        cities = await self._query(
            f"""
            SELECT
                country,
                region,
                city,
                COUNT(*) as views,
                AVG(latitude) as lat,
                AVG(longitude) as lon
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
        self, campaign: str, start_date: date | None = None, end_date: date | None = None
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

    # =========================================================================
    # AGGREGATED DATA QUERIES (Fast historical queries from daily_stats)
    # =========================================================================

    async def get_daily_stats(
        self, start_date: date, end_date: date
    ) -> list[DailyStats]:
        """
        Get aggregated daily stats from the daily_stats table.

        This is much faster than querying raw page_views for historical data.
        Note: daily_stats is populated by the nightly aggregation job.
        """
        results = await self._query(
            """
            SELECT * FROM daily_stats
            WHERE site = ? AND date >= ? AND date <= ?
            ORDER BY date ASC
            """,
            [self.site_name, start_date.isoformat(), end_date.isoformat()],
        )

        stats = []
        for row in results:
            stats.append(DailyStats(
                date=date.fromisoformat(row["date"]),
                site=row["site"],
                total_views=row["total_views"],
                unique_visitors=row["unique_visitors"],
                bot_views=row["bot_views"],
                top_pages=json.loads(row["top_pages"]) if row["top_pages"] else [],
                top_referrers=json.loads(row["top_referrers"]) if row["top_referrers"] else [],
                countries=json.loads(row["countries"]) if row["countries"] else {},
                devices=json.loads(row["devices"]) if row["devices"] else {},
                browsers=json.loads(row["browsers"]) if row["browsers"] else {},
                operating_systems=json.loads(row["operating_systems"]) if row["operating_systems"] else {},
                referrer_types=json.loads(row["referrer_types"]) if row["referrer_types"] else {},
                utm_sources=json.loads(row["utm_sources"]) if row["utm_sources"] else {},
                utm_campaigns=json.loads(row["utm_campaigns"]) if row["utm_campaigns"] else {},
                bot_breakdown=json.loads(row["bot_breakdown"]) if row["bot_breakdown"] else {},
            ))

        return stats

    async def get_dashboard_data_fast(
        self, period: str = "7d", include_bots: bool = False
    ) -> DashboardData:
        """
        Get dashboard data using aggregated daily_stats for historical data.

        This is faster than get_dashboard_data() for 7d/30d periods because it
        reads from pre-aggregated daily_stats instead of scanning all page_views.

        For "today", it falls back to raw page_views for real-time data.

        Args:
            period: Time period - 'today', '7d', or '30d'
            include_bots: If False (default), human traffic only.
        """
        today = date.today()

        if period == "today":
            # For today, use real-time raw data
            return await self.get_dashboard_data(period, include_bots)

        # Calculate date range
        if period == "7d":
            start_date = today - timedelta(days=7)
        elif period == "30d":
            start_date = today - timedelta(days=30)
        else:
            start_date = today - timedelta(days=7)

        # Get aggregated historical data (excludes today)
        yesterday = today - timedelta(days=1)
        daily_stats = await self.get_daily_stats(start_date, yesterday)

        # Get today's real-time data
        today_data = await self.get_dashboard_data("today", include_bots)

        # Combine historical aggregates with today
        total_views = sum(s.total_views for s in daily_stats) + today_data.total_views
        # Note: unique_visitors across days cannot be simply summed (visitors may repeat)
        # For now, we sum as an approximation; true unique would need raw data
        unique_visitors = sum(s.unique_visitors for s in daily_stats) + today_data.unique_visitors
        bot_views = sum(s.bot_views for s in daily_stats) + today_data.bot_views

        # Build views_by_day from daily_stats + today
        views_by_day = [
            {"day": s.date.isoformat(), "views": s.total_views, "visitors": s.unique_visitors}
            for s in daily_stats
        ]
        if today_data.views_by_day:
            views_by_day.extend(today_data.views_by_day)

        # Merge top pages (combine counts)
        pages_count: dict[str, int] = {}
        for s in daily_stats:
            for page in s.top_pages:
                url = page.get("url", "")
                pages_count[url] = pages_count.get(url, 0) + page.get("views", 0)
        for page in today_data.top_pages:
            url = page.get("url", "")
            pages_count[url] = pages_count.get(url, 0) + page.get("views", 0)
        top_pages = [{"url": k, "views": v} for k, v in sorted(pages_count.items(), key=lambda x: -x[1])[:10]]

        # Merge top referrers
        referrer_count: dict[str, dict] = {}
        for s in daily_stats:
            for ref in s.top_referrers:
                domain = ref.get("domain", "")
                if domain not in referrer_count:
                    referrer_count[domain] = {"domain": domain, "type": ref.get("type", ""), "views": 0}
                referrer_count[domain]["views"] += ref.get("views", 0)
        for ref in today_data.top_referrers:
            domain = ref.get("domain", "")
            if domain not in referrer_count:
                referrer_count[domain] = {"domain": domain, "type": ref.get("type", ""), "views": 0}
            referrer_count[domain]["views"] += ref.get("views", 0)
        top_referrers = sorted(referrer_count.values(), key=lambda x: -x["views"])[:10]

        # Merge dict-based aggregates
        def merge_dicts(dicts: list[dict[str, int]]) -> dict[str, int]:
            result: dict[str, int] = {}
            for d in dicts:
                for k, v in d.items():
                    result[k] = result.get(k, 0) + v
            return result

        referrer_types = merge_dicts([s.referrer_types for s in daily_stats] + [today_data.referrer_types])
        devices = merge_dicts([s.devices for s in daily_stats] + [today_data.devices])
        browsers = merge_dicts([s.browsers for s in daily_stats] + [today_data.browsers])
        operating_systems = merge_dicts([s.operating_systems for s in daily_stats] + [today_data.operating_systems])
        bot_breakdown = merge_dicts([s.bot_breakdown for s in daily_stats] + [today_data.bot_breakdown])

        # Merge UTM data
        utm_source_count: dict[str, int] = {}
        for s in daily_stats:
            for k, v in s.utm_sources.items():
                utm_source_count[k] = utm_source_count.get(k, 0) + v
        for item in today_data.utm_sources:
            k = item.get("source", "")
            utm_source_count[k] = utm_source_count.get(k, 0) + item.get("views", 0)
        utm_sources = [{"source": k, "medium": "", "views": v} for k, v in sorted(utm_source_count.items(), key=lambda x: -x[1])[:10]]

        utm_campaign_count: dict[str, int] = {}
        for s in daily_stats:
            for k, v in s.utm_campaigns.items():
                utm_campaign_count[k] = utm_campaign_count.get(k, 0) + v
        for item in today_data.utm_campaigns:
            k = item.get("campaign", "")
            utm_campaign_count[k] = utm_campaign_count.get(k, 0) + item.get("views", 0)
        utm_campaigns = [{"campaign": k, "source": "", "views": v} for k, v in sorted(utm_campaign_count.items(), key=lambda x: -x[1])[:10]]

        # Merge countries (convert from dict to list format)
        country_count: dict[str, int] = {}
        for s in daily_stats:
            for k, v in s.countries.items():
                country_count[k] = country_count.get(k, 0) + v
        for item in today_data.countries:
            k = item.get("country", "")
            country_count[k] = country_count.get(k, 0) + item.get("views", 0)
        countries = [{"country": k, "views": v} for k, v in sorted(country_count.items(), key=lambda x: -x[1])[:10]]

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
            regions=today_data.regions,  # Regions not in daily_stats
            cities=today_data.cities,  # Cities not in daily_stats
            devices=devices,
            browsers=browsers,
            operating_systems=operating_systems,
            bot_breakdown=bot_breakdown,
        )

    async def has_aggregated_data(self, start_date: date) -> bool:
        """Check if we have aggregated data for the date range."""
        result = await self._query(
            """
            SELECT COUNT(*) as count FROM daily_stats
            WHERE site = ? AND date >= ?
            """,
            [self.site_name, start_date.isoformat()],
        )
        return result[0]["count"] > 0 if result else False

    # =========================================================================
    # AUTHENTICATION (WebAuthn Passkeys)
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
        # Get the ID of the inserted row
        result = await self._query(
            "SELECT id FROM passkeys WHERE site = ? AND credential_id = ?",
            [self.site_name, credential_id],
        )
        return result[0]["id"] if result else 0

    async def update_passkey_sign_count(
        self, passkey_id: int, sign_count: int
    ) -> None:
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
        # First check how many passkeys exist
        count_result = await self._query(
            "SELECT COUNT(*) as count FROM passkeys WHERE site = ?",
            [self.site_name],
        )
        count = count_result[0]["count"] if count_result else 0

        if count <= 1:
            return False  # Cannot delete last passkey

        await self._query(
            "DELETE FROM passkeys WHERE id = ? AND site = ?",
            [passkey_id, self.site_name],
        )
        return True

    # -------------------------------------------------------------------------
    # Session Management
    # -------------------------------------------------------------------------

    async def create_session(
        self,
        token_hash: str,
        passkey_id: int | None = None,
        user_agent: str = "",
        ip_address: str = "",
        expires_hours: int = 168,  # 7 days
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

    async def cleanup_expired_sessions(self) -> int:
        """Remove expired sessions. Returns count deleted."""
        # D1 doesn't return rowcount easily, just execute
        await self._query(
            "DELETE FROM auth_sessions WHERE site = ? AND expires_at <= datetime('now')",
            [self.site_name],
        )
        return 0  # D1 doesn't return affected row count

    # -------------------------------------------------------------------------
    # WebAuthn Challenge Management
    # -------------------------------------------------------------------------

    async def store_challenge(self, challenge: str, challenge_type: str) -> None:
        """Store a WebAuthn challenge (expires in 5 minutes)."""
        # Clean up expired challenges first
        await self._query(
            "DELETE FROM webauthn_challenges WHERE site = ? AND expires_at <= datetime('now')",
            [self.site_name],
        )
        # Store new challenge
        await self._query(
            """
            INSERT INTO webauthn_challenges (site, challenge, challenge_type, expires_at)
            VALUES (?, ?, ?, datetime('now', '+5 minutes'))
            """,
            [self.site_name, challenge, challenge_type],
        )

    async def consume_challenge(self, challenge_type: str) -> str | None:
        """Get and delete the most recent valid challenge. Returns challenge string or None."""
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

        # Delete the consumed challenge
        await self._query(
            "DELETE FROM webauthn_challenges WHERE id = ?",
            [challenge_id],
        )
        return challenge
