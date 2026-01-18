"""Tests for DashboardFilters model and query builders."""

import pytest

from analytics_941.core.client import AnalyticsClient
from analytics_941.core.models import DashboardFilters


class TestDashboardFilters:
    """Test DashboardFilters Pydantic model."""

    def test_empty_filters(self):
        """All filters None by default."""
        filters = DashboardFilters()
        assert filters.is_empty() is True
        assert filters.active_filters() == {}

    def test_single_filter(self):
        """Single filter is active."""
        filters = DashboardFilters(country="US")
        assert filters.is_empty() is False
        assert filters.active_filters() == {"country": "US"}

    def test_multiple_filters(self):
        """Multiple filters are active."""
        filters = DashboardFilters(
            country="US",
            device="mobile",
            browser="Chrome"
        )
        assert filters.is_empty() is False
        active = filters.active_filters()
        assert active["country"] == "US"
        assert active["device"] == "mobile"
        assert active["browser"] == "Chrome"
        assert len(active) == 3

    def test_all_fields_available(self):
        """All filter fields can be set."""
        filters = DashboardFilters(
            country="US",
            region="California",
            city="San Francisco",
            device="desktop",
            browser="Firefox",
            os="macOS",
            source="google.com",
            source_type="organic",
            page="/about",
            utm_source="newsletter",
            utm_medium="email",
            utm_campaign="spring_sale"
        )
        assert filters.is_empty() is False
        active = filters.active_filters()
        assert len(active) == 12


class TestFilterQueryBuilder:
    """Test _build_filter_sql and related methods.

    These tests verify SQL is generated correctly without
    actually executing against a database.
    """

    @pytest.fixture
    def client(self):
        """Create a client instance for testing query builders."""
        return AnalyticsClient(
            d1_database_id="test-db",
            cf_account_id="test-account",
            cf_api_token="test-token",
            site_name="test.com"
        )

    def test_empty_filters_returns_empty(self, client):
        """Empty filters return empty SQL and params."""
        sql, params = client._build_filter_sql(None)
        assert sql == ""
        assert params == []

        sql, params = client._build_filter_sql(DashboardFilters())
        assert sql == ""
        assert params == []

    def test_single_country_filter(self, client):
        """Single country filter generates correct SQL."""
        filters = DashboardFilters(country="US")
        sql, params = client._build_filter_sql(filters)
        assert "AND country = ?" in sql
        assert params == ["US"]

    def test_multiple_filters_anded(self, client):
        """Multiple filters are AND'd together."""
        filters = DashboardFilters(country="US", device="mobile")
        sql, params = client._build_filter_sql(filters)
        assert "AND country = ?" in sql
        assert "AND device_type = ?" in sql
        assert params == ["US", "mobile"]

    def test_technology_filters(self, client):
        """Browser, OS, device filters use correct column names."""
        filters = DashboardFilters(
            browser="Chrome",
            os="macOS",
            device="desktop"
        )
        sql, params = client._build_filter_sql(filters)
        assert "AND device_type = ?" in sql
        assert "AND browser = ?" in sql
        assert "AND os = ?" in sql
        assert "Chrome" in params
        assert "macOS" in params
        assert "desktop" in params

    def test_source_filters(self, client):
        """Source and source_type use correct column names."""
        filters = DashboardFilters(
            source="google.com",
            source_type="organic"
        )
        sql, params = client._build_filter_sql(filters)
        assert "AND referrer_type = ?" in sql
        assert "AND referrer_domain = ?" in sql
        assert "organic" in params
        assert "google.com" in params

    def test_utm_filters(self, client):
        """UTM filters work correctly."""
        filters = DashboardFilters(
            utm_source="newsletter",
            utm_medium="email",
            utm_campaign="spring"
        )
        sql, params = client._build_filter_sql(filters)
        assert "AND utm_source = ?" in sql
        assert "AND utm_medium = ?" in sql
        assert "AND utm_campaign = ?" in sql
        assert len(params) == 3

    def test_page_filter(self, client):
        """Page filter uses url column."""
        filters = DashboardFilters(page="/about")
        sql, params = client._build_filter_sql(filters)
        assert "AND url = ?" in sql
        assert params == ["/about"]

    def test_geographic_filters(self, client):
        """Country, region, city filters work."""
        filters = DashboardFilters(
            country="US",
            region="California",
            city="San Francisco"
        )
        sql, params = client._build_filter_sql(filters)
        assert "AND country = ?" in sql
        assert "AND region = ?" in sql
        assert "AND city = ?" in sql
        assert len(params) == 3


class TestSessionFilterQueryBuilder:
    """Test _build_session_filter_sql for sessions table."""

    @pytest.fixture
    def client(self):
        return AnalyticsClient(
            d1_database_id="test-db",
            cf_account_id="test-account",
            cf_api_token="test-token",
            site_name="test.com"
        )

    def test_empty_session_filters(self, client):
        """Empty filters return empty SQL."""
        sql, params = client._build_session_filter_sql(None)
        assert sql == ""
        assert params == []

    def test_session_country_filter(self, client):
        """Session table supports country filter."""
        filters = DashboardFilters(country="US")
        sql, params = client._build_session_filter_sql(filters)
        assert "AND country = ?" in sql
        assert params == ["US"]

    def test_session_device_filter(self, client):
        """Session table supports device filter."""
        filters = DashboardFilters(device="mobile")
        sql, params = client._build_session_filter_sql(filters)
        assert "AND device_type = ?" in sql
        assert params == ["mobile"]

    def test_session_ignores_page_filter(self, client):
        """Session table doesn't support page filter (no url column)."""
        filters = DashboardFilters(page="/about")
        sql, params = client._build_session_filter_sql(filters)
        # page filter should be ignored for sessions
        assert "url" not in sql
        assert params == []

    def test_session_ignores_city_filter(self, client):
        """Session table doesn't support city filter."""
        filters = DashboardFilters(city="San Francisco")
        sql, params = client._build_session_filter_sql(filters)
        assert "city" not in sql
        assert params == []


class TestEventFilterQueryBuilder:
    """Test _build_event_filter_sql for events table."""

    @pytest.fixture
    def client(self):
        return AnalyticsClient(
            d1_database_id="test-db",
            cf_account_id="test-account",
            cf_api_token="test-token",
            site_name="test.com"
        )

    def test_empty_event_filters(self, client):
        """Empty filters return empty SQL."""
        sql, params = client._build_event_filter_sql(None)
        assert sql == ""
        assert params == []

    def test_event_country_filter(self, client):
        """Events table supports country filter."""
        filters = DashboardFilters(country="US")
        sql, params = client._build_event_filter_sql(filters)
        assert "AND country = ?" in sql
        assert params == ["US"]

    def test_event_device_filter(self, client):
        """Events table supports device filter."""
        filters = DashboardFilters(device="mobile")
        sql, params = client._build_event_filter_sql(filters)
        assert "AND device_type = ?" in sql

    def test_event_page_filter(self, client):
        """Events table uses page_url column for page filter."""
        filters = DashboardFilters(page="/about")
        sql, params = client._build_event_filter_sql(filters)
        assert "AND page_url = ?" in sql
        assert params == ["/about"]

    def test_event_ignores_browser_filter(self, client):
        """Events table doesn't support browser filter."""
        filters = DashboardFilters(browser="Chrome")
        sql, params = client._build_event_filter_sql(filters)
        assert "browser" not in sql
        assert params == []


class TestSQLInjectionPrevention:
    """Verify filters use parameterized queries to prevent SQL injection."""

    @pytest.fixture
    def client(self):
        return AnalyticsClient(
            d1_database_id="test-db",
            cf_account_id="test-account",
            cf_api_token="test-token",
            site_name="test.com"
        )

    def test_malicious_country_is_parameterized(self, client):
        """Malicious input in country is parameterized, not interpolated."""
        filters = DashboardFilters(country="US'; DROP TABLE page_views; --")
        sql, params = client._build_filter_sql(filters)

        # The malicious input should be in params, not in SQL string
        assert "DROP TABLE" not in sql
        assert "US'; DROP TABLE page_views; --" in params

    def test_malicious_page_is_parameterized(self, client):
        """Malicious input in page is parameterized."""
        filters = DashboardFilters(page="/page?id=1 OR 1=1")
        sql, params = client._build_filter_sql(filters)

        assert "OR 1=1" not in sql
        assert "/page?id=1 OR 1=1" in params

    def test_session_filters_use_params(self, client):
        """Session filter builder uses parameterized queries."""
        filters = DashboardFilters(country="US'; --")
        sql, params = client._build_session_filter_sql(filters)

        # Should use ? placeholder, not string interpolation
        assert "?" in sql
        assert "US'; --" in params
        assert "US'; --" not in sql

    def test_event_filters_use_params(self, client):
        """Event filter builder uses parameterized queries."""
        filters = DashboardFilters(country="US'; --")
        sql, params = client._build_event_filter_sql(filters)

        assert "?" in sql
        assert "US'; --" in params


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
