"""Tests for session metrics client methods."""

import asyncio
from datetime import date
from unittest.mock import AsyncMock

from analytics_941.core.models import DashboardFilters, MetricChange
from analytics_941.core.client import AnalyticsClient


def run_async(coro):
    """Helper to run async functions in sync tests."""
    return asyncio.get_event_loop().run_until_complete(coro)


class TestGetBounceRate:
    """Test get_bounce_rate method."""

    def _get_client(self):
        return AnalyticsClient(
            d1_database_id="test-db",
            cf_account_id="test-account",
            cf_api_token="test-token",
            site_name="test.com"
        )

    def test_returns_float_percentage(self):
        """Bounce rate returns float 0-100."""
        client = self._get_client()
        client._query = AsyncMock(return_value=[{"bounce_rate": 45.5}])

        result = run_async(client.get_bounce_rate(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 7)
        ))

        assert isinstance(result, MetricChange)
        assert result.value == 45.5
        assert 0 <= result.value <= 100

    def test_handles_zero_data_gracefully(self):
        """Returns 0 when no session data exists."""
        client = self._get_client()
        # D1 returns None for AVG on empty set
        client._query = AsyncMock(return_value=[{"bounce_rate": None}])

        result = run_async(client.get_bounce_rate(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 7)
        ))

        assert result.value == 0
        assert result.previous is None

    def test_handles_empty_result(self):
        """Returns 0 when query returns empty list."""
        client = self._get_client()
        client._query = AsyncMock(return_value=[])

        result = run_async(client.get_bounce_rate(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 7)
        ))

        assert result.value == 0

    def test_comparison_period_calculates_change(self):
        """Comparison period calculates trend correctly."""
        client = self._get_client()
        # First call: current period, second call: comparison period
        client._query = AsyncMock(side_effect=[
            [{"bounce_rate": 40.0}],  # Current: 40%
            [{"bounce_rate": 50.0}],  # Previous: 50%
        ])

        result = run_async(client.get_bounce_rate(
            start_date=date(2026, 1, 8),
            end_date=date(2026, 1, 14),
            compare_start=date(2026, 1, 1),
            compare_end=date(2026, 1, 7)
        ))

        assert result.value == 40.0
        assert result.previous == 50.0
        assert result.change_direction == "down"  # Bounce rate decreased (good)
        assert result.change_percent == 20.0

    def test_respects_filters(self):
        """Filters are applied to query."""
        client = self._get_client()
        client._query = AsyncMock(return_value=[{"bounce_rate": 35.0}])
        filters = DashboardFilters(country="US", device="mobile")

        run_async(client.get_bounce_rate(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 7),
            filters=filters
        ))

        # Verify query was called with filter params
        call_args = client._query.call_args
        params = call_args[0][1]  # Second positional arg is params
        assert "US" in params
        assert "mobile" in params


class TestGetAvgSessionDuration:
    """Test get_avg_session_duration method."""

    def _get_client(self):
        return AnalyticsClient(
            d1_database_id="test-db",
            cf_account_id="test-account",
            cf_api_token="test-token",
            site_name="test.com"
        )

    def test_returns_seconds(self):
        """Duration returns integer seconds."""
        client = self._get_client()
        client._query = AsyncMock(return_value=[{"avg_duration": 185.7}])

        result = run_async(client.get_avg_session_duration(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 7)
        ))

        assert isinstance(result, MetricChange)
        assert result.value == 186  # Rounded to int

    def test_handles_zero_data_gracefully(self):
        """Returns 0 when no completed sessions exist."""
        client = self._get_client()
        client._query = AsyncMock(return_value=[{"avg_duration": None}])

        result = run_async(client.get_avg_session_duration(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 7)
        ))

        assert result.value == 0

    def test_comparison_period_trend(self):
        """Comparison period shows duration trend."""
        client = self._get_client()
        client._query = AsyncMock(side_effect=[
            [{"avg_duration": 200}],  # Current: 200s
            [{"avg_duration": 150}],  # Previous: 150s
        ])

        result = run_async(client.get_avg_session_duration(
            start_date=date(2026, 1, 8),
            end_date=date(2026, 1, 14),
            compare_start=date(2026, 1, 1),
            compare_end=date(2026, 1, 7)
        ))

        assert result.value == 200
        assert result.previous == 150
        assert result.change_direction == "up"  # Duration increased (good)


class TestGetSessionsCount:
    """Test get_sessions_count method."""

    def _get_client(self):
        return AnalyticsClient(
            d1_database_id="test-db",
            cf_account_id="test-account",
            cf_api_token="test-token",
            site_name="test.com"
        )

    def test_returns_integer(self):
        """Sessions count returns whole number (stored as float in MetricChange)."""
        client = self._get_client()
        client._query = AsyncMock(return_value=[{"session_count": 1250}])

        result = run_async(client.get_sessions_count(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 7)
        ))

        assert isinstance(result, MetricChange)
        assert result.value == 1250
        assert result.value == int(result.value)  # Whole number (no decimal)

    def test_handles_zero_sessions(self):
        """Returns 0 when no sessions exist."""
        client = self._get_client()
        client._query = AsyncMock(return_value=[{"session_count": 0}])

        result = run_async(client.get_sessions_count(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 7)
        ))

        assert result.value == 0

    def test_handles_null_result(self):
        """Returns 0 when result is null."""
        client = self._get_client()
        client._query = AsyncMock(return_value=[{"session_count": None}])

        result = run_async(client.get_sessions_count(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 7)
        ))

        assert result.value == 0

    def test_comparison_calculates_change_percent(self):
        """Comparison period shows percentage change."""
        client = self._get_client()
        client._query = AsyncMock(side_effect=[
            [{"session_count": 1000}],  # Current
            [{"session_count": 800}],   # Previous
        ])

        result = run_async(client.get_sessions_count(
            start_date=date(2026, 1, 8),
            end_date=date(2026, 1, 14),
            compare_start=date(2026, 1, 1),
            compare_end=date(2026, 1, 7)
        ))

        assert result.value == 1000
        assert result.previous == 800
        assert result.change_direction == "up"
        assert result.change_percent == 25.0  # (1000-800)/800 * 100


class TestGetPagesPerSession:
    """Test get_pages_per_session method."""

    def _get_client(self):
        return AnalyticsClient(
            d1_database_id="test-db",
            cf_account_id="test-account",
            cf_api_token="test-token",
            site_name="test.com"
        )

    def test_returns_float(self):
        """Pages per session returns float."""
        client = self._get_client()
        client._query = AsyncMock(return_value=[{"pages_per_session": 3.5}])

        result = run_async(client.get_pages_per_session(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 7)
        ))

        assert isinstance(result, MetricChange)
        assert result.value == 3.5

    def test_handles_zero_data_gracefully(self):
        """Returns 0 when no sessions exist."""
        client = self._get_client()
        client._query = AsyncMock(return_value=[{"pages_per_session": None}])

        result = run_async(client.get_pages_per_session(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 7)
        ))

        assert result.value == 0

    def test_rounds_to_one_decimal(self):
        """Value is rounded to 1 decimal place."""
        client = self._get_client()
        client._query = AsyncMock(return_value=[{"pages_per_session": 2.666666}])

        result = run_async(client.get_pages_per_session(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 7)
        ))

        assert result.value == 2.7

    def test_comparison_period(self):
        """Comparison period calculates trend."""
        client = self._get_client()
        client._query = AsyncMock(side_effect=[
            [{"pages_per_session": 4.0}],
            [{"pages_per_session": 3.0}],
        ])

        result = run_async(client.get_pages_per_session(
            start_date=date(2026, 1, 8),
            end_date=date(2026, 1, 14),
            compare_start=date(2026, 1, 1),
            compare_end=date(2026, 1, 7)
        ))

        assert result.value == 4.0
        assert result.previous == 3.0
        assert result.change_direction == "up"


class TestMetricChangeCalculation:
    """Test the _metric_with_change helper for various scenarios."""

    def _get_client(self):
        return AnalyticsClient(
            d1_database_id="test-db",
            cf_account_id="test-account",
            cf_api_token="test-token",
            site_name="test.com"
        )

    def test_no_previous_data(self):
        """No comparison period returns value only."""
        client = self._get_client()
        result = client._metric_with_change(100, None)
        assert result.value == 100
        assert result.previous is None
        assert result.change_percent is None
        assert result.change_direction is None

    def test_previous_zero_current_positive(self):
        """Going from 0 to positive shows 100% increase."""
        client = self._get_client()
        result = client._metric_with_change(50, 0)
        assert result.value == 50
        assert result.previous == 0
        assert result.change_percent == 100.0
        assert result.change_direction == "up"

    def test_previous_zero_current_zero(self):
        """Going from 0 to 0 shows 0% change."""
        client = self._get_client()
        result = client._metric_with_change(0, 0)
        assert result.value == 0
        assert result.change_percent == 0.0
        assert result.change_direction == "same"

    def test_increase_calculates_correctly(self):
        """Increase shows positive change."""
        client = self._get_client()
        result = client._metric_with_change(120, 100)
        assert result.change_percent == 20.0
        assert result.change_direction == "up"

    def test_decrease_calculates_correctly(self):
        """Decrease shows negative change (absolute value)."""
        client = self._get_client()
        result = client._metric_with_change(80, 100)
        assert result.change_percent == 20.0  # Absolute value
        assert result.change_direction == "down"

    def test_no_change(self):
        """Same value shows same direction."""
        client = self._get_client()
        result = client._metric_with_change(100, 100)
        assert result.change_percent == 0.0
        assert result.change_direction == "same"


class TestSessionFiltersApplied:
    """Verify filters are correctly passed to session metric queries."""

    def _get_client(self):
        return AnalyticsClient(
            d1_database_id="test-db",
            cf_account_id="test-account",
            cf_api_token="test-token",
            site_name="test.com"
        )

    def test_country_filter_in_bounce_rate(self):
        """Country filter applied to bounce rate query."""
        client = self._get_client()
        client._query = AsyncMock(return_value=[{"bounce_rate": 50.0}])
        filters = DashboardFilters(country="DE")

        run_async(client.get_bounce_rate(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 7),
            filters=filters
        ))

        call_sql = client._query.call_args[0][0]
        call_params = client._query.call_args[0][1]
        assert "AND country = ?" in call_sql
        assert "DE" in call_params

    def test_device_filter_in_duration(self):
        """Device filter applied to duration query."""
        client = self._get_client()
        client._query = AsyncMock(return_value=[{"avg_duration": 120}])
        filters = DashboardFilters(device="mobile")

        run_async(client.get_avg_session_duration(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 7),
            filters=filters
        ))

        call_sql = client._query.call_args[0][0]
        call_params = client._query.call_args[0][1]
        assert "AND device_type = ?" in call_sql
        assert "mobile" in call_params

    def test_multiple_filters_combined(self):
        """Multiple filters AND'd in query."""
        client = self._get_client()
        client._query = AsyncMock(return_value=[{"session_count": 100}])
        filters = DashboardFilters(
            country="US",
            device="desktop",
            browser="Chrome"
        )

        run_async(client.get_sessions_count(
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 7),
            filters=filters
        ))

        call_params = client._query.call_args[0][1]
        assert "US" in call_params
        assert "desktop" in call_params
        assert "Chrome" in call_params


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
