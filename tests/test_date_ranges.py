"""Tests for custom date range support in dashboard routes."""

from datetime import date, timedelta

import pytest
from fastapi import HTTPException

# Import the _parse_date_range function by importing the module
# Note: Since _parse_date_range is inside create_dashboard_router,
# we'll test it indirectly or create a testable version
from analytics_941.routes.dashboard import _parse_date_range


class TestPresetDateRanges:
    """Test preset period parsing."""

    def test_24h_period(self):
        """24h preset returns today and yesterday."""
        start, end, compare_start, compare_end = _parse_date_range("24h")
        today = date.today()

        assert end == today
        assert start == today - timedelta(days=1)
        # Comparison is previous day
        assert compare_end == start
        assert compare_start == compare_end - timedelta(days=1)

    def test_7d_period(self):
        """7d preset returns last 7 days."""
        start, end, compare_start, compare_end = _parse_date_range("7d")
        today = date.today()

        assert end == today
        assert start == today - timedelta(days=7)
        assert compare_end == start
        assert compare_start == compare_end - timedelta(days=7)

    def test_30d_period(self):
        """30d preset returns last 30 days."""
        start, end, compare_start, compare_end = _parse_date_range("30d")
        today = date.today()

        assert end == today
        assert start == today - timedelta(days=30)
        assert compare_end == start
        assert compare_start == compare_end - timedelta(days=30)

    def test_90d_period(self):
        """90d preset returns last 90 days."""
        start, end, compare_start, compare_end = _parse_date_range("90d")
        today = date.today()

        assert end == today
        assert start == today - timedelta(days=90)

    def test_year_period(self):
        """year preset returns last 365 days."""
        start, end, compare_start, compare_end = _parse_date_range("year")
        today = date.today()

        assert end == today
        assert start == today - timedelta(days=365)

    def test_all_period_no_comparison(self):
        """all preset has no comparison period."""
        start, end, compare_start, compare_end = _parse_date_range("all")

        assert compare_start is None
        assert compare_end is None

    def test_invalid_period_defaults_to_30d(self):
        """Invalid period defaults to 30d."""
        start, end, compare_start, compare_end = _parse_date_range("invalid")
        today = date.today()

        assert end == today
        assert start == today - timedelta(days=30)


class TestCustomDateRanges:
    """Test custom date range parsing."""

    def test_custom_date_range(self):
        """Custom start and end dates are parsed correctly."""
        start, end, compare_start, compare_end = _parse_date_range(
            "custom", "2024-01-01", "2024-01-31"
        )

        assert start == date(2024, 1, 1)
        assert end == date(2024, 1, 31)

    def test_custom_dates_override_period(self):
        """Custom dates take precedence even with period set."""
        start, end, _, _ = _parse_date_range(
            "30d", "2024-06-01", "2024-06-15"
        )

        assert start == date(2024, 6, 1)
        assert end == date(2024, 6, 15)

    def test_comparison_period_calculated_automatically(self):
        """Comparison period is same duration, immediately prior."""
        # 10-day range: Jan 11-20
        start, end, compare_start, compare_end = _parse_date_range(
            "custom", "2024-01-11", "2024-01-20"
        )

        # Comparison should be Jan 1-10 (10 days prior)
        assert compare_end == date(2024, 1, 10)  # Day before start
        assert compare_start == date(2024, 1, 1)  # 10 days

    def test_single_day_range(self):
        """Single day range works correctly."""
        start, end, compare_start, compare_end = _parse_date_range(
            "custom", "2024-03-15", "2024-03-15"
        )

        assert start == end == date(2024, 3, 15)
        # Compare to single day before
        assert compare_end == date(2024, 3, 14)
        assert compare_start == date(2024, 3, 14)


class TestCustomDateValidation:
    """Test validation of custom date inputs."""

    def test_missing_start_date_raises_400(self):
        """Missing start date raises HTTPException."""
        with pytest.raises(HTTPException) as exc_info:
            _parse_date_range("custom", None, "2024-01-31")

        assert exc_info.value.status_code == 400
        assert "Both start and end dates are required" in exc_info.value.detail

    def test_missing_end_date_raises_400(self):
        """Missing end date raises HTTPException."""
        with pytest.raises(HTTPException) as exc_info:
            _parse_date_range("custom", "2024-01-01", None)

        assert exc_info.value.status_code == 400
        assert "Both start and end dates are required" in exc_info.value.detail

    def test_invalid_date_format_raises_400(self):
        """Invalid date format raises HTTPException."""
        with pytest.raises(HTTPException) as exc_info:
            _parse_date_range("custom", "01-01-2024", "2024-01-31")

        assert exc_info.value.status_code == 400
        assert "Invalid date format" in exc_info.value.detail
        assert "YYYY-MM-DD" in exc_info.value.detail

    def test_invalid_end_date_format_raises_400(self):
        """Invalid end date format raises HTTPException."""
        with pytest.raises(HTTPException) as exc_info:
            _parse_date_range("custom", "2024-01-01", "January 31")

        assert exc_info.value.status_code == 400
        assert "Invalid date format" in exc_info.value.detail

    def test_end_before_start_raises_400(self):
        """End date before start date raises HTTPException."""
        with pytest.raises(HTTPException) as exc_info:
            _parse_date_range("custom", "2024-01-31", "2024-01-01")

        assert exc_info.value.status_code == 400
        assert "End date must be on or after start date" in exc_info.value.detail

    def test_future_end_date_raises_400(self):
        """Future end date raises HTTPException."""
        future = (date.today() + timedelta(days=30)).isoformat()

        with pytest.raises(HTTPException) as exc_info:
            _parse_date_range("custom", "2024-01-01", future)

        assert exc_info.value.status_code == 400
        assert "cannot be in the future" in exc_info.value.detail


class TestDateRangeEdgeCases:
    """Test edge cases for date range handling."""

    def test_same_start_and_end_date(self):
        """Same start and end date is valid (single day)."""
        start, end, _, _ = _parse_date_range(
            "custom", "2024-06-15", "2024-06-15"
        )
        assert start == end

    def test_today_as_end_date(self):
        """Today as end date is valid."""
        today = date.today().isoformat()
        start, end, _, _ = _parse_date_range(
            "custom", "2024-01-01", today
        )
        assert end == date.today()

    def test_very_old_start_date(self):
        """Very old start date is valid (retention warning would be shown by UI)."""
        start, end, _, _ = _parse_date_range(
            "custom", "2020-01-01", "2020-12-31"
        )
        assert start == date(2020, 1, 1)
        assert end == date(2020, 12, 31)

    def test_leap_year_date(self):
        """Leap year date is handled correctly."""
        start, end, _, _ = _parse_date_range(
            "custom", "2024-02-29", "2024-03-01"
        )
        assert start == date(2024, 2, 29)
        assert end == date(2024, 3, 1)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
