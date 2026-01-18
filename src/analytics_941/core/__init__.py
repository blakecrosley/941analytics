"""
Core analytics module.

Contains the data models and client for querying analytics data.
"""

from .client import AnalyticsClient
from .models import (
    BrowserStats,
    CoreMetrics,
    CountryStats,
    DashboardData,
    DashboardFilters,
    DateRange,
    DeviceStats,
    Event,
    EventStats,
    GlobeData,
    MetricChange,
    PageStats,
    PageView,
    RealtimeData,
    Session,
    SourceStats,
    TimeSeriesPoint,
)

__all__ = [
    "PageView", "Session", "Event",
    "CoreMetrics", "MetricChange", "TimeSeriesPoint",
    "PageStats", "SourceStats", "CountryStats", "DeviceStats", "BrowserStats", "EventStats",
    "DashboardData", "DashboardFilters", "DateRange",
    "RealtimeData", "GlobeData",
    "AnalyticsClient",
]
