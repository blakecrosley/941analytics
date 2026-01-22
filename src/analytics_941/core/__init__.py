"""
Core analytics module.

Contains the data models and client for querying analytics data.
"""

from .client import AnalyticsClient
from .models import (
    BreakpointStats,
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
    LanguageStats,
    MetricChange,
    PageStats,
    PageView,
    RealtimeData,
    ScreenSizeStats,
    Session,
    SourceStats,
    TimeSeriesPoint,
)

__all__ = [
    "PageView", "Session", "Event",
    "CoreMetrics", "MetricChange", "TimeSeriesPoint",
    "PageStats", "SourceStats", "CountryStats", "DeviceStats", "BrowserStats", "EventStats",
    "ScreenSizeStats", "BreakpointStats", "LanguageStats",
    "DashboardData", "DashboardFilters", "DateRange",
    "RealtimeData", "GlobeData",
    "AnalyticsClient",
]
