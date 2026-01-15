"""
Core analytics module.

Contains the data models and client for querying analytics data.
"""

from .models import (
    PageView, Session, Event,
    CoreMetrics, MetricChange, TimeSeriesPoint,
    PageStats, SourceStats, CountryStats, DeviceStats, BrowserStats, EventStats,
    DashboardData, DashboardFilters, DateRange,
    RealtimeData, GlobeData
)
from .client import AnalyticsClient

__all__ = [
    "PageView", "Session", "Event",
    "CoreMetrics", "MetricChange", "TimeSeriesPoint",
    "PageStats", "SourceStats", "CountryStats", "DeviceStats", "BrowserStats", "EventStats",
    "DashboardData", "DashboardFilters", "DateRange",
    "RealtimeData", "GlobeData",
    "AnalyticsClient",
]
