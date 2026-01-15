"""
Pydantic models for analytics data.
"""
from datetime import datetime, date
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


# =============================================================================
# Raw Data Models
# =============================================================================

class PageView(BaseModel):
    """A single pageview event."""
    id: int
    site: str
    timestamp: datetime
    url: str
    page_title: Optional[str] = None

    # Session
    session_id: str
    visitor_hash: str

    # Referrer
    referrer: Optional[str] = None
    referrer_type: Optional[str] = None
    referrer_domain: Optional[str] = None

    # UTM
    utm_source: Optional[str] = None
    utm_medium: Optional[str] = None
    utm_campaign: Optional[str] = None
    utm_term: Optional[str] = None
    utm_content: Optional[str] = None

    # Geography
    country: Optional[str] = None
    region: Optional[str] = None
    city: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    # Technology
    device_type: Optional[str] = None
    browser: Optional[str] = None
    browser_version: Optional[str] = None
    os: Optional[str] = None
    os_version: Optional[str] = None
    screen_width: Optional[int] = None
    screen_height: Optional[int] = None
    language: Optional[str] = None

    # Bot
    is_bot: bool = False
    bot_name: Optional[str] = None
    bot_category: Optional[str] = None


class Session(BaseModel):
    """A visitor session (group of pageviews)."""
    id: int
    site: str
    session_id: str
    visitor_hash: str

    started_at: datetime
    last_activity_at: datetime
    ended_at: Optional[datetime] = None

    duration_seconds: int = 0
    pageview_count: int = 1
    event_count: int = 0
    is_bounce: bool = True

    entry_page: Optional[str] = None
    exit_page: Optional[str] = None

    # Attribution
    referrer_type: Optional[str] = None
    referrer_domain: Optional[str] = None
    utm_source: Optional[str] = None
    utm_campaign: Optional[str] = None

    # Demographics
    country: Optional[str] = None
    region: Optional[str] = None
    device_type: Optional[str] = None
    browser: Optional[str] = None
    os: Optional[str] = None


class Event(BaseModel):
    """An auto-tracked or custom event."""
    id: int
    site: str
    timestamp: datetime
    session_id: str
    visitor_hash: str

    event_type: str  # click, scroll, form, video, error
    event_name: str  # outbound_click, scroll_50, form_submit
    event_data: Optional[Dict[str, Any]] = None

    page_url: Optional[str] = None
    country: Optional[str] = None
    device_type: Optional[str] = None


# =============================================================================
# Aggregated Stats Models
# =============================================================================

class MetricChange(BaseModel):
    """A metric with its change from comparison period."""
    value: int
    previous: Optional[int] = None
    change_percent: Optional[float] = None
    change_direction: Optional[str] = None  # up, down, same


class CoreMetrics(BaseModel):
    """Core dashboard metrics."""
    views: MetricChange
    visitors: MetricChange
    sessions: MetricChange
    bounce_rate: MetricChange  # As percentage (0-100)
    avg_duration: MetricChange  # In seconds
    bot_views: int = 0


class PageStats(BaseModel):
    """Stats for a single page."""
    url: str
    views: int
    visitors: int
    avg_time: Optional[float] = None  # seconds
    bounce_rate: Optional[float] = None
    entries: int = 0
    exits: int = 0


class SourceStats(BaseModel):
    """Stats for a traffic source."""
    source: str
    source_type: str  # direct, organic, social, email, referral
    visits: int
    visitors: int
    bounce_rate: Optional[float] = None


class CountryStats(BaseModel):
    """Stats for a country."""
    country_code: str
    country_name: str
    visits: int
    visitors: int
    regions: Optional[List[Dict[str, Any]]] = None


class DeviceStats(BaseModel):
    """Stats for device breakdown."""
    device_type: str  # mobile, tablet, desktop
    visits: int
    percentage: float


class BrowserStats(BaseModel):
    """Stats for browser breakdown."""
    browser: str
    visits: int
    percentage: float


class EventStats(BaseModel):
    """Stats for an event type."""
    event_name: str
    event_type: str
    count: int
    unique_sessions: int


class TimeSeriesPoint(BaseModel):
    """A single point in a time series."""
    timestamp: datetime
    views: int = 0
    visitors: int = 0
    sessions: int = 0


# =============================================================================
# Dashboard Response Models
# =============================================================================

class DateRange(BaseModel):
    """Date range for queries."""
    start: date
    end: date
    compare_start: Optional[date] = None
    compare_end: Optional[date] = None


class DashboardFilters(BaseModel):
    """Filters applied to dashboard."""
    country: Optional[str] = None
    region: Optional[str] = None
    device: Optional[str] = None
    browser: Optional[str] = None
    source: Optional[str] = None
    source_type: Optional[str] = None
    page: Optional[str] = None
    utm_source: Optional[str] = None
    utm_campaign: Optional[str] = None


class DashboardData(BaseModel):
    """Complete dashboard response."""
    site: str
    date_range: DateRange
    filters: DashboardFilters

    # Core metrics
    metrics: CoreMetrics

    # Time series for chart
    time_series: List[TimeSeriesPoint]
    granularity: str = "day"  # hour, day

    # Breakdowns
    top_pages: List[PageStats]
    entry_pages: List[PageStats]
    exit_pages: List[PageStats]
    sources: List[SourceStats]
    countries: List[CountryStats]
    devices: List[DeviceStats]
    browsers: List[BrowserStats]
    operating_systems: List[Dict[str, Any]]
    languages: List[Dict[str, Any]]

    # Events (if enabled)
    events: Optional[List[EventStats]] = None
    scroll_depth: Optional[Dict[str, int]] = None  # {25: n, 50: n, 75: n, 100: n}

    # UTM
    utm_sources: List[Dict[str, Any]]
    utm_campaigns: List[Dict[str, Any]]


class RealtimeData(BaseModel):
    """Real-time visitor data."""
    active_visitors: int
    active_sessions: List[Dict[str, Any]]  # [{page, country, device, started}]
    pages: List[Dict[str, Any]]  # [{url, count}]
    countries: List[Dict[str, Any]]  # [{code, count}]
    sources: List[Dict[str, Any]]  # [{source, count}]


class GlobeData(BaseModel):
    """Data for the 3D globe visualization."""
    countries: List[Dict[str, Any]]  # [{code, name, lat, lon, visits}]
    regions: Optional[List[Dict[str, Any]]] = None  # For drill-down
    cities: Optional[List[Dict[str, Any]]] = None  # For drill-down
