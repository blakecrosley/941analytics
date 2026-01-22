"""
Pydantic models for analytics data.
"""
from datetime import date, datetime
from typing import Any

from pydantic import BaseModel

# =============================================================================
# Raw Data Models
# =============================================================================

class PageView(BaseModel):
    """A single pageview event."""
    id: int
    site: str
    timestamp: datetime
    url: str
    page_title: str | None = None

    # Session
    session_id: str
    visitor_hash: str

    # Referrer
    referrer: str | None = None
    referrer_type: str | None = None
    referrer_domain: str | None = None

    # UTM
    utm_source: str | None = None
    utm_medium: str | None = None
    utm_campaign: str | None = None
    utm_term: str | None = None
    utm_content: str | None = None

    # Geography
    country: str | None = None
    region: str | None = None
    city: str | None = None
    latitude: float | None = None
    longitude: float | None = None

    # Technology
    device_type: str | None = None
    browser: str | None = None
    browser_version: str | None = None
    os: str | None = None
    os_version: str | None = None
    screen_width: int | None = None
    screen_height: int | None = None
    language: str | None = None

    # Bot
    is_bot: bool = False
    bot_name: str | None = None
    bot_category: str | None = None


class Session(BaseModel):
    """A visitor session (group of pageviews)."""
    id: int
    site: str
    session_id: str
    visitor_hash: str

    started_at: datetime
    last_activity_at: datetime
    ended_at: datetime | None = None

    duration_seconds: int = 0
    pageview_count: int = 1
    event_count: int = 0
    is_bounce: bool = True

    entry_page: str | None = None
    exit_page: str | None = None

    # Attribution
    referrer_type: str | None = None
    referrer_domain: str | None = None
    utm_source: str | None = None
    utm_campaign: str | None = None

    # Demographics
    country: str | None = None
    region: str | None = None
    device_type: str | None = None
    browser: str | None = None
    os: str | None = None


class Event(BaseModel):
    """An auto-tracked or custom event."""
    id: int
    site: str
    timestamp: datetime
    session_id: str
    visitor_hash: str

    event_type: str  # click, scroll, form, video, error
    event_name: str  # outbound_click, scroll_50, form_submit
    event_data: dict[str, Any] | None = None

    page_url: str | None = None
    country: str | None = None
    device_type: str | None = None


# =============================================================================
# Aggregated Stats Models
# =============================================================================

class MetricChange(BaseModel):
    """A metric with its change from comparison period."""
    value: float  # Can be int or float (e.g., bounce_rate, avg_duration)
    previous: float | None = None
    change_percent: float | None = None
    change_direction: str | None = None  # up, down, same


class CoreMetrics(BaseModel):
    """Core dashboard metrics."""
    views: MetricChange
    visitors: MetricChange
    sessions: MetricChange
    bounce_rate: MetricChange  # As percentage (0-100)
    avg_duration: MetricChange  # In seconds
    pages_per_session: MetricChange  # Float, 1 decimal
    bot_views: int = 0


class PageStats(BaseModel):
    """Stats for a single page."""
    url: str
    views: int
    visitors: int
    avg_time: float | None = None  # seconds
    bounce_rate: float | None = None
    exit_rate: float | None = None  # percentage of pageviews that resulted in exit
    entries: int = 0
    exits: int = 0


class SourceStats(BaseModel):
    """Stats for a traffic source."""
    source: str
    source_type: str  # direct, organic, social, email, referral
    visits: int
    visitors: int
    bounce_rate: float | None = None


class CountryStats(BaseModel):
    """Stats for a country."""
    country_code: str
    country_name: str
    visits: int
    visitors: int
    regions: list[dict[str, Any]] | None = None


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


class ScreenSizeStats(BaseModel):
    """Stats for a single screen resolution."""
    resolution: str  # "1920x1080"
    width: int
    height: int
    visits: int
    percentage: float
    breakpoint: str  # "mobile", "tablet", "desktop", "large"


class BreakpointStats(BaseModel):
    """Stats for a responsive breakpoint group."""
    breakpoint: str  # "mobile", "tablet", "desktop", "large"
    label: str  # "Mobile (<768px)"
    visits: int
    percentage: float
    resolutions: list["ScreenSizeStats"] = []


class LanguageStats(BaseModel):
    """Stats for browser language."""
    code: str  # "en-US"
    name: str  # "English (United States)"
    base_language: str  # "en"
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
    compare_start: date | None = None
    compare_end: date | None = None


class DashboardFilters(BaseModel):
    """Filters applied to dashboard queries.

    All filters use parameterized queries to prevent SQL injection.
    Multiple filters are AND'd together.
    """
    country: str | None = None
    region: str | None = None
    city: str | None = None
    device: str | None = None
    browser: str | None = None
    os: str | None = None
    source: str | None = None
    source_type: str | None = None
    page: str | None = None
    utm_source: str | None = None
    utm_medium: str | None = None
    utm_campaign: str | None = None

    def is_empty(self) -> bool:
        """Check if all filters are None/empty."""
        return all(
            getattr(self, field) is None
            for field in self.__class__.model_fields.keys()
        )

    def active_filters(self) -> dict[str, str]:
        """Return dict of active (non-None) filters."""
        return {
            k: v for k, v in self.model_dump().items()
            if v is not None
        }


class DashboardData(BaseModel):
    """Complete dashboard response."""
    site: str
    date_range: DateRange
    filters: DashboardFilters

    # Core metrics
    metrics: CoreMetrics

    # Time series for chart
    time_series: list[TimeSeriesPoint]
    granularity: str = "day"  # hour, day

    # Breakdowns
    top_pages: list[PageStats]
    entry_pages: list[PageStats]
    exit_pages: list[PageStats]
    sources: list[SourceStats]
    countries: list[CountryStats]
    devices: list[DeviceStats]
    browsers: list[BrowserStats]
    operating_systems: list[dict[str, Any]]
    languages: list[dict[str, Any]]

    # Events (if enabled)
    events: list[EventStats] | None = None
    scroll_depth: dict[str, int] | None = None  # {25: n, 50: n, 75: n, 100: n}

    # UTM
    utm_sources: list[dict[str, Any]]
    utm_campaigns: list[dict[str, Any]]


class ActivityEvent(BaseModel):
    """A single real-time activity event."""
    id: str  # Unique event ID for deduplication
    event_type: str  # 'pageview' or custom event name
    page: str
    country: str | None = None
    device: str | None = None
    browser: str | None = None
    timestamp: str  # ISO format


class RealtimeData(BaseModel):
    """Real-time visitor data."""
    active_visitors: int
    active_sessions: list[dict[str, Any]]  # [{page, country, device, started}]
    pages: list[dict[str, Any]]  # [{url, count}]
    countries: list[dict[str, Any]]  # [{code, count}]
    sources: list[dict[str, Any]]  # [{source, count}]
    recent_activity: list[ActivityEvent] = []  # Last 20 events for activity feed


class GlobeData(BaseModel):
    """Data for the 3D globe visualization."""
    countries: list[dict[str, Any]]  # [{code, name, lat, lon, visits}]
    regions: list[dict[str, Any]] | None = None  # For drill-down
    cities: list[dict[str, Any]] | None = None  # For drill-down


# =============================================================================
# Funnel Models
# =============================================================================

class FunnelStep(BaseModel):
    """A single step in a conversion funnel."""
    type: str  # 'page' or 'event'
    value: str  # URL path for page, event_name for event
    label: str | None = None  # Optional display label


class FunnelDefinition(BaseModel):
    """A funnel definition."""
    id: int | None = None
    site: str
    name: str
    description: str | None = None
    steps: list[FunnelStep]
    is_preset: bool = False
    created_at: datetime | None = None
    updated_at: datetime | None = None


class FunnelStepResult(BaseModel):
    """Result for a single funnel step."""
    step_number: int
    label: str
    type: str
    value: str
    visitors: int
    sessions: int
    conversion_rate: float  # Percentage from previous step (100 for first step)
    drop_off_rate: float  # Percentage who didn't continue
    drop_off_count: int  # Absolute number who dropped off


class FunnelResult(BaseModel):
    """Complete funnel analysis result."""
    funnel: FunnelDefinition
    date_range: DateRange
    steps: list[FunnelStepResult]
    total_entered: int  # Visitors who entered step 1
    total_converted: int  # Visitors who completed all steps
    overall_conversion_rate: float  # total_converted / total_entered * 100
    avg_time_to_convert: float | None = None  # In seconds, if available


# =============================================================================
# Goal Models
# =============================================================================

class GoalDefinition(BaseModel):
    """A goal definition."""
    id: int | None = None
    site: str
    name: str
    description: str | None = None
    goal_type: str  # 'page' or 'event'
    goal_value: str  # URL path or event_name
    target_count: int | None = None
    is_active: bool = True
    created_at: datetime | None = None
    updated_at: datetime | None = None


class GoalResult(BaseModel):
    """Goal completion result."""
    goal: GoalDefinition
    date_range: DateRange
    completions: int
    unique_visitors: int
    conversion_rate: float  # Percentage of total visitors
    trend: list[dict[str, Any]]  # [{date, completions}] for sparkline


# =============================================================================
# Saved View Models
# =============================================================================

class SavedView(BaseModel):
    """A saved filter/view configuration."""
    id: int | None = None
    site: str
    name: str
    description: str | None = None
    filters: dict[str, str]  # Key-value pairs of filter settings
    date_preset: str | None = None  # 'today', '7d', '30d', '90d', 'custom'
    is_default: bool = False
    is_shared: bool = False
    created_at: datetime | None = None
    updated_at: datetime | None = None

    def to_query_params(self) -> str:
        """Convert filters to URL query parameters."""
        params = []
        for key, value in self.filters.items():
            if value:
                params.append(f"{key}={value}")
        if self.date_preset:
            params.append(f"range={self.date_preset}")
        return "&".join(params)
