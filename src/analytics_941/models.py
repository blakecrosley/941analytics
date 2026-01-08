"""Pydantic models for analytics data."""

from datetime import datetime, date
from typing import Optional
from pydantic import BaseModel, Field


class PageView(BaseModel):
    """A single page view event."""

    id: Optional[int] = None
    site: str
    timestamp: datetime
    url: str
    page_title: str = ""

    # Referrer tracking
    referrer: str = ""
    referrer_type: str = ""  # direct, organic, social, email, referral
    referrer_domain: str = ""

    # Geographic data
    country: str = ""
    region: str = ""  # State/province code (e.g., "CA")
    city: str = ""
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    # Device & browser
    device_type: str = ""  # mobile, tablet, desktop
    user_agent: str = ""
    browser: str = ""
    browser_version: str = ""
    os: str = ""
    os_version: str = ""

    # Bot detection
    is_bot: bool = False
    bot_name: str = ""
    bot_category: str = ""  # search_engine, ai_crawler, seo_tool, etc.

    # UTM parameters
    utm_source: str = ""
    utm_medium: str = ""
    utm_campaign: str = ""
    utm_term: str = ""
    utm_content: str = ""

    # Privacy-preserving identifier
    visitor_hash: str = ""  # daily-rotating hash for unique visitors


class DailyStats(BaseModel):
    """Aggregated stats for a single day."""

    date: date
    site: str
    total_views: int = 0
    unique_visitors: int = 0
    bot_views: int = 0
    top_pages: list[dict] = Field(default_factory=list)
    top_referrers: list[dict] = Field(default_factory=list)
    countries: dict[str, int] = Field(default_factory=dict)
    devices: dict[str, int] = Field(default_factory=dict)
    browsers: dict[str, int] = Field(default_factory=dict)
    operating_systems: dict[str, int] = Field(default_factory=dict)
    referrer_types: dict[str, int] = Field(default_factory=dict)
    utm_sources: dict[str, int] = Field(default_factory=dict)
    utm_campaigns: dict[str, int] = Field(default_factory=dict)
    bot_breakdown: dict[str, int] = Field(default_factory=dict)


class DashboardData(BaseModel):
    """Data for the analytics dashboard."""

    site: str
    period: str  # today, 7d, 30d
    total_views: int = 0
    unique_visitors: int = 0
    bot_views: int = 0  # Separate count of bot traffic

    # Time series
    views_by_day: list[dict] = Field(default_factory=list)

    # Content
    top_pages: list[dict] = Field(default_factory=list)

    # Traffic sources
    top_referrers: list[dict] = Field(default_factory=list)
    referrer_types: dict[str, int] = Field(default_factory=dict)  # direct/organic/social/email/referral

    # UTM attribution
    utm_sources: list[dict] = Field(default_factory=list)
    utm_campaigns: list[dict] = Field(default_factory=list)

    # Geography
    countries: list[dict] = Field(default_factory=list)
    regions: list[dict] = Field(default_factory=list)  # For drill-down by state/region
    cities: list[dict] = Field(default_factory=list)  # For drill-down by city

    # Technology
    devices: dict[str, int] = Field(default_factory=dict)
    browsers: dict[str, int] = Field(default_factory=dict)
    operating_systems: dict[str, int] = Field(default_factory=dict)

    # Bot tracking
    bot_breakdown: dict[str, int] = Field(default_factory=dict)  # By category


class CollectRequest(BaseModel):
    """Incoming pageview collection request."""

    site: str
    url: str
    title: str = ""
    ref: str = ""  # referrer hostname
    w: int = 0  # viewport width (for device detection)
