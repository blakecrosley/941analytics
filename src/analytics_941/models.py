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
    referrer: str = ""
    country: str = ""
    region: str = ""  # State/province code (e.g., "CA")
    city: str = ""
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    device_type: str = ""  # mobile, tablet, desktop
    visitor_hash: str = ""  # daily-rotating hash for unique visitors


class DailyStats(BaseModel):
    """Aggregated stats for a single day."""

    date: date
    site: str
    total_views: int = 0
    unique_visitors: int = 0
    top_pages: list[dict] = Field(default_factory=list)
    top_referrers: list[dict] = Field(default_factory=list)
    countries: dict[str, int] = Field(default_factory=dict)
    devices: dict[str, int] = Field(default_factory=dict)


class DashboardData(BaseModel):
    """Data for the analytics dashboard."""

    site: str
    period: str  # today, 7d, 30d
    total_views: int = 0
    unique_visitors: int = 0
    views_by_day: list[dict] = Field(default_factory=list)
    top_pages: list[dict] = Field(default_factory=list)
    top_referrers: list[dict] = Field(default_factory=list)
    countries: list[dict] = Field(default_factory=list)
    regions: list[dict] = Field(default_factory=list)  # For drill-down by state/region
    cities: list[dict] = Field(default_factory=list)  # For drill-down by city
    devices: dict[str, int] = Field(default_factory=dict)


class CollectRequest(BaseModel):
    """Incoming pageview collection request."""

    site: str
    url: str
    title: str = ""
    ref: str = ""  # referrer hostname
    w: int = 0  # viewport width (for device detection)
