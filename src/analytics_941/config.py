"""
Configuration for 941 Analytics.
"""
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class AnalyticsConfig:
    """Configuration for a single analytics instance."""

    # Required
    site_name: str
    worker_url: str
    d1_database_id: str
    cf_account_id: str
    cf_api_token: str

    # Optional authentication
    passkey: Optional[str] = None
    rp_id: Optional[str] = None  # WebAuthn Relying Party ID (domain)
    rp_origin: Optional[str] = None  # WebAuthn origin (https://domain)

    # Data retention
    retention_days: int = 90

    # Session settings
    session_timeout_minutes: int = 30
    heartbeat_interval_seconds: int = 15

    # Performance
    cache_ttl_seconds: int = 60  # Dashboard data cache

    # Feature flags
    enable_events: bool = True
    enable_sessions: bool = True
    enable_globe: bool = True

    @property
    def has_auth(self) -> bool:
        """Check if any authentication is configured."""
        return bool(self.passkey or (self.rp_id and self.rp_origin))

    @property
    def has_webauthn(self) -> bool:
        """Check if WebAuthn is configured."""
        return bool(self.rp_id and self.rp_origin)
