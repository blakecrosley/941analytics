"""
Configuration for 941 Analytics.
"""
import hashlib
import os
import secrets
from dataclasses import dataclass, field
from typing import Optional


def hash_passkey(passkey: str) -> str:
    """Hash a passkey using PBKDF2-SHA256 (sec-3).

    Returns a string in format: pbkdf2:iterations:salt_hex:hash_hex

    Use this function to generate a hashed passkey for the config:

        from analytics_941.config import hash_passkey
        print(hash_passkey("your-secret-passkey"))

    Then use the output as the passkey value in your config.
    Legacy plaintext passkeys are still supported but deprecated.
    """
    salt = os.urandom(16)
    iterations = 100_000
    dk = hashlib.pbkdf2_hmac("sha256", passkey.encode(), salt, iterations)
    return f"pbkdf2:{iterations}:{salt.hex()}:{dk.hex()}"


def verify_passkey(stored: str, provided: str) -> bool:
    """Verify a passkey using timing-safe comparison (sec-3).

    Handles both hashed (pbkdf2:...) and legacy plaintext passkeys.
    """
    if stored.startswith("pbkdf2:"):
        # Hashed passkey
        try:
            _, iterations_str, salt_hex, hash_hex = stored.split(":")
            iterations = int(iterations_str)
            salt = bytes.fromhex(salt_hex)
            expected_hash = bytes.fromhex(hash_hex)

            # Compute hash of provided passkey
            dk = hashlib.pbkdf2_hmac("sha256", provided.encode(), salt, iterations)

            # Timing-safe comparison
            return secrets.compare_digest(dk, expected_hash)
        except (ValueError, TypeError):
            return False
    else:
        # Legacy plaintext passkey - use timing-safe comparison
        # Note: This is less secure but maintains backward compatibility
        return secrets.compare_digest(stored.encode(), provided.encode())


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
