"""
Configuration for 941 Analytics.
"""
import hashlib
import logging
import os
import secrets
import warnings
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Passkey security constants
MIN_PASSKEY_LENGTH = 16


class PasskeyTooShortError(ValueError):
    """Raised when a passkey doesn't meet minimum length requirements."""
    pass


def validate_passkey_strength(passkey: str) -> None:
    """Validate passkey meets security requirements.

    Args:
        passkey: The plaintext passkey to validate

    Raises:
        PasskeyTooShortError: If passkey is shorter than MIN_PASSKEY_LENGTH
    """
    if len(passkey) < MIN_PASSKEY_LENGTH:
        raise PasskeyTooShortError(
            f"Passkey must be at least {MIN_PASSKEY_LENGTH} characters. "
            f"Got {len(passkey)} characters."
        )


def hash_passkey(passkey: str, validate: bool = True) -> str:
    """Hash a passkey using PBKDF2-SHA256 (sec-3).

    Returns a string in format: pbkdf2:iterations:salt_hex:hash_hex

    Use this function to generate a hashed passkey for the config:

        from analytics_941.config import hash_passkey
        print(hash_passkey("your-secret-passkey"))

    Then use the output as the passkey value in your config.
    Legacy plaintext passkeys are still supported but deprecated.

    Args:
        passkey: The plaintext passkey to hash
        validate: If True, validate passkey meets minimum length requirements

    Raises:
        PasskeyTooShortError: If validate=True and passkey is too short
    """
    if validate:
        validate_passkey_strength(passkey)

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
class ThemeColors:
    """Theme color overrides for the analytics dashboard.

    All colors should be valid CSS color values (hex, rgb, hsl, etc.).
    These override the default dark theme colors.

    Usage:
        theme = ThemeColors(
            accent="#3b82f6",      # Blue accent instead of white
            success="#10b981",     # Emerald green
        )
        config = AnalyticsConfig(..., theme_colors=theme)
    """

    # Core colors
    bg: str | None = None              # Background (#000000)
    surface: str | None = None          # Card/section background (#111111)
    surface_elevated: str | None = None # Elevated surfaces (#1a1a1a)
    border: str | None = None           # Border color (rgba(255,255,255,0.1))
    text: str | None = None             # Primary text (#ffffff)
    text_muted: str | None = None       # Secondary text (rgba(255,255,255,0.65))

    # Semantic colors
    accent: str | None = None           # Accent/primary color (#ffffff)
    success: str | None = None          # Success/positive (#22c55e)
    warning: str | None = None          # Warning (#f59e0b)
    error: str | None = None            # Error/negative (#ef4444)
    info: str | None = None             # Info (#3b82f6)

    def to_css(self) -> str:
        """Convert theme colors to CSS variable overrides.

        Returns a string of CSS variable declarations that can be
        injected into a <style> block.
        """
        mappings = {
            "bg": "--941-color-bg",
            "surface": "--941-color-surface",
            "surface_elevated": "--941-color-surface-elevated",
            "border": "--941-color-border",
            "text": "--941-color-text",
            "text_muted": "--941-color-text-muted",
            "accent": "--941-color-accent",
            "success": "--941-color-success",
            "warning": "--941-color-warning",
            "error": "--941-color-error",
            "info": "--941-color-info",
        }

        css_vars = []
        for attr, css_var in mappings.items():
            value = getattr(self, attr)
            if value is not None:
                css_vars.append(f"{css_var}: {value};")

        return "\n            ".join(css_vars)


@dataclass
class AnalyticsConfig:
    """Configuration for a single analytics instance."""

    # Required
    site_name: str  # Domain identifier (e.g., "blakecrosley.com")
    worker_url: str
    d1_database_id: str
    cf_account_id: str
    cf_api_token: str

    # Display settings
    display_name: str | None = None  # Friendly name (e.g., "Blake Crosley")
    timezone: str = "America/New_York"  # Site timezone for aggregation

    # Optional authentication
    passkey: str | None = None
    rp_id: str | None = None  # WebAuthn Relying Party ID (domain)
    rp_origin: str | None = None  # WebAuthn origin (https://domain)

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

    # Theme customization
    theme_colors: ThemeColors | None = None

    @property
    def has_auth(self) -> bool:
        """Check if any authentication is configured."""
        return bool(self.passkey or (self.rp_id and self.rp_origin))

    @property
    def has_webauthn(self) -> bool:
        """Check if WebAuthn is configured."""
        return bool(self.rp_id and self.rp_origin)

    @property
    def effective_display_name(self) -> str:
        """Get display name, falling back to site_name."""
        return self.display_name or self.site_name

    def __post_init__(self):
        """Validate configuration after initialization."""
        self._validate_passkey()

    def _validate_passkey(self) -> None:
        """Validate and warn about passkey configuration.

        - Warns if plaintext passkey is used (should be hashed)
        - Logs info about passkey status for debugging
        """
        if not self.passkey:
            return

        if self.passkey.startswith("pbkdf2:"):
            # Hashed passkey - good
            logger.debug(f"Site {self.site_name}: Using hashed passkey")
        else:
            # Plaintext passkey - deprecated
            warnings.warn(
                f"Site {self.site_name}: Using plaintext passkey is deprecated. "
                f"Use hash_passkey() to generate a hashed passkey:\n"
                f"  from analytics_941.config import hash_passkey\n"
                f"  print(hash_passkey('your-passkey'))",
                DeprecationWarning,
                stacklevel=3
            )
            # Still validate length for plaintext passkeys
            if len(self.passkey) < MIN_PASSKEY_LENGTH:
                logger.warning(
                    f"Site {self.site_name}: Passkey is shorter than "
                    f"recommended {MIN_PASSKEY_LENGTH} characters"
                )

    @property
    def is_passkey_hashed(self) -> bool:
        """Check if the passkey is properly hashed."""
        return bool(self.passkey and self.passkey.startswith("pbkdf2:"))

    @property
    def theme_css(self) -> str | None:
        """Generate CSS variable overrides from theme_colors.

        Returns None if no theme_colors are configured.
        """
        if self.theme_colors is None:
            return None
        css = self.theme_colors.to_css()
        return css if css else None
