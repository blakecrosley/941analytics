"""
User-Agent parsing for browser and OS detection.

This module extracts browser, OS, and device information from User-Agent
strings. User-Agents are notoriously messy (Chrome claims to be Mozilla,
Safari, and Chrome all at once), so we use careful pattern matching.

Key Design Decisions:
- Check for newer/specific browsers first (Edge before Chrome)
- Use version patterns to distinguish similar browsers
- Handle mobile variants correctly
- Return "Unknown" rather than guessing

Privacy Note:
We extract only the browser family and OS type, not specific versions
or device identifiers. This is sufficient for analytics without being
privacy-invasive.
"""

import re
from dataclasses import dataclass
from enum import Enum


class DeviceType(str, Enum):
    """Device category."""
    DESKTOP = "desktop"
    MOBILE = "mobile"
    TABLET = "tablet"
    TV = "tv"
    BOT = "bot"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class UserAgentInfo:
    """
    Parsed user-agent information.

    Attributes:
        browser: Browser family name (Chrome, Firefox, Safari, etc.)
        browser_version: Major version number (or None if not parseable)
        os: Operating system (Windows, macOS, iOS, Android, Linux)
        os_version: OS version (or None if not parseable)
        device_type: Device category (desktop, mobile, tablet)
        is_mobile: Convenience flag for mobile/tablet
    """
    browser: str = "Unknown"
    browser_version: str | None = None
    os: str = "Unknown"
    os_version: str | None = None
    device_type: DeviceType = DeviceType.UNKNOWN

    @property
    def is_mobile(self) -> bool:
        """Check if device is mobile or tablet."""
        return self.device_type in (DeviceType.MOBILE, DeviceType.TABLET)

    def to_dict(self) -> dict:
        """Convert to dictionary for storage."""
        return {
            "browser": self.browser,
            "browser_version": self.browser_version,
            "os": self.os,
            "os_version": self.os_version,
            "device_type": self.device_type.value,
            "is_mobile": self.is_mobile,
        }


# =============================================================================
# BROWSER DETECTION PATTERNS
# =============================================================================
# Order matters! Check specific browsers before generic ones.
# Each tuple: (pattern_in_ua, browser_name, version_regex)

BROWSER_PATTERNS = [
    # New Chromium-based browsers (check before Chrome)
    (r"Edg(?:e|A|iOS)?/(\d+)", "Edge"),
    (r"OPR/(\d+)", "Opera"),
    (r"Opera.*Version/(\d+)", "Opera"),
    (r"Vivaldi/(\d+)", "Vivaldi"),
    (r"Brave/(\d+)", "Brave"),
    (r"Arc/(\d+)", "Arc"),

    # Samsung Internet
    (r"SamsungBrowser/(\d+)", "Samsung Internet"),

    # UC Browser (popular in Asia)
    (r"UCBrowser/(\d+)", "UC Browser"),

    # Yandex Browser
    (r"YaBrowser/(\d+)", "Yandex"),

    # DuckDuckGo Browser
    (r"DuckDuckGo/(\d+)", "DuckDuckGo"),

    # Firefox variants
    (r"Firefox Focus/(\d+)", "Firefox Focus"),
    (r"Firefox/(\d+)", "Firefox"),
    (r"FxiOS/(\d+)", "Firefox"),  # Firefox on iOS

    # Chrome variants (after other Chromium browsers)
    (r"CriOS/(\d+)", "Chrome"),  # Chrome on iOS
    (r"Chrome/(\d+)", "Chrome"),
    (r"Chromium/(\d+)", "Chromium"),

    # Safari (must come after Chrome which also contains Safari)
    (r"Version/(\d+).*Safari", "Safari"),
    (r"Safari/(\d+)", "Safari"),

    # IE and legacy
    (r"MSIE (\d+)", "Internet Explorer"),
    (r"Trident.*rv:(\d+)", "Internet Explorer"),

    # Mobile app WebViews
    (r"Instagram", "Instagram WebView"),
    (r"FBAN|FBAV", "Facebook WebView"),
    (r"Twitter", "Twitter WebView"),
    (r"Line/(\d+)", "LINE"),
    (r"Snapchat", "Snapchat"),
    (r"TikTok", "TikTok"),
]

# =============================================================================
# OS DETECTION PATTERNS
# =============================================================================

OS_PATTERNS = [
    # Apple
    (r"iPhone|iPod", "iOS", r"OS (\d+[_\.]\d+)"),
    (r"iPad", "iPadOS", r"OS (\d+[_\.]\d+)"),
    (r"Macintosh|Mac OS X", "macOS", r"Mac OS X (\d+[_\.]\d+)"),

    # Android (before Linux since Android contains Linux)
    (r"Android", "Android", r"Android (\d+\.?\d*)"),

    # Windows
    (r"Windows NT 10\.0", "Windows", "10/11"),
    (r"Windows NT 6\.3", "Windows", "8.1"),
    (r"Windows NT 6\.2", "Windows", "8"),
    (r"Windows NT 6\.1", "Windows", "7"),
    (r"Windows NT 6\.0", "Windows", "Vista"),
    (r"Windows NT 5\.1", "Windows", "XP"),
    (r"Windows", "Windows", None),

    # Chrome OS
    (r"CrOS", "Chrome OS", None),

    # Linux variants
    (r"Ubuntu", "Ubuntu", None),
    (r"Fedora", "Fedora", None),
    (r"Debian", "Debian", None),
    (r"Linux", "Linux", None),

    # Other
    (r"PlayStation", "PlayStation", None),
    (r"Xbox", "Xbox", None),
    (r"Nintendo", "Nintendo", None),
    (r"FreeBSD", "FreeBSD", None),
]

# =============================================================================
# DEVICE TYPE DETECTION
# =============================================================================

MOBILE_INDICATORS = [
    r"Mobile",
    r"Android.*Mobile",
    r"iPhone",
    r"iPod",
    r"BlackBerry",
    r"IEMobile",
    r"Opera Mini",
    r"Opera Mobi",
    r"webOS",
    r"Windows Phone",
]

TABLET_INDICATORS = [
    r"iPad",
    r"Android(?!.*Mobile)",  # Android without Mobile = tablet
    r"Tablet",
    r"Kindle",
    r"Silk",
    r"PlayBook",
]

TV_INDICATORS = [
    r"SmartTV",
    r"Smart-TV",
    r"SMART-TV",
    r"Web0S",
    r"webOS",
    r"NetCast",
    r"Tizen",
    r"Roku",
    r"BRAVIA",
    r"AppleTV",
    r"tvOS",
    r"FireTV",
    r"Chromecast",
    r"PlayStation",
    r"Xbox",
]


def _detect_device_type(ua: str) -> DeviceType:
    """Detect device type from user-agent string."""
    if not ua:
        return DeviceType.UNKNOWN

    # Check TV first (some TVs include "Mobile" in their UA)
    for pattern in TV_INDICATORS:
        if re.search(pattern, ua, re.IGNORECASE):
            return DeviceType.TV

    # Check tablet before mobile (iPad contains Mobile in some cases)
    for pattern in TABLET_INDICATORS:
        if re.search(pattern, ua, re.IGNORECASE):
            return DeviceType.TABLET

    # Check mobile
    for pattern in MOBILE_INDICATORS:
        if re.search(pattern, ua, re.IGNORECASE):
            return DeviceType.MOBILE

    # Default to desktop for normal browsers
    if any(browser in ua for browser in ["Chrome", "Firefox", "Safari", "Edge"]):
        return DeviceType.DESKTOP

    return DeviceType.UNKNOWN


def _detect_browser(ua: str) -> tuple[str, str | None]:
    """
    Detect browser and version from user-agent.

    Returns: (browser_name, version_string)
    """
    if not ua:
        return ("Unknown", None)

    for pattern, browser_name in BROWSER_PATTERNS:
        match = re.search(pattern, ua, re.IGNORECASE)
        if match:
            version = match.group(1) if match.lastindex else None
            return (browser_name, version)

    return ("Unknown", None)


def _detect_os(ua: str) -> tuple[str, str | None]:
    """
    Detect OS and version from user-agent.

    Returns: (os_name, version_string)
    """
    if not ua:
        return ("Unknown", None)

    for os_pattern, os_name, version_pattern in OS_PATTERNS:
        if re.search(os_pattern, ua, re.IGNORECASE):
            version = None
            if version_pattern:
                if isinstance(version_pattern, str) and "/" not in version_pattern:
                    # It's a regex pattern
                    version_match = re.search(version_pattern, ua)
                    if version_match:
                        version = version_match.group(1).replace("_", ".")
                else:
                    # It's a direct version string
                    version = version_pattern

            return (os_name, version)

    return ("Unknown", None)


def parse_user_agent(user_agent: str) -> UserAgentInfo:
    """
    Parse a user-agent string into structured information.

    Args:
        user_agent: The User-Agent header value

    Returns:
        UserAgentInfo with browser, OS, and device details

    Examples:
        >>> parse_user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        UserAgentInfo(browser='Chrome', browser_version='120', os='macOS', os_version='10.15', device_type=<DeviceType.DESKTOP>)

        >>> parse_user_agent("Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15")
        UserAgentInfo(browser='Safari', os='iOS', os_version='17.0', device_type=<DeviceType.MOBILE>)
    """
    if not user_agent or not user_agent.strip():
        return UserAgentInfo()

    browser, browser_version = _detect_browser(user_agent)
    os_name, os_version = _detect_os(user_agent)
    device_type = _detect_device_type(user_agent)

    return UserAgentInfo(
        browser=browser,
        browser_version=browser_version,
        os=os_name,
        os_version=os_version,
        device_type=device_type
    )


def get_browser_summary(ua_infos: list[UserAgentInfo]) -> dict[str, int]:
    """
    Get browser usage breakdown.

    Args:
        ua_infos: List of UserAgentInfo from parse_user_agent()

    Returns:
        Dict mapping browser name to count
    """
    counts: dict[str, int] = {}
    for info in ua_infos:
        counts[info.browser] = counts.get(info.browser, 0) + 1
    return dict(sorted(counts.items(), key=lambda x: x[1], reverse=True))


def get_os_summary(ua_infos: list[UserAgentInfo]) -> dict[str, int]:
    """
    Get OS usage breakdown.

    Args:
        ua_infos: List of UserAgentInfo from parse_user_agent()

    Returns:
        Dict mapping OS name to count
    """
    counts: dict[str, int] = {}
    for info in ua_infos:
        counts[info.os] = counts.get(info.os, 0) + 1
    return dict(sorted(counts.items(), key=lambda x: x[1], reverse=True))


def get_device_summary(ua_infos: list[UserAgentInfo]) -> dict[str, int]:
    """
    Get device type breakdown.

    Args:
        ua_infos: List of UserAgentInfo from parse_user_agent()

    Returns:
        Dict mapping device type to count
    """
    counts: dict[str, int] = {}
    for info in ua_infos:
        key = info.device_type.value
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items(), key=lambda x: x[1], reverse=True))
