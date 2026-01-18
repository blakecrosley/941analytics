"""
Referrer classification for traffic source analysis.

This module classifies incoming traffic into meaningful sources:
- Direct: No referrer (typed URL, bookmarks, etc.)
- Organic: Search engine traffic (Google, Bing, DuckDuckGo, etc.)
- Social: Social media platforms (Facebook, Twitter, LinkedIn, etc.)
- Email: Email clients and webmail
- Referral: Other websites linking to yours
- Paid: Identified paid campaign traffic (via UTM or known ad domains)
- Internal: Same-site navigation (filtered by default)

Understanding traffic sources is fundamental to marketing analytics.
"""

from dataclasses import dataclass
from enum import Enum
from urllib.parse import urlparse


class ReferrerType(str, Enum):
    """Traffic source classification."""

    DIRECT = "direct"        # No referrer (bookmarks, typed URLs, dark social)
    ORGANIC = "organic"      # Search engine results
    SOCIAL = "social"        # Social media platforms
    EMAIL = "email"          # Email clients and newsletters
    REFERRAL = "referral"    # Other websites
    PAID = "paid"            # Paid advertising (detected via UTM or ad domain)
    INTERNAL = "internal"    # Same-site navigation


@dataclass(frozen=True)
class ReferrerInfo:
    """
    Classified referrer information.

    Attributes:
        type: The traffic source type
        domain: The referrer domain (normalized, without www)
        source_name: Human-readable source name (e.g., "Google", "Facebook")
        is_search: Whether this is a search engine
    """
    type: ReferrerType
    domain: str | None = None
    source_name: str | None = None
    is_search: bool = False


# =============================================================================
# REFERRER DOMAIN DATABASE
# =============================================================================

# Search engines - map domain patterns to display names
SEARCH_ENGINES = {
    # Google (many TLDs)
    "google.": "Google",
    "googlesyndication.com": "Google Ads",

    # Microsoft/Bing
    "bing.com": "Bing",
    "msn.com": "MSN/Bing",

    # Yahoo
    "yahoo.": "Yahoo",
    "search.yahoo": "Yahoo",

    # DuckDuckGo
    "duckduckgo.com": "DuckDuckGo",

    # Other search engines
    "baidu.com": "Baidu",
    "yandex.": "Yandex",
    "ecosia.org": "Ecosia",
    "qwant.com": "Qwant",
    "startpage.com": "Startpage",
    "brave.com/search": "Brave Search",
    "search.brave.com": "Brave Search",
    "neeva.com": "Neeva",
    "you.com": "You.com",
    "kagi.com": "Kagi",
    "ask.com": "Ask.com",
    "aol.com/search": "AOL",
    "search.aol.com": "AOL",
    "naver.com": "Naver",
    "daum.net": "Daum",
    "seznam.cz": "Seznam",
    "sogou.com": "Sogou",
    "so.com": "Qihoo 360",
    "coccoc.com": "Coc Coc",
    "yep.com": "Yep",
    "perplexity.ai": "Perplexity",
    "phind.com": "Phind",
}

# Social media platforms
SOCIAL_PLATFORMS = {
    # Meta
    "facebook.com": "Facebook",
    "fb.com": "Facebook",
    "fb.me": "Facebook",
    "m.facebook.com": "Facebook",
    "l.facebook.com": "Facebook",
    "lm.facebook.com": "Facebook",
    "instagram.com": "Instagram",
    "l.instagram.com": "Instagram",
    "threads.net": "Threads",
    "messenger.com": "Messenger",

    # Twitter/X
    "twitter.com": "Twitter/X",
    "x.com": "Twitter/X",
    "t.co": "Twitter/X",
    "mobile.twitter.com": "Twitter/X",

    # LinkedIn
    "linkedin.com": "LinkedIn",
    "lnkd.in": "LinkedIn",

    # YouTube
    "youtube.com": "YouTube",
    "youtu.be": "YouTube",
    "m.youtube.com": "YouTube",

    # TikTok
    "tiktok.com": "TikTok",
    "vm.tiktok.com": "TikTok",

    # Reddit
    "reddit.com": "Reddit",
    "old.reddit.com": "Reddit",
    "amp.reddit.com": "Reddit",
    "out.reddit.com": "Reddit",

    # Pinterest
    "pinterest.com": "Pinterest",
    "pin.it": "Pinterest",

    # Snapchat
    "snapchat.com": "Snapchat",

    # Discord
    "discord.com": "Discord",
    "discord.gg": "Discord",
    "discordapp.com": "Discord",

    # Telegram
    "telegram.org": "Telegram",
    "t.me": "Telegram",

    # WhatsApp
    "whatsapp.com": "WhatsApp",
    "wa.me": "WhatsApp",
    "api.whatsapp.com": "WhatsApp",

    # Other social
    "mastodon.social": "Mastodon",
    "tumblr.com": "Tumblr",
    "medium.com": "Medium",
    "quora.com": "Quora",
    "twitch.tv": "Twitch",
    "vimeo.com": "Vimeo",
    "flickr.com": "Flickr",
    "weibo.com": "Weibo",
    "wechat.com": "WeChat",
    "line.me": "LINE",
    "vk.com": "VK",
    "ok.ru": "Odnoklassniki",
}

# Email providers and clients
EMAIL_PROVIDERS = {
    # Webmail
    "mail.google.com": "Gmail",
    "mail.yahoo.com": "Yahoo Mail",
    "outlook.live.com": "Outlook",
    "outlook.office.com": "Outlook",
    "mail.aol.com": "AOL Mail",
    "mail.protonmail.com": "ProtonMail",
    "protonmail.com": "ProtonMail",
    "zoho.com/mail": "Zoho Mail",
    "mail.zoho.com": "Zoho Mail",
    "fastmail.com": "Fastmail",
    "icloud.com/mail": "iCloud Mail",
    "hey.com": "HEY",
    "tutanota.com": "Tutanota",

    # Email marketing platforms (when clicked from email)
    "mailchimp.com": "Mailchimp",
    "campaign-archive.com": "Mailchimp",
    "list-manage.com": "Mailchimp",
    "sendgrid.net": "SendGrid",
    "constantcontact.com": "Constant Contact",
    "hubspot.com": "HubSpot",
    "mailgun.com": "Mailgun",
    "klaviyo.com": "Klaviyo",
    "convertkit.com": "ConvertKit",
    "sendinblue.com": "Brevo",
    "brevo.com": "Brevo",
    "getresponse.com": "GetResponse",
    "aweber.com": "AWeber",
    "drip.com": "Drip",
    "activecampaign.com": "ActiveCampaign",
    "intercom.io": "Intercom",
    "customer.io": "Customer.io",
    "postmarkapp.com": "Postmark",
    "sparkpost.com": "SparkPost",
    "mailjet.com": "Mailjet",
}

# Known advertising/paid traffic domains
PAID_AD_DOMAINS = {
    "googleads.g.doubleclick.net": "Google Ads",
    "googleadservices.com": "Google Ads",
    "googlesyndication.com": "Google Ads",
    "doubleclick.net": "Google Ads",
    "adservice.google": "Google Ads",
    "facebook.com/ads": "Facebook Ads",
    "business.facebook.com": "Facebook Ads",
    "ads.linkedin.com": "LinkedIn Ads",
    "bing.com/ads": "Bing Ads",
    "ads.microsoft.com": "Microsoft Ads",
    "outbrain.com": "Outbrain",
    "taboola.com": "Taboola",
    "criteo.com": "Criteo",
}

# Generic email indicators (when domain doesn't match above)
EMAIL_INDICATORS = [
    "mail.",
    "webmail.",
    "/mail/",
    "email.",
    "newsletter",
    "campaign",
]


def _normalize_domain(domain: str) -> str:
    """Remove www. prefix and lowercase."""
    domain = domain.lower().strip()
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


def _extract_domain(referrer: str) -> str | None:
    """
    Extract and normalize domain from referrer URL.

    Returns None if referrer is empty or unparseable.
    """
    if not referrer or not referrer.strip():
        return None

    try:
        # Handle URLs without scheme
        if not referrer.startswith(("http://", "https://")):
            referrer = "https://" + referrer

        parsed = urlparse(referrer)
        domain = parsed.netloc

        if not domain:
            return None

        return _normalize_domain(domain)
    except Exception:
        return None


def classify_referrer(
    referrer: str,
    current_domain: str | None = None
) -> ReferrerInfo:
    """
    Classify a referrer URL into a traffic source category.

    Args:
        referrer: The Referer header value (can be empty or None)
        current_domain: Optional current site domain to detect internal traffic

    Returns:
        ReferrerInfo with type, domain, source_name, and is_search

    Examples:
        >>> classify_referrer("https://www.google.com/search?q=test")
        ReferrerInfo(type=<ReferrerType.ORGANIC>, domain='google.com', source_name='Google')

        >>> classify_referrer("https://t.co/abc123")
        ReferrerInfo(type=<ReferrerType.SOCIAL>, domain='t.co', source_name='Twitter/X')

        >>> classify_referrer("")
        ReferrerInfo(type=<ReferrerType.DIRECT>, domain=None, source_name=None)
    """
    # No referrer = direct traffic
    if not referrer or not referrer.strip():
        return ReferrerInfo(type=ReferrerType.DIRECT)

    domain = _extract_domain(referrer)
    if not domain:
        return ReferrerInfo(type=ReferrerType.DIRECT)

    referrer_lower = referrer.lower()

    # Check for internal traffic (same domain)
    if current_domain:
        current_normalized = _normalize_domain(current_domain)
        if domain == current_normalized or domain.endswith("." + current_normalized):
            return ReferrerInfo(
                type=ReferrerType.INTERNAL,
                domain=domain
            )

    # Check paid ad domains first (before search engines)
    for pattern, name in PAID_AD_DOMAINS.items():
        if pattern in domain or pattern in referrer_lower:
            return ReferrerInfo(
                type=ReferrerType.PAID,
                domain=domain,
                source_name=name
            )

    # Check email providers BEFORE search engines (mail.google.com should be email, not organic)
    for pattern, name in EMAIL_PROVIDERS.items():
        if pattern in domain or pattern in referrer_lower:
            return ReferrerInfo(
                type=ReferrerType.EMAIL,
                domain=domain,
                source_name=name
            )

    # Check generic email indicators
    for indicator in EMAIL_INDICATORS:
        if indicator in domain or indicator in referrer_lower:
            return ReferrerInfo(
                type=ReferrerType.EMAIL,
                domain=domain,
                source_name="Email"
            )

    # Check search engines (organic)
    for pattern, name in SEARCH_ENGINES.items():
        if pattern in domain:
            return ReferrerInfo(
                type=ReferrerType.ORGANIC,
                domain=domain,
                source_name=name,
                is_search=True
            )

    # Check social platforms
    for pattern, name in SOCIAL_PLATFORMS.items():
        if pattern in domain:
            return ReferrerInfo(
                type=ReferrerType.SOCIAL,
                domain=domain,
                source_name=name
            )

    # Default to referral (other websites)
    return ReferrerInfo(
        type=ReferrerType.REFERRAL,
        domain=domain
    )


def get_traffic_source_summary(referrer_infos: list[ReferrerInfo]) -> dict[str, int]:
    """
    Get traffic breakdown by source type.

    Args:
        referrer_infos: List of ReferrerInfo from classify_referrer()

    Returns:
        Dict mapping source type to count
    """
    counts: dict[str, int] = {
        "direct": 0,
        "organic": 0,
        "social": 0,
        "email": 0,
        "referral": 0,
        "paid": 0,
        "internal": 0,
    }

    for info in referrer_infos:
        counts[info.type.value] = counts.get(info.type.value, 0) + 1

    return counts


def get_top_referrers(
    referrer_infos: list[ReferrerInfo],
    limit: int = 10,
    exclude_direct: bool = True,
    exclude_internal: bool = True
) -> list[tuple[str, int]]:
    """
    Get the most common referrer sources.

    Args:
        referrer_infos: List of ReferrerInfo from classify_referrer()
        limit: Maximum number of sources to return
        exclude_direct: Whether to exclude direct traffic
        exclude_internal: Whether to exclude internal navigation

    Returns:
        List of (source_name_or_domain, count) tuples, sorted by count
    """
    counts: dict[str, int] = {}

    for info in referrer_infos:
        # Skip excluded types
        if exclude_direct and info.type == ReferrerType.DIRECT:
            continue
        if exclude_internal and info.type == ReferrerType.INTERNAL:
            continue

        # Use source name if available, otherwise domain
        key = info.source_name or info.domain or "Unknown"
        counts[key] = counts.get(key, 0) + 1

    sorted_refs = sorted(counts.items(), key=lambda x: x[1], reverse=True)
    return sorted_refs[:limit]
