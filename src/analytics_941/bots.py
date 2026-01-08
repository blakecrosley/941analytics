"""
Bot and crawler detection for privacy-first analytics.

This module identifies automated traffic from search engines, AI crawlers,
SEO tools, social media previews, and monitoring services. Accurate bot
detection is essential for meaningful analytics - without it, metrics
like "unique visitors" and "bounce rate" become unreliable.

Design Principles:
- Comprehensive: Covers major bot categories with 100+ known patterns
- Organized: Patterns grouped by category for maintainability
- Fast: O(n) pattern matching, no regex overhead for known bots
- Defensive: Empty/missing user-agents flagged as suspicious
- Future-proof: Generic patterns catch new bots following conventions
"""

import re
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class BotCategory(str, Enum):
    """Categories of automated traffic."""

    SEARCH_ENGINE = "search_engine"      # Google, Bing, etc.
    AI_CRAWLER = "ai_crawler"            # GPTBot, ClaudeBot, etc.
    SEO_TOOL = "seo_tool"                # Ahrefs, SEMrush, etc.
    SOCIAL_PREVIEW = "social_preview"    # Facebook, Twitter previews
    MONITORING = "monitoring"            # Uptime/health checks
    FEED_READER = "feed_reader"          # RSS readers
    SECURITY = "security"                # Security scanners
    ARCHIVER = "archiver"                # Internet Archive, etc.
    HEADLESS = "headless"                # Headless browsers
    LIBRARY = "library"                  # HTTP libraries (curl, requests)
    UNKNOWN = "unknown"                  # Generic bot patterns matched


@dataclass(frozen=True)
class BotInfo:
    """
    Information about detected bot traffic.

    Attributes:
        is_bot: Whether this is automated traffic
        name: Human-readable name of the bot (e.g., "Google")
        category: Classification for analytics grouping
        confidence: How confident we are (1.0 = exact match, 0.7 = generic pattern)
    """
    is_bot: bool
    name: Optional[str] = None
    category: Optional[BotCategory] = None
    confidence: float = 1.0

    def __bool__(self) -> bool:
        """Allow `if bot_info:` to check is_bot."""
        return self.is_bot


# =============================================================================
# BOT PATTERN DATABASE
# =============================================================================
# Organized by category for maintainability. Each dict maps a lowercase
# pattern to a human-readable name. Patterns are matched with `in` for speed.

SEARCH_ENGINE_BOTS = {
    # Google (various crawlers)
    "googlebot": "Google",
    "google-inspectiontool": "Google Search Console",
    "google-safety": "Google Safe Browsing",
    "googleother": "Google Other",
    "google-adwords": "Google Ads",
    "adsbot-google": "Google Ads",
    "mediapartners-google": "Google AdSense",
    "apis-google": "Google APIs",
    "feedfetcher-google": "Google Feeds",
    "google-read-aloud": "Google Read Aloud",

    # Microsoft/Bing
    "bingbot": "Bing",
    "bingpreview": "Bing Preview",
    "msnbot": "MSN/Bing",
    "adidxbot": "Bing Ads",

    # Other search engines
    "yandexbot": "Yandex",
    "yandex.com/bots": "Yandex",
    "duckduckbot": "DuckDuckGo",
    "duckduckgo": "DuckDuckGo",
    "baiduspider": "Baidu",
    "sogou": "Sogou",
    "qwantify": "Qwant",
    "ecosia": "Ecosia",
    "exabot": "Exalead",
    "ia_archiver": "Alexa",
    "applebot": "Apple",
    "petalbot": "Huawei Petal",
    "seznambot": "Seznam",
    "naver": "Naver",
    "daum": "Daum",
    "360spider": "Qihoo 360",
    "yisouspider": "Yisou",
}

AI_CRAWLER_BOTS = {
    # OpenAI
    "gptbot": "OpenAI GPT",
    "chatgpt-user": "ChatGPT",
    "oai-searchbot": "OpenAI Search",

    # Anthropic
    "anthropic-ai": "Anthropic",
    "claude-web": "Claude",
    "claudebot": "Claude",

    # Other AI
    "perplexitybot": "Perplexity",
    "cohere-ai": "Cohere",
    "google-extended": "Google AI/Bard",
    "bytespider": "ByteDance AI",
    "amazonbot": "Amazon Alexa AI",
    "omgili": "Omgili",
    "omgilibot": "Omgili",
    "diffbot": "Diffbot",
    "ccbot": "Common Crawl",
    "youbot": "You.com",
    "meta-externalfetcher": "Meta AI",
}

SEO_TOOL_BOTS = {
    # Major SEO platforms
    "ahrefsbot": "Ahrefs",
    "ahrefs.com": "Ahrefs",
    "semrushbot": "SEMrush",
    "semrush.com": "SEMrush",
    "mj12bot": "Majestic",
    "majestic.com": "Majestic",
    "dotbot": "Moz",
    "rogerbot": "Moz",
    "moz.com": "Moz",

    # Other SEO tools
    "screaming frog": "Screaming Frog",
    "seokicks": "SEOkicks",
    "sistrix": "SISTRIX",
    "blexbot": "Webmeup",
    "dataforseo": "DataForSEO",
    "serpstatbot": "Serpstat",
    "seobilitybot": "Seobility",
    "siteauditbot": "SiteAudit",
    "spyfu": "SpyFu",
    "linkdexbot": "Linkdex",
    "barkrowler": "Babbar",
    "domcopbot": "DomCop",
    "megaindex": "MegaIndex",
}

SOCIAL_PREVIEW_BOTS = {
    # Facebook/Meta
    "facebookexternalhit": "Facebook",
    "facebookcatalog": "Facebook Catalog",
    "meta-externalagent": "Meta",

    # Twitter/X
    "twitterbot": "Twitter",

    # LinkedIn
    "linkedinbot": "LinkedIn",

    # Pinterest
    "pinterestbot": "Pinterest",
    "pinterest.com": "Pinterest",

    # Messaging apps
    "slackbot": "Slack",
    "slack-imgproxy": "Slack",
    "telegrambot": "Telegram",
    "whatsapp": "WhatsApp",
    "discordbot": "Discord",
    "viber": "Viber",
    "line-poker": "LINE",
    "kakaotalk": "KakaoTalk",

    # Other social
    "redditbot": "Reddit",
    "embedly": "Embedly",
    "quora link preview": "Quora",
    "tumblr": "Tumblr",
    "flipboard": "Flipboard",
    "w3c_validator": "W3C Validator",
}

MONITORING_BOTS = {
    # Uptime monitoring
    "uptimerobot": "UptimeRobot",
    "pingdom": "Pingdom",
    "site24x7": "Site24x7",
    "statuscake": "StatusCake",
    "freshping": "Freshping",
    "hetrixtools": "HetrixTools",
    "nodeping": "NodePing",

    # APM/Observability
    "newrelicpinger": "New Relic",
    "new relic": "New Relic",
    "datadog": "Datadog",
    "dynatrace": "Dynatrace",
    "appoptics": "AppOptics",
    "sentry": "Sentry",

    # Other monitoring
    "jetmon": "Jetpack",
    "monitis": "Monitis",
    "catchpoint": "Catchpoint",
    "gomez": "Dynatrace Gomez",
    "webpagetest": "WebPageTest",
    "gtmetrix": "GTmetrix",
    "pagespeed": "PageSpeed",
}

FEED_READER_BOTS = {
    "feedly": "Feedly",
    "feedbin": "Feedbin",
    "inoreader": "Inoreader",
    "theoldreader": "The Old Reader",
    "newsblur": "NewsBlur",
    "netvibes": "Netvibes",
    "feedspot": "Feedspot",
    "superfeedr": "Superfeedr",
    "feedpress": "FeedPress",
    "feeder.co": "Feeder",
    "bazqux": "BazQux",
}

SECURITY_SCANNER_BOTS = {
    "nessus": "Nessus",
    "qualys": "Qualys",
    "netsparker": "Netsparker",
    "acunetix": "Acunetix",
    "burp": "Burp Suite",
    "nikto": "Nikto",
    "sqlmap": "SQLMap",
    "wpscan": "WPScan",
    "zgrab": "ZGrab",
    "masscan": "Masscan",
    "nmap": "Nmap",
    "censys": "Censys",
    "shodan": "Shodan",
}

ARCHIVER_BOTS = {
    "archive.org_bot": "Internet Archive",
    "ia_archiver": "Internet Archive",
    "wayback": "Wayback Machine",
    "arquivo.pt": "Arquivo.pt",
    "webarchive": "Web Archive",
    "httrack": "HTTrack",
}

HEADLESS_BROWSER_BOTS = {
    "headlesschrome": "Headless Chrome",
    "headless chrome": "Headless Chrome",
    "phantomjs": "PhantomJS",
    "slimerjs": "SlimerJS",
    "selenium": "Selenium",
    "puppeteer": "Puppeteer",
    "playwright": "Playwright",
    "electron": "Electron",
    "prerender": "Prerender",
    "rendertron": "Rendertron",
    "splash": "Splash",
    "browserless": "Browserless",
}

HTTP_LIBRARY_BOTS = {
    # Command line
    "wget": "Wget",
    "curl": "cURL",
    "httpie": "HTTPie",
    "lynx": "Lynx",

    # Programming languages
    "python-requests": "Python Requests",
    "python-urllib": "Python urllib",
    "python-httpx": "Python httpx",
    "aiohttp": "Python aiohttp",
    "go-http-client": "Go HTTP",
    "java/": "Java",
    "okhttp": "OkHttp (Java/Android)",
    "apache-httpclient": "Apache HttpClient",
    "node-fetch": "Node.js fetch",
    "axios": "Axios",
    "got": "Got (Node.js)",
    "needle": "Needle (Node.js)",
    "request/": "Request (Node.js)",
    "superagent": "SuperAgent",
    "ruby": "Ruby",
    "libwww-perl": "Perl LWP",
    "php/": "PHP",
    "guzzle": "Guzzle (PHP)",
    "dart": "Dart",
    "swift/": "Swift",
    "cfnetwork": "CFNetwork (Apple)",
    "restsharp": "RestSharp (.NET)",
    "httpclient": "HttpClient (.NET)",
}

# Generic patterns that indicate bot behavior when no specific match is found
GENERIC_BOT_PATTERNS = [
    r"\bbot\b",           # Contains word "bot"
    r"\bcrawl",           # crawler, crawling
    r"\bspider\b",        # spider
    r"\bscrape",          # scraper, scraping
    r"\bfetch",           # fetcher, fetching
    r"\bindex",           # indexer
    r"\barchive",         # archiver
    r"\bmonitor",         # monitoring
    r"\bcheck",           # checker
    r"\bscan",            # scanner
    r"\bvalidat",         # validator
    r"\bpreview",         # preview generator
    r"\bslurp",           # Yahoo Slurp legacy
    r"\brobots",          # robots.txt fetcher
    r"http://|https://",  # UA containing URL (bots often do this)
]

# Compiled regex for generic patterns (done once at module load)
_GENERIC_BOT_REGEX = re.compile("|".join(GENERIC_BOT_PATTERNS), re.IGNORECASE)


def detect_bot(user_agent: str) -> BotInfo:
    """
    Detect if a user-agent string indicates automated traffic.

    This function checks against a comprehensive database of known bot patterns
    and falls back to generic pattern matching for unknown bots.

    Args:
        user_agent: The User-Agent header string

    Returns:
        BotInfo with is_bot, name, category, and confidence

    Examples:
        >>> detect_bot("Mozilla/5.0 (compatible; Googlebot/2.1)")
        BotInfo(is_bot=True, name='Google', category=<BotCategory.SEARCH_ENGINE>)

        >>> detect_bot("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
        BotInfo(is_bot=False)

        >>> detect_bot("")
        BotInfo(is_bot=True, name='Empty User-Agent', category=<BotCategory.UNKNOWN>)
    """
    # Empty or missing user-agent is suspicious
    if not user_agent or not user_agent.strip():
        return BotInfo(
            is_bot=True,
            name="Empty User-Agent",
            category=BotCategory.UNKNOWN,
            confidence=0.8  # Could be a misconfigured client
        )

    ua_lower = user_agent.lower()

    # Check known bot patterns by category (ordered by frequency for speed)
    pattern_groups = [
        (SEARCH_ENGINE_BOTS, BotCategory.SEARCH_ENGINE),
        (SOCIAL_PREVIEW_BOTS, BotCategory.SOCIAL_PREVIEW),
        (AI_CRAWLER_BOTS, BotCategory.AI_CRAWLER),
        (SEO_TOOL_BOTS, BotCategory.SEO_TOOL),
        (MONITORING_BOTS, BotCategory.MONITORING),
        (HTTP_LIBRARY_BOTS, BotCategory.LIBRARY),
        (HEADLESS_BROWSER_BOTS, BotCategory.HEADLESS),
        (FEED_READER_BOTS, BotCategory.FEED_READER),
        (SECURITY_SCANNER_BOTS, BotCategory.SECURITY),
        (ARCHIVER_BOTS, BotCategory.ARCHIVER),
    ]

    for patterns, category in pattern_groups:
        for pattern, name in patterns.items():
            if pattern in ua_lower:
                return BotInfo(
                    is_bot=True,
                    name=name,
                    category=category,
                    confidence=1.0
                )

    # Fall back to generic pattern matching
    if _GENERIC_BOT_REGEX.search(ua_lower):
        return BotInfo(
            is_bot=True,
            name="Unknown Bot",
            category=BotCategory.UNKNOWN,
            confidence=0.7  # Less confident with generic patterns
        )

    # Not a bot
    return BotInfo(is_bot=False)


def is_bot(user_agent: str) -> bool:
    """
    Quick check if user-agent is a bot.

    Use this when you only need a boolean and don't need bot details.

    Args:
        user_agent: The User-Agent header string

    Returns:
        True if this appears to be automated traffic
    """
    return detect_bot(user_agent).is_bot


def get_category_counts(bot_infos: list[BotInfo]) -> dict[str, int]:
    """
    Get counts of bots by category.

    Useful for dashboard analytics showing breakdown of bot traffic.

    Args:
        bot_infos: List of BotInfo objects from detect_bot()

    Returns:
        Dict mapping category name to count
    """
    counts: dict[str, int] = {}
    for info in bot_infos:
        if info.is_bot and info.category:
            key = info.category.value
            counts[key] = counts.get(key, 0) + 1
    return counts


def get_top_bots(bot_infos: list[BotInfo], limit: int = 10) -> list[tuple[str, int]]:
    """
    Get the most frequent bots by name.

    Args:
        bot_infos: List of BotInfo objects from detect_bot()
        limit: Maximum number of bots to return

    Returns:
        List of (bot_name, count) tuples, sorted by count descending
    """
    counts: dict[str, int] = {}
    for info in bot_infos:
        if info.is_bot and info.name:
            counts[info.name] = counts.get(info.name, 0) + 1

    sorted_bots = sorted(counts.items(), key=lambda x: x[1], reverse=True)
    return sorted_bots[:limit]
