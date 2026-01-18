"""
UTM parameter parsing for campaign attribution.

UTM (Urchin Tracking Module) parameters are the industry standard for
tracking marketing campaigns. This module extracts and validates UTM
parameters from URLs.

Standard UTM Parameters:
- utm_source: Where the traffic came from (e.g., "google", "newsletter")
- utm_medium: Marketing medium (e.g., "cpc", "email", "social")
- utm_campaign: Campaign name (e.g., "spring_sale", "product_launch")
- utm_term: Paid search keywords (optional)
- utm_content: Differentiates similar content/links (optional)

Additionally, we support:
- utm_id: Campaign ID for Google Analytics 4 integration
- ref: Alternative to utm_source (used by some platforms)
- source: Another alternative (used by some affiliate programs)

Privacy Note:
UTM parameters are intentionally added by marketers and do not reveal
personal information. They're safe to store for analytics.
"""

from dataclasses import dataclass
from urllib.parse import parse_qs, urlparse


@dataclass(frozen=True)
class UTMParams:
    """
    Extracted UTM parameters from a URL.

    All fields are optional - a URL may have some, all, or none.

    Attributes:
        source: Traffic source (utm_source or ref)
        medium: Marketing medium (utm_medium)
        campaign: Campaign identifier (utm_campaign)
        term: Search keywords (utm_term)
        content: Content variant (utm_content)
        campaign_id: Campaign ID (utm_id)
        has_utm: Whether any UTM parameters were present
    """
    source: str | None = None
    medium: str | None = None
    campaign: str | None = None
    term: str | None = None
    content: str | None = None
    campaign_id: str | None = None

    @property
    def has_utm(self) -> bool:
        """Check if any UTM parameters are present."""
        return any([
            self.source,
            self.medium,
            self.campaign,
            self.term,
            self.content,
            self.campaign_id
        ])

    def to_dict(self) -> dict[str, str | None]:
        """Convert to dictionary, excluding None values."""
        result = {}
        if self.source:
            result["source"] = self.source
        if self.medium:
            result["medium"] = self.medium
        if self.campaign:
            result["campaign"] = self.campaign
        if self.term:
            result["term"] = self.term
        if self.content:
            result["content"] = self.content
        if self.campaign_id:
            result["campaign_id"] = self.campaign_id
        return result


# Maximum length for UTM parameter values (security/sanity limit)
MAX_UTM_LENGTH = 200

# Known medium values for classification
KNOWN_MEDIUMS = {
    # Paid
    "cpc": "paid",
    "ppc": "paid",
    "paid": "paid",
    "paidsearch": "paid",
    "paid_search": "paid",
    "paid-search": "paid",
    "cpm": "paid",
    "display": "paid",
    "banner": "paid",
    "retargeting": "paid",
    "remarketing": "paid",

    # Organic/Search
    "organic": "organic",
    "search": "organic",

    # Social
    "social": "social",
    "social-media": "social",
    "social_media": "social",
    "socialmedia": "social",
    "sm": "social",

    # Email
    "email": "email",
    "e-mail": "email",
    "newsletter": "email",
    "mail": "email",

    # Referral
    "referral": "referral",
    "affiliate": "referral",
    "partner": "referral",
    "partnership": "referral",

    # Content
    "content": "content",
    "blog": "content",
    "post": "content",
    "article": "content",

    # Other
    "video": "video",
    "podcast": "audio",
    "audio": "audio",
    "qr": "offline",
    "qrcode": "offline",
    "print": "offline",
    "tv": "offline",
    "radio": "offline",
    "direct": "direct",
    "none": "direct",
}


def _clean_param(value: str | None) -> str | None:
    """
    Clean and validate a UTM parameter value.

    - Strip whitespace
    - Truncate to max length
    - Return None for empty strings
    """
    if not value:
        return None

    cleaned = value.strip()

    # Truncate if too long (security measure)
    if len(cleaned) > MAX_UTM_LENGTH:
        cleaned = cleaned[:MAX_UTM_LENGTH]

    return cleaned if cleaned else None


def _get_first_param(params: dict, *keys: str) -> str | None:
    """Get the first non-empty value from multiple possible parameter names."""
    for key in keys:
        values = params.get(key, [])
        if values and values[0]:
            return _clean_param(values[0])
    return None


def parse_utm(url: str) -> UTMParams:
    """
    Extract UTM parameters from a URL.

    Handles standard UTM parameters plus common alternatives like 'ref'.

    Args:
        url: Full URL or just query string

    Returns:
        UTMParams dataclass with extracted values

    Examples:
        >>> parse_utm("https://example.com/?utm_source=google&utm_medium=cpc")
        UTMParams(source='google', medium='cpc', campaign=None, ...)

        >>> parse_utm("https://example.com/?ref=newsletter")
        UTMParams(source='newsletter', medium=None, ...)

        >>> parse_utm("https://example.com/page")
        UTMParams(source=None, medium=None, ...)  # has_utm = False
    """
    if not url:
        return UTMParams()

    try:
        # Parse URL
        parsed = urlparse(url)
        query_params = parse_qs(parsed.query, keep_blank_values=False)

        # Also check fragment (some SPAs put params there)
        if parsed.fragment:
            fragment_params = parse_qs(parsed.fragment, keep_blank_values=False)
            # Merge, preferring query params
            for key, value in fragment_params.items():
                if key not in query_params:
                    query_params[key] = value

        # Extract UTM parameters (check multiple names for compatibility)
        source = _get_first_param(
            query_params,
            "utm_source",
            "ref",
            "source",
            "via"
        )
        medium = _get_first_param(
            query_params,
            "utm_medium",
            "medium"
        )
        campaign = _get_first_param(
            query_params,
            "utm_campaign",
            "campaign",
            "utm_name"
        )
        term = _get_first_param(
            query_params,
            "utm_term",
            "term",
            "keyword",
            "keywords"
        )
        content = _get_first_param(
            query_params,
            "utm_content",
            "content"
        )
        campaign_id = _get_first_param(
            query_params,
            "utm_id",
            "campaign_id"
        )

        return UTMParams(
            source=source,
            medium=medium,
            campaign=campaign,
            term=term,
            content=content,
            campaign_id=campaign_id
        )

    except Exception:
        # If URL parsing fails, return empty
        return UTMParams()


def classify_medium(medium: str | None) -> str | None:
    """
    Classify a utm_medium value into a standard category.

    Args:
        medium: The utm_medium value

    Returns:
        Standardized category or the original value if unknown
    """
    if not medium:
        return None

    normalized = medium.lower().strip()

    # Check known mediums
    if normalized in KNOWN_MEDIUMS:
        return KNOWN_MEDIUMS[normalized]

    # Return original if not recognized
    return normalized


def get_campaign_summary(utm_list: list[UTMParams]) -> dict:
    """
    Summarize campaign data from a list of UTM parameters.

    Args:
        utm_list: List of UTMParams from multiple pageviews

    Returns:
        Dict with source, medium, and campaign breakdowns
    """
    sources: dict[str, int] = {}
    mediums: dict[str, int] = {}
    campaigns: dict[str, int] = {}

    for utm in utm_list:
        if not utm.has_utm:
            continue

        if utm.source:
            sources[utm.source] = sources.get(utm.source, 0) + 1
        if utm.medium:
            mediums[utm.medium] = mediums.get(utm.medium, 0) + 1
        if utm.campaign:
            campaigns[utm.campaign] = campaigns.get(utm.campaign, 0) + 1

    return {
        "sources": dict(sorted(sources.items(), key=lambda x: x[1], reverse=True)),
        "mediums": dict(sorted(mediums.items(), key=lambda x: x[1], reverse=True)),
        "campaigns": dict(sorted(campaigns.items(), key=lambda x: x[1], reverse=True)),
        "total_with_utm": sum(1 for u in utm_list if u.has_utm),
        "total_without_utm": sum(1 for u in utm_list if not u.has_utm),
    }


def build_utm_url(
    base_url: str,
    source: str,
    medium: str,
    campaign: str,
    term: str | None = None,
    content: str | None = None
) -> str:
    """
    Build a URL with UTM parameters.

    Useful for generating tracking links.

    Args:
        base_url: The destination URL
        source: Traffic source
        medium: Marketing medium
        campaign: Campaign name
        term: Optional keyword
        content: Optional content variant

    Returns:
        URL with UTM parameters appended
    """
    from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

    parsed = urlparse(base_url)
    existing_params = parse_qs(parsed.query)

    # Add UTM parameters
    utm_params = {
        "utm_source": source,
        "utm_medium": medium,
        "utm_campaign": campaign,
    }
    if term:
        utm_params["utm_term"] = term
    if content:
        utm_params["utm_content"] = content

    # Merge with existing params
    all_params = {**existing_params, **{k: [v] for k, v in utm_params.items()}}

    # Build query string
    query_string = urlencode(
        {k: v[0] if isinstance(v, list) and len(v) == 1 else v for k, v in all_params.items()},
        doseq=True
    )

    # Reconstruct URL
    return urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        parsed.params,
        query_string,
        parsed.fragment
    ))
