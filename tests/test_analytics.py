"""Tests for 941 Analytics detection modules."""

import pytest
from analytics_941.bots import detect_bot, BotCategory
from analytics_941.referrer import classify_referrer, ReferrerType
from analytics_941.utm import parse_utm
from analytics_941.user_agent import parse_user_agent, DeviceType


class TestBotDetection:
    """Test bot detection from user-agents."""

    def test_googlebot_detected(self):
        ua = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
        info = detect_bot(ua)
        assert info.is_bot is True
        assert info.name == "Google"
        assert info.category == BotCategory.SEARCH_ENGINE

    def test_bingbot_detected(self):
        ua = "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)"
        info = detect_bot(ua)
        assert info.is_bot is True
        assert info.name == "Bing"
        assert info.category == BotCategory.SEARCH_ENGINE

    def test_gptbot_detected(self):
        ua = "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; GPTBot/1.0; +https://openai.com/gptbot)"
        info = detect_bot(ua)
        assert info.is_bot is True
        assert info.name == "OpenAI GPT"
        assert info.category == BotCategory.AI_CRAWLER

    def test_chrome_not_bot(self):
        ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        info = detect_bot(ua)
        assert info.is_bot is False

    def test_safari_not_bot(self):
        ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
        info = detect_bot(ua)
        assert info.is_bot is False

    def test_empty_ua_is_bot(self):
        info = detect_bot("")
        assert info.is_bot is True
        assert info.category == BotCategory.UNKNOWN

    def test_curl_detected(self):
        ua = "curl/7.88.1"
        info = detect_bot(ua)
        assert info.is_bot is True
        assert info.name == "cURL"
        assert info.category == BotCategory.LIBRARY

    def test_semrush_detected(self):
        ua = "Mozilla/5.0 (compatible; SemrushBot/7~bl; +http://www.semrush.com/bot.html)"
        info = detect_bot(ua)
        assert info.is_bot is True
        assert info.name == "SEMrush"
        assert info.category == BotCategory.SEO_TOOL


class TestReferrerClassification:
    """Test referrer source classification."""

    def test_google_is_organic(self):
        info = classify_referrer("https://www.google.com/search?q=test")
        assert info.type == ReferrerType.ORGANIC
        assert "google" in info.domain

    def test_bing_is_organic(self):
        info = classify_referrer("https://www.bing.com/search?q=test")
        assert info.type == ReferrerType.ORGANIC

    def test_facebook_is_social(self):
        info = classify_referrer("https://www.facebook.com/post/123")
        assert info.type == ReferrerType.SOCIAL
        assert "facebook" in info.domain

    def test_twitter_is_social(self):
        info = classify_referrer("https://t.co/abc123")
        assert info.type == ReferrerType.SOCIAL

    def test_linkedin_is_social(self):
        info = classify_referrer("https://www.linkedin.com/feed/")
        assert info.type == ReferrerType.SOCIAL

    def test_gmail_is_email(self):
        info = classify_referrer("https://mail.google.com/mail/u/0/")
        assert info.type == ReferrerType.EMAIL

    def test_outlook_is_email(self):
        info = classify_referrer("https://outlook.live.com/mail/")
        assert info.type == ReferrerType.EMAIL

    def test_mailchimp_is_email(self):
        info = classify_referrer("https://mailchi.mp/campaign/xyz")
        assert info.type == ReferrerType.EMAIL

    def test_empty_is_direct(self):
        info = classify_referrer("")
        assert info.type == ReferrerType.DIRECT

    def test_none_is_direct(self):
        info = classify_referrer(None)
        assert info.type == ReferrerType.DIRECT

    def test_internal_referrer(self):
        info = classify_referrer("https://example.com/page1", current_domain="example.com")
        assert info.type == ReferrerType.INTERNAL

    def test_unknown_domain_is_referral(self):
        info = classify_referrer("https://random-blog.com/post")
        assert info.type == ReferrerType.REFERRAL


class TestUTMParsing:
    """Test UTM parameter extraction."""

    def test_basic_utm_params(self):
        url = "https://example.com/?utm_source=google&utm_medium=cpc&utm_campaign=spring_sale"
        params = parse_utm(url)
        assert params.source == "google"
        assert params.medium == "cpc"
        assert params.campaign == "spring_sale"
        assert params.has_utm is True

    def test_all_utm_params(self):
        url = "https://example.com/?utm_source=newsletter&utm_medium=email&utm_campaign=launch&utm_term=keyword&utm_content=variant_a"
        params = parse_utm(url)
        assert params.source == "newsletter"
        assert params.medium == "email"
        assert params.campaign == "launch"
        assert params.term == "keyword"
        assert params.content == "variant_a"

    def test_ref_as_source(self):
        url = "https://example.com/?ref=partner_site"
        params = parse_utm(url)
        assert params.source == "partner_site"
        assert params.has_utm is True

    def test_no_utm_params(self):
        url = "https://example.com/page?id=123"
        params = parse_utm(url)
        assert params.has_utm is False
        assert params.source is None
        assert params.medium is None

    def test_empty_url(self):
        params = parse_utm("")
        assert params.has_utm is False

    def test_utm_with_fragment(self):
        url = "https://example.com/page#utm_source=app"
        params = parse_utm(url)
        assert params.source == "app"


class TestUserAgentParsing:
    """Test browser and OS detection from user-agents."""

    def test_chrome_macos(self):
        ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        info = parse_user_agent(ua)
        assert info.browser == "Chrome"
        assert info.browser_version == "120"
        assert info.os == "macOS"
        assert info.device_type == DeviceType.DESKTOP

    def test_safari_ios(self):
        ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
        info = parse_user_agent(ua)
        assert info.browser == "Safari"
        assert info.os == "iOS"
        assert info.os_version == "17.0"
        assert info.device_type == DeviceType.MOBILE

    def test_firefox_windows(self):
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"
        info = parse_user_agent(ua)
        assert info.browser == "Firefox"
        assert info.browser_version == "121"
        assert info.os == "Windows"
        assert info.device_type == DeviceType.DESKTOP

    def test_edge_windows(self):
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0"
        info = parse_user_agent(ua)
        assert info.browser == "Edge"
        assert info.os == "Windows"

    def test_android_chrome(self):
        ua = "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.43 Mobile Safari/537.36"
        info = parse_user_agent(ua)
        assert info.browser == "Chrome"
        assert info.os == "Android"
        assert info.device_type == DeviceType.MOBILE

    def test_ipad_safari(self):
        ua = "Mozilla/5.0 (iPad; CPU OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
        info = parse_user_agent(ua)
        assert info.browser == "Safari"
        assert info.os == "iPadOS"
        assert info.device_type == DeviceType.TABLET

    def test_empty_ua(self):
        info = parse_user_agent("")
        assert info.browser == "Unknown"
        assert info.os == "Unknown"
        assert info.device_type == DeviceType.UNKNOWN


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
