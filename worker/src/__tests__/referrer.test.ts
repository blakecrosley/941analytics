/**
 * Referrer Classification Tests
 */
import { describe, it, expect } from "vitest";
import { classifyReferrer, parseUTM } from "../utils";

describe("classifyReferrer", () => {
  describe("direct traffic", () => {
    it("returns direct for empty referrer", () => {
      const result = classifyReferrer("");
      expect(result.type).toBe("direct");
      expect(result.domain).toBe("");
    });

    it("returns direct for whitespace-only referrer", () => {
      const result = classifyReferrer("   ");
      expect(result.type).toBe("direct");
      expect(result.domain).toBe("");
    });

    it("returns direct for null-like values", () => {
      const result = classifyReferrer(null as unknown as string);
      expect(result.type).toBe("direct");
    });
  });

  describe("organic search traffic", () => {
    it("detects Google search", () => {
      const result = classifyReferrer("https://www.google.com/search?q=test");
      expect(result.type).toBe("organic");
      expect(result.domain).toBe("google.com");
    });

    it("detects Google regional domains", () => {
      const result = classifyReferrer("https://www.google.co.uk/search?q=test");
      expect(result.type).toBe("organic");
      expect(result.domain).toBe("google.co.uk");
    });

    it("detects Bing search", () => {
      const result = classifyReferrer("https://www.bing.com/search?q=test");
      expect(result.type).toBe("organic");
      expect(result.domain).toBe("bing.com");
    });

    it("detects DuckDuckGo", () => {
      const result = classifyReferrer("https://duckduckgo.com/?q=test");
      expect(result.type).toBe("organic");
      expect(result.domain).toBe("duckduckgo.com");
    });

    it("detects Yahoo search", () => {
      const result = classifyReferrer("https://search.yahoo.com/search?p=test");
      expect(result.type).toBe("organic");
      expect(result.domain).toBe("search.yahoo.com");
    });

    it("detects Ecosia", () => {
      const result = classifyReferrer("https://www.ecosia.org/search?q=test");
      expect(result.type).toBe("organic");
      expect(result.domain).toBe("ecosia.org");
    });
  });

  describe("social traffic", () => {
    it("detects Facebook", () => {
      const result = classifyReferrer("https://www.facebook.com/");
      expect(result.type).toBe("social");
      expect(result.domain).toBe("facebook.com");
    });

    it("detects Twitter/X (t.co)", () => {
      const result = classifyReferrer("https://t.co/abc123");
      expect(result.type).toBe("social");
      expect(result.domain).toBe("t.co");
    });

    it("detects Twitter", () => {
      const result = classifyReferrer("https://twitter.com/user/status/123");
      expect(result.type).toBe("social");
      expect(result.domain).toBe("twitter.com");
    });

    it("detects X.com", () => {
      const result = classifyReferrer("https://x.com/user/status/123");
      expect(result.type).toBe("social");
      expect(result.domain).toBe("x.com");
    });

    it("detects LinkedIn", () => {
      const result = classifyReferrer("https://www.linkedin.com/feed/");
      expect(result.type).toBe("social");
      expect(result.domain).toBe("linkedin.com");
    });

    it("detects Reddit", () => {
      const result = classifyReferrer("https://www.reddit.com/r/programming");
      expect(result.type).toBe("social");
      expect(result.domain).toBe("reddit.com");
    });

    it("detects YouTube", () => {
      const result = classifyReferrer("https://www.youtube.com/watch?v=abc");
      expect(result.type).toBe("social");
      expect(result.domain).toBe("youtube.com");
    });

    it("detects TikTok", () => {
      const result = classifyReferrer("https://www.tiktok.com/@user");
      expect(result.type).toBe("social");
      expect(result.domain).toBe("tiktok.com");
    });

    it("detects Discord", () => {
      const result = classifyReferrer("https://discord.com/channels/123/456");
      expect(result.type).toBe("social");
      expect(result.domain).toBe("discord.com");
    });
  });

  describe("email traffic", () => {
    it("detects Gmail", () => {
      const result = classifyReferrer("https://mail.google.com/mail/u/0/");
      expect(result.type).toBe("email");
      expect(result.domain).toBe("mail.google.com");
    });

    it("detects Outlook", () => {
      const result = classifyReferrer("https://outlook.live.com/mail/0/");
      expect(result.type).toBe("email");
      expect(result.domain).toBe("outlook.live.com");
    });

    it("detects Yahoo Mail", () => {
      const result = classifyReferrer("https://mail.yahoo.com/");
      expect(result.type).toBe("email");
      expect(result.domain).toBe("mail.yahoo.com");
    });

    it("detects generic mail subdomain", () => {
      const result = classifyReferrer("https://mail.example.com/");
      expect(result.type).toBe("email");
      expect(result.domain).toBe("mail.example.com");
    });

    it("detects Mailchimp campaign", () => {
      const result = classifyReferrer("https://mailchimp.com/campaigns/123");
      expect(result.type).toBe("email");
    });

    it("detects newsletter in URL", () => {
      const result = classifyReferrer("https://example.com/newsletter/click?id=123");
      expect(result.type).toBe("email");
    });
  });

  describe("referral traffic", () => {
    it("classifies unknown domains as referral", () => {
      const result = classifyReferrer("https://www.example.com/page");
      expect(result.type).toBe("referral");
      expect(result.domain).toBe("example.com");
    });

    it("classifies blog links as referral", () => {
      const result = classifyReferrer("https://blog.techsite.com/article");
      expect(result.type).toBe("referral");
      expect(result.domain).toBe("blog.techsite.com");
    });

    it("strips www from domain", () => {
      const result = classifyReferrer("https://www.randomsite.org/link");
      expect(result.domain).toBe("randomsite.org");
    });
  });

  describe("edge cases", () => {
    it("handles invalid URLs gracefully", () => {
      const result = classifyReferrer("not-a-valid-url");
      expect(result.type).toBe("referral");
      expect(result.domain).toBe("not-a-valid-url");
    });

    it("handles URLs without protocol", () => {
      const result = classifyReferrer("google.com/search");
      expect(result.type).toBe("organic");
    });
  });
});

describe("parseUTM", () => {
  describe("standard UTM parameters", () => {
    it("extracts all UTM parameters", () => {
      const result = parseUTM(
        "https://example.com/page?utm_source=google&utm_medium=cpc&utm_campaign=spring_sale&utm_term=shoes&utm_content=banner_1"
      );
      expect(result.source).toBe("google");
      expect(result.medium).toBe("cpc");
      expect(result.campaign).toBe("spring_sale");
      expect(result.term).toBe("shoes");
      expect(result.content).toBe("banner_1");
    });

    it("handles partial UTM parameters", () => {
      const result = parseUTM("https://example.com/?utm_source=newsletter&utm_campaign=weekly");
      expect(result.source).toBe("newsletter");
      expect(result.medium).toBe("");
      expect(result.campaign).toBe("weekly");
      expect(result.term).toBe("");
      expect(result.content).toBe("");
    });

    it("handles ref parameter as source fallback", () => {
      const result = parseUTM("https://example.com/?ref=twitter");
      expect(result.source).toBe("twitter");
    });

    it("prefers utm_source over ref", () => {
      const result = parseUTM("https://example.com/?utm_source=google&ref=twitter");
      expect(result.source).toBe("google");
    });
  });

  describe("value truncation", () => {
    it("truncates long values to 200 characters", () => {
      const longValue = "a".repeat(300);
      const result = parseUTM(`https://example.com/?utm_source=${longValue}`);
      expect(result.source.length).toBe(200);
    });
  });

  describe("edge cases", () => {
    it("returns empty values for URL without UTM params", () => {
      const result = parseUTM("https://example.com/page");
      expect(result.source).toBe("");
      expect(result.medium).toBe("");
      expect(result.campaign).toBe("");
      expect(result.term).toBe("");
      expect(result.content).toBe("");
    });

    it("handles invalid URLs gracefully", () => {
      const result = parseUTM("not-a-valid-url");
      expect(result.source).toBe("");
    });

    it("handles URL-encoded values", () => {
      const result = parseUTM("https://example.com/?utm_source=email%20marketing&utm_campaign=spring%20sale");
      expect(result.source).toBe("email marketing");
      expect(result.campaign).toBe("spring sale");
    });

    it("handles empty parameter values", () => {
      const result = parseUTM("https://example.com/?utm_source=&utm_medium=cpc");
      expect(result.source).toBe("");
      expect(result.medium).toBe("cpc");
    });
  });
});
