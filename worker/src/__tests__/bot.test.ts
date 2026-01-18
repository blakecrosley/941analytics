/**
 * Bot Detection Tests
 */
import { describe, it, expect } from "vitest";
import { detectBot, BotInfo } from "../utils";

describe("detectBot", () => {
  describe("search engine bots", () => {
    it("detects Googlebot", () => {
      const result = detectBot(
        "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
      );
      expect(result.isBot).toBe(true);
      expect(result.name).toBe("Google");
      expect(result.category).toBe("search_engine");
    });

    it("detects Bingbot", () => {
      const result = detectBot(
        "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)"
      );
      expect(result.isBot).toBe(true);
      expect(result.name).toBe("Bing");
      expect(result.category).toBe("search_engine");
    });

    it("detects DuckDuckBot", () => {
      const result = detectBot("DuckDuckBot/1.0; (+http://duckduckgo.com/duckduckbot.html)");
      expect(result.isBot).toBe(true);
      expect(result.name).toBe("DuckDuckGo");
      expect(result.category).toBe("search_engine");
    });
  });

  describe("AI crawlers", () => {
    it("detects GPTBot", () => {
      const result = detectBot("Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko); compatible; GPTBot/1.0");
      expect(result.isBot).toBe(true);
      expect(result.name).toBe("OpenAI GPT");
      expect(result.category).toBe("ai_crawler");
    });

    it("detects ClaudeBot", () => {
      const result = detectBot("Mozilla/5.0 ClaudeBot");
      expect(result.isBot).toBe(true);
      expect(result.name).toBe("Claude");
      expect(result.category).toBe("ai_crawler");
    });

    it("detects Anthropic AI", () => {
      const result = detectBot("anthropic-ai crawler");
      expect(result.isBot).toBe(true);
      expect(result.name).toBe("Anthropic");
      expect(result.category).toBe("ai_crawler");
    });
  });

  describe("social preview bots", () => {
    it("detects Facebook", () => {
      const result = detectBot("facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)");
      expect(result.isBot).toBe(true);
      expect(result.name).toBe("Facebook");
      expect(result.category).toBe("social_preview");
    });

    it("detects Twitter", () => {
      const result = detectBot("Twitterbot/1.0");
      expect(result.isBot).toBe(true);
      expect(result.name).toBe("Twitter");
      expect(result.category).toBe("social_preview");
    });

    it("detects Slack", () => {
      const result = detectBot("Slackbot-LinkExpanding 1.0 (+https://api.slack.com/robots)");
      expect(result.isBot).toBe(true);
      expect(result.name).toBe("Slack");
      expect(result.category).toBe("social_preview");
    });
  });

  describe("SEO tools", () => {
    it("detects Ahrefs", () => {
      const result = detectBot("Mozilla/5.0 (compatible; AhrefsBot/7.0; +http://ahrefs.com/robot/)");
      expect(result.isBot).toBe(true);
      expect(result.name).toBe("Ahrefs");
      expect(result.category).toBe("seo_tool");
    });

    it("detects SEMrush", () => {
      const result = detectBot("Mozilla/5.0 (compatible; SemrushBot/7~bl; +http://www.semrush.com/bot.html)");
      expect(result.isBot).toBe(true);
      expect(result.name).toBe("SEMrush");
      expect(result.category).toBe("seo_tool");
    });
  });

  describe("monitoring services", () => {
    it("detects UptimeRobot", () => {
      const result = detectBot("Mozilla/5.0+(compatible; UptimeRobot/2.0; http://www.uptimerobot.com/)");
      expect(result.isBot).toBe(true);
      expect(result.name).toBe("UptimeRobot");
      expect(result.category).toBe("monitoring");
    });
  });

  describe("library user agents", () => {
    it("detects Python requests", () => {
      const result = detectBot("python-requests/2.28.1");
      expect(result.isBot).toBe(true);
      expect(result.name).toBe("Python Requests");
      expect(result.category).toBe("library");
    });

    it("detects cURL", () => {
      const result = detectBot("curl/7.79.1");
      expect(result.isBot).toBe(true);
      expect(result.name).toBe("cURL");
      expect(result.category).toBe("library");
    });
  });

  describe("headless browsers", () => {
    it("detects HeadlessChrome", () => {
      const result = detectBot(
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) HeadlessChrome/91.0.4472.124 Safari/537.36"
      );
      expect(result.isBot).toBe(true);
      expect(result.name).toBe("Headless Chrome");
      expect(result.category).toBe("headless");
    });

    it("detects Puppeteer", () => {
      const result = detectBot("Puppeteer/1.0");
      expect(result.isBot).toBe(true);
      expect(result.name).toBe("Puppeteer");
      expect(result.category).toBe("headless");
    });
  });

  describe("generic bot patterns", () => {
    it("detects generic bot keyword", () => {
      const result = detectBot("MyCustomBot/1.0");
      expect(result.isBot).toBe(true);
      expect(result.name).toBe("Unknown Bot");
      expect(result.category).toBe("unknown");
    });

    it("detects crawler keyword", () => {
      const result = detectBot("WebCrawler 2.0");
      expect(result.isBot).toBe(true);
      expect(result.name).toBe("Unknown Bot");
      expect(result.category).toBe("unknown");
    });

    it("detects spider keyword", () => {
      const result = detectBot("DataSpider 1.0");
      expect(result.isBot).toBe(true);
      expect(result.name).toBe("Unknown Bot");
      expect(result.category).toBe("unknown");
    });
  });

  describe("legitimate browsers", () => {
    it("allows Chrome", () => {
      const result = detectBot(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
      );
      expect(result.isBot).toBe(false);
      expect(result.name).toBe("");
      expect(result.category).toBe("");
    });

    it("allows Firefox", () => {
      const result = detectBot(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0"
      );
      expect(result.isBot).toBe(false);
    });

    it("allows Safari", () => {
      const result = detectBot(
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15"
      );
      expect(result.isBot).toBe(false);
    });

    it("allows Edge", () => {
      const result = detectBot(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59"
      );
      expect(result.isBot).toBe(false);
    });

    it("allows mobile Safari", () => {
      const result = detectBot(
        "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1"
      );
      expect(result.isBot).toBe(false);
    });
  });

  describe("edge cases", () => {
    it("treats empty UA as bot", () => {
      const result = detectBot("");
      expect(result.isBot).toBe(true);
      expect(result.name).toBe("Empty UA");
      expect(result.category).toBe("unknown");
    });

    it("treats whitespace-only UA as bot", () => {
      const result = detectBot("   ");
      expect(result.isBot).toBe(true);
      expect(result.name).toBe("Empty UA");
    });

    it("is case-insensitive", () => {
      const result = detectBot("GOOGLEBOT");
      expect(result.isBot).toBe(true);
      expect(result.name).toBe("Google");
    });
  });
});
