/**
 * 941 Analytics - Cloudflare Worker
 *
 * Privacy-first pageview collection with enhanced attribution:
 * - Bot detection and categorization
 * - Referrer classification (direct/organic/social/email/referral)
 * - UTM parameter extraction
 * - Browser and OS detection
 *
 * PRIVACY GUARANTEES:
 * - No cookies or persistent identifiers
 * - No IP addresses stored
 * - Daily-rotating visitor hash (can't track across days)
 * - User-agent stored only for aggregated browser/OS stats
 */

interface Env {
  DB: D1Database;
  ANALYTICS_SECRET: string;
}

interface PageViewData {
  site: string;
  url: string;
  title: string;
  ref: string;    // Full referrer URL
  w: number;      // Viewport width
}

// =============================================================================
// BOT DETECTION
// =============================================================================
// Comprehensive bot patterns organized by category

interface BotInfo {
  isBot: boolean;
  name: string;
  category: string;
}

const BOT_PATTERNS: Record<string, Record<string, string>> = {
  search_engine: {
    googlebot: "Google",
    "google-inspectiontool": "Google",
    bingbot: "Bing",
    bingpreview: "Bing",
    yandexbot: "Yandex",
    duckduckbot: "DuckDuckGo",
    baiduspider: "Baidu",
    applebot: "Apple",
    petalbot: "Huawei",
  },
  ai_crawler: {
    gptbot: "OpenAI GPT",
    "chatgpt-user": "ChatGPT",
    "anthropic-ai": "Anthropic",
    claudebot: "Claude",
    "claude-web": "Claude",
    perplexitybot: "Perplexity",
    "cohere-ai": "Cohere",
    "google-extended": "Google AI",
    bytespider: "ByteDance AI",
    amazonbot: "Amazon AI",
    ccbot: "Common Crawl",
  },
  seo_tool: {
    ahrefsbot: "Ahrefs",
    semrushbot: "SEMrush",
    mj12bot: "Majestic",
    dotbot: "Moz",
    rogerbot: "Moz",
    "screaming frog": "Screaming Frog",
    dataforseo: "DataForSEO",
  },
  social_preview: {
    facebookexternalhit: "Facebook",
    facebookcatalog: "Facebook",
    "meta-externalagent": "Meta",
    twitterbot: "Twitter",
    linkedinbot: "LinkedIn",
    pinterestbot: "Pinterest",
    slackbot: "Slack",
    discordbot: "Discord",
    telegrambot: "Telegram",
    whatsapp: "WhatsApp",
    redditbot: "Reddit",
  },
  monitoring: {
    uptimerobot: "UptimeRobot",
    pingdom: "Pingdom",
    site24x7: "Site24x7",
    statuscake: "StatusCake",
    newrelicpinger: "New Relic",
    datadog: "Datadog",
  },
  library: {
    "python-requests": "Python Requests",
    "go-http-client": "Go HTTP",
    curl: "cURL",
    wget: "Wget",
    axios: "Axios",
    "node-fetch": "Node.js",
  },
  headless: {
    headlesschrome: "Headless Chrome",
    phantomjs: "PhantomJS",
    selenium: "Selenium",
    puppeteer: "Puppeteer",
    playwright: "Playwright",
  },
};

const GENERIC_BOT_PATTERNS = [
  /\bbot\b/i,
  /crawl/i,
  /spider/i,
  /scrape/i,
  /fetch/i,
  /index/i,
  /monitor/i,
  /preview/i,
];

function detectBot(userAgent: string): BotInfo {
  if (!userAgent || !userAgent.trim()) {
    return { isBot: true, name: "Empty UA", category: "unknown" };
  }

  const uaLower = userAgent.toLowerCase();

  // Check known patterns by category
  for (const [category, patterns] of Object.entries(BOT_PATTERNS)) {
    for (const [pattern, name] of Object.entries(patterns)) {
      if (uaLower.includes(pattern)) {
        return { isBot: true, name, category };
      }
    }
  }

  // Check generic patterns
  for (const pattern of GENERIC_BOT_PATTERNS) {
    if (pattern.test(uaLower)) {
      return { isBot: true, name: "Unknown Bot", category: "unknown" };
    }
  }

  return { isBot: false, name: "", category: "" };
}

// =============================================================================
// REFERRER CLASSIFICATION
// =============================================================================

interface ReferrerInfo {
  type: string;   // direct, organic, social, email, referral, paid
  domain: string;
}

const SEARCH_ENGINES = [
  "google.",
  "bing.com",
  "yahoo.",
  "duckduckgo.com",
  "baidu.com",
  "yandex.",
  "ecosia.org",
  "qwant.com",
  "startpage.com",
  "brave.com",
];

const SOCIAL_PLATFORMS = [
  "facebook.com",
  "fb.com",
  "t.co",
  "twitter.com",
  "x.com",
  "linkedin.com",
  "instagram.com",
  "pinterest.com",
  "reddit.com",
  "youtube.com",
  "tiktok.com",
  "threads.net",
  "mastodon.",
  "discord.com",
  "telegram.org",
  "whatsapp.com",
];

const EMAIL_INDICATORS = [
  "mail.google.com",
  "outlook.live.com",
  "mail.yahoo.com",
  "mail.",
  "webmail.",
  "newsletter",
  "campaign",
  "mailchimp",
  "sendgrid",
  "constantcontact",
];

function classifyReferrer(referrer: string): ReferrerInfo {
  if (!referrer || !referrer.trim()) {
    return { type: "direct", domain: "" };
  }

  let domain = "";
  try {
    const url = new URL(referrer);
    domain = url.hostname.toLowerCase().replace(/^www\./, "");
  } catch {
    domain = referrer.toLowerCase();
  }

  const referrerLower = referrer.toLowerCase();

  // Check email indicators FIRST (more specific patterns like mail.google.com)
  for (const email of EMAIL_INDICATORS) {
    if (domain.includes(email) || referrerLower.includes(email)) {
      return { type: "email", domain };
    }
  }

  // Check search engines (organic)
  for (const se of SEARCH_ENGINES) {
    if (domain.includes(se)) {
      return { type: "organic", domain };
    }
  }

  // Check social platforms
  for (const social of SOCIAL_PLATFORMS) {
    if (domain.includes(social)) {
      return { type: "social", domain };
    }
  }

  // Default to referral
  return { type: "referral", domain };
}

// =============================================================================
// UTM PARAMETER EXTRACTION
// =============================================================================

interface UTMParams {
  source: string;
  medium: string;
  campaign: string;
  term: string;
  content: string;
}

function parseUTM(url: string): UTMParams {
  const result: UTMParams = {
    source: "",
    medium: "",
    campaign: "",
    term: "",
    content: "",
  };

  try {
    const urlObj = new URL(url);
    const params = urlObj.searchParams;

    result.source = (params.get("utm_source") || params.get("ref") || "").slice(0, 200);
    result.medium = (params.get("utm_medium") || "").slice(0, 200);
    result.campaign = (params.get("utm_campaign") || "").slice(0, 200);
    result.term = (params.get("utm_term") || "").slice(0, 200);
    result.content = (params.get("utm_content") || "").slice(0, 200);
  } catch {
    // Invalid URL, return empty params
  }

  return result;
}

// =============================================================================
// BROWSER/OS DETECTION
// =============================================================================

interface DeviceInfo {
  browser: string;
  browserVersion: string;
  os: string;
  osVersion: string;
  deviceType: string;
}

function parseUserAgent(ua: string): DeviceInfo {
  if (!ua) {
    return { browser: "Unknown", browserVersion: "", os: "Unknown", osVersion: "", deviceType: "unknown" };
  }

  const result: DeviceInfo = {
    browser: "Unknown",
    browserVersion: "",
    os: "Unknown",
    osVersion: "",
    deviceType: "desktop",
  };

  // Device type detection
  if (/iPad/i.test(ua)) {
    result.deviceType = "tablet";
  } else if (/Mobile|Android.*Mobile|iPhone|iPod|BlackBerry|IEMobile|Opera Mini/i.test(ua)) {
    result.deviceType = "mobile";
  } else if (/Android/i.test(ua)) {
    result.deviceType = "tablet";
  } else if (/SmartTV|Smart-TV|BRAVIA|AppleTV|FireTV|Roku|Chromecast/i.test(ua)) {
    result.deviceType = "tv";
  }

  // Browser detection (order matters - check specific before generic)
  const browserPatterns: [RegExp, string][] = [
    [/Edg(?:e|A|iOS)?\/(\d+)/i, "Edge"],
    [/OPR\/(\d+)/i, "Opera"],
    [/Vivaldi\/(\d+)/i, "Vivaldi"],
    [/Brave\/(\d+)/i, "Brave"],
    [/SamsungBrowser\/(\d+)/i, "Samsung Internet"],
    [/Firefox\/(\d+)/i, "Firefox"],
    [/FxiOS\/(\d+)/i, "Firefox"],
    [/CriOS\/(\d+)/i, "Chrome"],
    [/Chrome\/(\d+)/i, "Chrome"],
    [/Version\/(\d+).*Safari/i, "Safari"],
    [/Safari\/(\d+)/i, "Safari"],
    [/MSIE (\d+)/i, "Internet Explorer"],
    [/Trident.*rv:(\d+)/i, "Internet Explorer"],
  ];

  for (const [pattern, name] of browserPatterns) {
    const match = ua.match(pattern);
    if (match) {
      result.browser = name;
      result.browserVersion = match[1] || "";
      break;
    }
  }

  // OS detection
  if (/iPhone|iPod/i.test(ua)) {
    result.os = "iOS";
    const match = ua.match(/OS (\d+[_\.]\d+)/i);
    if (match) result.osVersion = match[1].replace("_", ".");
  } else if (/iPad/i.test(ua)) {
    result.os = "iPadOS";
    const match = ua.match(/OS (\d+[_\.]\d+)/i);
    if (match) result.osVersion = match[1].replace("_", ".");
  } else if (/Mac OS X/i.test(ua)) {
    result.os = "macOS";
    const match = ua.match(/Mac OS X (\d+[_\.]\d+)/i);
    if (match) result.osVersion = match[1].replace(/_/g, ".");
  } else if (/Android/i.test(ua)) {
    result.os = "Android";
    const match = ua.match(/Android (\d+\.?\d*)/i);
    if (match) result.osVersion = match[1];
  } else if (/Windows NT 10\.0/i.test(ua)) {
    result.os = "Windows";
    result.osVersion = "10/11";
  } else if (/Windows NT 6\.3/i.test(ua)) {
    result.os = "Windows";
    result.osVersion = "8.1";
  } else if (/Windows NT 6\.1/i.test(ua)) {
    result.os = "Windows";
    result.osVersion = "7";
  } else if (/Windows/i.test(ua)) {
    result.os = "Windows";
  } else if (/CrOS/i.test(ua)) {
    result.os = "Chrome OS";
  } else if (/Linux/i.test(ua)) {
    result.os = "Linux";
  }

  return result;
}

// =============================================================================
// DEVICE TYPE FROM VIEWPORT
// =============================================================================

function getDeviceType(width: number): string {
  if (width === 0) return "unknown";
  if (width < 768) return "mobile";
  if (width < 1024) return "tablet";
  return "desktop";
}

// =============================================================================
// VISITOR HASH GENERATION
// =============================================================================

async function generateVisitorHash(
  site: string,
  country: string,
  region: string,
  secret: string
): Promise<string> {
  const today = new Date().toISOString().split("T")[0];
  const data = `${secret}:${site}:${country}:${region}:${today}`;

  const encoder = new TextEncoder();
  const hashBuffer = await crypto.subtle.digest("SHA-256", encoder.encode(data));
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray
    .slice(0, 8)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

// =============================================================================
// MAIN WORKER
// =============================================================================

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    // CORS headers
    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
    };

    // Handle preflight
    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }

    // Only handle GET /collect
    if (url.pathname !== "/collect" || request.method !== "GET") {
      return new Response("Not Found", { status: 404 });
    }

    try {
      // Parse query parameters
      const params = url.searchParams;
      const data: PageViewData = {
        site: params.get("site") || "",
        url: params.get("url") || "",
        title: params.get("title") || "",
        ref: params.get("ref") || "",
        w: parseInt(params.get("w") || "0", 10),
      };

      // Validate required fields
      if (!data.site || !data.url) {
        return new Response("Missing required fields", { status: 400 });
      }

      // Get user agent from request
      const userAgent = request.headers.get("User-Agent") || "";

      // Detect bot
      const botInfo = detectBot(userAgent);

      // Classify referrer
      const referrerInfo = classifyReferrer(data.ref);

      // Parse UTM parameters
      const utmParams = parseUTM(data.url);

      // Parse browser/OS from user agent
      const deviceInfo = parseUserAgent(userAgent);

      // Get device type (prefer UA-based detection, fallback to viewport)
      const deviceType = deviceInfo.deviceType !== "desktop"
        ? deviceInfo.deviceType
        : getDeviceType(data.w);

      // Get geo data from Cloudflare
      const cf = (request.cf as Record<string, unknown>) || {};
      const country = (cf.country as string) || "";
      const region = (cf.region as string) || "";
      const city = (cf.city as string) || "";
      const latitude = (cf.latitude as number) || null;
      const longitude = (cf.longitude as number) || null;

      // Generate daily visitor hash
      const visitorHash = await generateVisitorHash(
        data.site,
        country,
        region,
        env.ANALYTICS_SECRET
      );

      // Insert into D1 with all enhanced data
      await env.DB.prepare(
        `INSERT INTO page_views (
          site, timestamp, url, page_title,
          referrer, referrer_type, referrer_domain,
          country, region, city, latitude, longitude,
          device_type, user_agent, browser, browser_version, os, os_version,
          is_bot, bot_name, bot_category,
          utm_source, utm_medium, utm_campaign, utm_term, utm_content,
          visitor_hash
        ) VALUES (
          ?, datetime('now'), ?, ?,
          ?, ?, ?,
          ?, ?, ?, ?, ?,
          ?, ?, ?, ?, ?, ?,
          ?, ?, ?,
          ?, ?, ?, ?, ?,
          ?
        )`
      )
        .bind(
          data.site,
          data.url,
          data.title,
          data.ref,
          referrerInfo.type,
          referrerInfo.domain,
          country,
          region,
          city,
          latitude,
          longitude,
          deviceType,
          userAgent.slice(0, 500), // Limit UA length for storage
          deviceInfo.browser,
          deviceInfo.browserVersion,
          deviceInfo.os,
          deviceInfo.osVersion,
          botInfo.isBot ? 1 : 0,
          botInfo.name,
          botInfo.category,
          utmParams.source,
          utmParams.medium,
          utmParams.campaign,
          utmParams.term,
          utmParams.content,
          visitorHash
        )
        .run();

      // Return 1x1 transparent GIF
      const gif = new Uint8Array([
        0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x01, 0x00, 0x01, 0x00, 0x80, 0x00,
        0x00, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x21, 0xf9, 0x04, 0x01, 0x00,
        0x00, 0x00, 0x00, 0x2c, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00,
        0x00, 0x02, 0x02, 0x44, 0x01, 0x00, 0x3b,
      ]);

      return new Response(gif, {
        status: 200,
        headers: {
          ...corsHeaders,
          "Content-Type": "image/gif",
          "Cache-Control": "no-store, no-cache, must-revalidate",
        },
      });
    } catch (error) {
      console.error("Analytics error:", error);
      // Still return success to avoid breaking the page
      return new Response("OK", {
        status: 200,
        headers: corsHeaders,
      });
    }
  },
};
