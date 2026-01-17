/**
 * 941 Analytics - Cloudflare Worker
 *
 * Privacy-first pageview collection with enhanced attribution:
 * - Bot detection and categorization
 * - Referrer classification (direct/organic/social/email/referral)
 * - UTM parameter extraction
 * - Browser and OS detection
 * - Rate limiting (privacy-preserving)
 * - Origin validation
 * - Daily aggregation (scheduled)
 * - Data retention cleanup (scheduled)
 *
 * PRIVACY GUARANTEES:
 * - No cookies or persistent identifiers
 * - No IP addresses stored
 * - Daily-rotating visitor hash (can't track across days)
 * - Rate limit keys are hashed and expire quickly
 */

interface Env {
  DB: D1Database;
  ANALYTICS_SECRET: string;
  ALLOWED_ORIGINS?: string; // Comma-separated list of allowed origins
  RATE_LIMIT_KV?: KVNamespace; // Optional KV for rate limiting
}

interface PageViewData {
  site: string;
  url: string;
  title: string;
  ref: string;
  w: number;
}

// =============================================================================
// CONFIGURATION
// =============================================================================

const CONFIG = {
  // Rate limiting
  RATE_LIMIT_REQUESTS: 100,     // Max requests per window
  RATE_LIMIT_WINDOW_SEC: 60,    // Window size in seconds

  // Data retention
  RETENTION_DAYS: 90,           // Keep raw data for 90 days

  // Aggregation
  TOP_PAGES_LIMIT: 50,          // Store top N pages per day
  TOP_REFERRERS_LIMIT: 30,      // Store top N referrers per day
};

// =============================================================================
// DEVELOPMENT TRAFFIC FILTER
// =============================================================================

const DEV_HOSTNAMES = [
  'localhost',
  '127.0.0.1',
  '0.0.0.0',
  '[::1]',
];

const DEV_PATTERNS = [
  /^https?:\/\/localhost(:\d+)?/i,
  /^https?:\/\/127\.0\.0\.1(:\d+)?/i,
  /^https?:\/\/0\.0\.0\.0(:\d+)?/i,
  /^https?:\/\/\[::1\](:\d+)?/i,
  /^https?:\/\/.*\.local(:\d+)?/i,
];

function isDevTraffic(url: string): boolean {
  if (!url) return false;

  // Check patterns
  for (const pattern of DEV_PATTERNS) {
    if (pattern.test(url)) {
      return true;
    }
  }

  // Parse URL and check hostname
  try {
    const parsed = new URL(url);
    const hostname = parsed.hostname.toLowerCase();
    if (DEV_HOSTNAMES.includes(hostname) || hostname.endsWith('.local')) {
      return true;
    }
  } catch {
    // Invalid URL
  }

  return false;
}

// =============================================================================
// BOT DETECTION
// =============================================================================

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

  for (const [category, patterns] of Object.entries(BOT_PATTERNS)) {
    for (const [pattern, name] of Object.entries(patterns)) {
      if (uaLower.includes(pattern)) {
        return { isBot: true, name, category };
      }
    }
  }

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
  type: string;
  domain: string;
}

const SEARCH_ENGINES = [
  "google.", "bing.com", "yahoo.", "duckduckgo.com", "baidu.com",
  "yandex.", "ecosia.org", "qwant.com", "startpage.com", "brave.com",
];

const SOCIAL_PLATFORMS = [
  "facebook.com", "fb.com", "t.co", "twitter.com", "x.com",
  "linkedin.com", "instagram.com", "pinterest.com", "reddit.com",
  "youtube.com", "tiktok.com", "threads.net", "mastodon.",
  "discord.com", "telegram.org", "whatsapp.com",
];

const EMAIL_INDICATORS = [
  "mail.google.com", "outlook.live.com", "mail.yahoo.com",
  "mail.", "webmail.", "newsletter", "campaign",
  "mailchimp", "sendgrid", "constantcontact",
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

  // Check email first (more specific patterns)
  for (const email of EMAIL_INDICATORS) {
    if (domain.includes(email) || referrerLower.includes(email)) {
      return { type: "email", domain };
    }
  }

  for (const se of SEARCH_ENGINES) {
    if (domain.includes(se)) {
      return { type: "organic", domain };
    }
  }

  for (const social of SOCIAL_PLATFORMS) {
    if (domain.includes(social)) {
      return { type: "social", domain };
    }
  }

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
  const result: UTMParams = { source: "", medium: "", campaign: "", term: "", content: "" };

  try {
    const urlObj = new URL(url);
    const params = urlObj.searchParams;

    result.source = (params.get("utm_source") || params.get("ref") || "").slice(0, 200);
    result.medium = (params.get("utm_medium") || "").slice(0, 200);
    result.campaign = (params.get("utm_campaign") || "").slice(0, 200);
    result.term = (params.get("utm_term") || "").slice(0, 200);
    result.content = (params.get("utm_content") || "").slice(0, 200);
  } catch {
    // Invalid URL
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

  // Device type
  if (/iPad/i.test(ua)) {
    result.deviceType = "tablet";
  } else if (/Mobile|Android.*Mobile|iPhone|iPod|BlackBerry|IEMobile|Opera Mini/i.test(ua)) {
    result.deviceType = "mobile";
  } else if (/Android/i.test(ua)) {
    result.deviceType = "tablet";
  } else if (/SmartTV|Smart-TV|BRAVIA|AppleTV|FireTV|Roku|Chromecast/i.test(ua)) {
    result.deviceType = "tv";
  }

  // Browser detection
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
  return hashArray.slice(0, 8).map((b) => b.toString(16).padStart(2, "0")).join("");
}

// =============================================================================
// RATE LIMITING (Privacy-Preserving)
// =============================================================================

async function hashForRateLimit(ip: string, secret: string): Promise<string> {
  // Hash IP with secret so we can't reverse it
  const data = `ratelimit:${secret}:${ip}`;
  const encoder = new TextEncoder();
  const hashBuffer = await crypto.subtle.digest("SHA-256", encoder.encode(data));
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.slice(0, 8).map((b) => b.toString(16).padStart(2, "0")).join("");
}

async function checkRateLimit(
  ip: string,
  env: Env
): Promise<{ allowed: boolean; remaining: number }> {
  // If no KV namespace configured, allow all
  if (!env.RATE_LIMIT_KV) {
    return { allowed: true, remaining: CONFIG.RATE_LIMIT_REQUESTS };
  }

  const key = await hashForRateLimit(ip, env.ANALYTICS_SECRET);
  const currentStr = await env.RATE_LIMIT_KV.get(key);
  const current = currentStr ? parseInt(currentStr, 10) : 0;

  if (current >= CONFIG.RATE_LIMIT_REQUESTS) {
    return { allowed: false, remaining: 0 };
  }

  // Increment counter with TTL
  await env.RATE_LIMIT_KV.put(key, String(current + 1), {
    expirationTtl: CONFIG.RATE_LIMIT_WINDOW_SEC,
  });

  return { allowed: true, remaining: CONFIG.RATE_LIMIT_REQUESTS - current - 1 };
}

// =============================================================================
// ORIGIN VALIDATION
// =============================================================================

function validateOrigin(request: Request, env: Env): boolean {
  // If no allowed origins configured, allow all (for development)
  if (!env.ALLOWED_ORIGINS) {
    return true;
  }

  const allowedOrigins = env.ALLOWED_ORIGINS.split(",").map((o) => o.trim().toLowerCase());

  // Check Origin header first
  const origin = request.headers.get("Origin");
  if (origin) {
    try {
      const originHost = new URL(origin).hostname.toLowerCase();
      if (allowedOrigins.some((allowed) => originHost === allowed || originHost.endsWith("." + allowed))) {
        return true;
      }
    } catch {
      // Invalid origin URL
    }
  }

  // Fall back to Referer header
  const referer = request.headers.get("Referer");
  if (referer) {
    try {
      const refererHost = new URL(referer).hostname.toLowerCase();
      if (allowedOrigins.some((allowed) => refererHost === allowed || refererHost.endsWith("." + allowed))) {
        return true;
      }
    } catch {
      // Invalid referer URL
    }
  }

  // If site parameter matches allowed origins, allow it (for scripts loaded from allowed sites)
  // This is checked in the main handler

  return false;
}

// =============================================================================
// DAILY AGGREGATION
// =============================================================================

interface AggregatedStats {
  total_views: number;
  unique_visitors: number;
  bot_views: number;
  top_pages: Array<{ url: string; views: number }>;
  top_referrers: Array<{ domain: string; type: string; views: number }>;
  countries: Record<string, number>;
  devices: Record<string, number>;
  browsers: Record<string, number>;
  operating_systems: Record<string, number>;
  referrer_types: Record<string, number>;
  utm_sources: Record<string, number>;
  utm_campaigns: Record<string, number>;
  bot_breakdown: Record<string, number>;
}

async function aggregateDay(db: D1Database, site: string, dateStr: string): Promise<AggregatedStats> {
  // Total views and unique visitors (humans only)
  const totalsResult = await db.prepare(`
    SELECT
      COUNT(*) as total_views,
      COUNT(DISTINCT visitor_hash) as unique_visitors
    FROM page_views
    WHERE site = ? AND date(timestamp) = ? AND is_bot = 0
  `).bind(site, dateStr).first<{ total_views: number; unique_visitors: number }>();

  // Bot views
  const botResult = await db.prepare(`
    SELECT COUNT(*) as bot_views
    FROM page_views
    WHERE site = ? AND date(timestamp) = ? AND is_bot = 1
  `).bind(site, dateStr).first<{ bot_views: number }>();

  // Top pages
  const pagesResult = await db.prepare(`
    SELECT url, COUNT(*) as views
    FROM page_views
    WHERE site = ? AND date(timestamp) = ? AND is_bot = 0
    GROUP BY url ORDER BY views DESC LIMIT ?
  `).bind(site, dateStr, CONFIG.TOP_PAGES_LIMIT).all<{ url: string; views: number }>();

  // Top referrers
  const referrersResult = await db.prepare(`
    SELECT referrer_domain as domain, referrer_type as type, COUNT(*) as views
    FROM page_views
    WHERE site = ? AND date(timestamp) = ? AND is_bot = 0 AND referrer_domain != ''
    GROUP BY referrer_domain ORDER BY views DESC LIMIT ?
  `).bind(site, dateStr, CONFIG.TOP_REFERRERS_LIMIT).all<{ domain: string; type: string; views: number }>();

  // Countries
  const countriesResult = await db.prepare(`
    SELECT country, COUNT(*) as views
    FROM page_views
    WHERE site = ? AND date(timestamp) = ? AND is_bot = 0 AND country != ''
    GROUP BY country
  `).bind(site, dateStr).all<{ country: string; views: number }>();

  // Devices
  const devicesResult = await db.prepare(`
    SELECT device_type, COUNT(*) as views
    FROM page_views
    WHERE site = ? AND date(timestamp) = ? AND is_bot = 0
    GROUP BY device_type
  `).bind(site, dateStr).all<{ device_type: string; views: number }>();

  // Browsers
  const browsersResult = await db.prepare(`
    SELECT browser, COUNT(*) as views
    FROM page_views
    WHERE site = ? AND date(timestamp) = ? AND is_bot = 0 AND browser != ''
    GROUP BY browser
  `).bind(site, dateStr).all<{ browser: string; views: number }>();

  // Operating systems
  const osResult = await db.prepare(`
    SELECT os, COUNT(*) as views
    FROM page_views
    WHERE site = ? AND date(timestamp) = ? AND is_bot = 0 AND os != ''
    GROUP BY os
  `).bind(site, dateStr).all<{ os: string; views: number }>();

  // Referrer types
  const refTypesResult = await db.prepare(`
    SELECT referrer_type, COUNT(*) as views
    FROM page_views
    WHERE site = ? AND date(timestamp) = ? AND is_bot = 0
    GROUP BY referrer_type
  `).bind(site, dateStr).all<{ referrer_type: string; views: number }>();

  // UTM sources
  const utmSourcesResult = await db.prepare(`
    SELECT utm_source, COUNT(*) as views
    FROM page_views
    WHERE site = ? AND date(timestamp) = ? AND is_bot = 0 AND utm_source != ''
    GROUP BY utm_source
  `).bind(site, dateStr).all<{ utm_source: string; views: number }>();

  // UTM campaigns
  const utmCampaignsResult = await db.prepare(`
    SELECT utm_campaign, COUNT(*) as views
    FROM page_views
    WHERE site = ? AND date(timestamp) = ? AND is_bot = 0 AND utm_campaign != ''
    GROUP BY utm_campaign
  `).bind(site, dateStr).all<{ utm_campaign: string; views: number }>();

  // Bot breakdown
  const botBreakdownResult = await db.prepare(`
    SELECT bot_category, COUNT(*) as views
    FROM page_views
    WHERE site = ? AND date(timestamp) = ? AND is_bot = 1
    GROUP BY bot_category
  `).bind(site, dateStr).all<{ bot_category: string; views: number }>();

  // Build result
  const countries: Record<string, number> = {};
  countriesResult.results?.forEach((r) => { countries[r.country] = r.views; });

  const devices: Record<string, number> = {};
  devicesResult.results?.forEach((r) => { devices[r.device_type || "unknown"] = r.views; });

  const browsers: Record<string, number> = {};
  browsersResult.results?.forEach((r) => { browsers[r.browser] = r.views; });

  const operating_systems: Record<string, number> = {};
  osResult.results?.forEach((r) => { operating_systems[r.os] = r.views; });

  const referrer_types: Record<string, number> = {};
  refTypesResult.results?.forEach((r) => { referrer_types[r.referrer_type || "direct"] = r.views; });

  const utm_sources: Record<string, number> = {};
  utmSourcesResult.results?.forEach((r) => { utm_sources[r.utm_source] = r.views; });

  const utm_campaigns: Record<string, number> = {};
  utmCampaignsResult.results?.forEach((r) => { utm_campaigns[r.utm_campaign] = r.views; });

  const bot_breakdown: Record<string, number> = {};
  botBreakdownResult.results?.forEach((r) => { bot_breakdown[r.bot_category || "unknown"] = r.views; });

  return {
    total_views: totalsResult?.total_views || 0,
    unique_visitors: totalsResult?.unique_visitors || 0,
    bot_views: botResult?.bot_views || 0,
    top_pages: pagesResult.results || [],
    top_referrers: referrersResult.results || [],
    countries,
    devices,
    browsers,
    operating_systems,
    referrer_types,
    utm_sources,
    utm_campaigns,
    bot_breakdown,
  };
}

async function saveAggregatedStats(
  db: D1Database,
  site: string,
  dateStr: string,
  stats: AggregatedStats
): Promise<void> {
  // Use INSERT OR REPLACE to handle re-runs
  await db.prepare(`
    INSERT OR REPLACE INTO daily_stats (
      site, date, total_views, unique_visitors, bot_views,
      top_pages, top_referrers, countries, devices, browsers,
      operating_systems, referrer_types, utm_sources, utm_campaigns, bot_breakdown
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).bind(
    site,
    dateStr,
    stats.total_views,
    stats.unique_visitors,
    stats.bot_views,
    JSON.stringify(stats.top_pages),
    JSON.stringify(stats.top_referrers),
    JSON.stringify(stats.countries),
    JSON.stringify(stats.devices),
    JSON.stringify(stats.browsers),
    JSON.stringify(stats.operating_systems),
    JSON.stringify(stats.referrer_types),
    JSON.stringify(stats.utm_sources),
    JSON.stringify(stats.utm_campaigns),
    JSON.stringify(stats.bot_breakdown),
  ).run();
}

// =============================================================================
// DATA RETENTION CLEANUP
// =============================================================================

async function cleanupOldData(db: D1Database): Promise<number> {
  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - CONFIG.RETENTION_DAYS);
  const cutoffStr = cutoffDate.toISOString().split("T")[0];

  const result = await db.prepare(`
    DELETE FROM page_views WHERE date(timestamp) < ?
  `).bind(cutoffStr).run();

  return result.meta.changes || 0;
}

// =============================================================================
// SCHEDULED HANDLER
// =============================================================================

async function handleScheduled(env: Env): Promise<void> {
  console.log("Starting scheduled aggregation job...");

  // Get yesterday's date
  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  const yesterdayStr = yesterday.toISOString().split("T")[0];

  // Get all unique sites that had traffic yesterday
  const sitesResult = await env.DB.prepare(`
    SELECT DISTINCT site FROM page_views WHERE date(timestamp) = ?
  `).bind(yesterdayStr).all<{ site: string }>();

  const sites = sitesResult.results || [];
  console.log(`Found ${sites.length} sites with traffic on ${yesterdayStr}`);

  // Aggregate each site
  for (const { site } of sites) {
    try {
      console.log(`Aggregating ${site}...`);
      const stats = await aggregateDay(env.DB, site, yesterdayStr);
      await saveAggregatedStats(env.DB, site, yesterdayStr, stats);
      console.log(`  ✓ ${site}: ${stats.total_views} views, ${stats.unique_visitors} visitors, ${stats.bot_views} bots`);
    } catch (error) {
      console.error(`  ✗ Error aggregating ${site}:`, error);
    }
  }

  // Cleanup old raw data
  console.log("Cleaning up old data...");
  const deleted = await cleanupOldData(env.DB);
  console.log(`  ✓ Deleted ${deleted} old pageview records`);

  console.log("Scheduled job complete.");
}

// =============================================================================
// TRACKING SCRIPT (served at /track.js)
// =============================================================================

const TRACKING_SCRIPT = `
(function() {
  'use strict';

  // Get script element and config
  var s = document.currentScript;
  var endpoint = s?.getAttribute('data-endpoint') || '';
  var site = s?.getAttribute('data-site') || '';

  if (!endpoint || !site) return;

  // Send pageview
  function track(extra) {
    var params = new URLSearchParams({
      site: site,
      url: window.location.href,
      title: document.title || '',
      ref: document.referrer || '',
      w: String(window.innerWidth || 0)
    });

    // Add any extra params
    if (extra) {
      for (var k in extra) params.set(k, extra[k]);
    }

    // Use image beacon for reliability
    var img = new Image();
    img.src = endpoint + '?' + params.toString();
  }

  // Track initial pageview
  if (document.readyState === 'complete') {
    track();
  } else {
    window.addEventListener('load', function() { track(); });
  }

  // Handle SPA navigation (History API)
  var pushState = history.pushState;
  history.pushState = function() {
    pushState.apply(history, arguments);
    setTimeout(track, 100);
  };

  window.addEventListener('popstate', function() {
    setTimeout(track, 100);
  });

  // Expose for manual tracking
  window._941 = { track: track };
})();
`;

// =============================================================================
// 1x1 TRANSPARENT GIF
// =============================================================================

const TRANSPARENT_GIF = new Uint8Array([
  0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x01, 0x00, 0x01, 0x00, 0x80, 0x00,
  0x00, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x21, 0xf9, 0x04, 0x01, 0x00,
  0x00, 0x00, 0x00, 0x2c, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00,
  0x00, 0x02, 0x02, 0x44, 0x01, 0x00, 0x3b,
]);

// =============================================================================
// STATS API HANDLER
// =============================================================================

interface StatsResponse {
  site: string;
  period: string;
  generated_at: string;
  summary: {
    total_views: number;
    unique_visitors: number;
    sessions: number;
    bot_views: number;
  };
  top_pages: Array<{ url: string; views: number }>;
  countries: Array<{ country: string; views: number }>;
  devices: Array<{ device_type: string; views: number }>;
  recent_visitors: Array<{
    url: string;
    country: string;
    device_type: string;
    timestamp: string;
    city: string;
  }>;
}

async function handleStats(
  url: URL,
  env: Env,
  corsHeaders: Record<string, string>
): Promise<Response> {
  try {
    const site = url.searchParams.get("site");
    const period = url.searchParams.get("period") || "today";

    if (!site) {
      return new Response(JSON.stringify({ error: "Missing site parameter" }), {
        status: 400,
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    // Calculate date range
    // Helper to format date as SQLite datetime (YYYY-MM-DD HH:MM:SS)
    const toSqliteDatetime = (d: Date): string => {
      return d.toISOString().replace('T', ' ').slice(0, 19);
    };

    const now = new Date();
    let startDate: string;
    if (period === "today") {
      startDate = toSqliteDatetime(new Date(now.getFullYear(), now.getMonth(), now.getDate()));
    } else if (period === "7d") {
      startDate = toSqliteDatetime(new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000));
    } else if (period === "30d") {
      startDate = toSqliteDatetime(new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000));
    } else {
      startDate = toSqliteDatetime(new Date(now.getFullYear(), now.getMonth(), now.getDate()));
    }

    // Query real-time stats from D1
    const statsQuery = `
      SELECT
        COUNT(*) as total_views,
        COUNT(DISTINCT visitor_hash) as unique_visitors,
        COUNT(DISTINCT session_id) as sessions,
        SUM(CASE WHEN is_bot = 1 THEN 1 ELSE 0 END) as bot_views
      FROM page_views
      WHERE site = ? AND timestamp >= ?
    `;

    const topPagesQuery = `
      SELECT url, COUNT(*) as views
      FROM page_views
      WHERE site = ? AND timestamp >= ? AND is_bot = 0
      GROUP BY url
      ORDER BY views DESC
      LIMIT 10
    `;

    const countriesQuery = `
      SELECT country, COUNT(*) as views
      FROM page_views
      WHERE site = ? AND timestamp >= ? AND is_bot = 0 AND country != ''
      GROUP BY country
      ORDER BY views DESC
      LIMIT 10
    `;

    const devicesQuery = `
      SELECT device_type, COUNT(*) as views
      FROM page_views
      WHERE site = ? AND timestamp >= ? AND is_bot = 0
      GROUP BY device_type
      ORDER BY views DESC
    `;

    const recentQuery = `
      SELECT url, country, device_type, timestamp, city
      FROM page_views
      WHERE site = ? AND is_bot = 0
      ORDER BY id DESC
      LIMIT 10
    `;

    // Execute queries in parallel
    const [statsResult, topPagesResult, countriesResult, devicesResult, recentResult] =
      await Promise.all([
        env.DB.prepare(statsQuery).bind(site, startDate).first(),
        env.DB.prepare(topPagesQuery).bind(site, startDate).all(),
        env.DB.prepare(countriesQuery).bind(site, startDate).all(),
        env.DB.prepare(devicesQuery).bind(site, startDate).all(),
        env.DB.prepare(recentQuery).bind(site).all(),
      ]);

    const response: StatsResponse = {
      site,
      period,
      generated_at: new Date().toISOString(),
      summary: {
        total_views: (statsResult?.total_views as number) || 0,
        unique_visitors: (statsResult?.unique_visitors as number) || 0,
        sessions: (statsResult?.sessions as number) || 0,
        bot_views: (statsResult?.bot_views as number) || 0,
      },
      top_pages: (topPagesResult?.results as Array<{ url: string; views: number }>) || [],
      countries: (countriesResult?.results as Array<{ country: string; views: number }>) || [],
      devices: (devicesResult?.results as Array<{ device_type: string; views: number }>) || [],
      recent_visitors:
        (recentResult?.results as Array<{
          url: string;
          country: string;
          device_type: string;
          timestamp: string;
          city: string;
        }>) || [],
    };

    return new Response(JSON.stringify(response), {
      headers: {
        ...corsHeaders,
        "Content-Type": "application/json",
        "Cache-Control": "no-cache, no-store, must-revalidate",
      },
    });
  } catch (error) {
    console.error("Stats API error:", error);
    return new Response(JSON.stringify({ error: "Internal server error" }), {
      status: 500,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }
}

// =============================================================================
// MAIN WORKER EXPORT
// =============================================================================

export default {
  // HTTP request handler
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
    };

    // Handle preflight
    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }

    // Serve tracking script
    if (url.pathname === "/track.js" && request.method === "GET") {
      return new Response(TRACKING_SCRIPT, {
        headers: {
          ...corsHeaders,
          "Content-Type": "application/javascript",
          "Cache-Control": "public, max-age=3600", // Cache for 1 hour
        },
      });
    }

    // Handle /stats endpoint for real-time analytics
    if (url.pathname === "/stats" && request.method === "GET") {
      return handleStats(url, env, corsHeaders);
    }

    // Only handle GET /collect
    if (url.pathname !== "/collect" || request.method !== "GET") {
      return new Response("Not Found", { status: 404 });
    }

    try {
      // Get client IP for rate limiting
      const clientIP = request.headers.get("CF-Connecting-IP") || "unknown";

      // Check rate limit
      const rateLimit = await checkRateLimit(clientIP, env);
      if (!rateLimit.allowed) {
        return new Response("Rate limit exceeded", {
          status: 429,
          headers: {
            ...corsHeaders,
            "Retry-After": String(CONFIG.RATE_LIMIT_WINDOW_SEC),
          },
        });
      }

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

      // Filter development/local traffic (don't pollute analytics with dev pageviews)
      if (isDevTraffic(data.url)) {
        // Silently accept but don't record - return tracking pixel to not break dev experience
        return new Response(TRANSPARENT_GIF, {
          status: 200,
          headers: {
            ...corsHeaders,
            "Content-Type": "image/gif",
            "Cache-Control": "no-store, no-cache, must-revalidate",
            "X-Dev-Traffic": "filtered",
          },
        });
      }

      // Origin validation - check if site is in allowed list OR if origin/referer matches
      const allowedOrigins = (env.ALLOWED_ORIGINS || "").split(",").map((o) => o.trim().toLowerCase()).filter(Boolean);
      if (allowedOrigins.length > 0) {
        const siteAllowed = allowedOrigins.some((allowed) =>
          data.site.toLowerCase() === allowed || data.site.toLowerCase().endsWith("." + allowed)
        );
        const originValid = validateOrigin(request, env);

        if (!siteAllowed && !originValid) {
          console.log(`Rejected request: site=${data.site}, origin validation failed`);
          return new Response("Origin not allowed", { status: 403 });
        }
      }

      // Get user agent
      const userAgent = request.headers.get("User-Agent") || "";

      // Run all detection
      const botInfo = detectBot(userAgent);
      const referrerInfo = classifyReferrer(data.ref);
      const utmParams = parseUTM(data.url);
      const deviceInfo = parseUserAgent(userAgent);
      const deviceType = deviceInfo.deviceType !== "desktop" ? deviceInfo.deviceType : getDeviceType(data.w);

      // Get geo data
      const cf = (request.cf as Record<string, unknown>) || {};
      const country = (cf.country as string) || "";
      const region = (cf.region as string) || "";
      const city = (cf.city as string) || "";
      const latitude = (cf.latitude as number) || null;
      const longitude = (cf.longitude as number) || null;

      // Generate visitor hash
      const visitorHash = await generateVisitorHash(data.site, country, region, env.ANALYTICS_SECRET);

      // Insert into D1
      await env.DB.prepare(`
        INSERT INTO page_views (
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
        )
      `).bind(
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
        userAgent.slice(0, 500),
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
      ).run();

      // Return tracking pixel
      return new Response(TRANSPARENT_GIF, {
        status: 200,
        headers: {
          ...corsHeaders,
          "Content-Type": "image/gif",
          "Cache-Control": "no-store, no-cache, must-revalidate",
          "X-RateLimit-Remaining": String(rateLimit.remaining),
        },
      });
    } catch (error) {
      console.error("Analytics error:", error);
      return new Response("OK", { status: 200, headers: corsHeaders });
    }
  },

  // Scheduled (cron) handler
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    ctx.waitUntil(handleScheduled(env));
  },
};
