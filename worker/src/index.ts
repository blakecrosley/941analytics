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
  ALLOWED_SITES?: string; // Deprecated: comma-separated list (use sites table instead)
  RATE_LIMIT_KV?: KVNamespace; // Optional KV for rate limiting
  LOGIN_RATE_LIMIT_KV?: KVNamespace; // KV for login rate limiting
  SITE_CONFIG_CACHE?: KVNamespace; // Optional KV for site config caching
}

// Session configuration
const SESSION_CONFIG = {
  TIMEOUT_MINUTES: 30,          // Session times out after 30 min inactivity
  ID_LENGTH: 16,                // Length of session ID (hex characters)
};

// Site configuration from D1
interface SiteConfig {
  id: number;
  domain: string;
  display_name: string | null;
  timezone: string;
  passkey_hash: string | null;
  is_active: boolean;
}

// In-memory cache for site configs (cleared on each request to stay fresh)
const siteConfigCache = new Map<string, { config: SiteConfig | null; timestamp: number }>();
const SITE_CONFIG_CACHE_TTL = 60 * 1000; // 1 minute in-memory cache

interface PageViewData {
  site: string;
  url: string;
  title: string;
  ref: string;
  w: number;
  sw: number;       // Screen width
  sh: number;       // Screen height
  lang: string;     // Browser language
  sid: string;      // Session ID (client-generated)
  type: string;     // 'pageview' or 'heartbeat'
}

interface EventData {
  site: string;
  url: string;
  sid: string;        // Session ID
  event_type: string; // scroll, click, form, error, custom
  event_name: string; // scroll_25, outbound_click, form_submit, etc.
  event_data?: string; // JSON stringified additional data
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
  /bot[^a-z]/i,    // "bot" followed by non-letter (catches "Bot/1.0", "bot-", etc.)
  /bot$/i,         // "bot" at end of string
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
// SESSION MANAGEMENT
// =============================================================================

interface SessionData {
  site: string;
  sessionId: string;
  visitorHash: string;
  entryPage: string;
  referrer: string;
  referrerType: string;
  referrerDomain: string;
  utmSource: string;
  utmMedium: string;
  utmCampaign: string;
  country: string;
  region: string;
  deviceType: string;
  browser: string;
  os: string;
}

/**
 * Create or update a session record.
 * On first pageview: creates session with entry page
 * On subsequent pageviews: updates last_activity, exit_page, pageview_count
 */
async function upsertSession(
  db: D1Database,
  data: SessionData,
  isHeartbeat: boolean
): Promise<void> {
  const now = new Date().toISOString().replace("T", " ").slice(0, 19);

  // Check if session already exists
  const existing = await db.prepare(`
    SELECT id, pageview_count FROM sessions WHERE session_id = ?
  `).bind(data.sessionId).first<{ id: number; pageview_count: number }>();

  if (existing) {
    // Update existing session
    if (isHeartbeat) {
      // Heartbeat only updates last_activity, doesn't increment pageview_count
      await db.prepare(`
        UPDATE sessions
        SET last_activity_at = ?
        WHERE session_id = ?
      `).bind(now, data.sessionId).run();
    } else {
      // Regular pageview updates more fields
      const newCount = existing.pageview_count + 1;
      await db.prepare(`
        UPDATE sessions
        SET last_activity_at = ?,
            exit_page = ?,
            pageview_count = ?,
            is_bounce = ?
        WHERE session_id = ?
      `).bind(
        now,
        data.entryPage, // Current page becomes potential exit page
        newCount,
        newCount === 1 ? 1 : 0, // Not a bounce if more than 1 pageview
        data.sessionId
      ).run();
    }
  } else {
    // Create new session (only on pageview, not heartbeat)
    if (!isHeartbeat) {
      await db.prepare(`
        INSERT INTO sessions (
          site, session_id, visitor_hash,
          started_at, last_activity_at,
          entry_page, exit_page, pageview_count, is_bounce,
          referrer, referrer_type, referrer_domain,
          utm_source, utm_medium, utm_campaign,
          country, region, device_type, browser, os
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).bind(
        data.site,
        data.sessionId,
        data.visitorHash,
        now,
        now,
        data.entryPage,
        data.entryPage, // Initially, entry = exit
        data.referrer,
        data.referrerType,
        data.referrerDomain,
        data.utmSource,
        data.utmMedium,
        data.utmCampaign,
        data.country,
        data.region,
        data.deviceType,
        data.browser,
        data.os
      ).run();
    }
  }
}

/**
 * Close expired sessions by calculating duration.
 * Called during scheduled aggregation.
 */
async function closeExpiredSessions(db: D1Database): Promise<number> {
  const timeoutMinutes = SESSION_CONFIG.TIMEOUT_MINUTES;

  // Close sessions where last_activity is more than 30 minutes ago and not already closed
  const result = await db.prepare(`
    UPDATE sessions
    SET ended_at = last_activity_at,
        duration_seconds = CAST(
          (julianday(last_activity_at) - julianday(started_at)) * 86400 AS INTEGER
        )
    WHERE ended_at IS NULL
      AND datetime(last_activity_at, '+${timeoutMinutes} minutes') < datetime('now')
  `).run();

  return result.meta.changes || 0;
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
// SITE CONFIGURATION (D1-based with caching)
// =============================================================================

async function getSiteConfig(domain: string, env: Env): Promise<SiteConfig | null> {
  const normalizedDomain = domain.toLowerCase().replace(/^www\./, "");

  // Check in-memory cache first
  const cached = siteConfigCache.get(normalizedDomain);
  if (cached && Date.now() - cached.timestamp < SITE_CONFIG_CACHE_TTL) {
    return cached.config;
  }

  // Check KV cache if available (longer TTL, persists across requests)
  if (env.SITE_CONFIG_CACHE) {
    const kvCached = await env.SITE_CONFIG_CACHE.get(`site:${normalizedDomain}`, "json");
    if (kvCached) {
      const config = kvCached as SiteConfig;
      siteConfigCache.set(normalizedDomain, { config, timestamp: Date.now() });
      return config;
    }
  }

  // Query D1
  const result = await env.DB.prepare(`
    SELECT id, domain, display_name, timezone, passkey_hash, is_active
    FROM sites
    WHERE domain = ? AND is_active = 1
  `).bind(normalizedDomain).first<{
    id: number;
    domain: string;
    display_name: string | null;
    timezone: string;
    passkey_hash: string | null;
    is_active: number;
  }>();

  const config: SiteConfig | null = result ? {
    id: result.id,
    domain: result.domain,
    display_name: result.display_name,
    timezone: result.timezone,
    passkey_hash: result.passkey_hash,
    is_active: result.is_active === 1,
  } : null;

  // Update in-memory cache
  siteConfigCache.set(normalizedDomain, { config, timestamp: Date.now() });

  // Update KV cache if available (5 minute TTL)
  if (env.SITE_CONFIG_CACHE && config) {
    await env.SITE_CONFIG_CACHE.put(`site:${normalizedDomain}`, JSON.stringify(config), {
      expirationTtl: 300,
    });
  }

  return config;
}

async function isValidSite(domain: string, env: Env): Promise<boolean> {
  // First check D1 sites table
  const config = await getSiteConfig(domain, env);
  if (config && config.is_active) {
    return true;
  }

  // Fallback to ALLOWED_SITES env var for backward compatibility
  const allowedSites = (env.ALLOWED_SITES || "").split(",").map((s) => s.trim().toLowerCase()).filter(Boolean);
  if (allowedSites.length > 0) {
    const siteNormalized = domain.toLowerCase();
    return allowedSites.some((allowed) =>
      siteNormalized === allowed || siteNormalized.endsWith("." + allowed)
    );
  }

  // If no sites configured at all, reject (secure by default)
  return false;
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

  // Close expired sessions
  console.log("Closing expired sessions...");
  const closedSessions = await closeExpiredSessions(env.DB);
  console.log(`  ✓ Closed ${closedSessions} expired sessions`);

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

  // Session management
  var SESSION_KEY = '_941_sid';
  var SESSION_TIMEOUT = 30 * 60 * 1000; // 30 minutes in ms
  var HEARTBEAT_INTERVAL = 30 * 1000;   // 30 seconds

  // Scroll tracking state
  var scrollDepths = { 25: false, 50: false, 75: false, 100: false };
  var scrollDebounceTimer = null;
  var SCROLL_DEBOUNCE_MS = 100;

  function generateSessionId() {
    var arr = new Uint8Array(8);
    crypto.getRandomValues(arr);
    return Array.from(arr).map(function(b) {
      return b.toString(16).padStart(2, '0');
    }).join('');
  }

  function getSession() {
    try {
      var stored = sessionStorage.getItem(SESSION_KEY);
      if (stored) {
        var data = JSON.parse(stored);
        var now = Date.now();
        // Check if session is still valid (not expired)
        if (now - data.lastActivity < SESSION_TIMEOUT) {
          data.lastActivity = now;
          sessionStorage.setItem(SESSION_KEY, JSON.stringify(data));
          return data.id;
        }
      }
    } catch (e) {}

    // Create new session
    var newId = generateSessionId();
    try {
      sessionStorage.setItem(SESSION_KEY, JSON.stringify({
        id: newId,
        lastActivity: Date.now()
      }));
    } catch (e) {}
    return newId;
  }

  // Get event endpoint (same origin, different path)
  function getEventEndpoint() {
    // Replace /collect with /event
    return endpoint.replace('/collect', '/event');
  }

  // Send pageview or heartbeat
  function track(extra) {
    var sid = getSession();
    var params = new URLSearchParams({
      site: site,
      url: window.location.href,
      title: document.title || '',
      ref: document.referrer || '',
      w: String(window.innerWidth || 0),
      sw: String(screen.width || 0),
      sh: String(screen.height || 0),
      lang: navigator.language || '',
      sid: sid,
      type: (extra && extra.type) || 'pageview'
    });

    // Add any extra params (except type which we handle above)
    if (extra) {
      for (var k in extra) {
        if (k !== 'type') params.set(k, extra[k]);
      }
    }

    // Use image beacon for reliability
    var img = new Image();
    img.src = endpoint + '?' + params.toString();
  }

  // Send custom event
  function trackEvent(eventName, eventType, eventData) {
    var sid = getSession();
    var params = new URLSearchParams({
      site: site,
      url: window.location.href,
      sid: sid,
      event_type: eventType || 'custom',
      event_name: eventName
    });

    if (eventData) {
      params.set('event_data', JSON.stringify(eventData));
    }

    var img = new Image();
    img.src = getEventEndpoint() + '?' + params.toString();
  }

  // Scroll depth tracking
  function trackScroll() {
    var scrollTop = window.pageYOffset || document.documentElement.scrollTop || document.body.scrollTop || 0;
    var docHeight = Math.max(
      document.body.scrollHeight,
      document.body.offsetHeight,
      document.documentElement.clientHeight,
      document.documentElement.scrollHeight,
      document.documentElement.offsetHeight
    );
    var winHeight = window.innerHeight || document.documentElement.clientHeight;
    var scrollableHeight = docHeight - winHeight;

    // Handle pages with no scrollable content
    if (scrollableHeight <= 0) {
      // Page fits in viewport, count as 100% scroll
      if (!scrollDepths[100]) {
        scrollDepths[100] = true;
        trackEvent('scroll_100', 'scroll', { depth: 100, max_depth: 100 });
      }
      return;
    }

    var scrollPercent = Math.min(100, Math.round((scrollTop / scrollableHeight) * 100));

    // Track each threshold once
    [25, 50, 75, 100].forEach(function(depth) {
      if (scrollPercent >= depth && !scrollDepths[depth]) {
        scrollDepths[depth] = true;
        trackEvent('scroll_' + depth, 'scroll', { depth: depth, max_depth: scrollPercent });
      }
    });
  }

  // Debounced scroll handler
  function handleScroll() {
    if (scrollDebounceTimer) {
      clearTimeout(scrollDebounceTimer);
    }
    scrollDebounceTimer = setTimeout(trackScroll, SCROLL_DEBOUNCE_MS);
  }

  // Reset scroll tracking on navigation (SPA)
  function resetScrollTracking() {
    scrollDepths = { 25: false, 50: false, 75: false, 100: false };
  }

  // Heartbeat to extend session without creating pageview
  function heartbeat() {
    track({ type: 'heartbeat' });
  }

  // Start heartbeat interval (only when page is visible)
  var heartbeatTimer = null;

  function startHeartbeat() {
    if (!heartbeatTimer) {
      heartbeatTimer = setInterval(heartbeat, HEARTBEAT_INTERVAL);
    }
  }

  function stopHeartbeat() {
    if (heartbeatTimer) {
      clearInterval(heartbeatTimer);
      heartbeatTimer = null;
    }
  }

  // Visibility change handling
  document.addEventListener('visibilitychange', function() {
    if (document.hidden) {
      stopHeartbeat();
    } else {
      // Page became visible - update session and restart heartbeat
      getSession();
      startHeartbeat();
    }
  });

  // Initialize scroll tracking
  window.addEventListener('scroll', handleScroll, { passive: true });

  // Track initial pageview
  if (document.readyState === 'complete') {
    track();
    startHeartbeat();
    // Check initial scroll position
    setTimeout(trackScroll, 100);
  } else {
    window.addEventListener('load', function() {
      track();
      startHeartbeat();
      // Check initial scroll position
      setTimeout(trackScroll, 100);
    });
  }

  // Handle SPA navigation (History API)
  var pushState = history.pushState;
  history.pushState = function() {
    pushState.apply(history, arguments);
    setTimeout(function() {
      track();
      resetScrollTracking();
    }, 100);
  };

  window.addEventListener('popstate', function() {
    setTimeout(function() {
      track();
      resetScrollTracking();
    }, 100);
  });

  // Outbound link tracking
  function isOutboundLink(href) {
    if (!href) return false;
    try {
      var linkUrl = new URL(href, window.location.href);
      // Check if different host (outbound)
      return linkUrl.host !== window.location.host;
    } catch (e) {
      return false;
    }
  }

  function getLinkText(element) {
    // Get link text for identification (truncated)
    var text = element.innerText || element.textContent || '';
    text = text.trim().substring(0, 50);
    // If no text, try alt from images inside
    if (!text) {
      var img = element.querySelector('img');
      if (img) text = img.alt || '';
    }
    // If still no text, use title attribute
    if (!text) text = element.title || '';
    return text.trim().substring(0, 50);
  }

  function trackOutboundClick(event) {
    var link = event.target.closest('a');
    if (!link) return;

    var href = link.href;
    if (!isOutboundLink(href)) return;

    var isNewTab = link.target === '_blank' || event.ctrlKey || event.metaKey;
    var linkText = getLinkText(link);

    // Track the outbound click
    trackEvent('outbound_click', 'click', {
      destination: href,
      text: linkText,
      new_tab: isNewTab
    });

    // For same-tab navigation, delay briefly to ensure beacon fires
    if (!isNewTab) {
      event.preventDefault();
      setTimeout(function() {
        window.location.href = href;
      }, 100);
    }
  }

  // Attach outbound click listener
  document.addEventListener('click', trackOutboundClick, true);

  // File download tracking
  var DOWNLOAD_EXTENSIONS = ['pdf', 'zip', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx', 'csv', 'txt', 'mp3', 'mp4', 'avi', 'mov', 'rar', '7z', 'gz', 'tar', 'dmg', 'exe', 'apk', 'ipa'];

  // Check for custom extensions from script attribute
  var customExtensions = s?.getAttribute('data-download-extensions');
  if (customExtensions) {
    DOWNLOAD_EXTENSIONS = customExtensions.split(',').map(function(e) { return e.trim().toLowerCase(); });
  }

  function isDownloadLink(href) {
    if (!href) return false;
    try {
      var url = new URL(href, window.location.href);
      var pathname = url.pathname.toLowerCase();
      var ext = pathname.split('.').pop();
      return DOWNLOAD_EXTENSIONS.indexOf(ext) !== -1;
    } catch (e) {
      return false;
    }
  }

  function getFilename(href) {
    try {
      var url = new URL(href, window.location.href);
      var pathname = url.pathname;
      return pathname.split('/').pop() || pathname;
    } catch (e) {
      return href;
    }
  }

  function getFileExtension(href) {
    try {
      var url = new URL(href, window.location.href);
      return url.pathname.toLowerCase().split('.').pop();
    } catch (e) {
      return '';
    }
  }

  function trackDownloadClick(event) {
    var link = event.target.closest('a');
    if (!link) return;

    var href = link.href;
    if (!isDownloadLink(href)) return;

    var filename = getFilename(href);
    var extension = getFileExtension(href);
    var isExternal = isOutboundLink(href);

    // Track the download
    trackEvent('file_download', 'click', {
      filename: filename,
      extension: extension,
      url: href,
      external: isExternal
    });

    // Don't delay downloads - let the browser handle normally
    // Download attribute or file response headers will trigger download
  }

  // Attach download click listener (runs before outbound click handler due to capture)
  document.addEventListener('click', trackDownloadClick, true);

  // Form submission tracking
  function isSearchForm(form) {
    // Exclude search forms by default
    if (form.getAttribute('role') === 'search') return true;
    var name = (form.name || '').toLowerCase();
    var id = (form.id || '').toLowerCase();
    var action = (form.action || '').toLowerCase();
    if (name.indexOf('search') !== -1 || id.indexOf('search') !== -1) return true;
    if (action.indexOf('/search') !== -1 || action.indexOf('?q=') !== -1 || action.indexOf('?s=') !== -1) return true;
    return false;
  }

  function getFormIdentifier(form) {
    // Get the best identifier for the form
    return form.id || form.name || form.getAttribute('data-form-name') || '';
  }

  function getFormAction(form) {
    // Get form action, stripping query params for privacy
    var action = form.action || '';
    try {
      var url = new URL(action, window.location.href);
      return url.pathname;
    } catch (e) {
      return action.split('?')[0];
    }
  }

  function trackFormSubmission(form, source) {
    if (isSearchForm(form)) return;

    var formId = getFormIdentifier(form);
    var formName = form.name || '';
    var formAction = getFormAction(form);
    var formMethod = (form.method || 'GET').toUpperCase();
    var fieldCount = form.querySelectorAll('input, select, textarea').length;

    trackEvent('form_submit', 'form', {
      form_id: formId,
      form_name: formName,
      action: formAction,
      method: formMethod,
      field_count: fieldCount,
      source: source // 'native' or 'htmx'
    });
  }

  // Native form submit handler
  function handleFormSubmit(event) {
    var form = event.target;
    if (form.tagName !== 'FORM') return;
    trackFormSubmission(form, 'native');
  }

  // Attach native form submit listener
  document.addEventListener('submit', handleFormSubmit, true);

  // HTMX form submission support
  document.addEventListener('htmx:beforeRequest', function(event) {
    var elt = event.detail.elt;
    if (elt && elt.tagName === 'FORM') {
      trackFormSubmission(elt, 'htmx');
    }
  });

  // JavaScript error tracking
  var errorCount = 0;
  var ERROR_RATE_LIMIT = 10; // Max errors per page load
  var ERROR_RESET_INTERVAL = 60000; // Reset count after 1 minute

  // Reset error count periodically to allow tracking after broken period
  setInterval(function() {
    errorCount = 0;
  }, ERROR_RESET_INTERVAL);

  function truncateStack(stack, maxLength) {
    if (!stack) return '';
    if (stack.length <= maxLength) return stack;
    return stack.substring(0, maxLength) + '... (truncated)';
  }

  function normalizeErrorMessage(message) {
    // Normalize message for grouping (remove variable parts like line numbers, URLs)
    if (!message) return 'Unknown error';
    return message
      .replace(/at line \d+/gi, 'at line X')
      .replace(/:\d+:\d+/g, ':X:X')
      .replace(/https?:\/\/[^\s]+/g, '[URL]')
      .substring(0, 200);
  }

  function trackError(message, source, lineno, colno, error) {
    // Rate limit to prevent flood on broken page
    if (errorCount >= ERROR_RATE_LIMIT) return;
    errorCount++;

    var stack = '';
    if (error && error.stack) {
      stack = truncateStack(error.stack, 500);
    }

    var normalizedMsg = normalizeErrorMessage(message);

    trackEvent('js_error', 'error', {
      message: message ? String(message).substring(0, 200) : 'Unknown error',
      normalized: normalizedMsg,
      source: source ? String(source).substring(0, 200) : '',
      lineno: lineno || 0,
      colno: colno || 0,
      stack: stack,
      user_agent: navigator.userAgent.substring(0, 100)
    });
  }

  // Capture window.onerror
  var originalOnError = window.onerror;
  window.onerror = function(message, source, lineno, colno, error) {
    trackError(message, source, lineno, colno, error);
    // Call original handler if it exists
    if (typeof originalOnError === 'function') {
      return originalOnError.apply(this, arguments);
    }
    return false;
  };

  // Capture unhandled promise rejections
  window.addEventListener('unhandledrejection', function(event) {
    var reason = event.reason;
    var message = 'Unhandled Promise Rejection';
    var stack = '';

    if (reason) {
      if (reason instanceof Error) {
        message = reason.message || message;
        stack = reason.stack || '';
      } else if (typeof reason === 'string') {
        message = reason;
      } else if (typeof reason === 'object') {
        message = reason.message || JSON.stringify(reason).substring(0, 200);
      }
    }

    trackError(message, window.location.href, 0, 0, reason instanceof Error ? reason : null);
  });

  // Expose for manual tracking (internal API)
  window._941 = {
    track: track,
    trackEvent: trackEvent,
    heartbeat: heartbeat,
    getSessionId: getSession
  };

  // Public API - window.analytics.track(name, properties)
  // Simple interface for custom event tracking from application code
  var analyticsApi = {
    // Track a custom event with optional properties
    // Example: analytics.track('signup_completed', { plan: 'pro', trial: true })
    track: function(eventName, properties) {
      if (!eventName || typeof eventName !== 'string') {
        console.warn('[941] analytics.track() requires an event name');
        return;
      }
      trackEvent(eventName, 'custom', properties || {});
    },

    // Track a page view (useful for SPAs after navigation)
    page: function(url, title) {
      track({
        type: 'pageview',
        url: url || window.location.href,
        title: title || document.title
      });
    },

    // Get current session ID
    getSessionId: function() {
      return getSession();
    },

    // Identify user (stored in session, sent with events)
    // Note: Does NOT store PII - just an anonymous identifier you provide
    identify: function(userId) {
      if (userId) {
        try {
          sessionStorage.setItem('_941_uid', String(userId));
        } catch (e) {}
      }
    }
  };

  // Expose public API
  window.analytics = window.analytics || analyticsApi;

  // Also expose as _941analytics for namespace safety
  window._941analytics = analyticsApi;

  // Process any queued events (for script loading before DOMContentLoaded)
  var queue = window._941q || [];
  for (var i = 0; i < queue.length; i++) {
    var call = queue[i];
    if (call && call.length >= 2) {
      var method = call[0];
      var args = call.slice(1);
      if (analyticsApi[method]) {
        analyticsApi[method].apply(null, args);
      }
    }
  }
  window._941q = { push: function(args) {
    var method = args[0];
    var callArgs = args.slice(1);
    if (analyticsApi[method]) {
      analyticsApi[method].apply(null, callArgs);
    }
  }};
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
// EVENT COLLECTION HANDLER
// =============================================================================

async function handleEventCollect(
  request: Request,
  url: URL,
  env: Env,
  corsHeaders: Record<string, string>
): Promise<Response> {
  try {
    // Get client IP for rate limiting
    const clientIP = request.headers.get("CF-Connecting-IP") || "unknown";
    const rateLimit = await checkRateLimit(clientIP, env);
    if (!rateLimit.allowed) {
      return new Response("Rate limit exceeded", {
        status: 429,
        headers: { ...corsHeaders, "Retry-After": String(CONFIG.RATE_LIMIT_WINDOW_SEC) },
      });
    }

    // Parse query parameters
    const params = url.searchParams;
    const eventData: EventData = {
      site: params.get("site") || "",
      url: params.get("url") || "",
      sid: params.get("sid") || "",
      event_type: params.get("event_type") || "",
      event_name: params.get("event_name") || "",
      event_data: params.get("event_data") || "",
    };

    // Validate required fields
    if (!eventData.site || !eventData.url || !eventData.event_type || !eventData.event_name) {
      return new Response("Missing required fields", { status: 400 });
    }

    // Validate site
    const siteValid = await isValidSite(eventData.site, env);
    if (!siteValid) {
      console.log(`[SECURITY] Rejected invalid site for event: ${eventData.site}`);
      return new Response("Invalid site", { status: 400 });
    }

    // Filter dev traffic
    if (isDevTraffic(eventData.url)) {
      return new Response(TRANSPARENT_GIF, {
        status: 200,
        headers: { ...corsHeaders, "Content-Type": "image/gif", "X-Dev-Traffic": "filtered" },
      });
    }

    // Get visitor info for event storage
    const cf = (request.cf as Record<string, unknown>) || {};
    const country = (cf.country as string) || "";
    const userAgent = request.headers.get("User-Agent") || "";
    const deviceInfo = parseUserAgent(userAgent);
    const deviceType = deviceInfo.deviceType;

    // Generate visitor hash (for deduplication/grouping)
    const region = (cf.region as string) || "";
    const visitorHash = await generateVisitorHash(eventData.site, country, region, env.ANALYTICS_SECRET);

    // Insert event into D1
    await env.DB.prepare(`
      INSERT INTO events (
        site, timestamp, session_id, visitor_hash,
        event_type, event_name, event_data,
        page_url, country, device_type
      ) VALUES (?, datetime('now'), ?, ?, ?, ?, ?, ?, ?, ?)
    `).bind(
      eventData.site,
      eventData.sid || null,
      visitorHash,
      eventData.event_type,
      eventData.event_name,
      eventData.event_data || null,
      eventData.url,
      country,
      deviceType
    ).run();

    // Return tracking pixel
    return new Response(TRANSPARENT_GIF, {
      status: 200,
      headers: {
        ...corsHeaders,
        "Content-Type": "image/gif",
        "Cache-Control": "no-store, no-cache, must-revalidate",
      },
    });
  } catch (error) {
    console.error("Event collection error:", error);
    return new Response("OK", { status: 200, headers: corsHeaders });
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

    // Handle /event endpoint for custom event tracking
    if (url.pathname === "/event" && request.method === "GET") {
      return handleEventCollect(request, url, env, corsHeaders);
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
        sw: parseInt(params.get("sw") || "0", 10),
        sh: parseInt(params.get("sh") || "0", 10),
        lang: params.get("lang") || "",
        sid: params.get("sid") || "",
        type: params.get("type") || "pageview",
      };

      // Validate required fields
      if (!data.site || !data.url) {
        return new Response("Missing required fields", { status: 400 });
      }

      // Validate type
      const isHeartbeat = data.type === "heartbeat";
      const isPageview = data.type === "pageview" || !data.type;

      // Validate site against D1 sites table (with fallback to env var)
      const siteValid = await isValidSite(data.site, env);
      if (!siteValid) {
        console.log(`[SECURITY] Rejected invalid site: ${data.site}`);
        return new Response("Invalid site", { status: 400 });
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

      // Prepare session data for upsert
      const sessionData: SessionData = {
        site: data.site,
        sessionId: data.sid,
        visitorHash,
        entryPage: data.url,
        referrer: data.ref,
        referrerType: referrerInfo.type,
        referrerDomain: referrerInfo.domain,
        utmSource: utmParams.source,
        utmMedium: utmParams.medium,
        utmCampaign: utmParams.campaign,
        country,
        region,
        deviceType,
        browser: deviceInfo.browser,
        os: deviceInfo.os,
      };

      // Update session (works for both pageviews and heartbeats)
      if (data.sid && !botInfo.isBot) {
        await upsertSession(env.DB, sessionData, isHeartbeat);
      }

      // Only insert pageview record for actual pageviews (not heartbeats)
      if (isPageview) {
        await env.DB.prepare(`
          INSERT INTO page_views (
            site, timestamp, url, page_title,
            referrer, referrer_type, referrer_domain,
            country, region, city, latitude, longitude,
            device_type, user_agent, browser, browser_version, os, os_version,
            screen_width, screen_height, language,
            is_bot, bot_name, bot_category,
            utm_source, utm_medium, utm_campaign, utm_term, utm_content,
            visitor_hash, session_id
          ) VALUES (
            ?, datetime('now'), ?, ?,
            ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?
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
          data.sw || null,
          data.sh || null,
          data.lang || null,
          botInfo.isBot ? 1 : 0,
          botInfo.name,
          botInfo.category,
          utmParams.source,
          utmParams.medium,
          utmParams.campaign,
          utmParams.term,
          utmParams.content,
          visitorHash,
          data.sid || null
        ).run();
      }

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
