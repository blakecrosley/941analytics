/**
 * 941 Analytics Worker - Utility Functions
 *
 * Extracted for testability. These are pure functions with no side effects.
 */

// =============================================================================
// BOT DETECTION
// =============================================================================

export interface BotInfo {
  isBot: boolean;
  name: string;
  category: string;
}

export const BOT_PATTERNS: Record<string, Record<string, string>> = {
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

export const GENERIC_BOT_PATTERNS = [
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

export function detectBot(userAgent: string): BotInfo {
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

export interface ReferrerInfo {
  type: string;
  domain: string;
}

export const SEARCH_ENGINES = [
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

export const SOCIAL_PLATFORMS = [
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

export const EMAIL_INDICATORS = [
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

export function classifyReferrer(referrer: string): ReferrerInfo {
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

  // Check email first (most specific)
  for (const email of EMAIL_INDICATORS) {
    if (domain.includes(email) || referrerLower.includes(email)) {
      return { type: "email", domain };
    }
  }

  // Check search engines
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
// UTM PARSING
// =============================================================================

export interface UTMParams {
  source: string;
  medium: string;
  campaign: string;
  term: string;
  content: string;
}

export function parseUTM(url: string): UTMParams {
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
// DEVICE TYPE DETECTION
// =============================================================================

export function getDeviceType(width: number): string {
  if (width === 0) return "unknown";
  if (width < 768) return "mobile";
  if (width < 1024) return "tablet";
  return "desktop";
}

// =============================================================================
// PAYLOAD VALIDATION
// =============================================================================

export interface CollectPayload {
  site: string;
  url: string;
  title?: string;
  ref?: string;
  w?: number;
  h?: number;
  sid?: string;
  type?: string;
  event_type?: string;
  event_name?: string;
  event_data?: Record<string, unknown>;
}

export interface ValidationResult {
  valid: boolean;
  error?: string;
}

export function validateCollectPayload(data: unknown): ValidationResult {
  if (!data || typeof data !== "object" || Array.isArray(data)) {
    return { valid: false, error: "Invalid payload" };
  }

  const payload = data as Record<string, unknown>;

  if (!payload.site || typeof payload.site !== "string") {
    return { valid: false, error: "Missing or invalid site" };
  }

  if (!payload.url || typeof payload.url !== "string") {
    return { valid: false, error: "Missing or invalid url" };
  }

  // Validate URL format
  try {
    new URL(payload.url as string);
  } catch {
    return { valid: false, error: "Invalid URL format" };
  }

  return { valid: true };
}

// =============================================================================
// DATACENTER ASN LABELING
// =============================================================================
// Headless fleets that fake a real Chrome UA pass detectBot (the OneCable
// AS401560 fleet of 2026-10 beaconed as 939 "humans" in one day). Their one
// unfakeable trait is the source network: consumer eyeballs do not browse from
// cloud-server ASNs, and request.cf.asn is set by Cloudflare at the edge.
// Curated: pure cloud/hosting networks only. CDN and privacy-relay egress
// (Cloudflare 13335, Apple Private Relay, Datacamp 212238 — which carries
// DuckDuckGo VPN and NordVPN users) are deliberately EXCLUDED so humans on
// VPNs are never mislabeled. Mirrors resumegeni's
// app/services/funnel_service.py DATACENTER_ASNS — keep the two lists aligned.
// Label, never filter: rows stay, is_bot=1 keeps them out of human counts.

export const DATACENTER_ASNS: ReadonlySet<number> = new Set([
  16509, 14618, // AWS
  8075, // Microsoft Azure
  15169, 396982, // Google / GCP
  45102, 37963, // Alibaba Cloud
  45090, 132203, // Tencent Cloud
  136907, // Huawei Cloud
  14061, // DigitalOcean
  16276, // OVH
  24940, // Hetzner
  63949, // Linode
  20473, // Vultr / Choopa
  31898, // Oracle Cloud
  60781, // Leaseweb
  135377, // UCloud
  38365, // Baidu Cloud
  55960, // Beijing Guanghuan Xinwang — Baidu's IDC arm
  398781, // Oculus Networks — hosting/proxy
  401560, // OneCable Network LLC — the 2026-10 headless Chrome fleet
]);

export function detectDatacenterAsn(asn: unknown): BotInfo {
  const n = typeof asn === "number" ? asn : parseInt(String(asn ?? ""), 10);
  if (Number.isInteger(n) && DATACENTER_ASNS.has(n)) {
    return { isBot: true, name: `Datacenter (AS${n})`, category: "datacenter" };
  }
  return { isBot: false, name: "", category: "" };
}
