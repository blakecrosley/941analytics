/**
 * 941 Analytics Worker v2.0
 *
 * Handles pageviews, sessions, events, and heartbeats.
 * Supports both legacy GET and new POST JSON payloads.
 */

// =============================================================================
// BOT DETECTION
// =============================================================================

const BOT_PATTERNS = {
  search_engine: {
    googlebot: "Google",
    "google-inspectiontool": "Google",
    bingbot: "Bing",
    bingpreview: "Bing",
    yandexbot: "Yandex",
    duckduckbot: "DuckDuckGo",
    baiduspider: "Baidu",
    applebot: "Apple",
    petalbot: "Huawei"
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
    ccbot: "Common Crawl"
  },
  seo_tool: {
    ahrefsbot: "Ahrefs",
    semrushbot: "SEMrush",
    mj12bot: "Majestic",
    dotbot: "Moz",
    rogerbot: "Moz",
    "screaming frog": "Screaming Frog",
    dataforseo: "DataForSEO"
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
    redditbot: "Reddit"
  },
  monitoring: {
    uptimerobot: "UptimeRobot",
    pingdom: "Pingdom",
    site24x7: "Site24x7",
    statuscake: "StatusCake",
    newrelicpinger: "New Relic",
    datadog: "Datadog"
  },
  library: {
    "python-requests": "Python Requests",
    "go-http-client": "Go HTTP",
    curl: "cURL",
    wget: "Wget",
    axios: "Axios",
    "node-fetch": "Node.js"
  },
  headless: {
    headlesschrome: "Headless Chrome",
    phantomjs: "PhantomJS",
    selenium: "Selenium",
    puppeteer: "Puppeteer",
    playwright: "Playwright"
  }
};

const GENERIC_BOT_PATTERNS = [
  /\bbot\b/i,
  /crawl/i,
  /spider/i,
  /scrape/i,
  /fetch/i,
  /index/i,
  /monitor/i,
  /preview/i
];

function detectBot(userAgent) {
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

const SEARCH_ENGINES = [
  "google.", "bing.com", "yahoo.", "duckduckgo.com", "baidu.com",
  "yandex.", "ecosia.org", "qwant.com", "startpage.com", "brave.com"
];

const SOCIAL_PLATFORMS = [
  "facebook.com", "fb.com", "t.co", "twitter.com", "x.com",
  "linkedin.com", "instagram.com", "pinterest.com", "reddit.com",
  "youtube.com", "tiktok.com", "threads.net", "mastodon.",
  "discord.com", "telegram.org", "whatsapp.com"
];

const EMAIL_INDICATORS = [
  "mail.google.com", "outlook.live.com", "mail.yahoo.com",
  "mail.", "webmail.", "newsletter", "campaign",
  "mailchimp", "sendgrid", "constantcontact"
];

function classifyReferrer(referrer) {
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
// UTM PARSING
// =============================================================================

function parseUTM(url) {
  const result = { source: "", medium: "", campaign: "", term: "", content: "" };
  try {
    const urlObj = new URL(url);
    const params = urlObj.searchParams;
    result.source = (params.get("utm_source") || params.get("ref") || "").slice(0, 200);
    result.medium = (params.get("utm_medium") || "").slice(0, 200);
    result.campaign = (params.get("utm_campaign") || "").slice(0, 200);
    result.term = (params.get("utm_term") || "").slice(0, 200);
    result.content = (params.get("utm_content") || "").slice(0, 200);
  } catch {}
  return result;
}

// =============================================================================
// USER AGENT PARSING
// =============================================================================

function parseUserAgent(ua) {
  if (!ua) {
    return { browser: "Unknown", browserVersion: "", os: "Unknown", osVersion: "", deviceType: "unknown" };
  }
  const result = {
    browser: "Unknown",
    browserVersion: "",
    os: "Unknown",
    osVersion: "",
    deviceType: "desktop"
  };

  // Device type
  if (/iPad/i.test(ua)) {
    result.deviceType = "tablet";
  } else if (/Mobile|Android.*Mobile|iPhone|iPod|BlackBerry|IEMobile|Opera Mini/i.test(ua)) {
    result.deviceType = "mobile";
  } else if (/Android/i.test(ua)) {
    result.deviceType = "tablet";
  }

  // Browser
  const browserPatterns = [
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
    [/Safari\/(\d+)/i, "Safari"]
  ];
  for (const [pattern, name] of browserPatterns) {
    const match = ua.match(pattern);
    if (match) {
      result.browser = name;
      result.browserVersion = match[1] || "";
      break;
    }
  }

  // OS
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
  } else if (/Windows/i.test(ua)) {
    result.os = "Windows";
  } else if (/CrOS/i.test(ua)) {
    result.os = "Chrome OS";
  } else if (/Linux/i.test(ua)) {
    result.os = "Linux";
  }

  return result;
}

function getDeviceType(width) {
  if (width === 0) return "unknown";
  if (width < 768) return "mobile";
  if (width < 1024) return "tablet";
  return "desktop";
}

// =============================================================================
// VISITOR HASH
// =============================================================================

async function generateVisitorHash(site, country, region, secret) {
  const today = new Date().toISOString().split("T")[0];
  const data = `${secret}:${site}:${country}:${region}:${today}`;
  const encoder = new TextEncoder();
  const hashBuffer = await crypto.subtle.digest("SHA-256", encoder.encode(data));
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.slice(0, 8).map(b => b.toString(16).padStart(2, "0")).join("");
}

// =============================================================================
// SESSION MANAGEMENT
// =============================================================================

async function getOrCreateSession(env, sessionId, site, visitorHash, pageUrl, referrerInfo, utmParams, country, region, deviceType, browser, os) {
  // Try to get existing session
  const existing = await env.DB.prepare(
    "SELECT id, pageview_count, event_count FROM sessions WHERE session_id = ?"
  ).bind(sessionId).first();

  if (existing) {
    // Update existing session
    await env.DB.prepare(`
      UPDATE sessions SET
        last_activity_at = datetime('now'),
        pageview_count = pageview_count + 1,
        is_bounce = 0,
        exit_page = ?
      WHERE session_id = ?
    `).bind(pageUrl, sessionId).run();
    return existing;
  }

  // Create new session
  await env.DB.prepare(`
    INSERT INTO sessions (
      site, session_id, visitor_hash,
      started_at, last_activity_at,
      entry_page, exit_page,
      referrer_type, referrer_domain, utm_source, utm_campaign,
      country, region, device_type, browser, os
    ) VALUES (
      ?, ?, ?,
      datetime('now'), datetime('now'),
      ?, ?,
      ?, ?, ?, ?,
      ?, ?, ?, ?, ?
    )
  `).bind(
    site, sessionId, visitorHash,
    pageUrl, pageUrl,
    referrerInfo.type, referrerInfo.domain, utmParams.source, utmParams.campaign,
    country, region, deviceType, browser, os
  ).run();

  return null;
}

async function updateSessionHeartbeat(env, sessionId) {
  await env.DB.prepare(`
    UPDATE sessions SET
      last_activity_at = datetime('now'),
      duration_seconds = CAST((julianday('now') - julianday(started_at)) * 86400 AS INTEGER)
    WHERE session_id = ?
  `).bind(sessionId).run();
}

async function endSession(env, sessionId, exitPage) {
  await env.DB.prepare(`
    UPDATE sessions SET
      ended_at = datetime('now'),
      last_activity_at = datetime('now'),
      duration_seconds = CAST((julianday('now') - julianday(started_at)) * 86400 AS INTEGER),
      exit_page = COALESCE(?, exit_page)
    WHERE session_id = ?
  `).bind(exitPage, sessionId).run();
}

// =============================================================================
// EVENT TRACKING
// =============================================================================

async function trackEvent(env, site, sessionId, visitorHash, eventType, eventName, eventData, pageUrl, country, deviceType) {
  await env.DB.prepare(`
    INSERT INTO events (
      site, session_id, visitor_hash,
      event_type, event_name, event_data,
      page_url, country, device_type
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).bind(
    site, sessionId, visitorHash,
    eventType, eventName, eventData ? JSON.stringify(eventData) : null,
    pageUrl, country, deviceType
  ).run();

  // Increment event count on session
  await env.DB.prepare(`
    UPDATE sessions SET event_count = event_count + 1 WHERE session_id = ?
  `).bind(sessionId).run();
}

// =============================================================================
// MAIN HANDLER
// =============================================================================

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type"
    };

    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }

    // Serve tracking script
    if (url.pathname === "/track.js") {
      return new Response(TRACKING_SCRIPT, {
        headers: {
          ...corsHeaders,
          "Content-Type": "application/javascript",
          "Cache-Control": "public, max-age=3600"
        }
      });
    }

    // Handle tracking endpoints
    if (url.pathname === "/collect") {
      return handleCollect(request, env, corsHeaders);
    }

    return new Response("Not Found", { status: 404 });
  }
};

async function handleCollect(request, env, corsHeaders) {
  try {
    const userAgent = request.headers.get("User-Agent") || "";
    const botInfo = detectBot(userAgent);

    // Skip bot tracking for events (still track pageviews for bot stats)
    const cf = request.cf || {};
    const country = cf.country || "";
    const region = cf.region || "";
    const city = cf.city || "";
    const latitude = cf.latitude || null;
    const longitude = cf.longitude || null;

    let data;

    // Support both GET (legacy) and POST (new)
    if (request.method === "POST") {
      data = await request.json();
    } else if (request.method === "GET") {
      const params = url.searchParams;
      data = {
        type: "pageview",
        site: params.get("site") || "",
        url: params.get("url") || "",
        title: params.get("title") || "",
        referrer: params.get("ref") || "",
        screen_width: parseInt(params.get("w") || "0", 10),
        screen_height: 0,
        session_id: params.get("sid") || generateSessionId()
      };
    } else {
      return new Response("Method not allowed", { status: 405 });
    }

    if (!data.site || !data.url) {
      return new Response("Missing required fields", { status: 400 });
    }

    const visitorHash = await generateVisitorHash(data.site, country, region, env.ANALYTICS_SECRET);
    const sessionId = data.session_id || generateSessionId();
    const deviceInfo = parseUserAgent(userAgent);
    const deviceType = deviceInfo.deviceType !== "desktop" ? deviceInfo.deviceType : getDeviceType(data.screen_width || 0);
    const referrerInfo = classifyReferrer(data.referrer);
    const utmParams = parseUTM(data.url);

    // Handle different tracking types
    switch (data.type) {
      case "pageview":
        // Create/update session
        await getOrCreateSession(
          env, sessionId, data.site, visitorHash, data.url,
          referrerInfo, utmParams, country, region, deviceType,
          deviceInfo.browser, deviceInfo.os
        );

        // Insert pageview
        await env.DB.prepare(`
          INSERT INTO page_views (
            site, timestamp, url, page_title, session_id,
            referrer, referrer_type, referrer_domain,
            country, region, city, latitude, longitude,
            device_type, user_agent, browser, browser_version, os, os_version,
            is_bot, bot_name, bot_category,
            utm_source, utm_medium, utm_campaign, utm_term, utm_content,
            visitor_hash, screen_width, screen_height, language
          ) VALUES (
            ?, datetime('now'), ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?
          )
        `).bind(
          data.site, data.url, data.title || "", sessionId,
          data.referrer || "", referrerInfo.type, referrerInfo.domain,
          country, region, city, latitude, longitude,
          deviceType, userAgent.slice(0, 500), deviceInfo.browser, deviceInfo.browserVersion,
          deviceInfo.os, deviceInfo.osVersion,
          botInfo.isBot ? 1 : 0, botInfo.name, botInfo.category,
          utmParams.source, utmParams.medium, utmParams.campaign, utmParams.term, utmParams.content,
          visitorHash, data.screen_width || null, data.screen_height || null, data.language || null
        ).run();
        break;

      case "event":
        if (!botInfo.isBot) {
          await trackEvent(
            env, data.site, sessionId, visitorHash,
            data.event_type, data.event_name, data.event_data,
            data.url, country, deviceType
          );
        }
        break;

      case "heartbeat":
        await updateSessionHeartbeat(env, sessionId);
        break;

      case "session_end":
        await endSession(env, sessionId, data.url);
        break;
    }

    // Return 1x1 transparent GIF for compatibility
    const gif = new Uint8Array([
      71, 73, 70, 56, 57, 97, 1, 0, 1, 0, 128, 0, 0,
      255, 255, 255, 0, 0, 0, 33, 249, 4, 1, 0, 0, 0, 0,
      44, 0, 0, 0, 0, 1, 0, 1, 0, 0, 2, 2, 68, 1, 0, 59
    ]);

    return new Response(gif, {
      status: 200,
      headers: {
        ...corsHeaders,
        "Content-Type": "image/gif",
        "Cache-Control": "no-store, no-cache, must-revalidate"
      }
    });

  } catch (error) {
    console.error("Analytics error:", error);
    return new Response("OK", { status: 200, headers: corsHeaders });
  }
}

function generateSessionId() {
  return Date.now().toString(36) + Math.random().toString(36).substr(2, 9);
}

// Inline tracking script (served at /track.js)
const TRACKING_SCRIPT = `(function(){'use strict';var s=document.currentScript,e=s.dataset.endpoint,t=s.dataset.site;if(!e||!t)return;var S=18e5,H=15e3,i=null,l=Date.now(),d={25:!1,50:!1,75:!1,100:!1};function g(){return Date.now().toString(36)+Math.random().toString(36).substr(2,9)}function G(){try{var o=sessionStorage.getItem('_941_session');if(o){var a=JSON.parse(o);if(Date.now()-a.lastActivity<S){i=a.id;l=Date.now();V();return i}}}catch(e){}i=g();V();return i}function V(){try{sessionStorage.setItem('_941_session',JSON.stringify({id:i,lastActivity:l}))}catch(e){}}function P(y,a){if(!i)G();var p=Object.assign({type:y,site:t,session_id:i,url:location.href,referrer:document.referrer||null,title:document.title,screen_width:window.screen.width,screen_height:window.screen.height,language:navigator.language,timestamp:new Date().toISOString()},a||{});if(navigator.sendBeacon){navigator.sendBeacon(e,JSON.stringify(p))}else{var x=new XMLHttpRequest();x.open('POST',e,!0);x.setRequestHeader('Content-Type','application/json');x.send(JSON.stringify(p))}}function T(){G();P('pageview')}function E(n,y,a){P('event',{event_name:n,event_type:y||'custom',event_data:a||null})}function B(){if(document.visibilityState==='visible'){l=Date.now();V();P('heartbeat')}}function D(){var c=window.pageYOffset||document.documentElement.scrollTop,h=document.documentElement.scrollHeight-window.innerHeight,p=h>0?Math.round((c/h)*100):0;[25,50,75,100].forEach(function(x){if(p>=x&&!d[x]){d[x]=!0;E('scroll_'+x,'scroll',{depth:x})}})}function O(v){var a=v.target.closest('a');if(!a)return;var h=a.href;if(!h)return;try{var u=new URL(h);if(u.hostname!==location.hostname){E('outbound_click','click',{url:h,text:a.textContent.trim().substring(0,100)})}}catch(e){}}function W(v){var a=v.target.closest('a');if(!a)return;var h=a.href||'',x=h.split('.').pop().toLowerCase().split('?')[0],f=['pdf','doc','docx','xls','xlsx','ppt','pptx','zip','rar','gz','dmg','exe'];if(f.indexOf(x)>-1){E('download','click',{url:h,extension:x})}}function F(v){var f=v.target;if(f.tagName!=='FORM')return;E('form_submit','form',{form_id:f.id||null,form_name:f.name||null})}function R(v){E('js_error','error',{message:v.message,source:v.filename,line:v.lineno})}function I(){T();setTimeout(function(){if(document.title.toLowerCase().indexOf('404')>-1){E('404','error',{url:location.href})}},1e3);setInterval(B,H);var r;window.addEventListener('scroll',function(){if(!r){r=setTimeout(function(){D();r=null},200)}},{passive:!0});document.addEventListener('click',function(v){O(v);W(v)});document.addEventListener('submit',F);window.addEventListener('error',R);document.addEventListener('visibilitychange',function(){if(document.visibilityState==='visible'){l=Date.now();V()}});window.addEventListener('pagehide',function(){P('session_end')});var p=history.pushState;history.pushState=function(){p.apply(history,arguments);setTimeout(function(){d={25:!1,50:!1,75:!1,100:!1};T()},100)}}window._941={track:E,pageview:T};if(document.readyState==='loading'){document.addEventListener('DOMContentLoaded',I)}else{I()}})();`;
