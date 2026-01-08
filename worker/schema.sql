-- 941 Analytics Schema
-- Privacy-first pageview tracking with enhanced attribution
--
-- PRIVACY GUARANTEES:
-- - No cookies or persistent identifiers
-- - No IP addresses stored
-- - visitor_hash rotates daily (can't track across days)
-- - User-agent stored only for browser/OS aggregation

CREATE TABLE IF NOT EXISTS page_views (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site TEXT NOT NULL,
    timestamp DATETIME NOT NULL,
    url TEXT NOT NULL,
    page_title TEXT DEFAULT '',

    -- Referrer tracking
    referrer TEXT DEFAULT '',           -- Full referrer URL
    referrer_type TEXT DEFAULT '',      -- direct, organic, social, email, referral, paid
    referrer_domain TEXT DEFAULT '',    -- Normalized domain

    -- Geographic data (from Cloudflare headers, no IP stored)
    country TEXT DEFAULT '',
    region TEXT DEFAULT '',             -- State/province code (e.g., "CA")
    city TEXT DEFAULT '',
    latitude REAL DEFAULT NULL,
    longitude REAL DEFAULT NULL,

    -- Device & browser detection
    device_type TEXT DEFAULT '',        -- mobile, tablet, desktop
    user_agent TEXT DEFAULT '',         -- Raw UA for re-processing
    browser TEXT DEFAULT '',            -- Chrome, Firefox, Safari, etc.
    browser_version TEXT DEFAULT '',    -- Major version
    os TEXT DEFAULT '',                 -- Windows, macOS, iOS, Android, etc.
    os_version TEXT DEFAULT '',         -- OS version

    -- Bot detection
    is_bot INTEGER DEFAULT 0,           -- 1 = bot, 0 = human
    bot_name TEXT DEFAULT '',           -- Google, Bing, etc.
    bot_category TEXT DEFAULT '',       -- search_engine, ai_crawler, etc.

    -- Campaign attribution (UTM parameters)
    utm_source TEXT DEFAULT '',         -- Traffic source (google, newsletter)
    utm_medium TEXT DEFAULT '',         -- Medium (cpc, email, social)
    utm_campaign TEXT DEFAULT '',       -- Campaign name
    utm_term TEXT DEFAULT '',           -- Paid keywords
    utm_content TEXT DEFAULT '',        -- Content variant (A/B test)

    -- Privacy-preserving visitor ID
    visitor_hash TEXT DEFAULT ''        -- Daily-rotating hash (can't track across days)
);

-- Core indexes for querying
CREATE INDEX IF NOT EXISTS idx_site_timestamp ON page_views(site, timestamp);
CREATE INDEX IF NOT EXISTS idx_site_visitor ON page_views(site, visitor_hash);

-- Indexes for analytics queries
CREATE INDEX IF NOT EXISTS idx_site_country_region ON page_views(site, country, region);
CREATE INDEX IF NOT EXISTS idx_site_is_bot ON page_views(site, is_bot);
CREATE INDEX IF NOT EXISTS idx_site_referrer_type ON page_views(site, referrer_type);
CREATE INDEX IF NOT EXISTS idx_site_browser ON page_views(site, browser);
CREATE INDEX IF NOT EXISTS idx_site_os ON page_views(site, os);
CREATE INDEX IF NOT EXISTS idx_site_utm_source ON page_views(site, utm_source);

-- Daily aggregates table (populated by scheduled job)
CREATE TABLE IF NOT EXISTS daily_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site TEXT NOT NULL,
    date DATE NOT NULL,

    -- Core metrics
    total_views INTEGER DEFAULT 0,
    unique_visitors INTEGER DEFAULT 0,
    bot_views INTEGER DEFAULT 0,        -- Views from bots

    -- JSON aggregates for detailed breakdowns
    top_pages TEXT DEFAULT '[]',        -- [{url, views}]
    top_referrers TEXT DEFAULT '[]',    -- [{domain, views}]
    countries TEXT DEFAULT '{}',        -- {country_code: views}
    devices TEXT DEFAULT '{}',          -- {device_type: views}
    browsers TEXT DEFAULT '{}',         -- {browser_name: views}
    operating_systems TEXT DEFAULT '{}', -- {os_name: views}
    referrer_types TEXT DEFAULT '{}',   -- {type: views}
    utm_sources TEXT DEFAULT '{}',      -- {source: views}
    utm_campaigns TEXT DEFAULT '{}',    -- {campaign: views}
    bot_breakdown TEXT DEFAULT '{}',    -- {category: views}

    UNIQUE(site, date)
);

CREATE INDEX IF NOT EXISTS idx_daily_site_date ON daily_stats(site, date);
