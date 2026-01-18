-- 941 Analytics - Enhanced Database Schema
-- Run this on your Cloudflare D1 database

-- =============================================================================
-- PAGEVIEWS TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS page_views (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site TEXT NOT NULL,
    timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    url TEXT NOT NULL,
    page_title TEXT,

    -- Session
    session_id TEXT NOT NULL,
    visitor_hash TEXT NOT NULL,

    -- Referrer
    referrer TEXT,
    referrer_type TEXT,
    referrer_domain TEXT,

    -- UTM
    utm_source TEXT,
    utm_medium TEXT,
    utm_campaign TEXT,
    utm_term TEXT,
    utm_content TEXT,

    -- Geography
    country TEXT,
    region TEXT,
    city TEXT,
    latitude REAL,
    longitude REAL,

    -- Technology
    device_type TEXT,
    browser TEXT,
    browser_version TEXT,
    os TEXT,
    os_version TEXT,
    screen_width INTEGER,
    screen_height INTEGER,
    language TEXT,

    -- Bot detection
    is_bot INTEGER DEFAULT 0,
    bot_name TEXT,
    bot_category TEXT
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_page_views_site_timestamp ON page_views(site, timestamp);
CREATE INDEX IF NOT EXISTS idx_page_views_site_date ON page_views(site, date(timestamp));
CREATE INDEX IF NOT EXISTS idx_page_views_session ON page_views(site, session_id);
CREATE INDEX IF NOT EXISTS idx_page_views_visitor ON page_views(site, visitor_hash);
CREATE INDEX IF NOT EXISTS idx_page_views_country ON page_views(site, country);
CREATE INDEX IF NOT EXISTS idx_page_views_device ON page_views(site, device_type);

-- =============================================================================
-- SESSIONS TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site TEXT NOT NULL,
    session_id TEXT NOT NULL UNIQUE,
    visitor_hash TEXT NOT NULL,

    started_at DATETIME NOT NULL,
    last_activity_at DATETIME NOT NULL,
    ended_at DATETIME,

    duration_seconds INTEGER DEFAULT 0,
    pageview_count INTEGER DEFAULT 1,
    event_count INTEGER DEFAULT 0,
    is_bounce INTEGER DEFAULT 1,

    entry_page TEXT,
    exit_page TEXT,

    -- Attribution (first touch)
    referrer_type TEXT,
    referrer_domain TEXT,
    utm_source TEXT,
    utm_campaign TEXT,

    -- Demographics
    country TEXT,
    region TEXT,
    device_type TEXT,
    browser TEXT,
    os TEXT
);

CREATE INDEX IF NOT EXISTS idx_sessions_site_started ON sessions(site, started_at);
CREATE INDEX IF NOT EXISTS idx_sessions_site_date ON sessions(site, date(started_at));
CREATE INDEX IF NOT EXISTS idx_sessions_session_id ON sessions(session_id);
CREATE INDEX IF NOT EXISTS idx_sessions_visitor ON sessions(site, visitor_hash);

-- =============================================================================
-- EVENTS TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site TEXT NOT NULL,
    timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    session_id TEXT NOT NULL,
    visitor_hash TEXT NOT NULL,

    event_type TEXT NOT NULL,  -- click, scroll, form, video, error, custom
    event_name TEXT NOT NULL,  -- outbound_click, scroll_50, form_submit, etc.
    event_data TEXT,           -- JSON string for additional data

    page_url TEXT,
    country TEXT,
    device_type TEXT
);

CREATE INDEX IF NOT EXISTS idx_events_site_timestamp ON events(site, timestamp);
CREATE INDEX IF NOT EXISTS idx_events_site_date ON events(site, date(timestamp));
CREATE INDEX IF NOT EXISTS idx_events_session ON events(site, session_id);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(site, event_type);
CREATE INDEX IF NOT EXISTS idx_events_name ON events(site, event_name);

-- =============================================================================
-- HOURLY STATS TABLE (Pre-aggregated for performance)
-- =============================================================================
CREATE TABLE IF NOT EXISTS hourly_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site TEXT NOT NULL,
    hour DATETIME NOT NULL,  -- Truncated to hour

    views INTEGER DEFAULT 0,
    visitors INTEGER DEFAULT 0,
    sessions INTEGER DEFAULT 0,
    bounces INTEGER DEFAULT 0,
    total_duration INTEGER DEFAULT 0,

    UNIQUE(site, hour)
);

CREATE INDEX IF NOT EXISTS idx_hourly_stats_site_hour ON hourly_stats(site, hour);

-- =============================================================================
-- WEBAUTHN CREDENTIALS TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS passkeys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site TEXT NOT NULL,
    credential_id TEXT NOT NULL UNIQUE,
    public_key TEXT NOT NULL,
    counter INTEGER DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_used_at DATETIME,
    user_agent TEXT,
    device_name TEXT
);

CREATE INDEX IF NOT EXISTS idx_passkeys_site ON passkeys(site);
CREATE INDEX IF NOT EXISTS idx_passkeys_credential ON passkeys(credential_id);

-- =============================================================================
-- HELPER VIEWS
-- =============================================================================

-- Daily stats view for quick dashboard queries
CREATE VIEW IF NOT EXISTS v_daily_stats AS
SELECT
    site,
    date(timestamp) as day,
    COUNT(*) as views,
    COUNT(DISTINCT visitor_hash) as visitors,
    COUNT(DISTINCT session_id) as sessions
FROM page_views
WHERE is_bot = 0
GROUP BY site, date(timestamp);

-- Top pages view
CREATE VIEW IF NOT EXISTS v_top_pages AS
SELECT
    site,
    url,
    COUNT(*) as views,
    COUNT(DISTINCT visitor_hash) as visitors,
    COUNT(DISTINCT session_id) as sessions
FROM page_views
WHERE is_bot = 0
GROUP BY site, url;

-- Source breakdown view
CREATE VIEW IF NOT EXISTS v_sources AS
SELECT
    site,
    COALESCE(referrer_domain, 'Direct') as source,
    referrer_type as source_type,
    COUNT(*) as visits,
    COUNT(DISTINCT visitor_hash) as visitors
FROM page_views
WHERE is_bot = 0
GROUP BY site, referrer_domain, referrer_type;

-- =============================================================================
-- SITES CONFIGURATION TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS sites (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    domain TEXT NOT NULL UNIQUE,
    display_name TEXT,
    timezone TEXT DEFAULT 'America/New_York',
    passkey_hash TEXT,  -- PBKDF2 hash of site-specific passkey
    is_active INTEGER DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sites_domain ON sites(domain);
CREATE INDEX IF NOT EXISTS idx_sites_active ON sites(is_active);

-- =============================================================================
-- SITE SETTINGS TABLE (Key-Value Overrides)
-- =============================================================================
CREATE TABLE IF NOT EXISTS site_settings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site_id INTEGER NOT NULL,
    key TEXT NOT NULL,
    value TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (site_id) REFERENCES sites(id) ON DELETE CASCADE,
    UNIQUE(site_id, key)
);

CREATE INDEX IF NOT EXISTS idx_site_settings_site ON site_settings(site_id);
CREATE INDEX IF NOT EXISTS idx_site_settings_key ON site_settings(key);
