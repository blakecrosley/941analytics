-- 941 Analytics Schema
-- Privacy-first pageview tracking

CREATE TABLE IF NOT EXISTS page_views (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site TEXT NOT NULL,
    timestamp DATETIME NOT NULL,
    url TEXT NOT NULL,
    page_title TEXT DEFAULT '',
    referrer TEXT DEFAULT '',
    country TEXT DEFAULT '',
    region TEXT DEFAULT '',
    city TEXT DEFAULT '',
    latitude REAL DEFAULT NULL,
    longitude REAL DEFAULT NULL,
    device_type TEXT DEFAULT '',
    visitor_hash TEXT DEFAULT ''
);

-- Index for querying by site and time range
CREATE INDEX IF NOT EXISTS idx_site_timestamp ON page_views(site, timestamp);

-- Index for unique visitor counting
CREATE INDEX IF NOT EXISTS idx_site_visitor ON page_views(site, visitor_hash);

-- Daily aggregates table (populated by scheduled job)
CREATE TABLE IF NOT EXISTS daily_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site TEXT NOT NULL,
    date DATE NOT NULL,
    total_views INTEGER DEFAULT 0,
    unique_visitors INTEGER DEFAULT 0,
    top_pages TEXT DEFAULT '[]',  -- JSON array
    top_referrers TEXT DEFAULT '[]',  -- JSON array
    countries TEXT DEFAULT '{}',  -- JSON object
    devices TEXT DEFAULT '{}',  -- JSON object
    UNIQUE(site, date)
);

CREATE INDEX IF NOT EXISTS idx_daily_site_date ON daily_stats(site, date);
