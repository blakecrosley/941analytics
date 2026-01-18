-- Migration: 001_sites_configuration
-- Created: 2026-01-17
-- Description: Create sites configuration table and migrate existing sites

-- =============================================================================
-- CREATE SITES TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS sites (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    domain TEXT NOT NULL UNIQUE,
    display_name TEXT,
    timezone TEXT DEFAULT 'America/New_York',
    passkey_hash TEXT,
    is_active INTEGER DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sites_domain ON sites(domain);
CREATE INDEX IF NOT EXISTS idx_sites_active ON sites(is_active);

-- =============================================================================
-- CREATE SITE SETTINGS TABLE
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

-- =============================================================================
-- MIGRATE EXISTING SITES FROM PAGEVIEW DATA
-- =============================================================================
-- Insert all unique sites that have traffic, with friendly display names

INSERT OR IGNORE INTO sites (domain, display_name, timezone, is_active)
SELECT DISTINCT
    site as domain,
    CASE site
        WHEN 'blakecrosley.com' THEN 'Blake Crosley'
        WHEN 'resumegeni.com' THEN 'ResumeGeni'
        WHEN '941return.com' THEN '941 Return'
        WHEN '941apps.com' THEN '941 Apps'
        WHEN 'acecitizenship.app' THEN 'Ace Citizenship'
        WHEN 'h3arted.com' THEN 'H3arted'
        ELSE site  -- Default to domain as display name
    END as display_name,
    'America/New_York' as timezone,
    1 as is_active
FROM page_views
WHERE site IS NOT NULL AND site != '';

-- Also insert any sites from the known list that might not have traffic yet
INSERT OR IGNORE INTO sites (domain, display_name, timezone, is_active) VALUES
    ('blakecrosley.com', 'Blake Crosley', 'America/New_York', 1),
    ('resumegeni.com', 'ResumeGeni', 'America/New_York', 1),
    ('941return.com', '941 Return', 'America/New_York', 1),
    ('941apps.com', '941 Apps', 'America/New_York', 1),
    ('acecitizenship.app', 'Ace Citizenship', 'America/New_York', 1),
    ('h3arted.com', 'H3arted', 'America/New_York', 1);
