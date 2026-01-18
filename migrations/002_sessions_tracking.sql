-- Migration: Add sessions tracking support
-- PRD: session-metrics (session-1)
-- Date: 2026-01-17
--
-- This migration adds:
-- 1. session_id column to page_views table
-- 2. sessions table for aggregated session metrics
-- 3. Necessary indexes for session queries

-- Add session_id to page_views (nullable for existing rows)
ALTER TABLE page_views ADD COLUMN session_id TEXT DEFAULT NULL;

-- Create index for session queries
CREATE INDEX IF NOT EXISTS idx_site_session ON page_views(site, session_id);

-- Sessions table for aggregated metrics
CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site TEXT NOT NULL,
    session_id TEXT NOT NULL UNIQUE,
    visitor_hash TEXT NOT NULL,

    -- Session timing
    started_at DATETIME NOT NULL,
    last_activity_at DATETIME NOT NULL,
    ended_at DATETIME DEFAULT NULL,
    duration_seconds INTEGER DEFAULT NULL,

    -- Entry/exit tracking
    entry_page TEXT NOT NULL,
    exit_page TEXT DEFAULT NULL,
    pageview_count INTEGER DEFAULT 1,

    -- Engagement metrics
    is_bounce INTEGER DEFAULT 1,        -- 1 = single pageview session

    -- Attribution (from first pageview)
    referrer TEXT DEFAULT '',
    referrer_type TEXT DEFAULT '',
    referrer_domain TEXT DEFAULT '',
    utm_source TEXT DEFAULT '',
    utm_medium TEXT DEFAULT '',
    utm_campaign TEXT DEFAULT '',

    -- Geography/device (from first pageview)
    country TEXT DEFAULT '',
    region TEXT DEFAULT '',
    device_type TEXT DEFAULT '',
    browser TEXT DEFAULT '',
    os TEXT DEFAULT ''
);

-- Indexes for session queries
CREATE INDEX IF NOT EXISTS idx_sessions_site_started ON sessions(site, started_at);
CREATE INDEX IF NOT EXISTS idx_sessions_site_session ON sessions(site, session_id);
CREATE INDEX IF NOT EXISTS idx_sessions_visitor ON sessions(site, visitor_hash);
CREATE INDEX IF NOT EXISTS idx_sessions_bounce ON sessions(site, is_bounce);

-- Migration complete
