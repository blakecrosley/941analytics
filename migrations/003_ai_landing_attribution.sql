-- 003: AI fetch → landing attribution (2026-10-10)
-- Written daily by resumegeni/analytics/ai_landing_attribution.py (GitHub
-- Action). Joins no-referrer deep-page landings (sessions) to AI live fetches
-- of the same URL within 2h (Cloudflare edge: Claude-User, ChatGPT-User,
-- Perplexity-User, OAI-SearchBot), with search-referred landings as the
-- coincidence control. Persisted here because Cloudflare keeps raw requests
-- ~8 days. The script creates the table if missing; this file is the record.
CREATE TABLE IF NOT EXISTS ai_landing_attribution_daily (
  date TEXT NOT NULL,
  site TEXT NOT NULL,
  cohort TEXT NOT NULL,          -- direct_deep | search_deep
  agent TEXT NOT NULL,           -- claude_user | chatgpt_user | perplexity_user | oai_search | any_ai
  landings INTEGER NOT NULL,     -- entry sessions in the cohort that day
  matched INTEGER NOT NULL,      -- of those, exact URL fetched by agent in the same/previous hour
  fetches INTEGER NOT NULL,      -- agent HTML fetches that day, site-wide
  window_hours INTEGER NOT NULL DEFAULT 2,
  computed_at TEXT NOT NULL,
  PRIMARY KEY (date, site, cohort, agent)
);
