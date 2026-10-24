-- 004: bot label on events at collect time (2026-10-24)
-- page_views has carried is_bot/bot_name since the start and the cloud-ASN rule since
-- 2026-10-10 (7a5ae8f), but events never did, so a report that wants human-only event
-- rates (the blakecrosley.com Workshop module panel) has to join events to page_views on
-- session_id over 28 days, which exceeds D1's CPU limit. The collector now runs the same
-- UA + datacenter-ASN detection on /event and writes the label here. Rows written before
-- the deploy keep NULL (unlabeled), so a report can tell "unlabeled" from "human".
-- Policy stays label-never-filter: the worker records, the report decides.
ALTER TABLE events ADD COLUMN is_bot INTEGER;
ALTER TABLE events ADD COLUMN bot_name TEXT;
