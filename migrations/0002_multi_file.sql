-- Add source_label to track which file each session came from
ALTER TABLE upload_sessions ADD COLUMN source_label TEXT;

-- Add period_key for grouping multiple files in same period (e.g. "2026-01")
ALTER TABLE upload_sessions ADD COLUMN period_key TEXT;

-- Backfill period_key for existing sessions
UPDATE upload_sessions SET period_key = printf('%04d-%02d', period_year, period_month) WHERE period_key IS NULL;
UPDATE upload_sessions SET source_label = filename WHERE source_label IS NULL;

-- Add case_id dedup index (case_id + session_id unique)
CREATE INDEX IF NOT EXISTS idx_consults_case_session ON consults(session_id, case_id);
