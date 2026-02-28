-- Add is_orderly flag to consults
-- Detects OrderlyMeds cases by organization name (not fee amount)
ALTER TABLE consults ADD COLUMN is_orderly INTEGER DEFAULT 0;

-- Backfill: mark all rows where org name contains "orderly"
UPDATE consults SET is_orderly = 1
WHERE LOWER(organization_name) LIKE '%orderly%';

-- Index for fast orderly filtering
CREATE INDEX IF NOT EXISTS idx_consults_orderly ON consults(is_orderly);
