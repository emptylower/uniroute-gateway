-- Phase 3.2: link a usage row to the billing snapshot it was (or, in record
-- mode, could have been) priced from. Nullable: pre-3.2 rows and rows whose
-- freeze point was skipped (Count Tokens, off mode) carry NULL.
ALTER TABLE usage_logs ADD COLUMN IF NOT EXISTS billing_snapshot_id TEXT;
CREATE INDEX IF NOT EXISTS idx_usage_logs_billing_snapshot_id
    ON usage_logs (billing_snapshot_id)
    WHERE billing_snapshot_id IS NOT NULL;
