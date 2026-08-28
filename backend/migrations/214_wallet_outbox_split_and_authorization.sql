-- 214_wallet_outbox_split_and_authorization.sql (Phase 3.5, redesign §11.9)
ALTER TABLE wallet_settlement_outbox ADD COLUMN IF NOT EXISTS authorization_id TEXT;
ALTER TABLE wallet_settlement_outbox ADD COLUMN IF NOT EXISTS parent_event_id TEXT;
ALTER TABLE wallet_settlement_outbox ADD COLUMN IF NOT EXISTS split_depth INTEGER NOT NULL DEFAULT 0;
ALTER TABLE wallet_settlement_outbox ADD COLUMN IF NOT EXISTS pending_release_units BIGINT;
CREATE INDEX IF NOT EXISTS idx_wallet_settlement_outbox_parent ON wallet_settlement_outbox (parent_event_id);
COMMENT ON COLUMN wallet_settlement_outbox.dead_letter_reason IS 'NULL = predates 212 (unknown); balance_shortfall | attempts_exhausted | contract_violation | payload_conflict | split_exhausted';
COMMENT ON COLUMN wallet_settlement_outbox.pending_release_units IS 'Phase 3.5 §11.3: units the dispatcher still owes back to the bound lease after a split (released on the next delivery, then NULL)';
