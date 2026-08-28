-- 212_wallet_outbox_dead_letter_reason.sql (Phase 3.4a, redesign §9.3)
ALTER TABLE wallet_settlement_outbox ADD COLUMN IF NOT EXISTS dead_letter_reason TEXT;
COMMENT ON COLUMN wallet_settlement_outbox.dead_letter_reason IS 'NULL = predates 212 (unknown); balance_shortfall | attempts_exhausted';
