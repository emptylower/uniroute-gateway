-- 216_wallet_outbox_billing_snapshot.sql (Phase 4.2-G Task 1, 4.1-G's hand-on)
-- The outbox row learns its billing snapshot: leg 4's outbox portion becomes
-- fx-verified once the id is written at ObserveSettlement (the surfaces that
-- hold it) and backfilled once from usage_logs by the guarded operator
-- script — deploy/scripts/wallet-outbox-backfill-billing-snapshot.sql.
-- Metadata-only, instant (round-1 MAJOR-3): the backfill is NOT here — the
-- transactional migrations runner (migrations_runner.go:244-256) would hold
-- ACCESS EXCLUSIVE on wallet_settlement_outbox for the whole join.
-- billing_snapshot_id: the frozen pricing basis of the settlement's usage
--   (wallet_billing_snapshot.id; NULL = pre-4.2, token-less, off-mode — the
--   row reports billing_fx null and is the operator script's backfill set).
-- redrive_count (Task 2): the receivable collector's own bound — how many
--   times a balance_shortfall dead-letter has been re-driven. attempt_count
--   stays the dispatcher's transport budget; the two are disjoint by
--   construction (round-2 note).
ALTER TABLE wallet_settlement_outbox ADD COLUMN IF NOT EXISTS billing_snapshot_id TEXT NULL;
ALTER TABLE wallet_settlement_outbox ADD COLUMN IF NOT EXISTS redrive_count INT NOT NULL DEFAULT 0;
