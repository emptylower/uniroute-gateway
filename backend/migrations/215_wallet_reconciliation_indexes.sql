-- 215_wallet_reconciliation_indexes.sql (Phase 4.1-G, redesign §15.3)
-- The two per-user read indexes the reconciliation summary's queries run on.
-- 208's partial indexes cover the dispatcher (pending by next_attempt_at,
-- in_flight by claimed_at), 214's covers the split's parent lookup, and
-- 213's (resolution, classified_at) covers the reaper — none serves a
-- per-user window scan, which the summary needs. wallet_live_provisional
-- is already covered by 211's (platform_user_id, status).
-- The trailing id on the outbox index makes both the cursor predicate
-- (id > $4) and the ORDER BY id index-resident, so a page boundary can
-- neither skip nor duplicate a row, including rows sharing an occurred_at
-- across the boundary.
CREATE INDEX IF NOT EXISTS idx_wallet_settlement_outbox_user_occurred ON wallet_settlement_outbox (platform_user_id, occurred_at, id);
CREATE INDEX IF NOT EXISTS idx_wallet_hold_outcome_user_armed ON wallet_hold_outcome (platform_user_id, armed_at);
