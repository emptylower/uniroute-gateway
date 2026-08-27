-- Phase 3.2: immutable billing snapshots. One row per billable attempt whose
-- settlement reached the record path; frozen before the upstream write, read
-- at settlement instead of re-resolving pricing/multipliers/FX. `payload`
-- is the JSON form of service.BillingSnapshot (versioned by `version`).
-- Rows are written best-effort from the settlement path with ON CONFLICT DO
-- NOTHING on id, so a redelivered settlement never rewrites a snapshot.
CREATE TABLE IF NOT EXISTS wallet_billing_snapshot (
    id TEXT PRIMARY KEY,
    version INT NOT NULL,
    user_id BIGINT NOT NULL,
    api_key_id BIGINT NOT NULL,
    group_id BIGINT,
    account_id BIGINT NOT NULL,
    billing_model TEXT NOT NULL,
    pricing_mode TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_wallet_billing_snapshot_user_created
    ON wallet_billing_snapshot (user_id, created_at DESC);
