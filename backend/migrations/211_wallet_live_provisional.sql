CREATE TABLE IF NOT EXISTS wallet_live_provisional (
    token               TEXT PRIMARY KEY,
    authorization_id    TEXT NOT NULL,
    call_hash           TEXT NOT NULL DEFAULT '',
    platform_user_id    TEXT NOT NULL DEFAULT '',
    user_id             BIGINT NOT NULL,
    api_key_id          BIGINT NOT NULL,
    account_id          BIGINT NOT NULL,
    billing_currency    TEXT NOT NULL,
    billing_snapshot_id TEXT NOT NULL DEFAULT '',
    estimated_units     BIGINT NOT NULL DEFAULT 0,
    status              TEXT NOT NULL,
    windows             JSONB NOT NULL DEFAULT '[]'::jsonb,
    settlement_event_id TEXT NOT NULL DEFAULT '',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    activated_at        TIMESTAMPTZ,
    terminal_at         TIMESTAMPTZ
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_wallet_live_provisional_call_hash
    ON wallet_live_provisional (call_hash) WHERE call_hash <> '';
CREATE INDEX IF NOT EXISTS idx_wallet_live_provisional_user_status
    ON wallet_live_provisional (platform_user_id, status);
CREATE INDEX IF NOT EXISTS idx_wallet_live_provisional_status_created
    ON wallet_live_provisional (status, created_at DESC);
