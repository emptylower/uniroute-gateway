-- Async KIE requests are owned by the gateway, including unknown submissions.
-- No TTL/delete cascade may discard a pending financial obligation.
ALTER TABLE usage_logs ADD COLUMN IF NOT EXISTS media_type TEXT;
CREATE TABLE IF NOT EXISTS gateway_media_provider (
    provider TEXT PRIMARY KEY CHECK (provider = 'kie'),
    account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT
);
WITH new_account AS (
    INSERT INTO accounts (name, platform, type, credentials, extra, status, schedulable)
    SELECT 'KIE Playground (managed)', 'openai', 'apikey', '{}'::jsonb,
           '{"gateway_media_provider":"kie"}'::jsonb, 'disabled', false
    WHERE NOT EXISTS (SELECT 1 FROM gateway_media_provider WHERE provider = 'kie')
    RETURNING id
)
INSERT INTO gateway_media_provider (provider, account_id)
SELECT 'kie', id FROM new_account ON CONFLICT (provider) DO NOTHING;

CREATE TABLE IF NOT EXISTS gateway_media_task (
    id TEXT PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    platform_user_id TEXT NOT NULL,
    api_key_id BIGINT NOT NULL REFERENCES api_keys(id) ON DELETE RESTRICT,
    idempotency_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    model TEXT NOT NULL,
    media_type TEXT NOT NULL CHECK (media_type IN ('image','video','music')),
    option TEXT NOT NULL,
    prompt TEXT NOT NULL,
    request_payload JSONB NOT NULL,
    billing_snapshot_id TEXT NOT NULL REFERENCES wallet_billing_snapshot(id) ON DELETE RESTRICT,
    quoted_units BIGINT NOT NULL CHECK (quoted_units > 0),
    authorization_id TEXT NOT NULL UNIQUE,
    lease_id TEXT,
    lease_basis JSONB,
    held_units BIGINT NOT NULL DEFAULT 0 CHECK (held_units >= 0),
    actual_units BIGINT CHECK (actual_units >= 0 AND actual_units <= held_units),
    settlement_event_id TEXT NOT NULL UNIQUE,
    authorization_token TEXT,
    provider_task_id TEXT UNIQUE,
    status TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued','authorizing','submitting','processing','settling','releasing','completed','failed','indeterminate')),
    pin_state TEXT NOT NULL DEFAULT 'none' CHECK (pin_state IN ('none','active','finished')),
    result JSONB NOT NULL DEFAULT '{"urls":[]}'::jsonb,
    error_code TEXT,
    error_message TEXT,
    claimed_by TEXT,
    claim_until TIMESTAMPTZ,
    next_poll_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deadline_at TIMESTAMPTZ NOT NULL,
    settled_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, idempotency_key),
    CHECK ((held_units = 0 AND lease_id IS NULL) OR (held_units > 0 AND lease_id IS NOT NULL)),
    CHECK (status NOT IN ('processing','settling','completed') OR (provider_task_id IS NOT NULL AND held_units > 0)),
    CHECK (status NOT IN ('settling','completed') OR (actual_units IS NOT NULL AND actual_units > 0)),
    CHECK (status <> 'failed' OR (actual_units = 0 AND pin_state = 'finished'))
);
CREATE INDEX IF NOT EXISTS idx_gateway_media_task_pending
    ON gateway_media_task (next_poll_at) WHERE status NOT IN ('completed','failed');
CREATE INDEX IF NOT EXISTS idx_gateway_media_task_user_created
    ON gateway_media_task (user_id, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_gateway_media_task_lease_active
    ON gateway_media_task (lease_id) WHERE pin_state <> 'finished';
