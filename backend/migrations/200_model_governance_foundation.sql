-- Additive evidence and inventory foundation for model governance.
-- Runtime authorization remains unchanged; connection_id stays nullable until Phase 5.

CREATE TABLE IF NOT EXISTS model_registry (
    id BIGSERIAL PRIMARY KEY,
    canonical_id VARCHAR(255) NOT NULL UNIQUE,
    provider VARCHAR(32) NOT NULL,
    modality VARCHAR(32) NOT NULL,
    lifecycle VARCHAR(32) NOT NULL DEFAULT 'active',
    version BIGINT NOT NULL DEFAULT 1,
    decided_by VARCHAR(255) NOT NULL,
    decided_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    evidence_ref TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_model_registry_provider
        CHECK (provider IN ('anthropic', 'openai', 'gemini', 'grok')),
    CONSTRAINT chk_model_registry_modality
        CHECK (modality IN ('text', 'image', 'audio', 'video', 'embedding', 'other')),
    CONSTRAINT chk_model_registry_lifecycle
        CHECK (lifecycle IN ('active', 'deprecated', 'retired')),
    CONSTRAINT chk_model_registry_version CHECK (version > 0)
);

CREATE TABLE IF NOT EXISTS model_registry_aliases (
    id BIGSERIAL PRIMARY KEY,
    registry_id BIGINT NOT NULL REFERENCES model_registry(id) ON DELETE CASCADE,
    alias VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS model_registry_events (
    id BIGSERIAL PRIMARY KEY,
    registry_id BIGINT,
    idempotency_key VARCHAR(255) NOT NULL UNIQUE,
    event_type VARCHAR(64) NOT NULL,
    registry_version BIGINT NOT NULL,
    actor_id VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_model_registry_events_version CHECK (registry_version > 0),
    CONSTRAINT chk_model_registry_events_payload CHECK (jsonb_typeof(payload) = 'object')
);

CREATE TABLE IF NOT EXISTS model_classification_batches (
    id BIGSERIAL PRIMARY KEY,
    batch_id VARCHAR(64) NOT NULL UNIQUE,
    idempotency_key VARCHAR(255) NOT NULL UNIQUE,
    account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    connection_id BIGINT,
    registry_version BIGINT,
    raw_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    observed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_model_classification_batches_registry_version
        CHECK (registry_version IS NULL OR registry_version > 0)
);

CREATE TABLE IF NOT EXISTS model_observations (
    id BIGSERIAL PRIMARY KEY,
    connection_id BIGINT,
    account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    upstream_model_id VARCHAR(255) NOT NULL,
    resolved_registry_id BIGINT REFERENCES model_registry(id) ON DELETE SET NULL,
    classification VARCHAR(32) NOT NULL DEFAULT 'discovered',
    classification_reason VARCHAR(128) NOT NULL DEFAULT 'awaiting_registry_classification',
    upstream_presence VARCHAR(16) NOT NULL DEFAULT 'present',
    miss_streak INTEGER NOT NULL DEFAULT 0,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    raw_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (account_id, upstream_model_id),
    CONSTRAINT chk_model_observations_classification
        CHECK (classification IN ('discovered', 'approved', 'cross_provider', 'unknown', 'ignored')),
    CONSTRAINT chk_model_observations_presence
        CHECK (upstream_presence IN ('present', 'missing')),
    CONSTRAINT chk_model_observations_miss_streak CHECK (miss_streak >= 0),
    CONSTRAINT chk_model_observations_seen_order CHECK (last_seen_at >= first_seen_at),
    CONSTRAINT chk_model_observations_raw_snapshot CHECK (jsonb_typeof(raw_snapshot) = 'object')
);

CREATE TABLE IF NOT EXISTS model_observation_events (
    id BIGSERIAL PRIMARY KEY,
    observation_id BIGINT NOT NULL,
    batch_id VARCHAR(64),
    account_id BIGINT NOT NULL,
    upstream_model_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    classification VARCHAR(32) NOT NULL,
    classification_reason VARCHAR(128) NOT NULL,
    upstream_presence VARCHAR(16) NOT NULL,
    miss_streak INTEGER NOT NULL DEFAULT 0,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_model_observation_events_classification
        CHECK (classification IN ('discovered', 'approved', 'cross_provider', 'unknown', 'ignored')),
    CONSTRAINT chk_model_observation_events_presence
        CHECK (upstream_presence IN ('present', 'missing')),
    CONSTRAINT chk_model_observation_events_miss_streak CHECK (miss_streak >= 0),
    CONSTRAINT chk_model_observation_events_payload CHECK (jsonb_typeof(payload) = 'object')
);

CREATE TABLE IF NOT EXISTS model_inventory_runs (
    id BIGSERIAL PRIMARY KEY,
    run_id VARCHAR(64) NOT NULL UNIQUE,
    status VARCHAR(32) NOT NULL DEFAULT 'running',
    inventory_hash VARCHAR(128),
    cutoff_7d TIMESTAMPTZ NOT NULL,
    cutoff_30d TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_model_inventory_runs_status
        CHECK (status IN ('running', 'completed', 'failed')),
    CONSTRAINT chk_model_inventory_runs_cutoffs CHECK (cutoff_7d >= cutoff_30d)
);

CREATE TABLE IF NOT EXISTS model_inventory_items (
    id BIGSERIAL PRIMARY KEY,
    run_id VARCHAR(64) NOT NULL REFERENCES model_inventory_runs(run_id) ON DELETE CASCADE,
    account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    group_id BIGINT REFERENCES groups(id) ON DELETE SET NULL,
    channel_id BIGINT REFERENCES channels(id) ON DELETE SET NULL,
    upstream_model_id VARCHAR(255) NOT NULL,
    classification VARCHAR(32) NOT NULL,
    requests_7d BIGINT NOT NULL DEFAULT 0,
    requests_30d BIGINT NOT NULL DEFAULT 0,
    revenue_7d_billing_micros BIGINT NOT NULL DEFAULT 0,
    revenue_30d_billing_micros BIGINT NOT NULL DEFAULT 0,
    billing_currency VARCHAR(3) NOT NULL,
    affected_api_keys_7d BIGINT NOT NULL DEFAULT 0,
    affected_api_keys_30d BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_model_inventory_items_classification
        CHECK (classification IN ('discovered', 'approved', 'cross_provider', 'unknown', 'ignored')),
    CONSTRAINT chk_model_inventory_items_nonnegative
        CHECK (
            requests_7d >= 0 AND requests_30d >= 0 AND
            revenue_7d_billing_micros >= 0 AND revenue_30d_billing_micros >= 0 AND
            affected_api_keys_7d >= 0 AND affected_api_keys_30d >= 0
        )
);

CREATE INDEX IF NOT EXISTS idx_model_registry_provider_lifecycle
    ON model_registry(provider, lifecycle);
CREATE INDEX IF NOT EXISTS idx_model_registry_aliases_registry_id
    ON model_registry_aliases(registry_id);
CREATE INDEX IF NOT EXISTS idx_model_registry_events_registry_created
    ON model_registry_events(registry_id, created_at, id);
CREATE INDEX IF NOT EXISTS idx_model_classification_batches_account_created
    ON model_classification_batches(account_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_model_observations_classification_presence
    ON model_observations(classification, upstream_presence);
CREATE INDEX IF NOT EXISTS idx_model_observation_events_observation_created
    ON model_observation_events(observation_id, created_at, id);
CREATE INDEX IF NOT EXISTS idx_model_inventory_items_run_classification
    ON model_inventory_items(run_id, classification);
CREATE UNIQUE INDEX IF NOT EXISTS uq_model_inventory_items_dimensions
    ON model_inventory_items(
        run_id,
        account_id,
        (group_id IS NULL),
        COALESCE(group_id, 0),
        (channel_id IS NULL),
        COALESCE(channel_id, 0),
        upstream_model_id
    );

CREATE OR REPLACE FUNCTION reject_append_only_event_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION '% is append-only; % is not allowed', TG_TABLE_NAME, TG_OP;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_model_registry_events_append_only ON model_registry_events;
CREATE TRIGGER trg_model_registry_events_append_only
    BEFORE UPDATE OR DELETE ON model_registry_events
    FOR EACH ROW
    EXECUTE FUNCTION reject_append_only_event_mutation();

DROP TRIGGER IF EXISTS trg_model_observation_events_append_only ON model_observation_events;
CREATE TRIGGER trg_model_observation_events_append_only
    BEFORE UPDATE OR DELETE ON model_observation_events
    FOR EACH ROW
    EXECUTE FUNCTION reject_append_only_event_mutation();
