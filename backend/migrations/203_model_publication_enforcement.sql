-- Phase 5: model publication eligibility projection, events, and enforce activation.
-- Additive only; never mutates channel prices, mappings, usage, route policy, or observations.
-- Governed provider IDs are exactly anthropic, openai, gemini, grok. Protocol (anthropic/openai/gemini)
-- and provider remain separate.

-- Channel version support for optimistic writes (model-level PATCH requires current versions).
ALTER TABLE channels ADD COLUMN IF NOT EXISTS governance_version BIGINT NOT NULL DEFAULT 1;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_channels_governance_version' AND conrelid = 'channels'::regclass
    ) THEN
        ALTER TABLE channels ADD CONSTRAINT chk_channels_governance_version CHECK (governance_version > 0);
    END IF;
END $$;

-- Current eligibility projection: one row per (account, canonical_model, channel).
-- This is the sole source for catalog and dispatch in enforce mode.
CREATE TABLE IF NOT EXISTS model_publication_eligibility (
    id BIGSERIAL PRIMARY KEY,
    account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    canonical_model_id TEXT NOT NULL,
    channel_id BIGINT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    eligibility VARCHAR(32) NOT NULL,
    reason VARCHAR(128) NOT NULL,
    registry_version BIGINT NOT NULL,
    channel_version BIGINT NOT NULL,
    quarantine_batch_id VARCHAR(64),
    batch_id VARCHAR(64),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_model_publication_eligibility_eligibility
        CHECK (eligibility IN ('eligible', 'blocked', 'quarantined', 'retired')),
    CONSTRAINT chk_model_publication_eligibility_reason CHECK (reason <> ''),
    CONSTRAINT chk_model_publication_eligibility_registry_version CHECK (registry_version > 0),
    CONSTRAINT chk_model_publication_eligibility_channel_version CHECK (channel_version > 0),
    CONSTRAINT chk_model_publication_eligibility_canonical CHECK (canonical_model_id <> ''),
    CONSTRAINT uq_model_publication_eligibility_identity
        UNIQUE (account_id, canonical_model_id, channel_id)
);

CREATE INDEX IF NOT EXISTS idx_model_publication_eligibility_account_channel
    ON model_publication_eligibility(account_id, channel_id);
CREATE INDEX IF NOT EXISTS idx_model_publication_eligibility_eligibility
    ON model_publication_eligibility(eligibility);
CREATE INDEX IF NOT EXISTS idx_model_publication_eligibility_canonical
    ON model_publication_eligibility(md5(canonical_model_id));

-- Exact canonical identity writes require READ COMMITTED (mirrors model_registry / observations).
CREATE OR REPLACE FUNCTION enforce_model_publication_eligibility_exact_unique()
RETURNS TRIGGER AS $$
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'exact model publication eligibility writes require READ COMMITTED isolation'
            USING ERRCODE = '0A000';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        jsonb_build_array('model_publication_eligibility', NEW.account_id, NEW.canonical_model_id, NEW.channel_id)::text, 0
    ));
    IF EXISTS (
        SELECT 1 FROM model_publication_eligibility existing
        WHERE existing.account_id = NEW.account_id
          AND existing.canonical_model_id = NEW.canonical_model_id
          AND existing.channel_id = NEW.channel_id
          AND existing.id <> NEW.id
    ) THEN
        RAISE EXCEPTION 'duplicate model publication eligibility identity'
            USING ERRCODE = '23505', CONSTRAINT = 'uq_model_publication_eligibility_identity';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_model_publication_eligibility_exact_unique ON model_publication_eligibility;
CREATE TRIGGER trg_model_publication_eligibility_exact_unique
    BEFORE INSERT OR UPDATE OF account_id, canonical_model_id, channel_id ON model_publication_eligibility
    FOR EACH ROW EXECUTE FUNCTION enforce_model_publication_eligibility_exact_unique();

CREATE OR REPLACE FUNCTION touch_model_publication_eligibility_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_model_publication_eligibility_updated_at ON model_publication_eligibility;
CREATE TRIGGER trg_model_publication_eligibility_updated_at
    BEFORE UPDATE ON model_publication_eligibility
    FOR EACH ROW EXECUTE FUNCTION touch_model_publication_eligibility_updated_at();

-- Append-only event history for every recompute / quarantine / restore / activation delta.
CREATE TABLE IF NOT EXISTS model_publication_events (
    id BIGSERIAL PRIMARY KEY,
    account_id BIGINT NOT NULL,
    canonical_model_id TEXT NOT NULL,
    channel_id BIGINT NOT NULL,
    eligibility VARCHAR(32) NOT NULL,
    reason VARCHAR(128) NOT NULL,
    registry_version BIGINT NOT NULL,
    channel_version BIGINT NOT NULL,
    quarantine_batch_id VARCHAR(64),
    batch_id VARCHAR(64) NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL UNIQUE,
    event_type VARCHAR(64) NOT NULL,
    actor_id VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_model_publication_events_eligibility
        CHECK (eligibility IN ('eligible', 'blocked', 'quarantined', 'retired')),
    CONSTRAINT chk_model_publication_events_reason CHECK (reason <> ''),
    CONSTRAINT chk_model_publication_events_registry_version CHECK (registry_version > 0),
    CONSTRAINT chk_model_publication_events_channel_version CHECK (channel_version > 0),
    CONSTRAINT chk_model_publication_events_canonical CHECK (canonical_model_id <> ''),
    CONSTRAINT chk_model_publication_events_event_type CHECK (event_type <> ''),
    CONSTRAINT chk_model_publication_events_actor CHECK (actor_id <> ''),
    CONSTRAINT chk_model_publication_events_batch CHECK (batch_id <> ''),
    CONSTRAINT chk_model_publication_events_payload CHECK (jsonb_typeof(payload) = 'object')
);

CREATE INDEX IF NOT EXISTS idx_model_publication_events_identity
    ON model_publication_events(account_id, canonical_model_id, channel_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_model_publication_events_batch
    ON model_publication_events(batch_id);
CREATE INDEX IF NOT EXISTS idx_model_publication_events_batch_canonical
    ON model_publication_events(batch_id, md5(canonical_model_id));

DROP TRIGGER IF EXISTS trg_model_publication_events_append_only ON model_publication_events;
CREATE TRIGGER trg_model_publication_events_append_only
    BEFORE UPDATE OR DELETE ON model_publication_events
    FOR EACH ROW
    EXECUTE FUNCTION reject_append_only_event_mutation();

DROP TRIGGER IF EXISTS trg_model_publication_events_reject_truncate ON model_publication_events;
CREATE TRIGGER trg_model_publication_events_reject_truncate
    BEFORE TRUNCATE ON model_publication_events
    FOR EACH STATEMENT
    EXECUTE FUNCTION reject_append_only_event_mutation();

-- Enforce activation records: transactionally validated domain operation, not an ordinary setting change.
-- Mode changes must go through the activation endpoint; an ordinary setting write cannot enter enforce.
CREATE TABLE IF NOT EXISTS model_authorization_activations (
    id BIGSERIAL PRIMARY KEY,
    inventory_hash VARCHAR(128) NOT NULL,
    registry_version BIGINT NOT NULL,
    channel_versions JSONB NOT NULL DEFAULT '{}'::jsonb,
    projected_batch_id VARCHAR(64) NOT NULL,
    acknowledged_by VARCHAR(255) NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL UNIQUE,
    mode_before VARCHAR(32) NOT NULL,
    mode_after VARCHAR(32) NOT NULL,
    actor_id VARCHAR(255) NOT NULL,
    evidence_ref TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_model_authorization_activations_inventory_hash CHECK (inventory_hash <> ''),
    CONSTRAINT chk_model_authorization_activations_registry_version CHECK (registry_version > 0),
    CONSTRAINT chk_model_authorization_activations_projected_batch CHECK (projected_batch_id <> ''),
    CONSTRAINT chk_model_authorization_activations_ack CHECK (acknowledged_by <> ''),
    CONSTRAINT chk_model_authorization_activations_mode_before
        CHECK (mode_before IN ('off', 'shadow', 'enforce')),
    CONSTRAINT chk_model_authorization_activations_mode_after
        CHECK (mode_after IN ('off', 'shadow', 'enforce')),
    CONSTRAINT chk_model_authorization_activations_payload CHECK (jsonb_typeof(channel_versions) = 'object')
);

CREATE INDEX IF NOT EXISTS idx_model_authorization_activations_created
    ON model_authorization_activations(created_at DESC);

DROP TRIGGER IF EXISTS trg_model_authorization_activations_append_only ON model_authorization_activations;
CREATE TRIGGER trg_model_authorization_activations_append_only
    BEFORE UPDATE OR DELETE ON model_authorization_activations
    FOR EACH ROW
    EXECUTE FUNCTION reject_append_only_event_mutation();

DROP TRIGGER IF EXISTS trg_model_authorization_activations_reject_truncate ON model_authorization_activations;
CREATE TRIGGER trg_model_authorization_activations_reject_truncate
    BEFORE TRUNCATE ON model_authorization_activations
    FOR EACH STATEMENT
    EXECUTE FUNCTION reject_append_only_event_mutation();

-- Governance idempotency records for quarantine / restore / recompute / activation operations.
CREATE TABLE IF NOT EXISTS governance_idempotency_records (
    id BIGSERIAL PRIMARY KEY,
    idempotency_key VARCHAR(255) NOT NULL UNIQUE,
    operation VARCHAR(64) NOT NULL,
    request_hash VARCHAR(128) NOT NULL,
    response_status INTEGER,
    response_body JSONB,
    actor_id VARCHAR(255) NOT NULL,
    registry_version BIGINT,
    channel_versions JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '7 days',
    CONSTRAINT chk_governance_idempotency_records_operation CHECK (operation <> ''),
    CONSTRAINT chk_governance_idempotency_records_actor CHECK (actor_id <> ''),
    CONSTRAINT chk_governance_idempotency_records_request_hash CHECK (request_hash <> ''),
    CONSTRAINT chk_governance_idempotency_records_payload CHECK (jsonb_typeof(channel_versions) = 'object')
);

CREATE INDEX IF NOT EXISTS idx_governance_idempotency_records_expires
    ON governance_idempotency_records(expires_at);

CREATE OR REPLACE FUNCTION touch_governance_idempotency_records_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_governance_idempotency_records_updated_at ON governance_idempotency_records;
CREATE TRIGGER trg_governance_idempotency_records_updated_at
    BEFORE UPDATE ON governance_idempotency_records
    FOR EACH ROW EXECUTE FUNCTION touch_governance_idempotency_records_updated_at();
