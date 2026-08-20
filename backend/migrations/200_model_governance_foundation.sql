-- Additive evidence and inventory foundation for model governance.
-- Runtime authorization remains unchanged; connection_id stays nullable until Phase 5.
-- Account IDs are permanent incarnation identifiers and must never be reused after deletion.
-- Accepted evidence batches are retained indefinitely in Phase 2; migration 200 is unshipped.

ALTER TABLE usage_logs
    ADD COLUMN IF NOT EXISTS governance_target_platform VARCHAR(32);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'chk_usage_logs_governance_target_platform'
          AND conrelid = 'usage_logs'::regclass
    ) THEN
        ALTER TABLE usage_logs
            ADD CONSTRAINT chk_usage_logs_governance_target_platform
            CHECK (governance_target_platform IS NULL OR governance_target_platform IN (
                'anthropic', 'openai', 'gemini', 'antigravity', 'grok'
            ));
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS account_incarnation_ids (
    account_id BIGINT PRIMARY KEY,
    issued_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE account_incarnation_ids IS
    'Permanent append-only ledger of every account incarnation ID ever issued; entries have no retention period.';
COMMENT ON COLUMN account_incarnation_ids.account_id IS
    'Account incarnation ID; once issued this numeric value can never be reused.';

-- The migration runner executes this file in one transaction, so this lock
-- closes the backfill-trigger gap while permitting concurrent account reads.
LOCK TABLE accounts IN SHARE ROW EXCLUSIVE MODE;

INSERT INTO account_incarnation_ids (account_id)
SELECT id FROM accounts
ON CONFLICT (account_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS model_registry (
    id BIGSERIAL PRIMARY KEY,
    canonical_id TEXT NOT NULL,
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
    alias TEXT NOT NULL,
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
    account_id BIGINT NOT NULL,
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
    upstream_model_id TEXT NOT NULL,
    snapshot_batch_id VARCHAR(64) NOT NULL REFERENCES model_classification_batches(batch_id) ON DELETE RESTRICT,
    resolved_registry_id BIGINT REFERENCES model_registry(id) ON DELETE SET NULL,
    classification VARCHAR(32) NOT NULL DEFAULT 'discovered',
    classification_reason VARCHAR(128) NOT NULL DEFAULT 'awaiting_registry_classification',
    upstream_presence VARCHAR(16) NOT NULL DEFAULT 'present',
    miss_streak INTEGER NOT NULL DEFAULT 0,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_model_observations_classification
        CHECK (classification IN ('discovered', 'approved', 'cross_provider', 'unknown', 'ignored')),
    CONSTRAINT chk_model_observations_presence
        CHECK (upstream_presence IN ('present', 'missing')),
    CONSTRAINT chk_model_observations_miss_streak CHECK (miss_streak >= 0),
    CONSTRAINT chk_model_observations_seen_order CHECK (last_seen_at >= first_seen_at)
);

CREATE TABLE IF NOT EXISTS model_observation_events (
    id BIGSERIAL PRIMARY KEY,
    observation_id BIGINT NOT NULL,
    batch_id VARCHAR(64) NOT NULL REFERENCES model_classification_batches(batch_id) ON DELETE RESTRICT,
    snapshot_batch_id VARCHAR(64) NOT NULL REFERENCES model_classification_batches(batch_id) ON DELETE RESTRICT,
    account_id BIGINT NOT NULL,
    upstream_model_id TEXT NOT NULL,
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
    target_platform VARCHAR(32) NOT NULL,
    upstream_model_id TEXT NOT NULL,
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
CREATE INDEX IF NOT EXISTS idx_model_registry_canonical_bucket
    ON model_registry(md5(canonical_id));
CREATE INDEX IF NOT EXISTS idx_model_registry_aliases_registry_id
    ON model_registry_aliases(registry_id);
CREATE INDEX IF NOT EXISTS idx_model_registry_aliases_alias_bucket
    ON model_registry_aliases(md5(alias));
CREATE INDEX IF NOT EXISTS idx_model_registry_events_registry_created
    ON model_registry_events(registry_id, created_at, id);
CREATE INDEX IF NOT EXISTS idx_model_classification_batches_account_created
    ON model_classification_batches(account_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_model_observations_classification_presence
    ON model_observations(classification, upstream_presence);
CREATE INDEX IF NOT EXISTS idx_model_observations_identity_bucket
    ON model_observations(account_id, md5(upstream_model_id));
CREATE INDEX IF NOT EXISTS idx_model_observation_events_observation_created
    ON model_observation_events(observation_id, created_at, id);
CREATE INDEX IF NOT EXISTS idx_model_inventory_items_run_classification
    ON model_inventory_items(run_id, classification);
CREATE INDEX IF NOT EXISTS idx_model_inventory_items_identity_bucket
    ON model_inventory_items(
        run_id,
        account_id,
        (group_id IS NULL),
        COALESCE(group_id, 0),
        (channel_id IS NULL),
        COALESCE(channel_id, 0),
        target_platform,
        md5(upstream_model_id)
    );

-- Fixed-size hashes are lookup buckets and advisory-lock inputs only. Exact TEXT
-- equality decides identity, so different values in the same bucket coexist.
-- Exact-identity writes support READ COMMITTED only: after an advisory-lock
-- wait, each exact probe must receive a fresh statement snapshot. Governance
-- production writes use the default READ COMMITTED isolation; inventory's
-- REPEATABLE READ transaction is read-only and therefore never invokes these
-- write triggers.
CREATE OR REPLACE FUNCTION enforce_model_registry_canonical_exact_unique()
RETURNS TRIGGER AS $$
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'exact model identity writes require READ COMMITTED isolation'
            USING ERRCODE = '0A000';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        jsonb_build_array('model_registry', NEW.canonical_id)::text, 0
    ));
    IF EXISTS (
        SELECT 1 FROM model_registry existing
        WHERE md5(existing.canonical_id) = md5(NEW.canonical_id)
          AND existing.canonical_id = NEW.canonical_id
          AND existing.id <> NEW.id
    ) THEN
        RAISE EXCEPTION 'duplicate model registry canonical_id'
            USING ERRCODE = '23505', CONSTRAINT = 'uq_model_registry_canonical_exact';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION enforce_model_registry_alias_exact_unique()
RETURNS TRIGGER AS $$
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'exact model identity writes require READ COMMITTED isolation'
            USING ERRCODE = '0A000';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        jsonb_build_array('model_registry_aliases', NEW.alias)::text, 0
    ));
    IF EXISTS (
        SELECT 1 FROM model_registry_aliases existing
        WHERE md5(existing.alias) = md5(NEW.alias)
          AND existing.alias = NEW.alias
          AND existing.id <> NEW.id
    ) THEN
        RAISE EXCEPTION 'duplicate model registry alias'
            USING ERRCODE = '23505', CONSTRAINT = 'uq_model_registry_alias_exact';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION enforce_model_observation_exact_unique()
RETURNS TRIGGER AS $$
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'exact model identity writes require READ COMMITTED isolation'
            USING ERRCODE = '0A000';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        jsonb_build_array('model_observations', NEW.account_id, NEW.upstream_model_id)::text, 0
    ));
    IF EXISTS (
        SELECT 1 FROM model_observations existing
        WHERE existing.account_id = NEW.account_id
          AND md5(existing.upstream_model_id) = md5(NEW.upstream_model_id)
          AND existing.upstream_model_id = NEW.upstream_model_id
          AND existing.id <> NEW.id
    ) THEN
        RAISE EXCEPTION 'duplicate model observation identity'
            USING ERRCODE = '23505', CONSTRAINT = 'uq_model_observations_identity_exact';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION enforce_model_inventory_item_exact_unique()
RETURNS TRIGGER AS $$
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'exact model identity writes require READ COMMITTED isolation'
            USING ERRCODE = '0A000';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        jsonb_build_array(
            'model_inventory_items', NEW.run_id, NEW.account_id,
            NEW.group_id, NEW.channel_id, NEW.target_platform, NEW.upstream_model_id
        )::text, 0
    ));
    IF EXISTS (
        SELECT 1 FROM model_inventory_items existing
        WHERE existing.run_id = NEW.run_id
          AND existing.account_id = NEW.account_id
          AND existing.group_id IS NOT DISTINCT FROM NEW.group_id
          AND existing.channel_id IS NOT DISTINCT FROM NEW.channel_id
          AND existing.target_platform = NEW.target_platform
          AND md5(existing.upstream_model_id) = md5(NEW.upstream_model_id)
          AND existing.upstream_model_id = NEW.upstream_model_id
          AND existing.id <> NEW.id
    ) THEN
        RAISE EXCEPTION 'duplicate model inventory item identity'
            USING ERRCODE = '23505', CONSTRAINT = 'uq_model_inventory_items_identity_exact';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION enforce_account_incarnation_id_not_reused()
RETURNS TRIGGER AS $$
BEGIN
	-- Only successfully inserted account incarnations consume IDs: this ledger
	-- insert is transactional and rolls back if the enclosing account insert fails.
	BEGIN
		INSERT INTO account_incarnation_ids (account_id) VALUES (NEW.id);
	EXCEPTION WHEN unique_violation THEN
        RAISE EXCEPTION 'account ID is a permanent incarnation identifier and cannot be reused: %', NEW.id
            USING ERRCODE = '23505', CONSTRAINT = 'account_id_incarnation_not_reused';
	END;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION reject_account_incarnation_id_update()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'account incarnation ID is immutable: %', OLD.id;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_model_registry_canonical_exact_unique ON model_registry;
CREATE TRIGGER trg_model_registry_canonical_exact_unique
    BEFORE INSERT OR UPDATE OF canonical_id ON model_registry
    FOR EACH ROW EXECUTE FUNCTION enforce_model_registry_canonical_exact_unique();

DROP TRIGGER IF EXISTS trg_model_registry_alias_exact_unique ON model_registry_aliases;
CREATE TRIGGER trg_model_registry_alias_exact_unique
    BEFORE INSERT OR UPDATE OF alias ON model_registry_aliases
    FOR EACH ROW EXECUTE FUNCTION enforce_model_registry_alias_exact_unique();

DROP TRIGGER IF EXISTS trg_model_observation_exact_unique ON model_observations;
CREATE TRIGGER trg_model_observation_exact_unique
    BEFORE INSERT OR UPDATE OF account_id, upstream_model_id ON model_observations
    FOR EACH ROW EXECUTE FUNCTION enforce_model_observation_exact_unique();

DROP TRIGGER IF EXISTS trg_model_inventory_item_exact_unique ON model_inventory_items;
CREATE TRIGGER trg_model_inventory_item_exact_unique
    BEFORE INSERT OR UPDATE OF run_id, account_id, group_id, channel_id, target_platform, upstream_model_id ON model_inventory_items
    FOR EACH ROW EXECUTE FUNCTION enforce_model_inventory_item_exact_unique();

DROP TRIGGER IF EXISTS trg_account_incarnation_id_not_reused ON accounts;
CREATE TRIGGER trg_account_incarnation_id_not_reused
    AFTER INSERT ON accounts
    FOR EACH ROW EXECUTE FUNCTION enforce_account_incarnation_id_not_reused();

DROP TRIGGER IF EXISTS trg_account_incarnation_id_immutable ON accounts;
CREATE TRIGGER trg_account_incarnation_id_immutable
    BEFORE UPDATE OF id ON accounts
    FOR EACH ROW EXECUTE FUNCTION reject_account_incarnation_id_update();

CREATE OR REPLACE FUNCTION reject_append_only_event_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION '% is append-only; % is not allowed', TG_TABLE_NAME, TG_OP;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_account_incarnation_ids_append_only ON account_incarnation_ids;
CREATE TRIGGER trg_account_incarnation_ids_append_only
    BEFORE UPDATE OR DELETE ON account_incarnation_ids
    FOR EACH ROW
    EXECUTE FUNCTION reject_append_only_event_mutation();

DROP TRIGGER IF EXISTS trg_account_incarnation_ids_reject_truncate ON account_incarnation_ids;
CREATE TRIGGER trg_account_incarnation_ids_reject_truncate
    BEFORE TRUNCATE ON account_incarnation_ids
    FOR EACH STATEMENT
    EXECUTE FUNCTION reject_append_only_event_mutation();

DROP TRIGGER IF EXISTS trg_model_registry_events_append_only ON model_registry_events;
CREATE TRIGGER trg_model_registry_events_append_only
    BEFORE UPDATE OR DELETE ON model_registry_events
    FOR EACH ROW
    EXECUTE FUNCTION reject_append_only_event_mutation();

DROP TRIGGER IF EXISTS trg_model_registry_events_reject_truncate ON model_registry_events;
CREATE TRIGGER trg_model_registry_events_reject_truncate
    BEFORE TRUNCATE ON model_registry_events
    FOR EACH STATEMENT
    EXECUTE FUNCTION reject_append_only_event_mutation();

DROP TRIGGER IF EXISTS trg_model_classification_batches_append_only ON model_classification_batches;
CREATE TRIGGER trg_model_classification_batches_append_only
    BEFORE UPDATE OR DELETE ON model_classification_batches
    FOR EACH ROW
    EXECUTE FUNCTION reject_append_only_event_mutation();

DROP TRIGGER IF EXISTS trg_model_classification_batches_reject_truncate ON model_classification_batches;
CREATE TRIGGER trg_model_classification_batches_reject_truncate
    BEFORE TRUNCATE ON model_classification_batches
    FOR EACH STATEMENT
    EXECUTE FUNCTION reject_append_only_event_mutation();

DROP TRIGGER IF EXISTS trg_model_observation_events_append_only ON model_observation_events;
CREATE TRIGGER trg_model_observation_events_append_only
    BEFORE UPDATE OR DELETE ON model_observation_events
    FOR EACH ROW
    EXECUTE FUNCTION reject_append_only_event_mutation();

DROP TRIGGER IF EXISTS trg_model_observation_events_reject_truncate ON model_observation_events;
CREATE TRIGGER trg_model_observation_events_reject_truncate
    BEFORE TRUNCATE ON model_observation_events
    FOR EACH STATEMENT
    EXECUTE FUNCTION reject_append_only_event_mutation();
