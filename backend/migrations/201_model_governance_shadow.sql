-- Phase 3 shadow persistence: model_shadow_decisions stores per-model publication evaluation without mutating routing.
-- Additive migration; does not affect Phase 2 tables.

-- Enforce global registry version uniqueness to serialize concurrent decisions without a global advisory lock.
CREATE UNIQUE INDEX IF NOT EXISTS uq_model_registry_events_version ON model_registry_events(registry_version);

CREATE TABLE IF NOT EXISTS model_shadow_decisions (
    id BIGSERIAL PRIMARY KEY,
    batch_id VARCHAR(64) NOT NULL REFERENCES model_classification_batches(batch_id) ON DELETE RESTRICT,
    upstream_model_id TEXT NOT NULL,
    canonical_id TEXT,
    provider VARCHAR(32),
    classification VARCHAR(32) NOT NULL,
    eligibility VARCHAR(32) NOT NULL,
    reason VARCHAR(128) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_model_shadow_decisions_classification
        CHECK (classification IN ('discovered', 'approved', 'cross_provider', 'unknown', 'ignored')),
    CONSTRAINT chk_model_shadow_decisions_eligibility
        CHECK (eligibility IN ('eligible', 'blocked', 'quarantined', 'retired')),
    CONSTRAINT chk_model_shadow_decisions_provider
        CHECK (provider IS NULL OR provider IN ('anthropic', 'openai', 'gemini', 'grok')),
    CONSTRAINT chk_model_shadow_decisions_reason CHECK (reason <> '')
);

CREATE INDEX IF NOT EXISTS idx_model_shadow_decisions_batch
    ON model_shadow_decisions(batch_id);
CREATE INDEX IF NOT EXISTS idx_model_shadow_decisions_classification
    ON model_shadow_decisions(classification, eligibility);
CREATE INDEX IF NOT EXISTS idx_model_shadow_decisions_batch_identity
    ON model_shadow_decisions(batch_id, md5(upstream_model_id));

-- Exact identity uniqueness per batch (one decision per upstream model per batch)
CREATE OR REPLACE FUNCTION enforce_model_shadow_decision_exact_unique()
RETURNS TRIGGER AS $$
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'exact model identity writes require READ COMMITTED isolation'
            USING ERRCODE = '0A000';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        jsonb_build_array('model_shadow_decisions', NEW.batch_id, NEW.upstream_model_id)::text, 0
    ));
    IF EXISTS (
        SELECT 1 FROM model_shadow_decisions existing
        WHERE existing.batch_id = NEW.batch_id
          AND md5(existing.upstream_model_id) = md5(NEW.upstream_model_id)
          AND existing.upstream_model_id = NEW.upstream_model_id
          AND existing.id <> NEW.id
    ) THEN
        RAISE EXCEPTION 'duplicate model shadow decision identity'
            USING ERRCODE = '23505', CONSTRAINT = 'uq_model_shadow_decisions_identity_exact';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_model_shadow_decisions_exact_unique ON model_shadow_decisions;
CREATE TRIGGER trg_model_shadow_decisions_exact_unique
    BEFORE INSERT OR UPDATE OF batch_id, upstream_model_id ON model_shadow_decisions
    FOR EACH ROW EXECUTE FUNCTION enforce_model_shadow_decision_exact_unique();

DROP TRIGGER IF EXISTS trg_model_shadow_decisions_append_only ON model_shadow_decisions;
CREATE TRIGGER trg_model_shadow_decisions_append_only
    BEFORE UPDATE OR DELETE ON model_shadow_decisions
    FOR EACH ROW
    EXECUTE FUNCTION reject_append_only_event_mutation();

DROP TRIGGER IF EXISTS trg_model_shadow_decisions_reject_truncate ON model_shadow_decisions;
CREATE TRIGGER trg_model_shadow_decisions_reject_truncate
    BEFORE TRUNCATE ON model_shadow_decisions
    FOR EACH STATEMENT
    EXECUTE FUNCTION reject_append_only_event_mutation();
