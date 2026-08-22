-- Phase 6: external model catalog ingestion — immutable evidence storage.
-- External catalogs (OpenRouter, models.dev, LiteLLM) provide candidate evidence only.
-- They never grant routing, publication authority, or production prices.
-- Snapshots and evidence rows are append-only; sync runs reach a terminal state exactly once.

-- One row per ingestion attempt per source.
CREATE TABLE IF NOT EXISTS model_catalog_sync_runs (
    id BIGSERIAL PRIMARY KEY,
    source VARCHAR(32) NOT NULL,
    triggered_by VARCHAR(32) NOT NULL DEFAULT 'scheduled',
    status VARCHAR(32) NOT NULL DEFAULT 'running',
    resolved_commit VARCHAR(64),
    request_url TEXT NOT NULL DEFAULT '',
    item_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    CONSTRAINT chk_model_catalog_sync_runs_source
        CHECK (source IN ('openrouter', 'modelsdev', 'litellm')),
    CONSTRAINT chk_model_catalog_sync_runs_triggered_by
        CHECK (triggered_by IN ('scheduled', 'manual')),
    CONSTRAINT chk_model_catalog_sync_runs_status
        CHECK (status IN ('running', 'succeeded', 'failed')),
    CONSTRAINT chk_model_catalog_sync_runs_commit
        CHECK (resolved_commit IS NULL OR resolved_commit <> ''),
    CONSTRAINT chk_model_catalog_sync_runs_item_count CHECK (item_count >= 0)
);

CREATE INDEX IF NOT EXISTS idx_model_catalog_sync_runs_source_started
    ON model_catalog_sync_runs(source, started_at DESC);

-- A finished sync run is immutable; identity columns never change.
CREATE OR REPLACE FUNCTION enforce_model_catalog_sync_run_terminal_once()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.source <> OLD.source OR NEW.started_at <> OLD.started_at THEN
        RAISE EXCEPTION 'model catalog sync run identity is immutable';
    END IF;
    IF OLD.status <> 'running' THEN
        RAISE EXCEPTION 'model catalog sync run % already finished', OLD.id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_model_catalog_sync_runs_terminal_once ON model_catalog_sync_runs;
CREATE TRIGGER trg_model_catalog_sync_runs_terminal_once
    BEFORE UPDATE ON model_catalog_sync_runs
    FOR EACH ROW
    EXECUTE FUNCTION enforce_model_catalog_sync_run_terminal_once();

DROP TRIGGER IF EXISTS trg_model_catalog_sync_runs_reject_delete ON model_catalog_sync_runs;
CREATE TRIGGER trg_model_catalog_sync_runs_reject_delete
    BEFORE DELETE ON model_catalog_sync_runs
    FOR EACH ROW
    EXECUTE FUNCTION reject_append_only_event_mutation();

-- Immutable raw payload snapshots: exact raw bytes, zstd-compressed.
CREATE TABLE IF NOT EXISTS model_catalog_snapshots (
    id BIGSERIAL PRIMARY KEY,
    sync_run_id BIGINT NOT NULL REFERENCES model_catalog_sync_runs(id),
    source VARCHAR(32) NOT NULL,
    external_version VARCHAR(255) NOT NULL,
    payload_zstd BYTEA NOT NULL,
    payload_sha256 VARCHAR(80) NOT NULL,
    payload_size INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_model_catalog_snapshots_source
        CHECK (source IN ('openrouter', 'modelsdev', 'litellm')),
    CONSTRAINT chk_model_catalog_snapshots_version CHECK (external_version <> ''),
    CONSTRAINT chk_model_catalog_snapshots_payload
        CHECK (payload_size > 0 AND octet_length(payload_zstd) > 0),
    CONSTRAINT chk_model_catalog_snapshots_sha256
        CHECK (payload_sha256 LIKE 'sha256:%' AND length(payload_sha256) = 71),
    CONSTRAINT chk_model_catalog_snapshots_created CHECK (created_at IS NOT NULL),
    CONSTRAINT uq_model_catalog_snapshots_identity UNIQUE (source, external_version)
);

CREATE INDEX IF NOT EXISTS idx_model_catalog_snapshots_created
    ON model_catalog_snapshots(source, created_at DESC);

DROP TRIGGER IF EXISTS trg_model_catalog_snapshots_append_only ON model_catalog_snapshots;
CREATE TRIGGER trg_model_catalog_snapshots_append_only
    BEFORE UPDATE OR DELETE ON model_catalog_snapshots
    FOR EACH ROW
    EXECUTE FUNCTION reject_append_only_event_mutation();

DROP TRIGGER IF EXISTS trg_model_catalog_snapshots_reject_truncate ON model_catalog_snapshots;
CREATE TRIGGER trg_model_catalog_snapshots_reject_truncate
    BEFORE TRUNCATE ON model_catalog_snapshots
    FOR EACH STATEMENT
    EXECUTE FUNCTION reject_append_only_event_mutation();

-- Normalized candidate evidence extracted from one snapshot. Evidence only ever
-- creates review candidates; provider_hint records the external claim verbatim
-- and grants no ownership.
CREATE TABLE IF NOT EXISTS model_catalog_candidate_evidence (
    id BIGSERIAL PRIMARY KEY,
    snapshot_id BIGINT NOT NULL REFERENCES model_catalog_snapshots(id),
    source VARCHAR(32) NOT NULL,
    canonical_model_id TEXT NOT NULL,
    provider_hint VARCHAR(64),
    display_name TEXT,
    context_window BIGINT,
    capabilities JSONB NOT NULL DEFAULT '[]'::jsonb,
    price JSONB,
    raw_ref TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_model_catalog_candidate_evidence_source
        CHECK (source IN ('openrouter', 'modelsdev', 'litellm')),
    CONSTRAINT chk_model_catalog_candidate_evidence_canonical
        CHECK (canonical_model_id <> ''),
    CONSTRAINT chk_model_catalog_candidate_evidence_context
        CHECK (context_window IS NULL OR context_window > 0),
    CONSTRAINT chk_model_catalog_candidate_evidence_capabilities
        CHECK (jsonb_typeof(capabilities) = 'array'),
    CONSTRAINT chk_model_catalog_candidate_evidence_price
        CHECK (price IS NULL OR jsonb_typeof(price) = 'object'),
    CONSTRAINT uq_model_catalog_candidate_evidence_identity
        UNIQUE (snapshot_id, canonical_model_id)
);

CREATE INDEX IF NOT EXISTS idx_model_catalog_candidate_evidence_canonical
    ON model_catalog_candidate_evidence(md5(canonical_model_id));

DROP TRIGGER IF EXISTS trg_model_catalog_candidate_evidence_append_only ON model_catalog_candidate_evidence;
CREATE TRIGGER trg_model_catalog_candidate_evidence_append_only
    BEFORE UPDATE OR DELETE ON model_catalog_candidate_evidence
    FOR EACH ROW
    EXECUTE FUNCTION reject_append_only_event_mutation();

-- Records that an expected canonical model was absent from a source catalog.
CREATE TABLE IF NOT EXISTS model_catalog_missing_evidence (
    id BIGSERIAL PRIMARY KEY,
    sync_run_id BIGINT NOT NULL REFERENCES model_catalog_sync_runs(id),
    source VARCHAR(32) NOT NULL,
    canonical_model_id TEXT NOT NULL,
    detail JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_model_catalog_missing_evidence_source
        CHECK (source IN ('openrouter', 'modelsdev', 'litellm')),
    CONSTRAINT chk_model_catalog_missing_evidence_canonical
        CHECK (canonical_model_id <> ''),
    CONSTRAINT chk_model_catalog_missing_evidence_detail
        CHECK (jsonb_typeof(detail) = 'object'),
    CONSTRAINT uq_model_catalog_missing_evidence_identity
        UNIQUE (sync_run_id, canonical_model_id)
);

DROP TRIGGER IF EXISTS trg_model_catalog_missing_evidence_append_only ON model_catalog_missing_evidence;
CREATE TRIGGER trg_model_catalog_missing_evidence_append_only
    BEFORE UPDATE OR DELETE ON model_catalog_missing_evidence
    FOR EACH ROW
    EXECUTE FUNCTION reject_append_only_event_mutation();

-- Mutable per-source settings (threshold changes are audited at the service layer).
-- Defaults per plan: count-drop threshold 20%; stale warnings OpenRouter 24h, others 72h.
CREATE TABLE IF NOT EXISTS model_catalog_source_settings (
    source VARCHAR(32) PRIMARY KEY,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    count_drop_threshold_percent INTEGER NOT NULL DEFAULT 20,
    stale_after_hours INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_model_catalog_source_settings_source
        CHECK (source IN ('openrouter', 'modelsdev', 'litellm')),
    CONSTRAINT chk_model_catalog_source_settings_threshold
        CHECK (count_drop_threshold_percent BETWEEN 1 AND 100),
    CONSTRAINT chk_model_catalog_source_settings_stale CHECK (stale_after_hours > 0)
);

INSERT INTO model_catalog_source_settings (source, stale_after_hours) VALUES
    ('openrouter', 24),
    ('modelsdev', 72),
    ('litellm', 72)
ON CONFLICT (source) DO NOTHING;

CREATE OR REPLACE FUNCTION touch_model_catalog_source_settings_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_model_catalog_source_settings_updated_at ON model_catalog_source_settings;
CREATE TRIGGER trg_model_catalog_source_settings_updated_at
    BEFORE UPDATE ON model_catalog_source_settings
    FOR EACH ROW EXECUTE FUNCTION touch_model_catalog_source_settings_updated_at();
