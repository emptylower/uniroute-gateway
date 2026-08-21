-- Phase 4: upstream connection separation and endpoint probe evidence.
-- Additive migration; does not mutate Phase 2/3 governance tables except adding nullable account connection.
-- Aggregator connection provider is null; first-party provider is concrete and immutable.
-- Probe validity is 24 hours and keyed by connection/account provider/protocol/endpoint/credential version/config version.
-- No shared quota or connection semaphore is introduced.

-- Upstream connections: reusable credentials/network identity separate from provider accounts.
CREATE TABLE IF NOT EXISTS upstream_connections (
    id BIGSERIAL PRIMARY KEY,
    kind VARCHAR(32) NOT NULL,
    provider VARCHAR(32),
    base_url TEXT NOT NULL,
    encrypted_credential TEXT NOT NULL,
    credential_version BIGINT NOT NULL DEFAULT 1,
    proxy_id BIGINT REFERENCES proxies(id) ON DELETE SET NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'active',
    evidence_ref TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ,
    CONSTRAINT chk_upstream_connections_kind
        CHECK (kind IN ('first_party', 'aggregator')),
    CONSTRAINT chk_upstream_connections_provider
        CHECK (provider IS NULL OR provider IN ('anthropic', 'openai', 'gemini', 'grok')),
    CONSTRAINT chk_upstream_connections_kind_provider
        CHECK (
            (kind = 'aggregator' AND provider IS NULL) OR
            (kind = 'first_party' AND provider IN ('anthropic', 'openai', 'gemini', 'grok'))
        ),
    CONSTRAINT chk_upstream_connections_credential_version CHECK (credential_version > 0),
    CONSTRAINT chk_upstream_connections_status
        CHECK (status IN ('active', 'suspended', 'disabled')),
    CONSTRAINT chk_upstream_connections_base_url CHECK (base_url <> ''),
    CONSTRAINT chk_upstream_connections_encrypted_credential CHECK (encrypted_credential <> '')
);

CREATE INDEX IF NOT EXISTS idx_upstream_connections_kind_provider
    ON upstream_connections(kind, provider);
CREATE INDEX IF NOT EXISTS idx_upstream_connections_proxy_id
    ON upstream_connections(proxy_id);
CREATE INDEX IF NOT EXISTS idx_upstream_connections_status
    ON upstream_connections(status);
CREATE INDEX IF NOT EXISTS idx_upstream_connections_deleted_at
    ON upstream_connections(deleted_at);

-- Accounts gain a nullable connection reference during backfill; populated by migration command in Task 3.
ALTER TABLE accounts
    ADD COLUMN IF NOT EXISTS connection_id BIGINT REFERENCES upstream_connections(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_accounts_connection_id ON accounts(connection_id);

-- Trigger to keep updated_at fresh for upstream_connections
CREATE OR REPLACE FUNCTION touch_upstream_connections_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_upstream_connections_updated_at ON upstream_connections;
CREATE TRIGGER trg_upstream_connections_updated_at
    BEFORE UPDATE ON upstream_connections
    FOR EACH ROW EXECUTE FUNCTION touch_upstream_connections_updated_at();

-- Aggregator designation events are append-only; preserve evidence. Table for reviewed first-party-to-aggregator events.
CREATE TABLE IF NOT EXISTS upstream_connection_events (
    id BIGSERIAL PRIMARY KEY,
    connection_id BIGINT NOT NULL REFERENCES upstream_connections(id) ON DELETE CASCADE,
    event_type VARCHAR(64) NOT NULL,
    from_kind VARCHAR(32) NOT NULL,
    to_kind VARCHAR(32) NOT NULL,
    actor_id VARCHAR(255) NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL UNIQUE,
    evidence_ref TEXT,
    credential_version BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT chk_upstream_connection_events_kind
        CHECK (from_kind IN ('first_party', 'aggregator') AND to_kind IN ('first_party', 'aggregator')),
    CONSTRAINT chk_upstream_connection_events_credential_version CHECK (credential_version > 0),
    CONSTRAINT chk_upstream_connection_events_payload CHECK (jsonb_typeof(payload) = 'object')
);

CREATE INDEX IF NOT EXISTS idx_upstream_connection_events_connection_created
    ON upstream_connection_events(connection_id, created_at, id);

DROP TRIGGER IF EXISTS trg_upstream_connection_events_append_only ON upstream_connection_events;
CREATE TRIGGER trg_upstream_connection_events_append_only
    BEFORE UPDATE OR DELETE ON upstream_connection_events
    FOR EACH ROW
    EXECUTE FUNCTION reject_append_only_event_mutation();

DROP TRIGGER IF EXISTS trg_upstream_connection_events_reject_truncate ON upstream_connection_events;
CREATE TRIGGER trg_upstream_connection_events_reject_truncate
    BEFORE TRUNCATE ON upstream_connection_events
    FOR EACH STATEMENT
    EXECUTE FUNCTION reject_append_only_event_mutation();

-- Account endpoint probes: real probe evidence per (connection/account provider/protocol/endpoint/credential version/config version).
CREATE TABLE IF NOT EXISTS account_endpoint_probes (
    id BIGSERIAL PRIMARY KEY,
    account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    connection_id BIGINT NOT NULL REFERENCES upstream_connections(id) ON DELETE CASCADE,
    provider VARCHAR(32) NOT NULL,
    protocol VARCHAR(32) NOT NULL,
    normalized_endpoint_path TEXT NOT NULL,
    credential_version BIGINT NOT NULL,
    config_version BIGINT NOT NULL,
    status VARCHAR(32) NOT NULL,
    probed_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    evidence_ref TEXT,
    request_fingerprint TEXT,
    response_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_account_endpoint_probes_provider
        CHECK (provider IN ('anthropic', 'openai', 'gemini', 'grok')),
    CONSTRAINT chk_account_endpoint_probes_protocol
        CHECK (protocol IN ('anthropic', 'openai', 'gemini')),
    CONSTRAINT chk_account_endpoint_probes_status
        CHECK (status IN ('success', 'failed')),
    CONSTRAINT chk_account_endpoint_probes_credential_version CHECK (credential_version > 0),
    CONSTRAINT chk_account_endpoint_probes_config_version CHECK (config_version > 0),
    CONSTRAINT chk_account_endpoint_probes_path CHECK (normalized_endpoint_path <> ''),
    CONSTRAINT chk_account_endpoint_probes_expires CHECK (expires_at > probed_at),
    CONSTRAINT chk_account_endpoint_probes_validity CHECK (expires_at = probed_at + INTERVAL '24 hours'),
    CONSTRAINT chk_account_endpoint_probes_payload CHECK (jsonb_typeof(response_summary) = 'object')
);

CREATE INDEX IF NOT EXISTS idx_account_endpoint_probes_account_probed
    ON account_endpoint_probes(account_id, probed_at DESC);
CREATE INDEX IF NOT EXISTS idx_account_endpoint_probes_connection_probed
    ON account_endpoint_probes(connection_id, probed_at DESC);
CREATE INDEX IF NOT EXISTS idx_account_endpoint_probes_expires
    ON account_endpoint_probes(expires_at);
CREATE INDEX IF NOT EXISTS idx_account_endpoint_probes_provider_protocol
    ON account_endpoint_probes(provider, protocol);

-- Uniqueness is exact per (account, connection, provider, protocol, normalized_endpoint_path, credential_version, config_version)
-- Use advisory lock + exact text comparison to avoid md5 collision and large-index bloat.
CREATE OR REPLACE FUNCTION enforce_account_endpoint_probe_exact_unique()
RETURNS TRIGGER AS $$
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'exact probe identity writes require READ COMMITTED isolation'
            USING ERRCODE = '0A000';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        jsonb_build_array('account_endpoint_probes', NEW.account_id, NEW.connection_id, NEW.provider, NEW.protocol, NEW.normalized_endpoint_path, NEW.credential_version, NEW.config_version)::text, 0
    ));
    IF EXISTS (
        SELECT 1 FROM account_endpoint_probes existing
        WHERE existing.account_id = NEW.account_id
          AND existing.connection_id = NEW.connection_id
          AND existing.provider = NEW.provider
          AND existing.protocol = NEW.protocol
          AND existing.normalized_endpoint_path = NEW.normalized_endpoint_path
          AND existing.credential_version = NEW.credential_version
          AND existing.config_version = NEW.config_version
          AND existing.id <> NEW.id
    ) THEN
        RAISE EXCEPTION 'duplicate endpoint probe identity'
            USING ERRCODE = '23505', CONSTRAINT = 'uq_account_endpoint_probes_identity_exact';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_account_endpoint_probes_exact_unique ON account_endpoint_probes;
CREATE TRIGGER trg_account_endpoint_probes_exact_unique
    BEFORE INSERT OR UPDATE OF account_id, connection_id, provider, protocol, normalized_endpoint_path, credential_version, config_version ON account_endpoint_probes
    FOR EACH ROW EXECUTE FUNCTION enforce_account_endpoint_probe_exact_unique();

-- Probes are append-only evidence; updates only allowed to append newer probe rows, not mutate history. For Phase 4, allow updates to status? Keep strict: no update/delete.
DROP TRIGGER IF EXISTS trg_account_endpoint_probes_append_only ON account_endpoint_probes;
CREATE TRIGGER trg_account_endpoint_probes_append_only
    BEFORE UPDATE OR DELETE ON account_endpoint_probes
    FOR EACH ROW
    EXECUTE FUNCTION reject_append_only_event_mutation();

DROP TRIGGER IF EXISTS trg_account_endpoint_probes_reject_truncate ON account_endpoint_probes;
CREATE TRIGGER trg_account_endpoint_probes_reject_truncate
    BEFORE TRUNCATE ON account_endpoint_probes
    FOR EACH STATEMENT
    EXECUTE FUNCTION reject_append_only_event_mutation();

CREATE OR REPLACE FUNCTION touch_account_endpoint_probes_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_account_endpoint_probes_updated_at ON account_endpoint_probes;
CREATE TRIGGER trg_account_endpoint_probes_updated_at
    BEFORE UPDATE ON account_endpoint_probes
    FOR EACH ROW EXECUTE FUNCTION touch_account_endpoint_probes_updated_at();

-- Explicit aggregator reuse tracking (idempotency scope: connection_id, provider, protocol, normalized_endpoint_path, client_request_id)
CREATE TABLE IF NOT EXISTS aggregator_reuse_requests (
    id BIGSERIAL PRIMARY KEY,
    connection_id BIGINT NOT NULL REFERENCES upstream_connections(id) ON DELETE CASCADE,
    provider VARCHAR(32) NOT NULL,
    protocol VARCHAR(32) NOT NULL,
    normalized_endpoint_path TEXT NOT NULL,
    client_request_id VARCHAR(255) NOT NULL,
    account_id BIGINT REFERENCES accounts(id) ON DELETE SET NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_aggregator_reuse_requests_provider
        CHECK (provider IN ('anthropic', 'openai', 'gemini', 'grok')),
    CONSTRAINT chk_aggregator_reuse_requests_protocol
        CHECK (protocol IN ('anthropic', 'openai', 'gemini')),
    CONSTRAINT chk_aggregator_reuse_requests_path CHECK (normalized_endpoint_path <> ''),
    CONSTRAINT chk_aggregator_reuse_requests_client_id CHECK (client_request_id <> ''),
    CONSTRAINT chk_aggregator_reuse_requests_status
        CHECK (status IN ('pending', 'success', 'failed')),
    CONSTRAINT uq_aggregator_reuse_requests_scope
        UNIQUE (connection_id, provider, protocol, normalized_endpoint_path, client_request_id)
);

CREATE INDEX IF NOT EXISTS idx_aggregator_reuse_requests_connection
    ON aggregator_reuse_requests(connection_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_aggregator_reuse_requests_account
    ON aggregator_reuse_requests(account_id);

DROP TRIGGER IF EXISTS trg_aggregator_reuse_requests_append_only ON aggregator_reuse_requests;
-- reuse requests are not append-only; they track lifecycle, so no append-only trigger.

CREATE OR REPLACE FUNCTION touch_aggregator_reuse_requests_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_aggregator_reuse_requests_updated_at ON aggregator_reuse_requests;
CREATE TRIGGER trg_aggregator_reuse_requests_updated_at
    BEFORE UPDATE ON aggregator_reuse_requests
    FOR EACH ROW EXECUTE FUNCTION touch_aggregator_reuse_requests_updated_at();
