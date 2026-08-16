-- Additive verifier-only projection for API keys owned by ShipAny.
-- Existing panel-created keys remain unchanged and keep these columns NULL.
ALTER TABLE api_keys
    ADD COLUMN IF NOT EXISTS platform_key_id VARCHAR(128),
    ADD COLUMN IF NOT EXISTS key_sha256 VARCHAR(64),
    ADD COLUMN IF NOT EXISTS key_prefix VARCHAR(32),
    ADD COLUMN IF NOT EXISTS platform_key_version BIGINT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_api_keys_platform_key_id
    ON api_keys (platform_key_id)
    WHERE platform_key_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_api_keys_projected_sha256
    ON api_keys (key_sha256)
    WHERE key_sha256 IS NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'api_keys_projected_verifier_check'
    ) THEN
        ALTER TABLE api_keys
            ADD CONSTRAINT api_keys_projected_verifier_check
            CHECK (
                platform_key_id IS NULL OR (
                    key_sha256 ~ '^[0-9a-f]{64}$'
                    AND key_prefix IS NOT NULL
                    AND platform_key_version IS NOT NULL
                    AND platform_key_version > 0
                )
            ) NOT VALID;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION prevent_platform_key_id_reassignment()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.platform_key_id IS NOT NULL
       AND NEW.platform_key_id IS DISTINCT FROM OLD.platform_key_id THEN
        RAISE EXCEPTION 'platform_key_id is immutable once assigned';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_api_keys_platform_key_id_immutable ON api_keys;
CREATE TRIGGER trg_api_keys_platform_key_id_immutable
    BEFORE UPDATE OF platform_key_id ON api_keys
    FOR EACH ROW
    EXECUTE FUNCTION prevent_platform_key_id_reassignment();
