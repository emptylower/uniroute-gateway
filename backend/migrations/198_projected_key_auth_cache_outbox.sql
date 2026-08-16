-- Make durable auth-cache invalidation verifier-aware for ShipAny projections.
-- Projected rows store a non-authenticating placeholder in api_keys.key, so
-- their already-hashed key_sha256 must be queued directly.

CREATE OR REPLACE FUNCTION api_key_auth_cache_key(projected_sha256 TEXT, raw_key TEXT)
RETURNS CHAR(64)
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    IF projected_sha256 ~ '^[0-9a-f]{64}$' THEN
        RETURN projected_sha256::CHAR(64);
    END IF;
    IF raw_key IS NULL OR raw_key = '' THEN
        RETURN NULL;
    END IF;
    RETURN encode(sha256(convert_to(raw_key, 'UTF8')), 'hex')::CHAR(64);
END;
$$;
CREATE OR REPLACE FUNCTION enqueue_api_key_auth_cache_key(projected_sha256 TEXT, raw_key TEXT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    resolved_cache_key CHAR(64);
BEGIN
    resolved_cache_key := api_key_auth_cache_key(projected_sha256, raw_key);
    IF resolved_cache_key IS NULL THEN
        RETURN;
    END IF;
    INSERT INTO auth_cache_invalidation_outbox (cache_key)
    VALUES (resolved_cache_key);
END;
$$;

CREATE OR REPLACE FUNCTION enqueue_api_key_auth_cache_invalidation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM enqueue_api_key_auth_cache_key(NEW.key_sha256, NEW.key);
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        PERFORM enqueue_api_key_auth_cache_key(OLD.key_sha256, OLD.key);
        RETURN OLD;
    END IF;

    IF OLD.key IS DISTINCT FROM NEW.key
       OR OLD.key_sha256 IS DISTINCT FROM NEW.key_sha256
       OR OLD.status IS DISTINCT FROM NEW.status
       OR OLD.deleted_at IS DISTINCT FROM NEW.deleted_at
       OR OLD.user_id IS DISTINCT FROM NEW.user_id
       OR OLD.group_id IS DISTINCT FROM NEW.group_id
       OR OLD.ip_whitelist IS DISTINCT FROM NEW.ip_whitelist
       OR OLD.ip_blacklist IS DISTINCT FROM NEW.ip_blacklist
       OR OLD.expires_at IS DISTINCT FROM NEW.expires_at THEN
        PERFORM enqueue_api_key_auth_cache_key(OLD.key_sha256, OLD.key);
        IF NEW.deleted_at IS NULL
           AND (NEW.key IS DISTINCT FROM OLD.key
                OR NEW.key_sha256 IS DISTINCT FROM OLD.key_sha256) THEN
            PERFORM enqueue_api_key_auth_cache_key(NEW.key_sha256, NEW.key);
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_api_keys_auth_cache_invalidation ON api_keys;
CREATE TRIGGER trg_api_keys_auth_cache_invalidation
AFTER INSERT OR UPDATE OR DELETE ON api_keys
FOR EACH ROW EXECUTE FUNCTION enqueue_api_key_auth_cache_invalidation();

CREATE OR REPLACE FUNCTION enqueue_user_auth_cache_invalidation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    target_user_id BIGINT;
BEGIN
    target_user_id := OLD.id;
    IF TG_OP = 'UPDATE'
       AND OLD.status IS NOT DISTINCT FROM NEW.status
       AND OLD.role IS NOT DISTINCT FROM NEW.role
       AND OLD.deleted_at IS NOT DISTINCT FROM NEW.deleted_at THEN
        RETURN NEW;
    END IF;

    INSERT INTO auth_cache_invalidation_outbox (cache_key)
    SELECT api_key_auth_cache_key(k.key_sha256, k.key)
    FROM api_keys AS k
    WHERE k.user_id = target_user_id
      AND k.deleted_at IS NULL
      AND api_key_auth_cache_key(k.key_sha256, k.key) IS NOT NULL;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION enqueue_group_auth_cache_invalidation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    target_group_id BIGINT;
BEGIN
    target_group_id := OLD.id;
    IF TG_OP = 'UPDATE'
       AND OLD.status IS NOT DISTINCT FROM NEW.status
       AND OLD.is_exclusive IS NOT DISTINCT FROM NEW.is_exclusive
       AND OLD.allow_image_generation IS NOT DISTINCT FROM NEW.allow_image_generation
       AND OLD.deleted_at IS NOT DISTINCT FROM NEW.deleted_at THEN
        RETURN NEW;
    END IF;

    INSERT INTO auth_cache_invalidation_outbox (cache_key)
    SELECT api_key_auth_cache_key(k.key_sha256, k.key)
    FROM api_keys AS k
    WHERE k.group_id = target_group_id
      AND k.deleted_at IS NULL
      AND api_key_auth_cache_key(k.key_sha256, k.key) IS NOT NULL;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION enqueue_allowed_group_auth_cache_invalidation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    target_user_id BIGINT;
    target_group_id BIGINT;
BEGIN
    IF TG_OP = 'UPDATE'
       AND (OLD.user_id IS DISTINCT FROM NEW.user_id
            OR OLD.group_id IS DISTINCT FROM NEW.group_id) THEN
        IF EXISTS (
            SELECT 1 FROM groups g
            WHERE g.id = OLD.group_id AND g.is_exclusive = TRUE
        ) THEN
            INSERT INTO auth_cache_invalidation_outbox (cache_key)
            SELECT api_key_auth_cache_key(k.key_sha256, k.key)
            FROM api_keys AS k
            WHERE k.user_id = OLD.user_id
              AND k.group_id = OLD.group_id
              AND k.deleted_at IS NULL
              AND api_key_auth_cache_key(k.key_sha256, k.key) IS NOT NULL;
        END IF;
        target_user_id := NEW.user_id;
        target_group_id := NEW.group_id;
    ELSIF TG_OP = 'UPDATE' THEN
        RETURN NEW;
    ELSIF TG_OP = 'INSERT' THEN
        target_user_id := NEW.user_id;
        target_group_id := NEW.group_id;
    ELSE
        target_user_id := OLD.user_id;
        target_group_id := OLD.group_id;
    END IF;

    IF EXISTS (
        SELECT 1 FROM groups g
        WHERE g.id = target_group_id AND g.is_exclusive = TRUE
    ) THEN
        INSERT INTO auth_cache_invalidation_outbox (cache_key)
        SELECT api_key_auth_cache_key(k.key_sha256, k.key)
        FROM api_keys AS k
        WHERE k.user_id = target_user_id
          AND k.group_id = target_group_id
          AND k.deleted_at IS NULL
          AND api_key_auth_cache_key(k.key_sha256, k.key) IS NOT NULL;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;
