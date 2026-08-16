-- Additive control-plane identity mapping for ShipAny-managed users.
-- Existing panel users remain unchanged and may keep a NULL platform_user_id.
ALTER TABLE users
    ADD COLUMN IF NOT EXISTS platform_user_id VARCHAR(128);

CREATE UNIQUE INDEX IF NOT EXISTS idx_users_platform_user_id
    ON users (platform_user_id)
    WHERE platform_user_id IS NOT NULL;

CREATE OR REPLACE FUNCTION prevent_platform_user_id_reassignment()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.platform_user_id IS NOT NULL
       AND NEW.platform_user_id IS DISTINCT FROM OLD.platform_user_id THEN
        RAISE EXCEPTION 'platform_user_id is immutable once assigned';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_users_platform_user_id_immutable ON users;
CREATE TRIGGER trg_users_platform_user_id_immutable
    BEFORE UPDATE OF platform_user_id ON users
    FOR EACH ROW
    EXECUTE FUNCTION prevent_platform_user_id_reassignment();
