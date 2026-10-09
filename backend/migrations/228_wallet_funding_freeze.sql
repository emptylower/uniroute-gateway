-- Permanent recovery identity for original-v5 signed source-backed returns.
CREATE TABLE wallet_funding_freeze (
 platform_user_id text NOT NULL,
 lease_id text NOT NULL,
 funding_scope text NOT NULL,
 funding_owner_id text NOT NULL DEFAULT '',
 funding_issuance_key text NOT NULL DEFAULT '',
 funded_units bigint NOT NULL CHECK(funded_units>0),
 returned_units bigint NOT NULL DEFAULT 0 CHECK(returned_units>=0),
 return_revision bigint NOT NULL DEFAULT 0 CHECK(return_revision>=0),
 receipt jsonb,
 closed boolean NOT NULL DEFAULT false,
 frozen_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(platform_user_id,lease_id),
 CHECK(returned_units<=funded_units)
);
CREATE FUNCTION wallet_funding_freeze_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.platform_user_id<>OLD.platform_user_id OR NEW.lease_id<>OLD.lease_id
  OR NEW.funding_scope<>OLD.funding_scope OR NEW.funding_owner_id<>OLD.funding_owner_id
  OR NEW.funding_issuance_key<>OLD.funding_issuance_key OR NEW.funded_units<>OLD.funded_units
  OR NEW.frozen_at<>OLD.frozen_at OR NEW.return_revision<OLD.return_revision
  OR NEW.returned_units<OLD.returned_units OR (OLD.closed AND NOT NEW.closed)
  OR (NEW.return_revision=OLD.return_revision AND ROW(NEW.returned_units,NEW.receipt,NEW.closed) IS DISTINCT FROM ROW(OLD.returned_units,OLD.receipt,OLD.closed))
 THEN RAISE EXCEPTION 'wallet funding identity/revision conflict'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER wallet_funding_freeze_monotone BEFORE UPDATE ON wallet_funding_freeze
FOR EACH ROW EXECUTE FUNCTION wallet_funding_freeze_guard();

CREATE FUNCTION wallet_funding_freeze_delete_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'wallet funding tombstone is permanent'; END $$;
CREATE TRIGGER wallet_funding_freeze_no_delete BEFORE DELETE ON wallet_funding_freeze
FOR EACH ROW EXECUTE FUNCTION wallet_funding_freeze_delete_guard();
