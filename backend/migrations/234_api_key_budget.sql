-- Durable allowance authorizations share the existing USD Key counters.
-- Wallet expiry is not zero-cost proof, so unknown allowances remain pending.
CREATE TABLE IF NOT EXISTS api_key_budget_authorization (
 authorization_id TEXT PRIMARY KEY,
 api_key_id BIGINT NOT NULL REFERENCES api_keys(id) ON DELETE RESTRICT,
 user_id BIGINT NOT NULL,
 snapshot_id TEXT NOT NULL,
 reserved_units BIGINT NOT NULL CHECK(reserved_units >= 0),
 captured_units BIGINT CHECK(captured_units >= 0),
 kind TEXT NOT NULL CHECK(kind IN ('llm','live','media')),
 state TEXT NOT NULL DEFAULT 'reserved' CHECK(state IN ('prepared','reserved','written','unknown','captured','released')),
 written_at TIMESTAMPTZ,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 CHECK((state='captured')=(captured_units IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS api_key_budget_pending_key ON api_key_budget_authorization(api_key_id) WHERE state NOT IN ('captured','released');
CREATE INDEX IF NOT EXISTS api_key_budget_recovery ON api_key_budget_authorization(updated_at,authorization_id) WHERE state NOT IN ('captured','released');
CREATE TABLE IF NOT EXISTS api_key_budget_epoch (
 api_key_id BIGINT PRIMARY KEY REFERENCES api_keys(id) ON DELETE RESTRICT,
 checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 blocked_reason TEXT
);
-- The admission barrier discovers legacy obligations again under the Key lock
-- on every acceptance, covering commits across rolling deployment boundaries.
CREATE OR REPLACE FUNCTION guard_api_key_budget_authorization() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF ROW(NEW.authorization_id,NEW.api_key_id,NEW.user_id,NEW.snapshot_id,NEW.reserved_units,NEW.kind)
 IS DISTINCT FROM ROW(OLD.authorization_id,OLD.api_key_id,OLD.user_id,OLD.snapshot_id,OLD.reserved_units,OLD.kind)
 OR OLD.state IN ('captured','released') AND ROW(NEW.state,NEW.captured_units,NEW.written_at) IS DISTINCT FROM ROW(OLD.state,OLD.captured_units,OLD.written_at)
 OR OLD.written_at IS NOT NULL AND NEW.written_at IS DISTINCT FROM OLD.written_at THEN
  RAISE EXCEPTION 'immutable API Key allowance identity or terminal state';
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS api_key_budget_identity_guard ON api_key_budget_authorization;
CREATE TRIGGER api_key_budget_identity_guard BEFORE UPDATE ON api_key_budget_authorization FOR EACH ROW EXECUTE FUNCTION guard_api_key_budget_authorization();
