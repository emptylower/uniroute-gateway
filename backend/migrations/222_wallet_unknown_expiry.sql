-- Finite customer reservation lifetime does not assert that provider cost is zero.
-- No historical money or evidence is rewritten by this additive migration.
ALTER TABLE wallet_authorization_segment DROP CONSTRAINT IF EXISTS wallet_authorization_segment_state_check;
ALTER TABLE wallet_authorization_segment ADD CONSTRAINT wallet_authorization_segment_state_check
 CHECK (state IN ('prepared','held','indeterminate','expiry_pending','expired_unknown','settling','released','finished'));
ALTER TABLE wallet_authorization_segment ADD COLUMN IF NOT EXISTS first_write_at TIMESTAMPTZ;
ALTER TABLE wallet_authorization_segment ADD COLUMN IF NOT EXISTS write_active_until TIMESTAMPTZ;
ALTER TABLE wallet_authorization_segment ADD COLUMN IF NOT EXISTS write_ended_at TIMESTAMPTZ;
ALTER TABLE wallet_authorization_segment ADD COLUMN IF NOT EXISTS legacy_completion_proof TEXT
 CHECK (legacy_completion_proof IS NULL OR legacy_completion_proof ~ '^[0-9a-f]{64}$');
ALTER TABLE wallet_authorization_segment ADD COLUMN IF NOT EXISTS expiry_deadline TIMESTAMPTZ;
ALTER TABLE wallet_authorization_segment ADD COLUMN IF NOT EXISTS expiry_intent_version INTEGER NOT NULL DEFAULT 0 CHECK (expiry_intent_version IN (0,1));
ALTER TABLE wallet_authorization_segment ADD COLUMN IF NOT EXISTS expiry_ack_at TIMESTAMPTZ;
ALTER TABLE wallet_authorization_segment ADD COLUMN IF NOT EXISTS expiry_receipt_id TEXT;
ALTER TABLE wallet_authorization_segment ADD COLUMN IF NOT EXISTS expiry_cleanup_at TIMESTAMPTZ;
CREATE OR REPLACE FUNCTION guard_wallet_unknown_expiry() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.expiry_deadline IS NOT NULL AND NEW.expiry_deadline IS DISTINCT FROM OLD.expiry_deadline
    OR OLD.first_write_at IS NOT NULL AND NEW.first_write_at IS DISTINCT FROM OLD.first_write_at
    OR OLD.legacy_completion_proof IS NOT NULL AND NEW.legacy_completion_proof IS DISTINCT FROM OLD.legacy_completion_proof
    OR NEW.expiry_intent_version<OLD.expiry_intent_version
    OR OLD.expiry_ack_at IS NOT NULL AND NEW.expiry_ack_at IS DISTINCT FROM OLD.expiry_ack_at
    OR OLD.expiry_receipt_id IS NOT NULL AND NEW.expiry_receipt_id IS DISTINCT FROM OLD.expiry_receipt_id
 THEN RAISE EXCEPTION 'wallet expiry evidence is immutable'; END IF;
 IF NEW.state IN ('expiry_pending','expired_unknown') AND (NEW.kind<>'llm' OR NEW.expiry_intent_version<>1 OR NEW.expiry_deadline IS NULL)
 THEN RAISE EXCEPTION 'wallet expiry intent is incomplete'; END IF;
 IF NEW.state='expired_unknown' AND (NEW.expiry_ack_at IS NULL OR NEW.expiry_receipt_id IS NULL)
 THEN RAISE EXCEPTION 'wallet expiry receipt is missing'; END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS wallet_unknown_expiry_guard ON wallet_authorization_segment;
CREATE TRIGGER wallet_unknown_expiry_guard BEFORE UPDATE ON wallet_authorization_segment FOR EACH ROW EXECUTE FUNCTION guard_wallet_unknown_expiry();
CREATE INDEX IF NOT EXISTS idx_wallet_unknown_expiry_pending ON wallet_authorization_segment(updated_at) WHERE kind='llm' AND state IN ('expiry_pending','expired_unknown');
