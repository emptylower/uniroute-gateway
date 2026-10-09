-- Add the v5 financial boundary without rewriting v1 deadlines or receipts.
ALTER TABLE wallet_authorization_segment
 DROP CONSTRAINT IF EXISTS wallet_authorization_segment_expiry_intent_version_check;
ALTER TABLE wallet_authorization_segment ADD CONSTRAINT wallet_authorization_segment_expiry_intent_version_check
 CHECK (expiry_intent_version IN (0,1,2));
ALTER TABLE wallet_authorization_segment ADD COLUMN IF NOT EXISTS terminal_sealed_at TIMESTAMPTZ;
ALTER TABLE wallet_authorization_segment ADD COLUMN IF NOT EXISTS terminal_evidence JSONB;
ALTER TABLE wallet_authorization_segment ADD COLUMN IF NOT EXISTS evidence_pending BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE wallet_authorization_segment ADD COLUMN IF NOT EXISTS fee_pending BOOLEAN NOT NULL DEFAULT false;
-- NULL means no trusted fee, and is deliberately different from known zero.
ALTER TABLE wallet_authorization_segment ADD COLUMN IF NOT EXISTS known_fee_units BIGINT CHECK (known_fee_units IS NULL OR known_fee_units>=0);
ALTER TABLE wallet_authorization_segment ADD COLUMN IF NOT EXISTS expiry_v2_deadline TIMESTAMPTZ;
ALTER TABLE wallet_authorization_segment ADD COLUMN IF NOT EXISTS expiry_policy_version TEXT;
ALTER TABLE wallet_authorization_segment ADD COLUMN IF NOT EXISTS expiry_terminal_proof TEXT
 CHECK (expiry_terminal_proof IS NULL OR expiry_terminal_proof ~ '^[0-9a-f]{64}$');
ALTER TABLE wallet_authorization_segment ADD COLUMN IF NOT EXISTS expiry_legacy_mapping_proof TEXT
 CHECK (expiry_legacy_mapping_proof IS NULL OR expiry_legacy_mapping_proof ~ '^[0-9a-f]{64}$');
ALTER TABLE wallet_authorization_segment ADD COLUMN IF NOT EXISTS expiry_released_at TIMESTAMPTZ;
ALTER TABLE wallet_authorization_segment ADD COLUMN IF NOT EXISTS expiry_receipt_signature TEXT
 CHECK (expiry_receipt_signature IS NULL OR expiry_receipt_signature ~ '^[0-9a-f]{64}$');
ALTER TABLE wallet_authorization_segment ADD COLUMN IF NOT EXISTS expiry_receipt JSONB;

CREATE OR REPLACE FUNCTION guard_wallet_unknown_expiry() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.expiry_deadline IS NOT NULL AND NEW.expiry_deadline IS DISTINCT FROM OLD.expiry_deadline
    OR OLD.first_write_at IS NOT NULL AND NEW.first_write_at IS DISTINCT FROM OLD.first_write_at
    OR OLD.legacy_completion_proof IS NOT NULL AND NEW.legacy_completion_proof IS DISTINCT FROM OLD.legacy_completion_proof
    OR NEW.expiry_intent_version<OLD.expiry_intent_version
    OR OLD.expiry_intent_version>0 AND NEW.expiry_intent_version<>OLD.expiry_intent_version
    OR OLD.expiry_ack_at IS NOT NULL AND NEW.expiry_ack_at IS DISTINCT FROM OLD.expiry_ack_at
    OR OLD.expiry_receipt_id IS NOT NULL AND NEW.expiry_receipt_id IS DISTINCT FROM OLD.expiry_receipt_id
    OR OLD.expiry_v2_deadline IS NOT NULL AND NEW.expiry_v2_deadline IS DISTINCT FROM OLD.expiry_v2_deadline
    OR OLD.expiry_policy_version IS NOT NULL AND NEW.expiry_policy_version IS DISTINCT FROM OLD.expiry_policy_version
    OR OLD.expiry_terminal_proof IS NOT NULL AND NEW.expiry_terminal_proof IS DISTINCT FROM OLD.expiry_terminal_proof
    OR OLD.expiry_legacy_mapping_proof IS NOT NULL AND NEW.expiry_legacy_mapping_proof IS DISTINCT FROM OLD.expiry_legacy_mapping_proof
    OR OLD.expiry_released_at IS NOT NULL AND NEW.expiry_released_at IS DISTINCT FROM OLD.expiry_released_at
    OR OLD.expiry_receipt_signature IS NOT NULL AND NEW.expiry_receipt_signature IS DISTINCT FROM OLD.expiry_receipt_signature
    OR OLD.expiry_receipt IS NOT NULL AND NEW.expiry_receipt IS DISTINCT FROM OLD.expiry_receipt
    OR OLD.terminal_sealed_at IS NOT NULL AND NEW.terminal_sealed_at IS DISTINCT FROM OLD.terminal_sealed_at
    OR OLD.terminal_sealed_at IS NOT NULL AND NEW.terminal_evidence IS DISTINCT FROM OLD.terminal_evidence
    OR OLD.expiry_intent_version=2 AND (NEW.kind IS DISTINCT FROM OLD.kind
       OR NEW.authorization_token IS DISTINCT FROM OLD.authorization_token
       OR NEW.parent_authorization_id IS DISTINCT FROM OLD.parent_authorization_id
       OR NEW.platform_user_id IS DISTINCT FROM OLD.platform_user_id
       OR NEW.billing_snapshot_id IS DISTINCT FROM OLD.billing_snapshot_id
       OR NEW.lease_id IS DISTINCT FROM OLD.lease_id OR NEW.held_units IS DISTINCT FROM OLD.held_units
       OR NEW.event_id IS DISTINCT FROM OLD.event_id)
 THEN RAISE EXCEPTION 'wallet expiry evidence is immutable'; END IF;
 IF NEW.state IN ('expiry_pending','expired_unknown') AND NEW.expiry_intent_version=1
    AND (NEW.kind<>'llm' OR NEW.expiry_deadline IS NULL)
 THEN RAISE EXCEPTION 'wallet v1 expiry intent is incomplete'; END IF;
 IF NEW.expiry_intent_version=2 AND (NEW.kind NOT IN ('llm','media')
    OR NEW.expiry_v2_deadline IS NULL OR NEW.expiry_policy_version IS DISTINCT FROM 'wallet-immediate-v5'
    OR NEW.expiry_terminal_proof IS NULL OR NEW.authorization_token IS NULL OR NEW.authorization_token=''
    OR NEW.terminal_sealed_at IS NULL OR NEW.terminal_evidence IS NULL)
 THEN RAISE EXCEPTION 'wallet v2 expiry intent is incomplete'; END IF;
 IF NEW.state IN ('expiry_pending','expired_unknown') AND NEW.expiry_intent_version=0
 THEN RAISE EXCEPTION 'wallet expiry version is missing'; END IF;
 IF OLD.expiry_intent_version<>2 AND NEW.expiry_intent_version=2
    AND (NEW.evidence_pending OR NEW.fee_pending OR NEW.known_fee_units IS NOT NULL OR NEW.actual_units<>0
         OR NEW.settlement_payload IS NOT NULL OR NEW.remainder_payload IS NOT NULL)
 THEN RAISE EXCEPTION 'wallet trusted fee or pending evidence prevents unknown expiry'; END IF;
 IF NEW.state='expired_unknown' AND (NEW.expiry_ack_at IS NULL OR NEW.expiry_receipt_id IS NULL)
 THEN RAISE EXCEPTION 'wallet expiry receipt is missing'; END IF;
 IF NEW.expiry_intent_version=2 AND NEW.expiry_ack_at IS NOT NULL AND
    (NEW.expiry_released_at IS NULL OR NEW.expiry_receipt_signature IS NULL OR NEW.expiry_receipt IS NULL
     OR NEW.expiry_receipt_id IS DISTINCT FROM NEW.authorization_id||':expiry:2')
 THEN RAISE EXCEPTION 'wallet signed v2 receipt is missing'; END IF;
 RETURN NEW;
END $$;

CREATE TABLE IF NOT EXISTS wallet_risk_admission (
 parent_authorization_id TEXT PRIMARY KEY,
 platform_user_id TEXT NOT NULL,
 billing_snapshot_id TEXT NOT NULL REFERENCES wallet_billing_snapshot(id) ON DELETE RESTRICT,
 kind TEXT NOT NULL CHECK (kind IN ('llm','media')),
 policy_version TEXT NOT NULL CHECK (policy_version='wallet-immediate-v5'),
 admitted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS wallet_unknown_release_counter (
 authorization_id TEXT PRIMARY KEY REFERENCES wallet_authorization_segment(authorization_id) ON DELETE RESTRICT,
 parent_authorization_id TEXT NOT NULL,
 platform_user_id TEXT NOT NULL,
 authorization_token TEXT NOT NULL CHECK (authorization_token<>''),
 billing_snapshot_id TEXT NOT NULL REFERENCES wallet_billing_snapshot(id) ON DELETE RESTRICT,
 kind TEXT NOT NULL CHECK (kind IN ('llm','media')),
 held_units BIGINT NOT NULL CHECK (held_units>0),
 expiry_version INTEGER NOT NULL CHECK (expiry_version=2),
 receipt_id TEXT NOT NULL UNIQUE,
 receipt_signature TEXT NOT NULL CHECK (receipt_signature ~ '^[0-9a-f]{64}$'),
 released_at TIMESTAMPTZ NOT NULL,
 receipt JSONB NOT NULL,
 acknowledged_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 CHECK (receipt_id=authorization_id||':expiry:2')
);
CREATE INDEX IF NOT EXISTS idx_wallet_unknown_release_window
 ON wallet_unknown_release_counter(platform_user_id,released_at,authorization_id) INCLUDE (held_units);
CREATE INDEX IF NOT EXISTS idx_wallet_immediate_expiry_pending
 ON wallet_authorization_segment(updated_at) WHERE expiry_intent_version=2 AND (expiry_ack_at IS NULL OR expiry_cleanup_at IS NULL);

CREATE OR REPLACE FUNCTION guard_wallet_release_counter() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'wallet release counter is immutable'; END IF;
 IF NOT EXISTS (SELECT 1 FROM wallet_authorization_segment s WHERE
     s.authorization_id=NEW.authorization_id AND s.parent_authorization_id=NEW.parent_authorization_id
     AND s.platform_user_id=NEW.platform_user_id AND s.authorization_token=NEW.authorization_token
     AND s.billing_snapshot_id=NEW.billing_snapshot_id AND s.kind=NEW.kind AND s.held_units=NEW.held_units
     AND s.expiry_intent_version=2 AND s.expiry_ack_at IS NOT NULL
     AND s.expiry_receipt_id=NEW.receipt_id AND s.expiry_receipt_signature=NEW.receipt_signature
     AND s.expiry_released_at=NEW.released_at AND s.expiry_receipt=NEW.receipt)
 THEN RAISE EXCEPTION 'wallet release counter does not match acknowledged segment'; END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS wallet_release_counter_guard ON wallet_unknown_release_counter;
CREATE TRIGGER wallet_release_counter_guard BEFORE INSERT OR UPDATE OR DELETE ON wallet_unknown_release_counter
 FOR EACH ROW EXECUTE FUNCTION guard_wallet_release_counter();

CREATE OR REPLACE FUNCTION guard_wallet_risk_admission() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 RAISE EXCEPTION 'wallet risk admission is immutable';
END $$;
DROP TRIGGER IF EXISTS wallet_risk_admission_guard ON wallet_risk_admission;
CREATE TRIGGER wallet_risk_admission_guard BEFORE UPDATE OR DELETE ON wallet_risk_admission
 FOR EACH ROW EXECUTE FUNCTION guard_wallet_risk_admission();
