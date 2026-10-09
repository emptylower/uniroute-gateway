-- Immutable normalized billing handoff and signed-zero acknowledgements.
-- Version 223 owns the shared terminal/v2 fields; historical rows stay intact.
ALTER TABLE wallet_authorization_segment
    ADD COLUMN IF NOT EXISTS zero_receipt JSONB,
    ADD COLUMN IF NOT EXISTS zero_ack_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS zero_receipt_id TEXT,
    ADD COLUMN IF NOT EXISTS zero_released_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS zero_receipt_signature TEXT,
    ADD COLUMN IF NOT EXISTS legacy_zero_candidate INTEGER,
    ADD COLUMN IF NOT EXISTS reader_evidence JSONB,
    ADD COLUMN IF NOT EXISTS reader_started BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS reader_handoff_at TIMESTAMPTZ,
	ADD COLUMN IF NOT EXISTS reader_owner_id TEXT,
	ADD COLUMN IF NOT EXISTS reader_journal_host TEXT,
	ADD COLUMN IF NOT EXISTS reader_journal_scan_at TIMESTAMPTZ,
	ADD COLUMN IF NOT EXISTS reader_journal_cleanup_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS zero_intent_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS wallet_billing_pending (
    id TEXT PRIMARY KEY,
    parent_authorization_id TEXT NOT NULL,
    authorization_token TEXT NOT NULL,
    platform_user_id TEXT NOT NULL,
    billing_snapshot_id TEXT NOT NULL REFERENCES wallet_billing_snapshot(id) ON DELETE RESTRICT,
    event_id TEXT NOT NULL,
    provider_account_id BIGINT NOT NULL,
    evidence_source TEXT NOT NULL CHECK (evidence_source IN ('llm_http_usage','llm_ws_usage')),
    policy_version TEXT NOT NULL CHECK (policy_version='wallet-immediate-v5'),
    fee_units BIGINT NOT NULL CHECK (fee_units>=0),
    command JSONB NOT NULL,
    charge_receipt JSONB,
    apply_ack_at TIMESTAMPTZ,
    canonical_ack_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(parent_authorization_id,authorization_token)
);
CREATE INDEX IF NOT EXISTS wallet_billing_pending_recovery
    ON wallet_billing_pending(updated_at,id) WHERE canonical_ack_at IS NULL;

CREATE TABLE IF NOT EXISTS wallet_billing_charge_receipt (
    request_id TEXT NOT NULL,
    api_key_id BIGINT NOT NULL,
    request_fingerprint TEXT NOT NULL,
    pending_id TEXT NOT NULL UNIQUE REFERENCES wallet_billing_pending(id) ON DELETE RESTRICT,
    receipt JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(request_id,api_key_id)
);

CREATE TABLE IF NOT EXISTS wallet_billing_anomaly (
    parent_authorization_id TEXT NOT NULL,
    authorization_token TEXT NOT NULL,
    reason TEXT NOT NULL CHECK (reason IN ('positive_after_zero','different_fee_after_receipt')),
    evidence JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(parent_authorization_id,authorization_token,reason)
);

CREATE OR REPLACE FUNCTION wallet_billing_evidence_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_TABLE_NAME='wallet_billing_charge_receipt' OR TG_TABLE_NAME='wallet_billing_anomaly' THEN
    RAISE EXCEPTION 'wallet billing evidence is immutable';
  END IF;
  IF ROW(NEW.id,NEW.parent_authorization_id,NEW.authorization_token,NEW.platform_user_id,
      NEW.billing_snapshot_id,NEW.event_id,NEW.provider_account_id,NEW.evidence_source,
      NEW.policy_version,NEW.fee_units,NEW.command,NEW.created_at)
      IS DISTINCT FROM ROW(OLD.id,OLD.parent_authorization_id,OLD.authorization_token,
      OLD.platform_user_id,OLD.billing_snapshot_id,OLD.event_id,OLD.provider_account_id,
      OLD.evidence_source,OLD.policy_version,OLD.fee_units,OLD.command,OLD.created_at)
      OR (OLD.charge_receipt IS NOT NULL AND NEW.charge_receipt IS DISTINCT FROM OLD.charge_receipt)
      OR (OLD.apply_ack_at IS NOT NULL AND NEW.apply_ack_at IS DISTINCT FROM OLD.apply_ack_at)
      OR (OLD.canonical_ack_at IS NOT NULL AND NEW.canonical_ack_at IS DISTINCT FROM OLD.canonical_ack_at) THEN
    RAISE EXCEPTION 'wallet billing handoff cannot change its original receipt or identity';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER wallet_billing_pending_immutable BEFORE UPDATE ON wallet_billing_pending
    FOR EACH ROW EXECUTE FUNCTION wallet_billing_evidence_immutable();
CREATE TRIGGER wallet_billing_charge_receipt_immutable BEFORE UPDATE OR DELETE ON wallet_billing_charge_receipt
    FOR EACH ROW EXECUTE FUNCTION wallet_billing_evidence_immutable();
CREATE TRIGGER wallet_billing_anomaly_immutable BEFORE UPDATE OR DELETE ON wallet_billing_anomaly
    FOR EACH ROW EXECUTE FUNCTION wallet_billing_evidence_immutable();

CREATE OR REPLACE FUNCTION wallet_zero_ack_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF (OLD.reader_owner_id IS NOT NULL AND NEW.reader_owner_id IS DISTINCT FROM OLD.reader_owner_id)
      OR (OLD.reader_journal_host IS NOT NULL AND NEW.reader_journal_host IS DISTINCT FROM OLD.reader_journal_host) THEN
    RAISE EXCEPTION 'wallet reader journal owner identity is immutable';
  END IF;
  IF OLD.reader_journal_cleanup_at IS NOT NULL AND NEW.reader_journal_cleanup_at IS DISTINCT FROM OLD.reader_journal_cleanup_at THEN
    RAISE EXCEPTION 'wallet reader journal cleanup receipt is immutable';
  END IF;
  IF (OLD.zero_intent_at IS NOT NULL AND NEW.zero_intent_at IS DISTINCT FROM OLD.zero_intent_at)
      OR (OLD.reader_handoff_at IS NOT NULL AND NEW.reader_handoff_at IS DISTINCT FROM OLD.reader_handoff_at)
      OR (OLD.zero_ack_at IS NOT NULL AND ROW(NEW.zero_ack_at,NEW.zero_receipt,NEW.zero_receipt_id,
        NEW.zero_released_at,NEW.zero_receipt_signature) IS DISTINCT FROM ROW(OLD.zero_ack_at,
        OLD.zero_receipt,OLD.zero_receipt_id,OLD.zero_released_at,OLD.zero_receipt_signature)) THEN
    RAISE EXCEPTION 'wallet reader handoff and first signed zero receipt are immutable';
  END IF;
  IF NEW.zero_ack_at IS NOT NULL AND (NEW.zero_receipt IS NULL OR NEW.zero_receipt_id IS NULL
      OR NEW.zero_released_at IS NULL OR NEW.zero_receipt_signature IS NULL OR NEW.zero_receipt_signature !~ '^[0-9a-f]{64}$'
      OR NEW.known_fee_units IS DISTINCT FROM 0 OR NEW.actual_units<>0 OR NEW.evidence_pending
      OR NEW.terminal_sealed_at IS NULL OR NEW.reader_handoff_at IS NULL) THEN
    RAISE EXCEPTION 'wallet zero acknowledgement lacks a sealed signed receipt';
  END IF;
  IF OLD.zero_intent_at IS NOT NULL AND NEW.actual_units>0 THEN
    RAISE EXCEPTION 'positive evidence after zero is a platform anomaly';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER wallet_zero_ack_immutable BEFORE UPDATE ON wallet_authorization_segment
    FOR EACH ROW EXECUTE FUNCTION wallet_zero_ack_immutable();
