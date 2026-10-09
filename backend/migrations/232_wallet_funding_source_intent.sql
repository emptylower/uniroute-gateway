-- Source-side financial fencing precedes immutable PG228 principal. A lost D1
-- source ACK replays the same named operation and exact persisted expectation.
CREATE TABLE wallet_funding_source_intent (
 platform_user_id text NOT NULL,
 lease_id text NOT NULL,
 freeze_id text NOT NULL,
 requested_mode text NOT NULL CHECK(requested_mode IN ('partial','close')),
 expected_request jsonb,
 source_receipt jsonb,
 updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(platform_user_id,lease_id),
 UNIQUE(freeze_id),
 CHECK(freeze_id=lease_id||'.source-freeze.v1'),
 CHECK(expected_request IS NULL OR jsonb_typeof(expected_request)='object'),
 CHECK(source_receipt IS NULL OR jsonb_typeof(source_receipt)='object')
);
CREATE FUNCTION wallet_funding_source_intent_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'wallet funding source intent is permanent'; END IF;
 IF TG_OP='UPDATE' AND (NEW.platform_user_id<>OLD.platform_user_id OR NEW.lease_id<>OLD.lease_id
  OR NEW.freeze_id<>OLD.freeze_id OR (OLD.requested_mode='close' AND NEW.requested_mode<>'close')
  OR (OLD.source_receipt IS NOT NULL AND NEW.source_receipt IS DISTINCT FROM OLD.source_receipt)
  OR (OLD.expected_request IS NOT NULL AND NEW.expected_request IS NOT NULL AND NEW.expected_request IS DISTINCT FROM OLD.expected_request)
  OR (OLD.source_receipt IS NOT NULL AND NEW.expected_request IS DISTINCT FROM OLD.expected_request))
 THEN RAISE EXCEPTION 'wallet funding source fence identity conflict'; END IF;
 IF NEW.expected_request IS NOT NULL AND (
  NEW.expected_request->>'platform_user_id' IS DISTINCT FROM NEW.platform_user_id
  OR NEW.expected_request->>'lease_id' IS DISTINCT FROM NEW.lease_id
  OR NEW.expected_request->>'freeze_id' IS DISTINCT FROM NEW.freeze_id)
 THEN RAISE EXCEPTION 'wallet funding source request identity conflict'; END IF;
 IF NEW.source_receipt IS NOT NULL AND (NEW.expected_request IS NULL
  OR NEW.source_receipt->>'protocol' IS DISTINCT FROM 'wallet-funding-source-v1'
  OR NEW.source_receipt->>'platform_user_id' IS DISTINCT FROM NEW.platform_user_id
  OR NEW.source_receipt->>'lease_id' IS DISTINCT FROM NEW.lease_id
  OR NEW.source_receipt->>'freeze_id' IS DISTINCT FROM NEW.freeze_id
  OR NEW.source_receipt->>'funded_units' IS DISTINCT FROM NEW.expected_request->'expected_funded'->>'amount_units'
  OR NEW.source_receipt->>'budget_revision' IS DISTINCT FROM NEW.expected_request->>'expected_budget_revision')
 THEN RAISE EXCEPTION 'wallet funding source ACK identity conflict'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER wallet_funding_source_intent_identity BEFORE INSERT OR UPDATE OR DELETE ON wallet_funding_source_intent
FOR EACH ROW EXECUTE FUNCTION wallet_funding_source_intent_guard();

-- The immutable PG228 principal is accepted only after its matching D1 fence.
CREATE FUNCTION wallet_funding_source_principal_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM wallet_funding_source_intent s
  WHERE s.platform_user_id=NEW.platform_user_id AND s.lease_id=NEW.lease_id
  AND s.source_receipt->>'funded_units'=NEW.funded_units::text
  AND s.source_receipt->>'funding_scope'=NEW.funding_scope
  AND COALESCE(s.source_receipt->>'funding_owner_id','')=NEW.funding_owner_id
  AND COALESCE(s.source_receipt->>'funding_issuance_key','')=NEW.funding_issuance_key)
 THEN RAISE EXCEPTION 'wallet funding principal lacks canonical source fence'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER wallet_funding_source_principal BEFORE INSERT ON wallet_funding_freeze
FOR EACH ROW EXECUTE FUNCTION wallet_funding_source_principal_guard();
