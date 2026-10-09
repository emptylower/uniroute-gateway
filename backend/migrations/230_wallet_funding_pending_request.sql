-- Persist the exact atomic Redis observation before a return can commit in D1.
-- A response-loss/restart replays this request; it never substitutes fresh money.
ALTER TABLE wallet_funding_freeze ADD COLUMN pending_request jsonb;
ALTER TABLE wallet_funding_freeze ADD COLUMN redis_applied_revision bigint NOT NULL DEFAULT 0
 CHECK(redis_applied_revision>=0 AND redis_applied_revision<=return_revision);
ALTER TABLE wallet_funding_freeze ADD CONSTRAINT wallet_funding_pending_object
 CHECK (pending_request IS NULL OR jsonb_typeof(pending_request)='object');
CREATE FUNCTION wallet_funding_pending_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE basis jsonb; request jsonb;
BEGIN
 IF NEW.pending_request IS NOT NULL THEN
  basis := NEW.pending_request->'basis'->'lease';
  request := NEW.pending_request->'request';
  IF NEW.closed OR NEW.pending_request->>'protocol' IS DISTINCT FROM 'wallet-funding-request-v1'
   OR basis->>'platform_user_id' IS DISTINCT FROM NEW.platform_user_id
   OR basis->>'lease_id' IS DISTINCT FROM NEW.lease_id
   OR basis->>'funding_scope' IS DISTINCT FROM NEW.funding_scope
   OR COALESCE(basis->>'funding_owner_id','')<>NEW.funding_owner_id
   OR COALESCE(basis->>'funding_issuance_key','')<>NEW.funding_issuance_key
   OR basis->>'funding_frozen' IS DISTINCT FROM 'true'
   OR COALESCE((basis->>'funded_units')::numeric,-1)<>NEW.funded_units
   OR COALESCE((basis->>'returned_units')::numeric,0)<>NEW.returned_units
   OR COALESCE((basis->>'return_revision')::bigint,0)<>NEW.return_revision
   OR COALESCE((basis->>'budget_units')::numeric,-1)+COALESCE((basis->>'returned_units')::numeric,0)<>NEW.funded_units
   OR request->>'platform_user_id' IS DISTINCT FROM NEW.platform_user_id
   OR request->>'lease_id' IS DISTINCT FROM NEW.lease_id
   OR COALESCE((request->>'base_return_revision')::bigint,-1)<>NEW.return_revision
   OR COALESCE(request->>'mode','') NOT IN ('partial','close')
   OR jsonb_typeof(NEW.pending_request->'basis'->'holds') IS DISTINCT FROM 'array'
   OR jsonb_typeof(request->'holds') IS DISTINCT FROM 'array'
  THEN RAISE EXCEPTION 'wallet funding pending request identity conflict'; END IF;
 END IF;
 IF TG_OP='UPDATE' THEN
  IF NEW.redis_applied_revision<OLD.redis_applied_revision
  THEN RAISE EXCEPTION 'wallet funding Redis ACK revision conflict'; END IF;
  IF OLD.pending_request IS NOT NULL AND NEW.pending_request IS NOT NULL
   AND NEW.pending_request IS DISTINCT FROM OLD.pending_request
  THEN RAISE EXCEPTION 'wallet funding pending request is immutable'; END IF;
  IF NEW.return_revision>OLD.return_revision THEN
   request := OLD.pending_request->'request';
   IF OLD.pending_request IS NULL OR NEW.pending_request IS NOT NULL
    OR NEW.return_revision<>OLD.return_revision+1 OR NEW.receipt IS NULL
    OR NEW.receipt->>'protocol' IS DISTINCT FROM 'wallet-funding-return-v1'
    OR NEW.receipt->>'platform_user_id' IS DISTINCT FROM NEW.platform_user_id
    OR NEW.receipt->>'lease_id' IS DISTINCT FROM NEW.lease_id
    OR COALESCE((NEW.receipt->>'return_revision')::bigint,-1)<>NEW.return_revision
    OR COALESCE((NEW.receipt->>'returned_before_units')::numeric,-1)<>OLD.returned_units
    OR COALESCE((NEW.receipt->>'returned_after_units')::numeric,-1)<>NEW.returned_units
    OR NEW.receipt->>'mode' IS DISTINCT FROM request->>'mode'
    OR NEW.receipt->>'gateway_consumed_units' IS DISTINCT FROM request->'gateway_consumed'->>'amount_units'
    OR NEW.receipt->>'gateway_released_units' IS DISTINCT FROM request->'gateway_released'->>'amount_units'
   THEN RAISE EXCEPTION 'wallet funding ACK lacks its exact pending request'; END IF;
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER wallet_funding_pending_identity BEFORE INSERT OR UPDATE ON wallet_funding_freeze
FOR EACH ROW EXECUTE FUNCTION wallet_funding_pending_guard();
