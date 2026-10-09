-- Preserve close-only intent when a process stops between intent and the
-- exact atomic Redis observation. Partial intent cannot downgrade a close.
ALTER TABLE wallet_funding_freeze ADD COLUMN requested_mode text NOT NULL DEFAULT 'partial'
 CHECK(requested_mode IN ('partial','close'));
UPDATE wallet_funding_freeze SET requested_mode='close' WHERE closed OR pending_request->'request'->>'mode'='close';
CREATE FUNCTION wallet_funding_requested_mode_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='UPDATE' AND OLD.requested_mode='close' AND NEW.requested_mode<>'close'
 THEN RAISE EXCEPTION 'wallet funding close intent is permanent'; END IF;
 IF NEW.closed AND NEW.requested_mode<>'close'
 THEN RAISE EXCEPTION 'wallet funding close ACK lacks close intent'; END IF;
 IF NEW.pending_request IS NOT NULL AND NEW.pending_request->'request'->>'mode'='close'
  AND NEW.requested_mode<>'close'
 THEN RAISE EXCEPTION 'wallet funding close request lacks close intent'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER wallet_funding_requested_mode_identity BEFORE INSERT OR UPDATE ON wallet_funding_freeze
FOR EACH ROW EXECUTE FUNCTION wallet_funding_requested_mode_guard();
