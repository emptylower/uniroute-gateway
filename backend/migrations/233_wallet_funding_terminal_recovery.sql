-- Durable source-return work is part of the original terminal PG handoff.
-- A completed Redis release cannot lose its source recovery on process death.
ALTER TABLE wallet_authorization_segment ADD COLUMN funding_terminal_cleanup_at timestamptz;
CREATE FUNCTION wallet_funding_terminal_cleanup_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.funding_terminal_cleanup_at IS NOT NULL
  AND NEW.funding_terminal_cleanup_at IS DISTINCT FROM OLD.funding_terminal_cleanup_at
 THEN RAISE EXCEPTION 'wallet funding terminal cleanup receipt is immutable'; END IF;
 IF NEW.funding_terminal_cleanup_at IS NOT NULL AND NOT (
  (NEW.state='finished' AND NEW.pin_state='finished')
  OR (NEW.expiry_intent_version=2 AND NEW.expiry_ack_at IS NOT NULL
   AND NEW.expiry_cleanup_at IS NOT NULL AND NEW.pin_state='finished'))
 THEN RAISE EXCEPTION 'wallet funding terminal cleanup lacks original terminal state'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER wallet_funding_terminal_cleanup_identity BEFORE UPDATE ON wallet_authorization_segment
 FOR EACH ROW EXECUTE FUNCTION wallet_funding_terminal_cleanup_guard();

CREATE TABLE wallet_funding_terminal_work (
 platform_user_id text NOT NULL,
 lease_id text NOT NULL,
 generation bigint NOT NULL CHECK(generation>0),
 applied_generation bigint NOT NULL DEFAULT 0 CHECK(applied_generation>=0 AND applied_generation<=generation),
 next_attempt_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(platform_user_id,lease_id),
 FOREIGN KEY(platform_user_id,lease_id) REFERENCES wallet_funding_freeze(platform_user_id,lease_id) ON DELETE RESTRICT
);
CREATE INDEX wallet_funding_terminal_due ON wallet_funding_terminal_work(next_attempt_at,updated_at)
 WHERE applied_generation<generation;
CREATE FUNCTION wallet_funding_terminal_work_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'wallet funding terminal work is permanent'; END IF;
 IF NEW.platform_user_id<>OLD.platform_user_id OR NEW.lease_id<>OLD.lease_id
  OR NEW.generation<OLD.generation OR NEW.applied_generation<OLD.applied_generation
 THEN RAISE EXCEPTION 'wallet funding terminal generation conflict'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER wallet_funding_terminal_work_identity BEFORE UPDATE OR DELETE ON wallet_funding_terminal_work
 FOR EACH ROW EXECUTE FUNCTION wallet_funding_terminal_work_guard();

CREATE FUNCTION enqueue_wallet_funding_terminal() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.kind IS DISTINCT FROM 'live' AND (
   (TG_OP='INSERT' AND (NEW.zero_ack_at IS NOT NULL
      OR (NEW.expiry_ack_at IS NOT NULL AND NEW.expiry_intent_version=2)
      OR NEW.settlement_payload IS NOT NULL OR NEW.remainder_payload IS NOT NULL))
   OR (TG_OP='UPDATE' AND (
      (OLD.zero_ack_at IS NULL AND NEW.zero_ack_at IS NOT NULL)
      OR (OLD.expiry_ack_at IS NULL AND NEW.expiry_ack_at IS NOT NULL AND NEW.expiry_intent_version=2)
      OR NEW.settlement_payload IS DISTINCT FROM OLD.settlement_payload
      OR NEW.remainder_payload IS DISTINCT FROM OLD.remainder_payload
      OR (OLD.state IS DISTINCT FROM NEW.state AND NEW.state='finished')
      OR (OLD.expiry_cleanup_at IS NULL AND NEW.expiry_cleanup_at IS NOT NULL)
      OR (OLD.funding_terminal_cleanup_at IS NULL AND NEW.funding_terminal_cleanup_at IS NOT NULL))))
 THEN
  INSERT INTO wallet_funding_terminal_work(platform_user_id,lease_id,generation)
  SELECT NEW.platform_user_id,NEW.lease_id,1 FROM wallet_funding_freeze f
   WHERE f.platform_user_id=NEW.platform_user_id AND f.lease_id=NEW.lease_id
    AND NOT f.closed AND f.funding_scope IN ('legacy','llm')
  ON CONFLICT(platform_user_id,lease_id) DO UPDATE SET
   generation=wallet_funding_terminal_work.generation+1,next_attempt_at=now(),updated_at=now();
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER wallet_funding_terminal_enqueue AFTER INSERT OR UPDATE ON wallet_authorization_segment
 FOR EACH ROW EXECUTE FUNCTION enqueue_wallet_funding_terminal();

-- The original terminal acknowledgement may precede its source-principal row.
-- Discover it in that row's own transaction, including interrupted cleanup.
CREATE FUNCTION discover_wallet_funding_terminal() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='INSERT' AND NOT NEW.closed AND NEW.funding_scope IN ('legacy','llm') THEN
  INSERT INTO wallet_funding_terminal_work(platform_user_id,lease_id,generation)
   SELECT NEW.platform_user_id,NEW.lease_id,count(*) FROM wallet_authorization_segment a
    WHERE a.platform_user_id=NEW.platform_user_id AND a.lease_id=NEW.lease_id
     AND a.kind IS DISTINCT FROM 'live'
     AND (a.zero_ack_at IS NOT NULL OR (a.expiry_ack_at IS NOT NULL AND a.expiry_intent_version=2)
      OR a.settlement_payload IS NOT NULL OR a.remainder_payload IS NOT NULL)
   HAVING count(*)>0;
 END IF;
 IF TG_OP='UPDATE' AND NOT NEW.closed AND NEW.funding_scope IN ('legacy','llm')
  AND NEW.receipt IS DISTINCT FROM OLD.receipt
  AND NEW.receipt->>'mode'='partial' AND NEW.receipt->>'held_units'='0' THEN
  INSERT INTO wallet_funding_terminal_work(platform_user_id,lease_id,generation)
   VALUES(NEW.platform_user_id,NEW.lease_id,1)
   ON CONFLICT(platform_user_id,lease_id) DO UPDATE SET
    generation=wallet_funding_terminal_work.generation+1,next_attempt_at=now(),updated_at=now();
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER wallet_funding_terminal_discover AFTER INSERT OR UPDATE ON wallet_funding_freeze
 FOR EACH ROW EXECUTE FUNCTION discover_wallet_funding_terminal();

-- A late immutable payload can use another settlement lease. Its successful
-- original outbox handoff still wakes the source whose reservation was released.
CREATE FUNCTION deliver_wallet_funding_terminal() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.status='delivered' AND OLD.status IS DISTINCT FROM NEW.status THEN
  INSERT INTO wallet_funding_terminal_work(platform_user_id,lease_id,generation)
   SELECT a.platform_user_id,a.lease_id,1 FROM wallet_authorization_segment a
    JOIN wallet_funding_freeze f USING(platform_user_id,lease_id)
    WHERE a.authorization_id=NEW.authorization_id AND a.kind IS DISTINCT FROM 'live'
     AND NOT f.closed AND f.funding_scope IN ('legacy','llm')
   ON CONFLICT(platform_user_id,lease_id) DO UPDATE SET
    generation=wallet_funding_terminal_work.generation+1,next_attempt_at=now(),updated_at=now();
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER wallet_funding_terminal_deliver AFTER UPDATE ON wallet_settlement_outbox
 FOR EACH ROW EXECUTE FUNCTION deliver_wallet_funding_terminal();

-- Existing already-ACKed terminal rows also remain recoverable at upgrade.
INSERT INTO wallet_funding_terminal_work(platform_user_id,lease_id,generation)
 SELECT f.platform_user_id,f.lease_id,count(*) FROM wallet_funding_freeze f
 JOIN wallet_authorization_segment a ON a.platform_user_id=f.platform_user_id AND a.lease_id=f.lease_id
 WHERE NOT f.closed AND f.funding_scope IN ('legacy','llm') AND a.kind IS DISTINCT FROM 'live'
  AND (a.zero_ack_at IS NOT NULL OR (a.expiry_ack_at IS NOT NULL AND a.expiry_intent_version=2)
   OR a.settlement_payload IS NOT NULL OR a.remainder_payload IS NOT NULL)
 GROUP BY f.platform_user_id,f.lease_id;

-- Upgrade can encounter an already-applied, fully free partial return with
-- no authorization segments at all. Its unchanged ACK still owns a caller
-- slot, so it requires the same strict fresh close observation after restart.
INSERT INTO wallet_funding_terminal_work(platform_user_id,lease_id,generation)
 SELECT f.platform_user_id,f.lease_id,1 FROM wallet_funding_freeze f
 WHERE NOT f.closed AND f.funding_scope IN ('legacy','llm')
  AND f.receipt->>'mode'='partial' AND f.receipt->>'held_units'='0'
 ON CONFLICT(platform_user_id,lease_id) DO NOTHING;
