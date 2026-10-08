\set ON_ERROR_STOP on
BEGIN;
SELECT pg_advisory_xact_lock(hashtext('uniroute-usd-only-v1'));
LOCK TABLE users IN SHARE ROW EXCLUSIVE MODE;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM batch_image_jobs WHERE status NOT IN ('completed','failed','cancelled','output_deleted') OR (hold_amount>0 AND settled_at IS NULL)) THEN
  RAISE EXCEPTION 'Drain legacy batch jobs and native batch holds before USD cutover';
 END IF;
 IF EXISTS(SELECT 1 FROM gateway_media_task WHERE status NOT IN ('completed','failed')) THEN
  RAISE EXCEPTION 'Drain all media tasks before USD cutover';
 END IF;
 IF EXISTS(SELECT 1 FROM wallet_live_provisional WHERE terminal_at IS NULL) THEN
  RAISE EXCEPTION 'Drain all live sessions before USD cutover';
 END IF;
 IF EXISTS(SELECT 1 FROM wallet_hold_outcome WHERE resolved_at IS NULL) THEN
  RAISE EXCEPTION 'Resolve all wallet holds before USD cutover';
 END IF;
 IF EXISTS(SELECT 1 FROM wallet_settlement_outbox WHERE status IN ('pending','in_flight')) THEN
  RAISE EXCEPTION 'Drain settlement outbox before USD cutover';
 END IF;
 IF NOT EXISTS(SELECT 1 FROM usd_only_migration_state WHERE name='prepare-usd-v1') THEN
  RAISE EXCEPTION 'Run USD preparation and verify its exports first';
 END IF;
 IF EXISTS(SELECT 1 FROM users WHERE frozen_balance<>0) THEN RAISE EXCEPTION 'Native funds still frozen'; END IF;
 IF EXISTS(SELECT 1 FROM usd_only_native_balance_export e LEFT JOIN usd_only_native_balance_receipt r USING(user_id)
  WHERE e.retired_at IS NULL AND (r.user_id IS NULL OR r.console_grant_id='' OR r.platform_user_id<>e.platform_user_id OR r.amount_units<>e.amount_units OR r.idempotency_key<>e.idempotency_key)) THEN
  RAISE EXCEPTION 'Matching console grant receipts are required before retiring any native funds';
 END IF;
 IF EXISTS(SELECT 1 FROM users u LEFT JOIN usd_only_native_balance_export e ON e.user_id=u.id
  WHERE u.balance<>0 AND (e.user_id IS NULL OR e.retired_at IS NOT NULL OR u.balance<>e.original_balance OR u.billing_currency<>e.source_currency OR u.platform_user_id<>e.platform_user_id)) THEN
  RAISE EXCEPTION 'Native balances differ from the approved export';
 END IF;
 -- The check above starts from users with funds; a user whose exported balance
 -- has since dropped (even to exactly 0) must also stop the retirement, or the
 -- console grant would pay out funds the gateway already spent.
 IF EXISTS(SELECT 1 FROM usd_only_native_balance_export e LEFT JOIN users u ON u.id=e.user_id
  WHERE e.retired_at IS NULL AND (u.id IS NULL OR u.balance IS DISTINCT FROM e.original_balance
   OR u.billing_currency IS DISTINCT FROM e.source_currency OR u.platform_user_id IS DISTINCT FROM e.platform_user_id)) THEN
  RAISE EXCEPTION 'An exported native balance changed after export';
 END IF;
 IF EXISTS(SELECT 1 FROM payment_orders WHERE status IN ('PENDING','PAID','RECHARGING','REFUND_REQUESTED','REFUNDING','REFUND_PENDING')) THEN
  RAISE EXCEPTION 'Drain in-flight native payment orders before USD cutover';
 END IF;
 -- Dashboard rows only record aggregates whose stored costs were kept as-is;
 -- every other entity (legacy usage, quota counters) affects money and gates.
 IF EXISTS(SELECT 1 FROM usd_only_migration_review WHERE reviewed_at IS NULL
   AND entity NOT IN ('dashboard_hourly','dashboard_daily')) THEN
  RAISE EXCEPTION 'USD migration manual review unresolved';
 END IF;
END $$;
UPDATE users u SET balance=0 FROM usd_only_native_balance_export e WHERE e.user_id=u.id AND e.retired_at IS NULL;
UPDATE usd_only_native_balance_export SET retired_at=now() WHERE retired_at IS NULL;
UPDATE users SET billing_currency='USD' WHERE billing_currency<>'USD';
-- Any code path that still writes the legacy default is refused outright
-- rather than producing a CNY-tagged user that settlement could skip.
ALTER TABLE users DROP CONSTRAINT IF EXISTS chk_users_billing_currency_usd;
ALTER TABLE users ADD CONSTRAINT chk_users_billing_currency_usd CHECK (billing_currency='USD');
-- Preserve legacy multiplier columns and all history for the observation window.
CREATE OR REPLACE FUNCTION reject_native_wallet_funds() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.balance <> 0 OR NEW.frozen_balance <> 0 THEN
  RAISE EXCEPTION 'Native gateway wallet retired; credit the console USD ledger';
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS reject_native_wallet_funds ON users;
CREATE TRIGGER reject_native_wallet_funds BEFORE INSERT OR UPDATE OF balance,frozen_balance ON users FOR EACH ROW EXECUTE FUNCTION reject_native_wallet_funds();
CREATE OR REPLACE FUNCTION reject_non_usd_usage_insert() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.settlement_currency <> 'USD' OR NEW.source_currency <> 'USD' OR NEW.exchange_rate <> 1 THEN
  RAISE EXCEPTION 'New usage must be USD with identity pricing';
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS reject_non_usd_usage_insert ON usage_logs;
CREATE TRIGGER reject_non_usd_usage_insert BEFORE INSERT ON usage_logs
 FOR EACH ROW EXECUTE FUNCTION reject_non_usd_usage_insert();
-- Old-currency dead letters are immutable audit rows and can never be replayed.
CREATE OR REPLACE FUNCTION reject_legacy_wallet_replay() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='INSERT' AND NEW.currency<>'USD' THEN
  RAISE EXCEPTION 'New settlement events must use USD';
 END IF;
 IF TG_OP='UPDATE' AND OLD.currency<>'USD' AND
    (NEW.currency<>OLD.currency OR NEW.status IN ('pending','in_flight')) THEN
  RAISE EXCEPTION 'Historical non-USD settlement cannot be relabelled or replayed';
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS reject_legacy_wallet_replay ON wallet_settlement_outbox;
CREATE TRIGGER reject_legacy_wallet_replay BEFORE INSERT OR UPDATE ON wallet_settlement_outbox
 FOR EACH ROW EXECUTE FUNCTION reject_legacy_wallet_replay();
INSERT INTO usd_only_migration_state(name) VALUES('finalize-usd-v1') ON CONFLICT DO NOTHING;
COMMIT;
