\set ON_ERROR_STOP on
\if :{?dashboard_timezone}
\else
\set dashboard_timezone Asia/Shanghai
\endif
-- Match config.timezone (default Asia/Shanghai); override with -v dashboard_timezone=...
\ir ../backend/migrations/221_usd_only_usage_projections.sql
-- Persist the manual review list even when preparation refuses ambiguous history.
INSERT INTO usd_only_migration_review(entity,entity_id,reason,original_amount)
 SELECT 'usage_legacy',id::text,'Legacy unit requires independent confirmation as USD',actual_cost
 FROM usage_logs WHERE exchange_rate_source='legacy' ON CONFLICT DO NOTHING;
BEGIN;
SELECT pg_advisory_xact_lock(hashtext('uniroute-usd-only-v1'));
-- Stop new traffic and drain before preparing. This transaction is rerunnable.
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
 IF EXISTS(SELECT 1 FROM users WHERE frozen_balance <> 0) THEN
  RAISE EXCEPTION 'Drain frozen native balances before USD migration';
 END IF;
 -- The USD build retires native payment webhooks (410); an order still in
 -- flight would be paid at the provider and never credited.
 IF EXISTS(SELECT 1 FROM payment_orders WHERE status IN ('PENDING','PAID','RECHARGING','REFUND_REQUESTED','REFUNDING','REFUND_PENDING')) THEN
  RAISE EXCEPTION 'Drain in-flight native payment orders before USD cutover';
 END IF;
 IF EXISTS(SELECT 1 FROM users WHERE balance < 0 OR (balance <> 0 AND (platform_user_id IS NULL OR trim(platform_user_id)=''))) THEN
  RAISE EXCEPTION 'Native funds lack an eligible console identity';
 END IF;
 IF EXISTS(SELECT 1 FROM users u JOIN usd_only_native_balance_export e ON e.user_id=u.id
   WHERE e.retired_at IS NULL AND (u.balance<>e.original_balance OR u.billing_currency<>e.source_currency OR u.platform_user_id<>e.platform_user_id)) THEN
  RAISE EXCEPTION 'Native balance changed after export';
 END IF;
END $$;
INSERT INTO usd_only_native_balance_export(user_id,platform_user_id,source_currency,original_balance,amount_units,idempotency_key)
 SELECT id,platform_user_id,billing_currency,balance,
  CASE WHEN billing_currency='CNY' THEN ceil(balance*100000000*5/36)::bigint ELSE ceil(balance*100000000)::bigint END,
  'gateway-native-balance-usd-v1:'||id FROM users WHERE balance>0
 ON CONFLICT(user_id) DO NOTHING;
-- The current 2026-10-02 inventory has no legacy rows. On other snapshots,
-- explicitly review each legacy row and set reviewed_at before rerunning.
INSERT INTO usd_only_migration_review(entity,entity_id,reason,original_amount)
 SELECT 'usage_legacy',id::text,'Legacy unit requires independent confirmation as USD',actual_cost
 FROM usage_logs WHERE exchange_rate_source='legacy'
 ON CONFLICT DO NOTHING;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM usd_only_migration_review WHERE entity='usage_legacy' AND reviewed_at IS NULL) THEN
  RAISE EXCEPTION 'Unreviewed legacy usage: run inventory and approve USD interpretation';
 END IF;
 IF EXISTS(SELECT 1 FROM usage_logs WHERE exchange_rate_source<>'legacy' AND
   (settlement_currency NOT IN ('USD','CNY') OR (settlement_currency='CNY' AND exchange_rate<=0))) THEN
  RAISE EXCEPTION 'Unconvertible frozen usage';
 END IF;
END $$;
-- Batch by id. Audit columns are never rewritten. A failed batch rolls back the
-- full preparation transaction; the same snapshot can safely be rerun.
DO $$ DECLARE last_id BIGINT := 0; upper_id BIGINT; BEGIN
 LOOP
  SELECT max(id) INTO upper_id FROM (SELECT id FROM usage_logs WHERE id>last_id ORDER BY id LIMIT 5000) batch;
  EXIT WHEN upper_id IS NULL;
  UPDATE usage_logs SET
   actual_cost_usd=CASE WHEN exchange_rate_source='legacy' OR settlement_currency='USD' THEN actual_cost ELSE actual_cost/exchange_rate END,
   base_cost_usd=CASE WHEN exchange_rate_source='legacy' THEN total_cost WHEN settlement_currency='USD' THEN base_cost ELSE base_cost/exchange_rate END
   WHERE id>last_id AND id<=upper_id;
  last_id := upper_id;
 END LOOP;
 IF EXISTS(SELECT 1 FROM usage_logs WHERE abs(actual_cost_usd - CASE WHEN exchange_rate_source='legacy' OR settlement_currency='USD' THEN actual_cost ELSE actual_cost/exchange_rate END)>0.0000000001) THEN
  RAISE EXCEPTION 'USD usage conservation failed';
 END IF;
END $$;
-- Quota resets cannot be reconstructed from aggregate usage. Divide only when
-- all surviving logs prove one frozen currency/rate; list all other keys intact.
DO $$ DECLARE k RECORD; rates RECORD; BEGIN
 IF NOT EXISTS(SELECT 1 FROM usd_only_migration_state WHERE name='quota-usd-v1') THEN
  FOR k IN SELECT * FROM api_keys WHERE quota_used>0 OR usage_5h>0 OR usage_1d>0 OR usage_7d>0 LOOP
   SELECT count(DISTINCT (settlement_currency,exchange_rate)) AS variants,
    min(settlement_currency) AS currency,min(exchange_rate) AS rate,
    bool_and(exchange_rate_source<>'legacy') AS known
    INTO rates FROM usage_logs WHERE api_key_id=k.id;
   IF rates.variants=1 AND rates.known AND rates.currency='CNY' AND rates.rate>0 THEN
    UPDATE api_keys SET quota_used=quota_used/rates.rate,usage_5h=usage_5h/rates.rate,
      usage_1d=usage_1d/rates.rate,usage_7d=usage_7d/rates.rate WHERE id=k.id;
   ELSIF NOT(rates.variants=1 AND rates.known AND rates.currency='USD') THEN
    INSERT INTO usd_only_migration_review(entity,entity_id,reason,original_amount)
     VALUES('api_key_quota',k.id::text,'Absent, mixed or ambiguous frozen rates; counters left unchanged',k.quota_used) ON CONFLICT DO NOTHING;
   END IF;
  END LOOP;
  INSERT INTO usd_only_migration_review(entity,entity_id,reason,original_amount)
   SELECT 'platform_quota',id::text,'Platform counters need per-window frozen-rate proof; left unchanged',daily_usage_usd
    FROM user_platform_quotas WHERE daily_usage_usd>0 OR weekly_usage_usd>0 OR monthly_usage_usd>0 ON CONFLICT DO NOTHING;
  INSERT INTO usd_only_migration_state(name) VALUES('quota-usd-v1');
 END IF;
END $$;
-- Preserve the currently effective CNY group multiplier exactly once.
DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM usd_only_migration_state WHERE name='group-rate-usd-v1') THEN
  UPDATE groups SET rate_multiplier=COALESCE(rate_multiplier_cny,rate_multiplier);
  INSERT INTO usd_only_migration_state(name) VALUES('group-rate-usd-v1');
 END IF;
END $$;
-- Recompute USD aggregate costs without changing request/token facts or buckets.
-- usage_logs are retained for less time than the aggregates (90 vs 180/730
-- days by default), so only a bucket whose retained logs still account for
-- every request it counted is recomputed. Older or partially pruned buckets
-- keep their stored costs and are listed for the record instead of being
-- zeroed. These rows are informational (no funds move) and do not gate
-- finalize. One grouped scan per table; bucket expressions match the aggregator.
CREATE TEMP TABLE usd_only_hourly_recompute ON COMMIT DROP AS
 SELECT date_trunc('hour', created_at AT TIME ZONE :'dashboard_timezone') AT TIME ZONE :'dashboard_timezone' AS bucket_start,
  count(*) AS requests,
  COALESCE(sum(base_cost_usd),0) AS total_cost,
  COALESCE(sum(actual_cost_usd),0) AS actual_cost,
  COALESCE(sum(COALESCE(account_stats_cost,base_cost_usd)*COALESCE(account_rate_multiplier,1)),0) AS account_cost
 FROM usage_logs GROUP BY 1;
UPDATE usage_dashboard_hourly a SET actual_cost=r.actual_cost,total_cost=r.total_cost,account_cost=r.account_cost
 FROM usd_only_hourly_recompute r WHERE r.bucket_start=a.bucket_start AND r.requests=a.total_requests;
INSERT INTO usd_only_migration_review(entity,entity_id,reason,original_amount)
 SELECT 'dashboard_hourly',to_char(a.bucket_start AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"'),'Usage logs no longer cover this bucket; stored costs kept unchanged',a.actual_cost
 FROM usage_dashboard_hourly a LEFT JOIN usd_only_hourly_recompute r ON r.bucket_start=a.bucket_start
 WHERE a.total_requests>0 AND (r.bucket_start IS NULL OR r.requests<>a.total_requests)
 ON CONFLICT DO NOTHING;
CREATE TEMP TABLE usd_only_daily_recompute ON COMMIT DROP AS
 SELECT (created_at AT TIME ZONE :'dashboard_timezone')::date AS bucket_date,
  count(*) AS requests,
  COALESCE(sum(base_cost_usd),0) AS total_cost,
  COALESCE(sum(actual_cost_usd),0) AS actual_cost,
  COALESCE(sum(COALESCE(account_stats_cost,base_cost_usd)*COALESCE(account_rate_multiplier,1)),0) AS account_cost
 FROM usage_logs GROUP BY 1;
UPDATE usage_dashboard_daily a SET actual_cost=r.actual_cost,total_cost=r.total_cost,account_cost=r.account_cost
 FROM usd_only_daily_recompute r WHERE r.bucket_date=a.bucket_date AND r.requests=a.total_requests;
INSERT INTO usd_only_migration_review(entity,entity_id,reason,original_amount)
 SELECT 'dashboard_daily',a.bucket_date::text,'Usage logs no longer cover this bucket; stored costs kept unchanged',a.actual_cost
 FROM usage_dashboard_daily a LEFT JOIN usd_only_daily_recompute r ON r.bucket_date=a.bucket_date
 WHERE a.total_requests>0 AND (r.bucket_date IS NULL OR r.requests<>a.total_requests)
 ON CONFLICT DO NOTHING;
INSERT INTO usd_only_migration_state(name) VALUES('prepare-usd-v1') ON CONFLICT DO NOTHING;
COMMIT;
SELECT user_id,platform_user_id,amount_units::text,idempotency_key FROM usd_only_native_balance_export WHERE retired_at IS NULL ORDER BY user_id;
SELECT * FROM usd_only_migration_review WHERE reviewed_at IS NULL ORDER BY entity,entity_id;
