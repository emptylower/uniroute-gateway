-- wallet-outbox-backfill-billing-snapshot.sql (Phase 4.2-G Task 1, round-1
-- MAJOR-3b): the ONE-SHOT operator backfill of
-- wallet_settlement_outbox.billing_snapshot_id (migration 216's column) from
-- usage_logs.billing_snapshot_id (migration 210) by gateway_request_id.
--
-- WHY an operator script and not migration SQL: the migrations runner is
-- transactional (migrations_runner.go:244-256) and a join over usage_logs
-- inside it would hold ACCESS EXCLUSIVE on wallet_settlement_outbox for the
-- whole backfill. Run this by hand (or batched by cron) AFTER 216 is applied
-- and BEFORE 4.1-S narrows provider_fx_unverified, per the 4.3 runbook.
--
-- AMBIGUITY GUARD: usage_logs' uniqueness is (request_id, api_key_id)
-- (migration 027). A gateway_request_id matching exactly ONE log row with a
-- non-NULL billing_snapshot_id is backfilled. A request id matching MORE
-- THAN ONE (two api keys) is AMBIGUOUS and never touched — the row keeps
-- billing_fx null and leg 4 keeps it in the unverified tail. A request id
-- matching NONE (pre-210 rows, unlogged surfaces) stays NULL — the expected
-- NULL rate, not an error.
--
-- BEFORE RUNNING, record the three counts (the runbook's operator step and
-- this file's README line):
--   backfillable (exactly one match)
--   ambiguous (more than one match)
--   unmatched (no match — the expected NULL rate)
--   SELECT count(*), CASE
--     WHEN (SELECT count(*) FROM usage_logs u
--            WHERE u.request_id = o.gateway_request_id
--              AND u.billing_snapshot_id IS NOT NULL) = 1 THEN 'backfillable'
--     WHEN (SELECT count(*) FROM usage_logs u
--            WHERE u.request_id = o.gateway_request_id
--              AND u.billing_snapshot_id IS NOT NULL) > 1 THEN 'ambiguous'
--     ELSE 'unmatched' END AS coverage
--   FROM wallet_settlement_outbox o WHERE o.billing_snapshot_id IS NULL
--   GROUP BY 2
-- Record them AFTER running too (backfillable must read 0, ambiguous
-- unchanged, unmatched grown by exactly the backfilled count).
--
-- Batched by id range, idempotent: each pass updates at most 10 000 rows,
-- and the guard's o.billing_snapshot_id IS NULL clause makes a re-run a
-- no-op. Re-run this file until the pass's remaining_null stops moving.

UPDATE wallet_settlement_outbox o
SET billing_snapshot_id = u.billing_snapshot_id
FROM usage_logs u
WHERE u.request_id = o.gateway_request_id
  AND u.billing_snapshot_id IS NOT NULL
  AND o.billing_snapshot_id IS NULL
  AND (SELECT count(*) FROM usage_logs u2
        WHERE u2.request_id = o.gateway_request_id
          AND u2.billing_snapshot_id IS NOT NULL) = 1
  AND o.id IN (
      SELECT id FROM wallet_settlement_outbox
       WHERE billing_snapshot_id IS NULL
         AND id >= COALESCE((SELECT min(id) FROM wallet_settlement_outbox WHERE billing_snapshot_id IS NULL), 0)
       ORDER BY id
       LIMIT 10000
  );

-- The pass's report — run and record after every pass.
SELECT count(*) FILTER (WHERE billing_snapshot_id IS NOT NULL) AS with_snapshot,
       count(*) FILTER (WHERE billing_snapshot_id IS NULL) AS remaining_null
FROM wallet_settlement_outbox;
