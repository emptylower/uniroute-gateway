# Canonical Wallet Failover Drill & Disaster Recovery Runbook

**Document Version:** 1.0 (Phase 4.3-G Task 3)  
**Target Component:** Sub2API Unirouter — Canonical Wallet Subsystem  
**Scope:** Gateway crash, Redis partition/outage, and PostgreSQL recovery procedures under `canonical_wallet.mode=enforce|shadow`

---

## 1. Subsystem Architecture Overview

The Canonical Wallet subsystem integrates Sub2API (the data plane gateway) with the ShipAny control plane to manage user credit balances, holds, leases, and settlements:
- **Redis**: Stores volatile distributed state including active leases (`canonical_wallet:lease:{user}:{lease_id}`), hold hashes (`canonical_wallet:hold:{user}:{auth_id}`), per-user hold indexes (`canonical_wallet:holds:{user}`), and the deployment-wide orphan reaper leader lock (`canonical_wallet:reaper:tick`).
- **PostgreSQL**: Stores durable financial records across crashes:
  - `wallet_settlement_outbox`: At-least-once outbox for delivery to ShipAny control plane.
  - `wallet_hold_outcome`: Durable resolutions (`settled`, `abandoned`, `expired`) of all holds.
  - `wallet_live_provisional`: Active and terminal WebRTC/SSE streaming provisional windows.
  - `wallet_billing_snapshot`: Immutable billing snapshots.
- **Orphan Sweeper (Reaper)**: Background tick loop running in each gateway instance (coordinated via Redis SETNX leader lock) that sweeps orphaned holds older than `orphan_grace_seconds` and releases them to reclaim headroom.

---

## 2. Failure Modes, Symptoms & Impact

### A. Gateway Crash (Abrupt Termination / OOM / SIGKILL)
- **Root Cause**: Gateway process killed mid-stream or mid-request while a hold was armed in Redis.
- **Symptoms**:
  - In-flight client streams severed.
  - Hold remains in Redis in `state=armed`.
  - In PostgreSQL, `wallet_live_provisional` stays active/finalized, or `wallet_hold_outcome` has no matching conversion.
- **Impact**: Held units temporarily reserve user lease budget until the orphan sweeper reclaims them.
- **Convergence**: Sweeper automatically reaps abandoned hold after `orphan_grace_seconds` (default: 900s; drill: 60s) + `orphan_sweep_interval_seconds` (default: 60s; drill: 10s).

### B. Redis Outage or Network Partition
- **Root Cause**: Redis service unavailable, restart, or network partition between Sub2API and Redis.
- **Symptoms**:
  - Prometheus counter `canonical_wallet_bridge_reaper_error_total` increments.
  - Gateway logs warning: `canonical wallet: reaper tick failed` (the sweeper never panics).
  - New authorization requests fall back or reject cleanly according to mode.
- **Impact**: Sweeper pauses work during partition. No holds are reaped incorrectly during Redis downtime.
- **Convergence**: Once Redis reconnects, the next sweeper tick immediately resumes and clears pending orphaned holds.

### C. PostgreSQL Outage or Crash
- **Root Cause**: PostgreSQL restart or disk pressure.
- **Symptoms**:
  - Outbox dispatcher fails to claim rows; `wallet_settlement_outbox` inserts fail.
- **Impact**: All Postgres writes use `synchronous_commit=on` and `fsync=on` in compose; committed transactions are durable.
- **Convergence**: Outbox dispatcher retries with exponential backoff on DB reconnection.

---

## 3. Verification & Observability Commands

### A. Prometheus Metrics Inspection
Check Prometheus metrics at `/metrics` (or monitoring dashboard):
```bash
# Total sweeper ticks executed
curl -s http://localhost:8080/metrics | grep canonical_wallet_bridge_reaper_ticks_total

# Sweeper error count (should be 0 under normal operation)
curl -s http://localhost:8080/metrics | grep canonical_wallet_bridge_reaper_error_total

# Number of orphaned holds reaped and released
curl -s http://localhost:8080/metrics | grep canonical_wallet_bridge_holds_abandoned_total

# Number of expired hold outcomes recorded
curl -s http://localhost:8080/metrics | grep canonical_wallet_bridge_hold_outcomes_expired_total

# Startup Redis policy verification status
curl -s http://localhost:8080/metrics | grep canonical_wallet_redis_policy
```

### B. Redis Verification Queries
```bash
# Check Redis memory eviction policy (must be noeviction)
redis-cli config get maxmemory-policy
redis-cli config get maxmemory

# Inspect active hold users
redis-cli sscan canonical_wallet:holds_users 0

# Inspect holds for a specific platform user
redis-cli smembers canonical_wallet:holds:<platform_user_id>

# Inspect a specific hold hash
redis-cli hgetall canonical_wallet:hold:<platform_user_id>:<authorization_id>

# Inspect lease state
redis-cli hgetall canonical_wallet:lease:<platform_user_id>:<lease_id>
```

### C. PostgreSQL Durable Consistency Queries
```sql
-- 1. Check outbox delivery backlog
SELECT status, count(*), coalesce(sum(amount_units), 0) AS total_units
FROM wallet_settlement_outbox
GROUP BY status;

-- 2. Check hold outcomes by resolution
SELECT coalesce(resolution, 'open') AS resolution, count(*), sum(held_units) AS total_held_units
FROM wallet_hold_outcome
GROUP BY resolution;

-- 3. Check for stale in-flight outbox events (unresolved > 5 minutes)
SELECT event_id, platform_user_id, status, attempt_count, claimed_at
FROM wallet_settlement_outbox
WHERE status = 'in_flight' AND claimed_at < now() - INTERVAL '5 minutes';

-- 4. Check active streaming live provisional sessions
SELECT token, status, billing_currency, estimated_units, created_at, activated_at, terminal_at
FROM wallet_live_provisional
WHERE status IN ('provisional', 'active', 'finalizing')
ORDER BY created_at DESC
LIMIT 20;
```

---

## 4. Step-by-Step Disaster Recovery Procedure

### Scenario 1: Recovering from Gateway Crash
1. Check container status:
   ```bash
   docker compose -f deploy/docker-compose.yml ps sub2api
   ```
2. Restart the gateway service:
   ```bash
   docker compose -f deploy/docker-compose.yml restart sub2api
   ```
3. Inspect startup logs to verify Redis policy check and DB connection:
   ```bash
   docker compose -f deploy/docker-compose.yml logs --tail=100 sub2api
   ```
4. Verify the orphan reaper resumes leadership and processes any abandoned holds:
   ```bash
   docker exec sub2api-postgres psql -U unirouter -d unirouter_control -c \
     "SELECT count(*) FROM wallet_hold_outcome WHERE resolution = 'abandoned';"
   ```

### Scenario 2: Recovering from Redis Outage / Restart
1. Check Redis health:
   ```bash
   docker compose -f deploy/docker-compose.yml ps redis
   docker exec sub2api-redis redis-cli ping
   ```
2. Verify Redis persistence configuration:
   ```bash
   docker exec sub2api-redis redis-cli config get appendonly
   docker exec sub2api-redis redis-cli config get maxmemory-policy
   ```
3. Allow 1–2 reaper intervals (`orphan_sweep_interval_seconds`, default 60s) for the reaper tick to acquire leader lock and clean up hold keys.
4. Verify no orphan keys leak in Redis:
   ```bash
   docker exec sub2api-redis redis-cli scard canonical_wallet:holds_users
   ```

### Scenario 3: Recovering from PostgreSQL Outage
1. Verify PostgreSQL service health:
   ```bash
   docker exec sub2api-postgres pg_isready -U unirouter -d unirouter_control
   ```
2. Check database durability parameters:
   ```sql
   SHOW synchronous_commit; -- must be 'on'
   SHOW fsync;              -- must be 'on'
   ```
3. Verify outbox backlog begins draining to control plane:
   ```sql
   SELECT count(*) FROM wallet_settlement_outbox WHERE status = 'pending';
   ```

---

## 5. Expected Convergence Windows

| Parameter | Default Value | Drill Value | Description |
|---|---|---|---|
| `orphan_grace_seconds` | 900s (15 min) | 60s | Minimum age before an un-settled hold is eligible for reap |
| `orphan_sweep_interval_seconds` | 60s (1 min) | 10s | Frequency of reaper leader sweep ticks |
| `orphan_sweep_batch` | 200 | 100 | Maximum holds evaluated per user per tick |
| **Max Convergence Time** | **960s (16 min)** | **70s–80s** | Time from gateway drop to complete hold reap & outcome recording |

**Automated Test Reference:** `internal/service/openai_live_failover_drill_test.go` (`TestOpenAILiveFailoverDrillTest74`).

---

## 6. ShipAny 4.3-S Operator Handoff, Cross-Plane Verification & Emergency Rollback

### A. ShipAny 4.3-S Verification Checklist
ShipAny operators responsible for control plane operations should verify the following points when Sub2API operates in `canonical_wallet.mode=enforce`:
1. **Ensure Route Health**: Verify Sub2API instances are successfully negotiating leases over `/api/v1/wallet/lease/ensure` without unexpected 4xx/5xx responses.
2. **Zero Orphan Leaks**: Verify that lease reservations granted by ShipAny match the sum of active holds plus released/settled amounts in Sub2API.
3. **Dispatcher Delivery Latency**: Outbox events from Sub2API should be claimed and resolved within normal HTTP dispatch timeouts (p99 < 500ms).

### B. Shared Metrics Cross-Reference Matrix

| Sub2API Gateway Metric | ShipAny Control Plane Metric | Meaning / Verification Condition |
|---|---|---|
| `canonical_wallet_bridge_settlements_enqueued_total` | `shipany_wallet_settlements_received_total` | Settlement events queued on data plane match incoming events at control plane. |
| `canonical_wallet_bridge_settlements_delivered_total` | `shipany_wallet_settlement_events_applied_total` | Delivered settlement count equals applied ledger adjustments. |
| `canonical_wallet_bridge_holds_abandoned_total` | `shipany_wallet_holds_abandoned_total` | Holds reaped by Sub2API match abandoned releases recorded by control plane. |
| `canonical_wallet_settlement_uncollectable_units` | `shipany_wallet_receivable_balance_units` | Uncollectable units (balance shortfalls) match the recorded receivable debt. |

### C. Cross-Plane Reconciliation Drill Query Procedure
To verify penny-exact ledger balance agreement across both planes for any given platform user:

1. **Query Sub2API Outbox Net Delivered Settlement Sum:**
   ```sql
   -- Sub2API PostgreSQL:
   SELECT 
     platform_user_id,
     currency,
     coalesce(sum(amount_units), 0) AS total_settled_units
   FROM wallet_settlement_outbox
   WHERE status = 'delivered' AND platform_user_id = 'shipany-user-example-123'
   GROUP BY platform_user_id, currency;
   ```

2. **Query ShipAny Control Plane User Wallet Ledger:**
   ```sql
   -- ShipAny PostgreSQL:
   SELECT 
     user_id,
     currency,
     initial_balance_units - current_balance_units AS ledger_consumed_units,
     receivable_debt_units
   FROM user_wallets
   WHERE user_id = 'shipany-user-example-123';
   ```

3. **Verification Condition:**
   `total_settled_units (Sub2API)` **MUST EQUAL** `ledger_consumed_units (ShipAny) + receivable_debt_units (ShipAny)`.

### D. Emergency Rollback Procedure
If unexpected upstream degradation, control-plane network issues, or operational anomalies occur while in `canonical_wallet.mode=enforce`:

1. **Switch Mode to `shadow` (or `disabled`):**
   Update environment variable on Sub2API host / container:
   ```bash
   # In .env or docker-compose environment:
   CANONICAL_WALLET_MODE=shadow
   ```
2. **Reload / Restart Gateway Service:**
   ```bash
   docker compose -f deploy/docker-compose.yml up -d sub2api
   ```
3. **Verify Seamless Fallback:**
   - In shadow mode, user traffic continues without interruption.
   - Hold failures and refusals are admitted continuously with non-blocking metric increments (`canonical_wallet_live_window_refused_shadow_total`).
   - The outbox dispatcher remains active and continues draining existing pending outbox rows to ensure no financial events are lost.

