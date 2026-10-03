# Playground media rollout and recovery

This release adds PostgreSQL migrations 217–219 and a separate additive ShipAny D1
task-pin migration. Review both SQL changes independently before release. The
PostgreSQL migrations create an empty managed provider account task ledger and authorization-segment ledger,
adds `usage_logs.media_type`, and does not rewrite existing balances, prices,
leases, orders or usage. The account stays disabled and unschedulable.

1. Deploy the D1 additive pin table, atomic lease-close guards and signed
   `wallet:task-pin` create/finish routes and `wallet:lease` pool read first.
   Pins accept multiple segments for one media or auth parent. Ensure is compatible with absolute
   `top_up_lease_id`/`minimum_budget_units` requests and keeps original expiry.
2. Preserve PostgreSQL/Redis and environment backups using the existing gateway
   deployment procedure. Never print environment file contents or provider keys.
3. Deploy the new gateway binary with `MEDIA_TASKS_ENABLED=false`. The normal
   migration runner applies 217–219 and verifies historical migration checksums.
   Confirm the new tables/indexes and managed account properties using read-only
   SQL. Keep canonical USD wallet enforce, holds on and snapshot settle enabled.
4. Supply `MEDIA_TASKS_KIE_API_KEY` privately through the gateway environment.
   `MEDIA_TASKS_KIE_BASE_URL=https://api.kie.ai`, `MEDIA_TASKS_POLL_SECONDS=5`,
   `MEDIA_TASKS_DEADLINE_SECONDS=86400`. No secret is stored in the account row.
5. Deploy ShipAny's server-only signed inference client and authenticated
   Playground routes. Confirm anonymous create is 401 and unavailable gateway
   does not fall back to direct KIE or legacy credit consumption.
6. Enable `MEDIA_TASKS_ENABLED=true` only after both halves and their policy/pin
   routes are deployed. Verify signed catalog/quote, task ownership checks,
   availability and configuration through read-only calls. Do not run a paid
   generation without a separate explicit test budget.

Turning `MEDIA_TASKS_ENABLED=false` stops new creation only. Keep the new binary,
provider credential, task worker, canonical outbox and pin protections running
until every pending media, HTTP/WS/Live attempt and financial obligation is reconciled. An old binary
does not know this task ledger and can reap its holds: rolling it back while tasks, generic attempts
or pins are active is unsafe. Stop new inference traffic before any full binary
rollback and keep the current recovery-capable binary until outstanding captures
and zero releases have been acknowledged. Do not delete tables, pins, snapshots or Redis keys
as a rollback action.

Read-only release checks:

```sql
SELECT status, pin_state, count(*) FROM gateway_media_task GROUP BY 1,2;
SELECT status, count(*) FROM wallet_settlement_outbox GROUP BY 1;
SELECT kind, state, pin_state, count(*) FROM wallet_authorization_segment GROUP BY 1,2,3;
SELECT p.provider, a.status, a.schedulable, a.credentials = '{}'::jsonb AS empty_credentials
FROM gateway_media_provider p JOIN accounts a ON a.id=p.account_id;
```

Indeterminate tasks preserve held funds indefinitely. An operator must establish
the provider task identity/outcome from trusted KIE records before reconciling it.
A missing URL, expired provider link, timeout or missing cache is not failure
proof. Never issue a replacement POST under the old idempotency key, mutate the
frozen price, or manually close a pinned lease to recover apparent free funds.
Recorded delivered settlement and confirmed D1 capture precede a settled unpin;
confirmed zero usage and a durable release precede a released unpin.

Verification uses real isolated PostgreSQL/Redis and local provider HTTP doubles:

```sh
go test -tags unit ./internal/service ./internal/config ./internal/repository ./internal/handler ./internal/server/routes -run 'TestMedia|TestAuthorization|TestCanonicalWallet|TestUSDWallet|TestLive|TestWalletReconciliation' -count=1
go test -tags media_integration ./internal/media_integration -count=1 -v
CGO_ENABLED=0 go build ./cmd/server
```

The external integration suite requires `UNIROUTE_MEDIA_TEST_POSTGRES_DSN` with a
database name beginning `uniroute_media_test_`, PostgreSQL `127.0.0.1:55432` and isolated
Redis `127.0.0.1:56379`. It fails when infrastructure is absent, does not silently
skip, applies the complete migration chain and clones disposable databases from
the migrated template. It never targets the production database or Redis.

The real-store suite covers the complete current `MediaTaskCatalog()` (including
the extended models), with per-task usage and settlement uniqueness checks,
concurrent idempotency, unknown provider
submission, restart/Redis flush, pin ACK loss, disabled creation with ongoing
recovery, multi-segment single provider/usage behavior, partial actual FIFO release,
rollback of a second-segment outbox failure, expired-claim fencing, signed basis
refusals, availability deduplication, and mixed leased/unleased USD funding. The
HTTP/Live pool fixture proves $4 + $4 + $2 unleased can authorize/capture $9;
separate cold/ordinary-hold fixtures verify a $10 request is not constrained by the
normal prelease provisioning size. Every provider in this suite is an HTTP double.
Permanent regressions also cover unknown same-handle replay refusal, reliable
zero-byte/HTTP validation rejection, cancellation retention, unsubmitted timeout,
recovery fairness beyond 32 groups, whole-group positive/zero races, repeated
HTTP correction with fresh authorization parents and mandatory old pin ACK,
cache-loss pin refusal, and Live next-window activation/expiry/Redis recovery.
The controller unit regression confirms a resolved next authorization stops the
Live session instead of being retried indefinitely.
After known HTTP rejection, every old zero pin must finish before a fresh
immutable authorization parent can send a correction. Unknown writes keep their
original parent and cannot retry. Live window advance and segment activation
commit atomically. Exact positive segment outbox events retain their funded
lease through expiry/cache recovery; existing reservation markers permit
idempotent settlement, while new reservations still reject expired/sealed leases.

The architecture note is `docs/playground-media-gateway.md`; the repository's
`docs/*` ignore rule requires explicit inclusion of this reviewed release document.

## Production Compose invocation

The production release uses a separate private `deploy/.env.unified-gateway`
(mode 600) and `deploy/docker-compose.unified-gateway.yml`. Compose's `--env-file`
loads interpolation values; it does not inject new variables unless the service
override explicitly maps them. The reviewed override maps all five
`MEDIA_TASKS_*` variables and selects the verified release image. Preserve the
existing `.env.shipany`, both original Compose files and image assets.

Every subsequent gateway restart must include both environment files and all
three Compose files:

```sh
cd /opt/sub2api
docker compose --project-name deploy \
  --env-file deploy/.env.shipany \
  --env-file deploy/.env.unified-gateway \
  -f deploy/docker-compose.yml \
  -f deploy/docker-compose.shipany.yml \
  -f deploy/docker-compose.unified-gateway.yml \
  up -d --no-build --no-deps sub2api
```

First cut over with `MEDIA_TASKS_ENABLED=false`, verify the new schema and signed
catalog/quote/availability behavior, then change only that flag to `true` after
the rollout coordinator's second approval. A feature disable retains the same
recovery-capable image, provider credential and original wallet policy. Never
print a rendered Compose configuration: it contains secrets.

The 2026-10-02 production binary was built from clean source commit
`430bf5c808f052c133dfbc34e4f64c7af02f0e6a`; its SHA256 is
`a8f1c4b3d2a59f5a872c5c7d65e07c016ae69e48e5751c604fc5d8b448dc8e83`.
The release image `uniroute-gateway:unified-430bf5c80` has ID
`sha256:b49260a8a464b072d8c3aa3b200d27b8f8ad1204ad7ce14b77931b290828bf82`.
It retains the prior image's entrypoint, health check, configuration and resource
layers. Fresh backups and private release results are under
`/opt/uniroute-backups/20261002-unified-gateway/` (directory 700, files 600).
Both cutovers passed health, image/policy and financial baseline checks; no paid
generation was performed. Independent signed endpoint acceptance is recorded
separately in the release verification document.
