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

The real-store suite has 26 top-level regression tests and covers all 11 models,
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
