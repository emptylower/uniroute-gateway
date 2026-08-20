# Model Governance Phase 2 Re-Acceptance Report

Date: 2026-08-20

Status: **CHANGES REQUIRED**

## Scope

- Repository: `/Users/mac/project/sub2api-unirouter-model-governance-foundation-inventory`
- Branch: `model-governance-foundation-inventory`
- Formal base: `cad5bc5606bfc059e2da91a54db9479f312b9dd2`
- Committed HEAD: `fb28063969782037ec7f5a9e2200bc73ecfc7f3a`
- Reviewed implementation: the committed range `cad5bc560..fb280639` plus 57
  current uncommitted backend implementation, migration, generated-source, and
  test paths, including three untracked backend files.
- Excluded user-owned change: `.gitignore`.
- Acceptance contract:
  `/Users/mac/project/all-model-router/docs/superpowers/plans/2026-08-18-model-governance-foundation-inventory.md`
- Remediation contracts: `docs/model-governance-phase-2-second-review.md` and
  `docs/model-governance-phase-2-third-review-2026-08-20.md`.

Two status-visible review documents and three ignored review documents accompany
the backend overlay. The user-owned `.gitignore` change remains excluded. None
of the overlay may be described as committed behavior.

## Verdict

The current mutable worktree technical verdict is **ACCEPTED**. A fresh,
independent scoped review reported Critical 0, Important 0, Minor 0. It confirmed
that all prior final-review endpoint/dimension findings and documentation
findings are addressed in the current overlay.

Final merge PASS is intentionally withheld because the intended implementation,
generated files, migration, tests, and review artifacts remain uncommitted. The
formal second-review checklist requires a committed review range before merge.
No commit was created because commit authorization was not provided.

## Finding Resolution

### P2-R2-001: Runtime Inventory Reachability

Status: **Resolved in current worktree**

- Account-native platform and effective request target platform are separate
  identities. Mixed Antigravity scheduling uses Anthropic/Gemini channel mapping
  and pricing namespaces under the correct target dimension.
- Simple mode includes legal ungrouped paths for group-bound accounts; standard
  mode does not invent those paths.
- Composite inventory composes finite witnesses through explicit-route
  precedence, detector fallback, endpoint compatibility, channel mapping,
  scheduler support, account forwarding, and channel restriction/pricing.
- Account-only dimensions add provider-generated request witnesses for eligible
  compact-only mappings and Bedrock default aliases, then resolve and filter
  them through the same finite endpoint chain. Already-final provider targets
  are never inserted directly as approved current models.
- Endpoint `any` is treated as a route wildcard, not as a synthetic request
  endpoint.
- Overlapping channel wildcard mappings conservatively include every result the
  current map-order runtime can select; exact mappings suppress wildcard paths.
- Concrete channel-pricing keys are source-aware finite witnesses. Wildcard
  pricing namespaces filter finite witnesses and never become model IDs.
- Invalid legacy billing sources follow current restriction and unrestricted
  runtime fallbacks without mutating persisted configuration.
- Observations contribute only where complete current-chain replay reaches the
  same exact final upstream ID. Historical usage remains independently visible.
- Current keys come only from service-approved final upstream IDs; aggregate SQL
  no longer re-expands routes, mappings, pricing, or observations.
- Inventory remains a read-only `REPEATABLE READ` snapshot with set-based fact
  loading and one aggregate statement.

### P2-R2-002: OpenAI Passthrough

Status: **Resolved in current worktree**

- Actual forwarding uses the endpoint-aware resolver.
- Normal passthrough preserves the original request model.
- Compact passthrough resolves compact mapping against the original request
  domain and preserves OAuth compact non-normalization.
- Grouped/composite Responses dimensions include both normal and compact finite
  targets when compact is enabled.
- Projection excludes padded, malformed, non-printable, invalid UTF-8, wildcard
  namespace, and fabricated allow-all identities.
- Scheduler and channel-restriction resolution retain the committed baseline
  normal-map-then-compact sequence; forwarding changes do not alter account
  eligibility.

### P2-R2-003: Canonical Watermarks

Status: **Resolved in current worktree**

`CanonicalGovernanceTime` converts governance timestamps to UTC microsecond
precision before idempotency material, provenance digest, persistence,
arbitration, projection timestamps, and event state. Same-stored-microsecond
inputs converge by digest; distinct stored microseconds remain time-ordered.

### P2-R2-004: Arbitrary-Length Exact IDs

Status: **Resolved in current worktree**

- Exact governance identities use `TEXT` and remain unchanged end to end.
- Fixed-width digests are lookup/locking buckets only; complete `TEXT` equality
  defines identity.
- Transaction advisory locks plus exact triggers enforce duplicate semantics for
  inserts and identity updates without btree tuple-size failures.
- Governance identity writes fail closed above `READ COMMITTED`; read-only
  repeatable-read inventory is unaffected.
- Disposable PostgreSQL tests cover IDs over 3 KiB, exact duplicates,
  collision-tolerant identity, concurrent insertion, inventory, and legacy sync.

### P2-R2-005: Durable Evidence and Account Incarnations

Status: **Resolved in current worktree**

- Classification batches retain scalar account identity without a live account
  foreign key and survive soft or hard account deletion.
- Current observations may be deleted independently; immutable batches and
  observation events remain append-only.
- `account_incarnation_ids` is an append-only issued-ID ledger backfilled under
  a migration-held account table lock. All successful future account inserts
  atomically claim their numeric ID.
- Deleted account IDs cannot be reused even when no governance batch was ever
  recorded. Concurrent reuse fails closed with SQLSTATE `23505` and constraint
  `account_id_incarnation_not_reused`.
- Ledger UPDATE, DELETE, and TRUNCATE are rejected. Migration installation,
  rerun, rollback, sequence-generated ID, and PostgreSQL 14 cases are covered.

### P2-R2-006: Single-Copy Snapshot Evidence

Status: **Resolved by owner-approved design**

Owner-approved decision:

- Store the complete lossless raw snapshot once in immutable
  `model_classification_batches`.
- Store `snapshot_batch_id` references in current observations and events.
- Preserve event `batch_id` as the causal batch and `snapshot_batch_id` as the
  committed evidence owner.
- Retain batches after account deletion; add no Phase 2 retention policy.

Replacement rollback restores evidence references from committed event state.
A deterministic test persists 1,000 models with an approximately 2 MiB snapshot
and proves one complete raw copy plus resolving projection/event references.

### P2-R2-007: Usage Boundaries

Status: **Resolved in current worktree**

- One frozen UTC end bounds all usage-derived effects with `[lower, end)`.
- Distinct API keys at exact 7-day and 30-day lower bounds prove independent
  distinct-key counts.
- Future mixed currency, future negative revenue, and future overflow are
  isolated and cannot affect current inventory or validation.
- Exact-end and after-end rows are excluded; immediately-before-end is included.

## Third-Review Resolution Matrix

| Finding | Status and accepted resolution in current worktree |
| --- | --- |
| `P2-R3-001` | Resolved. Shared account-type-aware Anthropic resolution matches Messages forwarding: API-key mapping, the dedicated Service-account mapping path, and OAuth/SetupToken Claude normalization. `count_tokens` remains intentionally separate: API-key mapping, otherwise Claude normalization only. |
| `P2-R3-002` | Resolved. Stable request-shape and endpoint gates cover embeddings, Responses text/image shape union, OpenAI Images, and positive Grok media eligibility. Transient runtime health is excluded. |
| `P2-R3-003` | Resolved. Grok media normalization precedes support and mapping; image and video text/image-input finite outcomes are represented. |
| `P2-R3-004` | Resolved. Antigravity thinking applies only to Claude-compatible endpoints, not Gemini-native paths, with account-only/grouped/mixed/forced/composite parity. |
| `P2-R3-005` | Resolved. All four append-only tables reject statement-level TRUNCATE; direct, `CASCADE`, multi-table, rerun, and PostgreSQL 14 cases pass. |
| `P2-R3-006` | Resolved. Cyber handling snapshots evidence-only target platform before detachment, never reads `gin.Context` in the goroutine, and preserves billing/quota baselines. |
| `P2-R3-007` | Resolved with option 1. Existing dashboard/stats attribution is restored to `fb280639`; only inventory/evidence consume governance target. Behavioral tests cover native, composite, forced-style, ungrouped, and `NULL` fallback. |
| `P2-R3-008` | Resolved. Ledger claim is `AFTER INSERT`, account ID updates are immutable, conflict-skipped/update forms create no ghost claims, and rollback/concurrency/rerun/PG14 coverage passes. |

Usage-inclusive verification also required a test-only fixture correction. Stale
integration fixtures omitted the mandatory currency snapshot and failed before
their intended assertions. `usage_log_fixture_integration_test.go` adds
`USD/USD/1/identity/as-of` only when the snapshot is completely absent. It does
not complete explicitly partial or invalid snapshots, so production rejection
coverage and production validation remain unchanged.

Round-2 remediation also excludes `count_tokens` for the mixed path whose
account platform is Antigravity and whose target platform is Anthropic. This
preserves the endpoint-specific runtime boundary rather than projecting a
Messages-capable mixed account as `count_tokens`-reachable.

## Preserved Boundaries

- Effective model authorization remains `off`; no `shadow` or `enforce`
  admission behavior was activated.
- Inventory is signed, read-only, and audited through the accepted asynchronous
  sensitive-read mechanism.
- Governance target platform is observational usage evidence only. Quota,
  billing, rate lookup, scheduler, and channel restriction retain baseline
  behavior.
- No registry/classification mutation API, production migration, deployment,
  production data change, audit redesign, or speculative compatibility bridge
  was introduced.
- Migration 200 was exercised only against disposable Testcontainers databases.

## Fresh Coordinator Gate

Run on 2026-08-20 against committed HEAD `fb28063969782037ec7f5a9e2200bc73ecfc7f3a`
plus the complete current overlay:

```bash
go test ./internal/config -run ModelGovernance -count=1
go test ./migrations -run ModelGovernanceFoundation -count=1

go test ./internal/service \
  -run 'ModelGovernance|ProjectInventoryAccountMappings|PersistUpstreamModelDiscovery|PersistDiscoveredModels|CyberPolicy' \
  -count=1

go test ./internal/handler ./internal/handler/admin \
  ./internal/server/middleware ./internal/server/routes \
  -run 'ModelGovernance|AccountHandlerSyncUpstreamModels|CyberPolicy' -count=1

go test -tags=integration ./internal/repository \
  -run 'MigrationsRunner|ModelGovernanceRepository|ModelGovernanceInventory|UsageLog' \
  -count=1

SUB2API_TEST_POSTGRES_IMAGE=postgres:14-alpine \
go test -tags=integration ./internal/repository \
  -run 'MigrationsRunner_ModelGovernanceFoundation|ModelGovernanceRepository|ModelGovernanceInventory' \
  -count=1

go test -tags=unit ./internal/service \
  -run '^(TestBillingModelForRestriction|TestIsUpstreamModelRestrictedByChannel)' -count=1

go test ./... -count=1
make generate
make build
git diff --check cad5bc5606bfc059e2da91a54db9479f312b9dd2..HEAD
git diff --check
git status --short --branch
```

Result: **PASS**. Focused config/migration/handlers/routes/unit selectors passed.
The default usage-inclusive formal integration selector passed in 13.956s; the
formal PostgreSQL 14 selector passed in 12.367s. `go test ./... -count=1` passed,
with `internal/service` completing in 102.197s. `make generate`, `make build`,
and `git diff --check` passed. No production migration or deploy ran.

The unrestricted repository integration command
`go test -tags=integration ./internal/repository -count=1` is outside this gate
and currently reports unrelated failures in API-key `routing_mode` fixtures,
usage-billing wallet currency fixtures, and shared-row group counts. It is not
represented as passing and is not classified here as a Phase 2 regression.

The coordinator baseline check covers only the unchanged scheduler production
files:

```bash
git diff fb28063969782037ec7f5a9e2200bc73ecfc7f3a --exit-code -- \
  backend/internal/service/openai_gateway_service.go \
  backend/internal/service/openai_gateway_scheduling.go
```

Result: **PASS**.

`backend/internal/service/gateway_forward.go` is intentionally different from
`fb280639`: Messages forwarding now delegates final model resolution to shared
`ResolveAnthropicFinalModel`. It is therefore excluded from the no-diff check.
Its baseline behavior and the endpoint-specific `count_tokens` distinction are
verified by focused resolver, forwarding, and inventory-projection tests instead.

## Independent Technical Review

A fresh independent scoped review of the current post-fixture mutable worktree
reported Critical 0, Important 0, Minor 0 and returned **ACCEPTED**. All prior
final-review endpoint/dimension and documentation findings were confirmed
addressed. Its fresh evidence was:

- Focused service selector: **PASS in 2.526s**.
- Full `internal/service`: **PASS in 96.337s**.
- Focused database selector: **PASS in 4.114s**.
- Default formal integration selector: **PASS in 9.203s**.
- PostgreSQL 14 formal integration selector: **PASS in 8.005s**.
- Diff and scheduler baseline checks: **PASS**.

This focused review supplements rather than replaces the complete coordinator
gate. The authoritative complete gate remains the run recorded above: default
formal integration 13.956s, PostgreSQL 14 formal integration 12.367s, and full
service 102.197s, with the remaining listed checks passing.

## Merge Readiness

Current mutable worktree technical status: **ACCEPTED**.

Mechanical merge status: **BLOCKED ON COMMIT AND COMMITTED-STATE REVIEW**.

Final status remains **CHANGES REQUIRED** for final merge PASS. The accepted
overlay is uncommitted, the review documents are not yet tracked or are ignored,
and the committed-state full gate and independent review remain required. The
user-owned `.gitignore` change stays excluded from this work.

Before final merge PASS:

1. Commit only the intended Phase 2 implementation, generated sources,
   migration, tests, and tracked review artifacts; preserve the user-owned
   `.gitignore` change according to owner intent.
2. Record the resulting committed HEAD and exact committed review range.
3. Re-run the required gate against that committed state.
4. Run an independent review against that committed state.
5. Update this status to final `PASS` only after the committed-state gate and
   independent review pass with no findings.
