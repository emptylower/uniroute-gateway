# Model Governance Phase 2 Second Review

Date: 2026-08-19

Status: **HISTORICAL; SUPERSEDED BY THIRD REVIEW**

## 1. Purpose

This document originated as the remediation handoff for the second independent
review of Phase 2, Model Governance Foundation and Inventory. Sections 4 through
10 preserve the findings and evidence as a dated historical snapshot. Current
status, scope, and re-acceptance evidence are authoritative in Section 15 and in
`docs/model-governance-phase-2-acceptance-report.md`. The later third-review
findings and current status are authoritative in
`docs/model-governance-phase-2-third-review-2026-08-20.md`.

The fixing agent must independently verify each finding against the current
code before implementing it. The expected outcome is the smallest correction
that restores the formal Phase 2 contract without introducing Phase 3
classification, Phase 5 enforcement, production migrations, or unrelated
refactors.

## 2. Review Scope

- Repository: `/Users/mac/project/sub2api-unirouter-model-governance-foundation-inventory`
- Branch: `model-governance-foundation-inventory`
- Formal base: `cad5bc5606bfc059e2da91a54db9479f312b9dd2`
- Committed reviewed head: `fb28063969782037ec7f5a9e2200bc73ecfc7f3a`
- Committed review range: `cad5bc560..fb280639`
- Additional scope at the second-review re-acceptance checkpoint: the then-current
  uncommitted implementation, migration, generated-source, and test overlay: 49 code/generated/
  migration/test paths, plus the two review documents. The user-owned
  `.gitignore` modification is excluded from implementation scope.
- Acceptance contract:
  `/Users/mac/project/all-model-router/docs/superpowers/plans/2026-08-18-model-governance-foundation-inventory.md`
- Prior remediation report:
  `docs/model-governance-phase-2-acceptance-report.md`

The committed review range remains `cad5bc560..fb280639`; all remediation after
that HEAD is an explicitly uncommitted overlay. The formal plan, including its
approved execution clarifications and acceptance corrections, remains
authoritative. A report verdict is evidence to review, not a substitute for a
committed-state acceptance decision.

## 3. Verdict

At the second-review re-acceptance checkpoint, Phase 2 was **technically accepted
in that mutable worktree**. Final merge PASS was withheld pending commit and a
gate against the resulting committed review range. The current superseding
status is the pointer below.

Current-status pointer: a third review subsequently raised `P2-R3-001` through
`P2-R3-008`. Those findings and all later final-review endpoint/dimension and
documentation findings are resolved in the accepted committed range
`cad5bc5606bfc059e2da91a54db9479f312b9dd2..557507a18ab31bcf410bc132fb2487cd189440b7`.
The committed-state coordinator gate passed, and an independent review returned
**FINAL PASS**, Critical 0, Important 0, Minor 0. See the third-review document
and acceptance report; this historical body is not the current acceptance
verdict.

The remediation resolves `P2-R2-001` through `P2-R2-007`. A final independent
technical review found no Critical, Important, or Minor implementation findings,
and the complete fresh gate in Section 13 passed on 2026-08-20. The remaining
blocker is mechanical traceability, not an open code defect.

At the time of the second review, the previous remediation had fixed the
half-open usage-window behavior, but runtime inventory remained incomplete or
inaccurate for mixed routing and OpenAI passthrough, and equal-watermark
arbitration mixed Go nanosecond timestamps with PostgreSQL microsecond
timestamps. Two additional foundation defects violated the additive and
durable-evidence boundaries: governance persistence rejected exact model IDs
longer than 255 characters before legacy sync, and account deletion cascaded
away accepted batch/raw evidence. Those historical defects are resolved in the
current worktree as recorded in Section 15.

Required before final merge PASS:

1. Commit all intended implementation, generated, migration, test, and tracked
   review-artifact changes before merge.
2. Record the resulting committed HEAD and exact committed review range.
3. Rerun the complete verification gate in Section 13 against that committed
   state.

## 4. Required Fixes

### P2-R2-001: Mixed Routing Uses the Wrong Channel Platform

Severity: High

Priority: P1 before Phase 2 merge

Status: Resolved in current worktree; final PASS withheld pending commit

#### Contract

The inventory must enumerate every finite effective upstream mapping used by
configuration-enabled runtime routing and must include every enabled
account/group/channel dimension without inventing dimensions that runtime cannot
schedule.

#### Actual Behavior

`enabled_routes` exposes the account platform as the only platform value, and
channel mappings and channel pricing are selected with that account platform:

- `backend/internal/repository/model_governance_inventory_repo.go:169-183`
- `backend/internal/repository/model_governance_inventory_repo.go:185-204`

Runtime allows Antigravity accounts with `mixed_scheduling=true` to participate
in Anthropic and Gemini groups:

- `backend/internal/service/gateway_scheduling.go:972-994`
- `backend/internal/service/account.go:1618-1632`

Runtime channel lookup normally uses the group/request platform rather than the
selected account platform:

- `backend/internal/service/channel_service.go:365-376`
- `backend/internal/service/channel_service.go:481-494`

Consequently, an Antigravity account in an Anthropic group uses
`channel.model_mapping["anthropic"]` at runtime, while the inventory reads
`channel.model_mapping["antigravity"]`. The same mismatch affects
`channel_model_pricing.platform`.

The query also attaches active group bindings to accounts without proving that
the account platform is schedulable for that group platform. This can project
account mappings into group/channel dimensions that runtime cannot select.

#### Reproduction

1. Create an active, schedulable Antigravity account with
   `extra.mixed_scheduling=true`.
2. Bind it to an active Anthropic group and active channel.
3. Configure a finite mapping only in `channel.model_mapping.anthropic` and a
   pricing capability with `platform='anthropic'`.
4. Confirm runtime scheduling can select the Antigravity account for the
   Anthropic request and channel lookup uses the Anthropic platform.
5. Run the inventory.
6. Observe that the Anthropic mapping/pricing target is omitted or an
   Antigravity-only target is incorrectly included.

#### Required Implementation Properties

- Represent account platform and runtime channel lookup/target platform as
  separate concepts.
- Include an account/group dimension only when the account is eligible for that
  group under stable configuration semantics.
- Preserve the accepted scope of active, non-deleted, `schedulable=true`
  accounts; do not make transient cooldown/quota state hide configuration.
- Reuse or faithfully centralize established mixed-routing eligibility rather
  than creating a second drifting SQL-only platform matrix.
- Preserve composite target-platform behavior and force-platform semantics where
  they produce finite enabled dimensions.
- Keep the inventory transaction read-only and repeatable-read.

#### Required Regression Tests

- Anthropic group plus mixed-enabled Antigravity account reads Anthropic channel
  mapping and pricing.
- Gemini group plus mixed-enabled Antigravity account reads Gemini channel
  mapping and pricing.
- The same Antigravity account with mixed scheduling disabled does not
  contribute to Anthropic/Gemini group dimensions.
- Unsupported account/group platform combinations are not projected.
- Native account/group platform combinations remain unchanged.
- Composite route/platform cases remain covered.
- Read-only before/after assertions remain green.

#### Acceptance Criteria

- Every projected group/channel dimension has a corresponding stable runtime
  scheduling path.
- Every finite channel mapping and pricing target that runtime can use in mixed
  scheduling appears under the correct account/group/channel dimension.
- No mapping or pricing target is selected solely because its key matches the
  account platform when runtime uses the group/request platform.

### P2-R2-002: OpenAI Passthrough Projection Uses Non-Passthrough Order

Severity: High

Priority: P1 before Phase 2 merge

Status: Resolved in current worktree; final PASS withheld pending commit

#### Contract

Inventory projection must match actual forwarding reachability. Generic
passthrough must not fabricate finite model IDs, while concrete targets that are
actually reachable through stable mappings must be included.

#### Actual Behavior

`ProjectInventoryAccountMappings` always treats normal OpenAI mapping as an
outbound rewrite and applies compact lookup to the post-normal target:

- `backend/internal/service/model_governance_inventory.go:78-119`

Runtime passthrough behaves differently. The normal passthrough path forwards
the original `reqModel`; normal account mapping is used only for local reasoning
policy checks:

- `backend/internal/service/openai_gateway_forward.go:148-178`

Compact passthrough applies compact mapping directly to the original request
model:

- `backend/internal/service/openai_gateway_passthrough.go:39-50`

For this stable configuration:

```json
{
  "model_mapping": {"public": "normal-upstream"},
  "compact_model_mapping": {"public": "compact-upstream"}
}
```

with:

```json
{"openai_passthrough": true}
```

runtime can send `public` on normal passthrough and `compact-upstream` on compact
passthrough. Current inventory instead emits `normal-upstream` and can miss both
reachable values.

#### Required Implementation Properties

- Branch on `Account.IsOpenAIPassthroughEnabled()` before applying mapping order.
- Support both `extra.openai_passthrough` and the legacy
  `extra.openai_oauth_passthrough` through the shared account helper.
- For ordinary passthrough, do not claim normal mapping targets are outbound
  models when runtime retains the original request model.
- For compact passthrough, resolve compact mapping against the original request
  domain, not the post-normal target domain.
- Do not invent an unbounded list for passthrough allow-all behavior. Include
  only concrete, finitely enumerable targets or exact configured requested
  values whose runtime reachability is proven.
- Preserve non-passthrough normal-to-compact sequencing and OAuth normalization.

#### Required Regression Tests

- New passthrough flag plus normal and compact mappings.
- Legacy passthrough flag plus normal and compact mappings.
- Normal passthrough does not emit an unreachable normal mapping target.
- Compact passthrough emits the concrete compact target reachable from the
  original request model.
- Passthrough allow-all does not create `*` or a fabricated catalog.
- Existing non-passthrough exact/wildcard precedence and OAuth normalization
  remain green.
- Runtime witnesses use shared runtime helpers or invoke the actual forwarding
  resolution seam; they must not simply duplicate projector assumptions.

#### Acceptance Criteria

- The projected finite target set matches both normal and compact runtime
  forwarding for passthrough and non-passthrough accounts.
- Neither reachable targets are omitted nor unreachable normal-mapping targets
  invented.

### P2-R2-003: Equal-Watermark Order Mixes Nanoseconds and Microseconds

Severity: High

Priority: P1 before Phase 2 merge

Status: Resolved in current worktree; final PASS withheld pending commit

#### Contract

Equal-`ObservedAt` conflicting batches use the stable total order
`(ObservedAt, provenance digest)`. The highest digest at that timestamp must own
the current projection regardless of arrival or account-lock order.

#### Actual Behavior

PostgreSQL stores `TIMESTAMPTZ` at microsecond precision, but the provenance
digest contains the original nanosecond `RFC3339Nano` value.

The batch persists the original Go time:

- `backend/internal/repository/model_governance_repo.go:89-95`

The existing winner is read back at PostgreSQL precision:

- `backend/internal/repository/model_governance_repo.go:120-128`

The stale/equal/replacement decision compares that value with the original Go
timestamp:

- `backend/internal/repository/model_governance_repo.go:133-142`

The digest hashes the unrounded nanosecond value:

- `backend/internal/repository/model_governance_repo.go:428-451`

Production supplies `time.Now().UTC()` without microsecond canonicalization:

- `backend/internal/handler/admin/account_handler.go:2582-2583`
- `backend/internal/service/upstream_models.go:244-253`

Two distinct Go times within the same stored microsecond can therefore be
treated as strictly ordered by Go while PostgreSQL treats them as one watermark.
The later arrival can bypass equal-watermark rollback/replacement even though
the persisted winner query orders those rows by digest.

#### Reproduction

1. Prepare two conflicting same-account catalogs.
2. Use `ObservedAt` values separated only within one PostgreSQL microsecond, for
   example `...000000100` and `...000000200`.
3. Record both arrival orders.
4. Load the persisted batches and current observation projection.
5. Observe that stored timestamps collide while Go arbitration can take the
   strictly-newer path, allowing current metadata or presence to depend on
   arrival order rather than the persisted total order.

#### Required Implementation Properties

- Define one canonical timestamp representation equal to PostgreSQL persistence
  precision.
- Use that canonical value consistently for batch insertion, provenance digest,
  stale/equal comparisons, projection timestamps, event payloads, and
  idempotency material where applicable.
- Alternatively, perform arbitration entirely from persisted canonical values.
- Preserve durable loser evidence and append-only actual-history events.
- Preserve the account-first lock order and existing deadlock correction.
- Do not introduce sleep-based concurrency tests.

#### Required Regression Tests

- Conflicting catalogs with different nanoseconds in the same stored
  microsecond converge across opposite arrival orders.
- Digest/winner selection uses the documented canonical timestamp.
- Current presence, miss streak, connection, snapshot, and first/last-seen
  metadata converge.
- Strictly different stored microseconds remain ordered by time.
- Exact canonical equal tuple remains durable evidence-only and emits no
  duplicate transition.
- Existing empty-winner, chained replacement, stale, concurrent, and admin-state
  preservation tests remain green.

#### Acceptance Criteria

- No decision compares a nanosecond input against a microsecond persisted
  watermark as if they had the same precision.
- Current projection is independent of transaction start and lock acquisition
  order for all timestamps that PostgreSQL stores as equal.

### P2-R2-004: A Governance-Only 255-Character Limit Blocks Legacy Sync

Severity: High

Priority: P1 before Phase 2 merge

Status: Resolved in current worktree; final PASS withheld pending commit

#### Contract

Phase 2 evidence is additive and non-authoritative. It must preserve exact IDs
and must not change the existing account mapping behavior.

#### Actual Behavior

The governance schema limits observation and event IDs to `VARCHAR(255)`:

- `backend/migrations/200_model_governance_foundation.sql:63`
- `backend/migrations/200_model_governance_foundation.sql:89`

Discovery parsing preserves arbitrary JSON string IDs and has no matching
255-character protocol limit:

- `backend/internal/service/upstream_models.go:697-753`
- `backend/internal/service/upstream_models.go:837-849`

Evidence persistence runs before the legacy mapping update:

- `backend/internal/service/upstream_models.go:234-278`

The current integration test explicitly demonstrates that a 256-character ID
causes the complete governance transaction to fail and removes the batch:

- `backend/internal/repository/model_governance_repo_integration_test.go:195-218`

At the service boundary this error also prevents the existing JSONB mapping
update. Governance has therefore become authoritative over an established sync
operation.

#### Required Implementation Properties

- Preserve the exact ID; do not trim, normalize, hash-replace, or silently
  truncate it.
- Align governance storage with the accepted discovery ID domain, normally by
  using `TEXT` unless an explicit upstream protocol limit is approved.
- Apply the correction consistently to all governance tables and indexes that
  store exact upstream IDs, including inventory persistence schema where
  applicable.
- If an explicit limit is retained, define a non-authoritative failure boundary
  so evidence limitations cannot silently block legacy mapping behavior, and
  obtain explicit approval for that deviation.
- Migration 200 remains disposable-test-only and must not be applied to
  production during this work.

#### Required Regression Tests

- An exact ID longer than 255 characters persists unchanged in batch projection
  and event identity.
- The same ID reaches the established legacy mapping update behavior.
- Idempotent replay and exact-ID uniqueness work for long IDs.
- Inventory can return the long exact ID without truncation.
- Existing punctuation, whitespace, case, duplicate, and wildcard evidence
  tests remain green.

#### Acceptance Criteria

- No governance-only storage constraint rejects an ID accepted by the discovery
  and legacy mapping path.
- Exact-ID preservation is maintained end to end.

### P2-R2-005: Account Deletion Erases Durable Batch Evidence

Severity: High

Priority: P1 before Phase 2 merge

Status: Resolved in current worktree; final PASS withheld pending commit

#### Contract

Every accepted batch, including stale batches, equal-watermark losers, and empty
catalogs, remains durable batch/raw evidence.

#### Actual Behavior

`model_classification_batches.account_id` uses `ON DELETE CASCADE`:

- `backend/migrations/200_model_governance_foundation.sql:45-57`

Administrator account deletion is an active production path:

- `backend/internal/service/admin_account.go:1182-1196`
- `backend/internal/handler/admin/account_handler.go:1050`

Deleting an account consequently removes the classification batch, lossless raw
snapshot, provenance digest, idempotency key, observed timestamp, and connection
lineage.

Observation events intentionally preserve scalar account/model identity, but
they do not replace batch evidence. In particular, a successfully accepted empty
catalog can have no observation event when there are no prior projections.

The existing deletion test verifies only that one non-empty observation event
survives:

- `backend/internal/repository/model_governance_repo_integration_test.go:998-1024`

It does not assert classification-batch retention and does not cover an empty
accepted catalog.

#### Required Implementation Properties

- Preserve immutable scalar account identity after account deletion without
  requiring the live account row.
- Do not cascade-delete accepted classification batches.
- Preserve batch-level raw evidence, provenance digest, idempotency key,
  observed time, and nullable connection lineage.
- Keep current projection cleanup semantics separate from immutable evidence
  retention.
- Preserve observation-event scalar identity and append-only triggers.
- Ensure account deletion still succeeds under the intended administrative
  workflow.

#### Required Regression Tests

- A non-empty accepted batch survives account deletion with raw/provenance and
  scalar account identity intact.
- An accepted empty catalog with no observation events survives account
  deletion.
- Stale/equal-loser batch evidence survives account deletion.
- Current observations follow the approved deletion behavior without deleting
  immutable events/batches.
- Idempotency evidence is still queryable after deletion.

#### Acceptance Criteria

- Account deletion cannot erase any accepted discovery batch.
- Immutable evidence remains reconstructable without a live account foreign key.

## 5. Capacity Decision Required

### P2-R2-006: Raw Snapshot Storage Amplifies Per Model and Event

Severity: Medium, potentially High under large accepted catalogs

Priority: P1 design decision before Phase 2 merge

Status: Resolved by owner-approved single-copy batch-reference design; final
PASS withheld pending commit

#### Current Behavior

The same accepted catalog snapshot is stored:

1. Once in `model_classification_batches.raw_snapshot`.
2. Once in every affected `model_observations.raw_snapshot`.
3. Once in every affected `model_observation_events.payload.raw_snapshot`.

Relevant code:

- `backend/internal/repository/model_governance_repo.go:89-95`
- `backend/internal/repository/model_governance_repo.go:180-218`
- `backend/internal/repository/model_governance_repo.go:533-586`

The accepted upstream body limit is 8 MiB:

- `backend/internal/service/upstream_models.go:22`

For snapshot size `S` and `N` affected models, a discovery can write
approximately `S + 2*N*S`, excluding JSONB/TOAST/WAL overhead. Missing
transitions can also copy prior snapshots into new events.

#### Required Decision

Choose one of the following and record explicit owner approval:

1. Store the complete lossless payload only at immutable batch scope; store a
   batch reference and bounded model post-state in projections/events.
2. Establish and enforce explicit accepted-payload and model-cardinality budgets
   that prove worst-case storage/WAL growth is bounded and operationally safe.
3. Retain duplication only with a written capacity analysis, production budget,
   retention strategy, and explicit risk acceptance.

#### Constraints

- Lossless original payload evidence must remain reconstructable.
- Append-only events must retain enough committed post-state for deterministic
  replacement and audit.
- Do not weaken the accepted empty/stale/equal-loser evidence contract.
- Avoid introducing an unbounded external blob dependency unless separately
  designed and approved.

#### Required Tests or Evidence

- A deterministic storage-complexity test or benchmark using a representative
  large catalog.
- Documented maximum payload/cardinality and resulting database/WAL budget.
- Replacement and historical reconstruction tests after any payload-reference
  redesign.

## 6. Required Test Hardening

### P2-R2-007: Usage Boundary Assertions Are Not Fully Isolated

Severity: Low

Priority: P2, required before re-acceptance because the report claims complete
coverage

Status: Resolved in current worktree; final PASS withheld pending commit

The production implementation of `P2-ACCEPT-002` is accepted as correct:

- One frozen UTC `windowEnd` is used.
- Usage windows are half-open.
- Every usage-derived key, aggregate, validation, and API-key count derives from
  the same bounded CTE.

Relevant implementation:

- `backend/internal/service/model_governance_inventory.go:49-52`
- `backend/internal/repository/model_governance_inventory_repo.go:220-253`

Two test claims require isolation:

1. Lower-bound rows currently share one API key, so distinct API-key inclusion at
   exact 7-day and 30-day lower boundaries is not independently proven.
2. The excluded future row is both negative and mixed-currency, so mixed-currency
   validation can mask whether future negative-source validation is independently
   excluded.

Required tests:

- Use distinct API keys at exact lower boundaries and assert both 7-day and
  30-day distinct counts.
- Test future mixed currency without negative revenue.
- Test future negative revenue without mixed currency.
- Preserve exact-end and after-end exclusion plus immediately-before-end
  inclusion.

## 7. Findings Not Blocking This Phase

The fixing agent must not expand scope into the following items without a new
instruction or explicit plan deviation.

### 7.1 Administrator Classification Concurrency

The current repository replacement path should eventually share a lock protocol
with administrator classification mutation. Phase 2 does not expose the formal
classification mutation route, so that concurrency contract should be completed
when the write path is introduced rather than invented in this remediation.

### 7.2 Legacy Event-Payload Compatibility

The replacement implementation assumes event payload fields emitted by the final
Phase 2 code. Migration 200 has not been applied to production, so there is no
concrete persisted pre-fix dataset requiring backward compatibility. Do not add
compatibility code unless deployment evidence changes this assumption.

### 7.3 Registry Delete Lock Ordering

Concurrent registry deletion and discovery may require a unified lock order when
registry mutation becomes active. Phase 2 has no production registry mutation
workflow. Record the risk for the phase that introduces that path rather than
adding speculative transaction machinery now.

### 7.4 Asynchronous Audit

The signed inventory GET uses the existing asynchronous best-effort sensitive
read audit. Durable synchronous audit persistence is explicitly deferred beyond
this plan.

### 7.5 Usage Index

Do not add a usage-log index without a production-shaped PostgreSQL 14
`EXPLAIN (ANALYZE, BUFFERS)`. The current plan explicitly defers that decision.

### 7.6 Existing Accepted Semantics

Preserve these decisions:

- Disabled or unschedulable accounts do not contribute current projections, but
  qualifying historical usage remains visible.
- No-usage inventory currency is `CNY`.
- Reordered exact model IDs remain an idempotency collision mismatch, while
  exact duplicates collapse after first occurrence.
- Generic HTTP/Codex endpoint/request-ID provenance enrichment remains deferred.
- Observation events represent actual arrival/replacement history and need not
  be byte-identical across opposite arrival orders when current state converges.

## 8. Previously Resolved Item

### P2-ACCEPT-002: Half-Open UTC Usage Windows

Status: Resolved, subject to test hardening in `P2-R2-007`.

Verified properties:

- Service samples one UTC `windowEnd`.
- Repository receives 7-day lower, 30-day lower, and common exclusive end.
- `usage_rows` applies `[windowEnd-30d, windowEnd)`.
- Seven-day filters use the same bounded source and an inclusive lower boundary.
- Future-only rows cannot create inventory keys.
- Revenue, currency, negative-source, overflow, and distinct API-key calculations
  share the same bounded source.

Do not redesign this implementation while fixing unrelated findings.

## 9. Historical Worktree State at Second Review

At the end of the original second review, before the complete remediation, the
worktree contained:

```text
M  .gitignore
M  backend/internal/repository/model_governance_inventory_repo.go
M  backend/internal/repository/model_governance_inventory_repo_integration_test.go
M  backend/internal/service/model_governance_inventory.go
M  backend/internal/service/model_governance_inventory_projection_test.go
?? docs/model-governance-phase-2-acceptance-report.md
```

This is historical evidence only. The current scope is the complete overlay
described in Sections 2 and 15 and must be obtained from fresh `git status`.

## 10. Historical Verification Evidence from Second Review

The following commands passed during the original second review against
committed head `fb280639` plus its then-current four-file implementation overlay:

```bash
go test ./internal/config -run ModelGovernance -count=1
go test ./migrations -run ModelGovernanceFoundation -count=1

go test ./internal/service ./internal/handler/admin \
  ./internal/server/middleware ./internal/server/routes \
  -run 'ModelGovernance|AccountHandlerSyncUpstreamModels' -count=1

go test -tags=integration ./internal/repository \
  -run 'MigrationsRunner|ModelGovernanceRepository|ModelGovernanceInventory' \
  -count=1

SUB2API_TEST_POSTGRES_IMAGE=postgres:14-alpine \
  go test -tags=integration ./internal/repository \
  -run MigrationsRunner_ModelGovernanceFoundation -count=1

go test ./... -count=1
make generate
make build
git diff --check cad5bc560..HEAD
git diff --check
```

All commands exited successfully. `make generate` left no
`backend/cmd/server/wire_gen.go` diff.

At that historical point, these green tests did not prove the findings absent.
The complete remediation and current fresh evidence are recorded in Section 15
and the re-acceptance report.

## 11. Fixing-Agent Workflow

Use a test-first, one-finding-at-a-time workflow:

1. Read the formal plan, this document, and relevant runtime paths.
2. Reproduce `P2-R2-001`; add a failing focused test; implement the smallest
   correction; run focused and neighboring tests.
3. Repeat for `P2-R2-002` through `P2-R2-005`.
4. Resolve the design/capacity decision in `P2-R2-006` before changing storage
   shape. Record explicit approval if the chosen approach changes the plan.
5. Add isolated tests for `P2-R2-007`.
6. Re-read all diffs for accidental Phase 3/5 behavior, mutation, or routing
   changes.
7. Run the complete gate in Section 13.
8. Produce a fix report mapping every finding and acceptance criterion to code,
   tests, and fresh command output.

For every new plan deviation, record:

- Original plan text.
- Implemented behavior.
- Evidence and reason.
- Scope and risk.
- Explicit owner approval.

## 12. Prohibited Scope Expansion

Do not:

- Activate `shadow` or `enforce` authorization.
- Change scheduler, mapping, pricing, routing, group, channel, or eligibility
  state as a side effect of governance evidence or inventory reads.
- Add registry/classification admin mutation APIs.
- Apply migration 200 to production.
- Deploy or alter production data.
- Redesign audit persistence.
- Add speculative backward compatibility for data that has never shipped.
- Commit unrelated dirty-worktree changes.
- Treat current report assertions as proof without inspecting code and running
  tests.

## 13. Required Re-Verification Gate

After all corrections, run from `backend/` unless the command is a Git command:

```bash
go test ./internal/config -run ModelGovernance -count=1
go test ./migrations -run ModelGovernanceFoundation -count=1

go test ./internal/service \
  -run 'ModelGovernance|ProjectInventoryAccountMappings|PersistUpstreamModelDiscovery|PersistDiscoveredModels' \
  -count=1

go test ./internal/handler/admin ./internal/server/middleware ./internal/server/routes \
  -run 'ModelGovernance|AccountHandlerSyncUpstreamModels' \
  -count=1

go test -tags=integration ./internal/repository \
  -run 'MigrationsRunner|ModelGovernanceRepository|ModelGovernanceInventory' \
  -count=1

SUB2API_TEST_POSTGRES_IMAGE=postgres:14-alpine \
  go test -tags=integration ./internal/repository \
  -run 'MigrationsRunner_ModelGovernanceFoundation|ModelGovernanceRepository|ModelGovernanceInventory' \
  -count=1

go test ./... -count=1
make generate
make build
```

Then, from the repository root:

```bash
git diff --check cad5bc560..HEAD
git diff --check
git status --short --branch
```

All commands must exit successfully. Review the complete output, not only the
last package. Generated source must be committed if it changes. Integration and
migration tests must use disposable databases only.

## 14. Re-Acceptance Checklist

- [x] `P2-R2-001` mixed route platform semantics are fixed and tested.
- [x] `P2-R2-002` passthrough and non-passthrough projections match runtime.
- [x] `P2-R2-003` uses one canonical persisted timestamp precision.
- [x] `P2-R2-004` preserves exact long IDs without governance blocking sync.
- [x] `P2-R2-005` preserves every accepted batch after account deletion.
- [x] `P2-R2-006` has an implemented bounded design or explicit approved risk.
- [x] `P2-R2-007` independently tests all accepted window effects.
- [x] No routing, mapping, pricing, scheduler, eligibility, or authorization
      mutation was introduced.
- [x] Effective authorization behavior remains `off`.
- [x] Inventory remains signed, audited through the accepted mechanism, and
      read-only.
- [x] Migration 200 was tested only against disposable PostgreSQL.
- [x] Full tests, PostgreSQL 14 integration, generation, build, and diff checks
      pass freshly.
- [x] All intended code and generated changes are committed before merge.
- [x] The remediation reports state the exact committed review range and
      separately identify the user-owned uncommitted `.gitignore` change.

This checklist is now satisfied against the accepted committed range. The
preceding finding bodies and mutable-worktree checkpoints remain historical.

## 15. 2026-08-20 Re-Acceptance Addendum

The complete remediation overlay supersedes the obsolete four-file scope in the
earlier acceptance report. The authoritative current evidence is recorded in
`docs/model-governance-phase-2-acceptance-report.md`.

Resolved implementation properties include:

- Runtime-target dimensions compose explicit routes, detector fallback,
  endpoint compatibility, channel mapping, stable account eligibility, account
  forwarding, pricing/restriction, observations, and historical usage without
  SQL re-expansion.
- Concrete channel-pricing capabilities seed source-aware finite witnesses;
  wildcard namespaces remain non-identities.
- OpenAI passthrough normal and compact forwarding targets are projected in the
  dimensions where runtime can reach them while scheduler/channel restriction
  keep committed baseline semantics.
- Governance timestamps use PostgreSQL-equivalent UTC microsecond precision.
- Arbitrary-length exact IDs use collision-tolerant digest buckets plus complete
  `TEXT` equality and advisory locking.
- Immutable batches own the sole complete raw snapshot; observations/events use
  causal and evidence batch references.
- Account IDs are permanent incarnation identifiers enforced by an append-only
  issued-ID ledger with migration-install locking and TRUNCATE protection.
- Usage windows and target-platform evidence are isolated from future rows and
  from quota/billing behavior.

Latest coordinator current-overlay gate result: focused config, migration,
handlers, routes, and unit selectors passed. The default usage-inclusive
formal integration selector passed in 13.956s and the PostgreSQL 14 formal
integration selector passed in 12.367s. `go test ./... -count=1` passed with
`internal/service` completing in 102.197s. `make generate`, `make build`, and
`git diff --check` passed. No production migration or deployment occurred.

Latest current-overlay independent scoped technical verdict: **ACCEPTED**, with
Critical 0, Important 0, Minor 0. Focused service, full service, focused database,
default formal integration, and PostgreSQL 14 formal integration passed in
2.526s, 96.337s, 4.114s, 9.203s, and 8.005s; diff and scheduler baseline checks
passed. The complete coordinator gate remains authoritative at 13.956s /
12.367s / service 102.197s.

### Final Committed-State Pointer

Final status: **FINAL PASS**. Formal base is
`cad5bc5606bfc059e2da91a54db9479f312b9dd2`; accepted implementation HEAD is
`557507a18ab31bcf410bc132fb2487cd189440b7`; and the accepted range is
`cad5bc5606bfc059e2da91a54db9479f312b9dd2..557507a18ab31bcf410bc132fb2487cd189440b7`.
The commit includes the intended implementation, generated sources, migration,
tests, and all five review documents; `.gitignore` remains user-owned,
uncommitted, and excluded.

The committed coordinator gate passed: default usage-inclusive integration
18.952s, PostgreSQL 14 integration 17.489s, and the full suite with service
106.031s; focused suites, generation, build, diff, and generated-no-diff checks
also passed. The independent committed-state review reported Critical 0,
Important 0, Minor 0 and passed default integration in 17.162s, PostgreSQL 14 in
12.094s, and the full suite with service in 105.369s; build, generation, and diff
checks passed. All five documents are tracked, and no production operation was
performed.

The technical range is accepted; this addendum is administrative traceability.
A later documentation-only commit SHA, if created, is the final documentation
HEAD and may be recorded after commit rather than required self-referentially.
