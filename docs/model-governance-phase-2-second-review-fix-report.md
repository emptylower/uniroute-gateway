# Model Governance Phase 2 Second Review Fix Report

Date: 2026-08-19

Current status: **CHANGES REQUIRED**

The later committed-state FINAL PASS is **superseded** by a newly validated
`P2-R3-002` contract violation. Text Responses projection used Chat Completions
capability and admitted persisted Responses-disabled API-key accounts without an
approved plan deviation. The strict contract requires
`OpenAIEndpointCapabilityResponses` for all OpenAI Responses projections;
runtime text `/responses` fallback remains unchanged. The correction and all
five review updates require commit, committed-state gate, and independent review
rerun. A fresh full gate passed only against the current mutable-worktree overlay
and is recorded below; it is not committed-state acceptance.

## Current Mutable-Worktree Gate

Fresh current-overlay evidence after the strict-contract `P2-R3-002` correction:

- Focused config, migration, service, handler, admin, middleware, routes, and
  unit selectors: **PASS**.
- Default usage-inclusive integration selector: **PASS in 20.834s**.
- PostgreSQL 14 formal integration selector: **PASS in 19.145s**.
- Full `go test ./... -count=1`: **PASS**; `internal/service` completed in
  **105.001s**.
- `make generate`: **PASS**.
- `make build`: **PASS**.
- Runtime no-diff check: **PASS**.
- Generated no-diff check: **PASS**.
- `git diff --check`: **PASS**.
- Production operations: **not performed**.

This evidence applies to the mutable overlay only. Overall status remains
**CHANGES REQUIRED** pending commit, committed-state gate, and independent
review.

Scope: `P2-R2-003`, `P2-R2-004`, `P2-R2-005`, and owner-approved
`P2-R2-006`. This ignored working-evidence report does not modify the Phase 2
acceptance report or the second-review source.

## P2-R2-003: Canonical Governance Time

### RED

Service command:

```bash
go test ./internal/service \
  -run TestPersistUpstreamModelDiscoveryCanonicalizesTimeBeforeIdempotencyAndRepositoryInput \
  -count=1
```

Result: FAIL as expected. The repository input retained `...000000100` instead
of the canonical UTC microsecond `...000000000`; the two same-microsecond calls
therefore also had distinct time material in their idempotency seeds.

Repository command:

```bash
go test -tags=integration ./internal/repository \
  -run 'CanonicalMicrosecondConvergesAcrossArrivalOrders|DifferentCanonicalMicrosecondsRemainTimeOrdered' \
  -count=1
```

Result: FAIL as expected. Opposite arrival orders for `+100ns` and `+200ns`
selected opposite current projections even though PostgreSQL stored both at the
same microsecond. The adjacent canonical-microsecond ordering case passed.

### GREEN

Implementation:

- `service.CanonicalGovernanceTime` defines the shared representation as UTC
  truncated to `time.Microsecond`.
- `PersistUpstreamModelDiscovery` canonicalizes `syncedAt` before the
  idempotency seed and passes that exact value to repository persistence and
  legacy discovery metadata.
- `RecordDiscovery` canonicalizes `input.ObservedAt` immediately after required
  time validation. The canonical value therefore flows through the provenance
  digest, batch insertion, persisted-watermark comparisons, equal-watermark
  rollback boundaries, projection timestamps, and event payloads.
- Repository imports remain acyclic because repository already depends on the
  service interfaces and types.
- Account-first locking, raw evidence, equal-watermark digest ordering, loser
  evidence, and append-only event semantics are unchanged.

Focused commands:

```bash
go test ./internal/service \
  -run 'PersistUpstreamModelDiscoveryCanonicalizesTimeBeforeIdempotencyAndRepositoryInput|PersistDiscoveredModelsRecordsObservationBeforeCredentialUpdate|PersistUpstreamModelDiscoveryRecordsEmptyCatalogBeforeLegacyFailure' \
  -count=1

go test -tags=integration ./internal/repository \
  -run 'CanonicalMicrosecondConvergesAcrossArrivalOrders|DifferentCanonicalMicrosecondsRemainTimeOrdered' \
  -count=1
```

Result: PASS.

Neighboring regression commands:

```bash
go test ./internal/service \
  -run 'PersistUpstreamModelDiscovery|PersistDiscoveredModels' \
  -count=1

go test -tags=integration ./internal/repository \
  -run 'RecordDiscovery(EqualWatermark|EqualTuple|Stale|Canonical|Replacement|HistoricalRemoved|RemovalPreserves|StrictlyOlder|Concurrent)' \
  -count=1

git diff --check
```

Result: PASS.

## Files

- `backend/internal/service/model_governance_types.go`
- `backend/internal/service/upstream_models.go`
- `backend/internal/service/upstream_models_test.go`
- `backend/internal/repository/model_governance_repo.go`
- `backend/internal/repository/model_governance_repo_integration_test.go`
- `docs/model-governance-phase-2-second-review-fix-report.md` (ignored)
- `docs/superpowers/plans/2026-08-19-p2-r2-003-canonical-governance-time.md` (ignored working plan)

## P2-R2-003 Verification Boundary

- P2-R2-003 was originally verified with focused service and PostgreSQL
  integration regressions. The combined P2-R2-003 through P2-R2-006 final gate
  is recorded below.

## P2-R2-004: Unbounded Exact Model Identities

### RED

```bash
go test ./migrations -run TestModelGovernanceFoundationMigrationContract -count=1
go test -tags=integration ./internal/repository \
  -run TestMigrationsRunner_ModelGovernanceFoundationSchema -count=1
```

Result: FAIL as expected. The static contract found `canonical_id`, `alias`,
and `upstream_model_id` declarations still using `VARCHAR(255)`. Disposable
PostgreSQL reported `character varying` instead of `text` for
`model_registry.canonical_id`.

The repository long-ID regression also initially failed at the old projection
`raw_snapshot` write after migration 200 was changed, proving the persistence
path still required the P2-R2-006 repository implementation before a 256+
identity could traverse governance.

### GREEN

- Exact model identities in registry, aliases, observations, observation
  events, and inventory items use `TEXT`.
- Large exact `TEXT` identities are absent from btree unique/index keys.
  Registry canonical IDs, aliases, account/model observations, and null-safe
  inventory dimensions use fixed-width `md5(text)` lookup buckets plus
  transaction advisory locks and exact equality checks in INSERT/identity
  UPDATE triggers.
- The hash is never identity. It narrows candidate scans and lock contention;
  only complete dimensions plus exact `TEXT` equality produce SQLSTATE `23505`.
  Distinct exact values remain valid even if a hash/lock collision occurs.
- Live disposable-PostgreSQL tests insert IDs over 3 KiB through registry,
  alias, observation, event, and inventory tables, accept distinct values,
  reject exact duplicates, preserve null-safe inventory dimensions, and prove
  one winner/one `23505` result for concurrent duplicate observations.
- Governance persistence accepts and emits 256+ exact IDs.
- Inventory returns the same 256+ exact ID.
- Legacy `PersistDiscoveredModels` passes the exact ID to governance first,
  then to `UpdateModelDiscovery` mapping and metadata without truncation.
- The focused service long-ID test passed immediately because JSON-backed
  service/legacy storage already preserved arbitrary strings; the schema was
  the failing boundary. No artificial service implementation change was added.

## P2-R2-005: Durable Batches Across Account Deletion

### RED

Migration static and schema tests failed because
`model_classification_batches.account_id` still referenced `accounts(id) ON
DELETE CASCADE`, and batches lacked append-only protection.

### GREEN

- Batch `account_id` remains a `NOT NULL BIGINT` scalar with no live account
  foreign key.
- Account deletion still cascades current `model_observations` but succeeds
  without deleting immutable events or batches.
- Tests cover non-empty batches, a first empty batch with zero events, an empty
  accepted batch with events, a stale loser, and an equal-tuple loser.
- Retained assertions cover raw evidence, provenance digest, idempotency key,
  observed time, connection ID, account scalar, event identity, and normalized
  evidence joins.
- No account ID is deleted and reinserted as a new incarnation. Arrival-order
  tests use fresh accounts and compute provenance ordering for each account.
- Owner decision: account IDs are permanent incarnation identifiers and must
  never be reused after deletion. Migration 200 records this invariant.
- `accountRepository.Delete` coverage exercises the production soft-delete
  workflow: the account receives `deleted_at`, and projection, immutable batch,
  and immutable event evidence remain. Separate raw-SQL hard-delete tests prove
  physical deletion cascades current projections while retaining batches and
  events.
- No Phase 2 retention deletion was added.

## P2-R2-006: Single-Copy Raw Snapshot Storage

### RED

```bash
go test -tags=integration ./internal/repository \
  -run 'RecordDiscoveryPersistsLongExactModelID|SnapshotStorageIsNormalizedAndReferencesResolve|ObservationEventsSnapshotCompleteCommittedPostState|ObservationEventsRetainIdentityAfterAccountDeletion' \
  -count=1
```

Result: FAIL as expected with `pq: column "raw_snapshot" does not exist` at
the repository's old projection insert/update and event post-state queries.

### GREEN

- `model_classification_batches.raw_snapshot` is the sole complete evidence
  owner.
- `model_observations.snapshot_batch_id` is a non-null normalized reference to
  the durable batch that last contained the model.
- Present and reappeared projection transitions adopt the causal batch as the
  snapshot batch. Missing transitions retain the previous snapshot batch while
  their event `batch_id` records the omission batch.
- `model_observation_events` persists both non-null `batch_id` and
  `snapshot_batch_id`; both use `ON DELETE RESTRICT` foreign keys.
- Event payloads contain bounded scalar post-state and no `raw_snapshot`.
- Equal-watermark rollback restores `snapshot_batch_id` from historical event
  state. Raw evidence is recovered by joining the event snapshot reference to
  `model_classification_batches.raw_snapshot`.
- Batches reject both UPDATE and DELETE via the append-only trigger; insertion
  remains permitted.
- Semantic convergence checks resolve every compared `snapshot_batch_id` to
  its provenance digest and exact evidence instead of blanking identifiers.
  Coverage includes opposite fresh-account equal-watermark orders, canonical
  microseconds, chained replacements, an empty winner, and admin metadata.
  Present rows reference the winning containing batch; missing rows preserve
  the last batch that contained the model.
- The structural capacity test persists 1,000 models with an approximately
  2 MiB snapshot and asserts one complete batch copy, 1,000 projections and
  bounded metadata events, no projection raw column, no event snapshot copy,
  and fully resolving causal/evidence references. It uses no timing or physical
  database-size threshold.

## Owner-Approved Exact Identity Decision

- Migration 200 is unshipped, so its uniqueness representation may be corrected
  directly without a compatibility bridge.
- Exact unbounded model IDs are authoritative; digest values are collision-
  tolerant implementation buckets only.
- Transaction-scoped advisory locking serializes equal complete identities
  before exact duplicate checks. Mutable identity/dimension columns are covered
  on UPDATE as well as INSERT.
- This tradeoff avoids PostgreSQL btree tuple-size failures without imposing an
  artificial identifier length or treating a digest collision as equality.

## Final Verification

```bash
go test ./migrations -run ModelGovernanceFoundation -count=1
go test -tags=integration ./internal/repository \
  -run 'TestMigrationsRunner_ModelGovernanceFoundation' -count=1
go test -tags=integration ./internal/repository \
  -run 'TestModelGovernanceRepository_|TestModelGovernanceInventory' -count=1
go test ./internal/service \
  -run 'PersistUpstreamModelDiscovery|PersistDiscoveredModels' -count=1
go test ./internal/repository -run 'UpdateModelDiscovery' -count=1
git diff --check
```

Result: PASS. `git diff --check` produced no output.

Post-review PostgreSQL compatibility gate:

```bash
go test -tags=integration ./internal/repository \
  -run 'TestMigrationsRunner_ModelGovernanceFoundation|TestModelGovernanceRepository_|TestModelGovernanceInventory' \
  -count=1

SUB2API_TEST_POSTGRES_IMAGE=postgres:14-alpine \
go test -tags=integration ./internal/repository \
  -run 'TestMigrationsRunner_ModelGovernanceFoundation|TestModelGovernanceRepository_|TestModelGovernanceInventory' \
  -count=1
```

Result: PASS on both the default disposable PostgreSQL image and PostgreSQL 14.
The PostgreSQL 14 run includes over-3-KiB exact identities, duplicate INSERT and
identity UPDATE SQLSTATE `23505`, concurrent duplicate serialization, semantic
snapshot ownership, 1,000-model/approximately-2-MiB structural capacity, and
production `accountRepository.Delete` retention coverage.

## Additional Files for P2-R2-004 through P2-R2-006

- `backend/migrations/200_model_governance_foundation.sql`
- `backend/migrations/model_governance_foundation_migration_test.go`
- `backend/internal/repository/migrations_schema_integration_test.go`
- `backend/internal/repository/model_governance_repo.go`
- `backend/internal/repository/model_governance_repo_integration_test.go`
- `backend/internal/repository/model_governance_inventory_repo_integration_test.go`
- `backend/internal/service/upstream_models_test.go`
- `docs/model-governance-phase-2-second-review-fix-report.md` (ignored)

No commit was created.

## Final Technical Audit Findings

Date: 2026-08-20

### A: Simple-Mode Ungrouped Dimensions

RED:

```bash
go test ./internal/service \
  -run 'Test(ModelGovernanceInventoryUsesOneUTCWindowPerRequest|ProjectInventoryRuntimeDimensionsUsesConfiguredRunModeForUngroupedAccounts)' \
  -count=1
```

The tests initially failed to compile because inventory had no run-mode input
and candidate metadata could not distinguish a truly ungrouped account from a
bound account considered by simple-mode scheduling.

GREEN:

- `NewModelGovernanceInventoryService` now requires `*config.Config`; Wire and
  direct callers pass it explicitly. No compatibility overload was added.
- The service normalizes and captures `RunMode` in its projector closure.
- Repository candidate loading emits every active/schedulable account's
  ungrouped native/Anthropic/Gemini possibilities plus a durable
  `account_has_group_bindings` fact from the same repeatable-read snapshot.
- Go retains those rows for bound accounts only in simple mode. Standard mode
  preserves group-bound semantics, while truly ungrouped accounts remain
  inventoried.
- Stable native/mixed platform eligibility remains the final filter.

Repository integration coverage proves a group-bound mixed Antigravity account
gets ungrouped native, Anthropic, and Gemini dimensions in simple mode but no
fabricated ungrouped dimension in standard mode.

### B: Composite Model-Domain Reachability

RED:

```bash
go test ./internal/service \
  -run 'TestProjectInventoryRuntimeDimensions(RestrictsCompositeMappingsToRouteReachability|CompositeRoutePrecedenceAndMixedEligibility|DoesNotInventDynamicPrefixModelForAllowAllAccount)' \
  -count=1
```

The first run failed to compile because candidates carried only target platform.
After the metadata seam existed, prefix tests failed because only route-public
witnesses were considered. The dynamic allow-all regression then failed by
fabricating the prefix itself as a model ID.

GREEN:

- Composite candidates carry route ID, public model, match type, target
  platform, configured upstream, endpoint, and priority.
- Projection builds a finite request domain from exact routes and structural
  intersections between route prefixes and concrete/trailing-wildcard account
  mapping keys. There is no arbitrary witness cap and no control-character
  suffix construction.
- Every witness is checked with `matchCompositeRoute`, including exact versus
  prefix, endpoint-specific versus `any`, longest prefix, priority, and ID
  precedence. A shadowed route contributes no account target.
- Prefix witness construction treats exact route blockers as individual
  strings rather than whole branches, so a prefix route can still prove a
  reachable mapped extension after any finite set of exact blockers.
- Scheduler support reuses account support/Antigravity/Bedrock behavior, and
  forwarding reuses `resolveAccountUpstreamModel` or the existing OpenAI and
  Bedrock forwarding helpers.
- Multiple reachable routes union their actual account-forwarded targets into
  one account/group/channel/target projection. The complete account catalog is
  no longer attached to each composite target.
- SQL still independently contributes exact pass-through and configured fixed
  composite upstream models under the existing exact/prefix finite rules.
- An allow-all account behind a dynamic prefix pass-through route contributes
  no invented model ID unless a concrete finite account mapping intersection
  exists.

Integration coverage proves disjoint `x-*` routes exclude a `y` account target,
while matching exact and prefix domains include the actual account-forwarded
targets.

### C: Account Incarnation ID Enforcement

RED:

```bash
go test ./migrations -run TestModelGovernanceFoundationMigrationContract -count=1
```

The static contract failed because migration 200 documented permanent account
incarnation IDs but had no enforcing trigger.

GREEN:

- Migration 200 defines a rerun-safe `BEFORE INSERT ON accounts` trigger.
- If `NEW.id` exists in durable `model_classification_batches.account_id`, the
  insert fails with SQLSTATE `23505`, constraint
  `account_id_incarnation_not_reused`, and a clear permanent-incarnation
  message.
- INSERT-only scope leaves updates untouched. Restoring an explicit ID before
  it has durable history remains valid. Normal BIGSERIAL allocation remains
  valid.
- Live integration coverage hard-deletes an account after durable evidence,
  rejects repeated/concurrent explicit-ID import attempts, checks SQLSTATE and
  constraint name, permits pre-history restore and update, and verifies a fresh
  sequence-generated account.
- Existing direct migration SQL rerun coverage executes migration 200 twice on
  an already migrated schema.

Owner decision is now enforced: account IDs identify incarnations, not reusable
slots.

### D: Malformed Persisted Target Defense

No live malformed-row insertion test was added. The integration harness shares
one disposable database across tests, so dropping or disabling the global
`chk_usage_logs_governance_target_platform` constraint would expose concurrent
tests to invalid schema state and destabilize the suite. PostgreSQL cannot use
`NOT VALID` to bypass the check for a new malformed row.

The primary defense remains the live migration check constraint, covered by
schema and direct-writer integration tests. The inventory fallback remains
locked by
`TestModelGovernanceInventoryQueryDefensivelyAllowsOnlyConcretePersistedPlatforms`,
which asserts the persisted value is trusted only through the exact concrete
allowlist. This is the safe documented boundary requested for a shared harness.

### Final Verification

```bash
go generate ./cmd/server

go test ./internal/service \
  -run 'Test(ModelGovernanceInventoryUsesOneUTCWindowPerRequest|ProjectInventoryRuntimeDimensions|ProjectInventoryAccountMappings|StableRuntimePlatformEligibility)' \
  -count=1

go test ./migrations -run ModelGovernanceFoundation -count=1
go test ./internal/repository \
  -run TestModelGovernanceInventoryQueryDefensivelyAllowsOnlyConcretePersistedPlatforms \
  -count=1
go test ./internal/server/routes -run ModelGovernance -count=1

go test ./internal/service -count=1

go test -tags=integration ./internal/repository \
  -run 'TestMigrationsRunner_ModelGovernanceFoundation|TestModelGovernanceInventory' \
  -count=1

SUB2API_TEST_POSTGRES_IMAGE=postgres:14-alpine \
go test -tags=integration ./internal/repository \
  -run 'TestMigrationsRunner_ModelGovernanceFoundation|TestModelGovernanceInventory' \
  -count=1

go build ./...
git diff --check
```

Result: PASS. Full service completed in 96.575 seconds. The default disposable
PostgreSQL matrix completed in 6.751 seconds and PostgreSQL 14 in 6.338 seconds.
Wire generation, focused migration/repository/routes tests, full build, and
`git diff --check` all passed. No commit was created.

## Final Routing Review Blockers

Date: 2026-08-20

### Baseline Scheduler and Channel Restriction

RED:

```bash
go test ./internal/service \
  -run 'TestIsUpstreamModelRestrictedByChannel_OpenAIPassthroughKeepsBaselineSchedulerSemantics|TestOpenAIGatewayService_SelectAccountWithScheduler_PassthroughSelectionRemainsBaseline' \
  -count=1
```

Result: FAIL before the production correction. The scheduler helper returned
the endpoint-aware passthrough model instead of the pre-overlay sequence from
`fb280639`.

GREEN:

- `resolveOpenAIAccountUpstreamModelForRequest` again runs
  `resolveOpenAIForwardModel`, then applies compact mapping to that result only.
- Passthrough forwarding continues to use
  `resolveOpenAIForwardModelForEndpoint`; the regression proves forwarding can
  select `compact-upstream` while scheduler/channel restriction evaluates the
  baseline `normal-upstream` and preserves account selection.
- The stale tests that made scheduler eligibility endpoint-aware were removed
  or rewritten. Generic platform eligibility also matches `fb280639` exactly.
- Inventory still projects endpoint-aware forwarding, but only inside the
  finite model domain supported by baseline scheduler selection.

### Governance Evidence and Billing Isolation

RED:

```bash
go test ./internal/service \
  -run TestOpenAIGatewayServiceRecordUsage_InvalidForcedTargetFallsBackToNativePlatform \
  -count=1

go test -tags=unit ./internal/service \
  -run TestGatewayServiceRecordUsage_PersistsForcedTargetFromRequestContext \
  -count=1
```

Result: FAIL before correction because billing derived its platform from
`UsageLog.GovernanceTargetPlatform`.

GREEN:

- Generic billing again uses input `QuotaPlatform`, then
  `PlatformFromAPIKey`, then the historical composite-to-account fallback.
- OpenAI billing again uses input `QuotaPlatform`, then
  `PlatformFromAPIKey`, with no new composite fallback.
- Governance target validation and persistence remain independent evidence.
  Conflicting or invalid routing evidence can change the persisted concrete
  field but cannot change the quota platform supplied to billing, quota
  deduction, rate-limit accounting, or user-platform quota accounting.

### Stored Platform Allowlist

RED:

```bash
go test -tags=integration ./internal/repository \
  -run 'TestMigrationsRunner_ModelGovernanceFoundationExactChecks|TestUsageLogRepoSuite/TestCreateRejectsInvalidGovernanceTargetPlatform' \
  -count=1
```

Result: FAIL before correction: migration 200 had no check constraint and the
Ent repository accepted `composite`.

GREEN:

- Migration 200 adds nullable
  `chk_usage_logs_governance_target_platform` with the exact concrete list:
  `anthropic`, `openai`, `gemini`, `antigravity`, and `grok`.
- The Ent schema has the same local switch validator; `go generate ./ent` was
  run and generated builders enforce it.
- Integration coverage rejects invalid values through both the repository and
  direct SQL writer paths.
- Inventory SQL trusts the stored field only when it is in the same concrete
  allowlist, otherwise applying the legacy group/account fallback. A static
  query regression locks this defensive CASE behavior for malformed pre-check
  or bypassed rows.

### Verification

```bash
go generate ./ent

go test ./migrations -run ModelGovernanceFoundation -count=1
go test ./internal/repository \
  -run TestModelGovernanceInventoryQueryDefensivelyAllowsOnlyConcretePersistedPlatforms \
  -count=1

go test -tags=integration ./internal/repository \
  -run 'TestMigrationsRunner_ModelGovernanceFoundation|TestUsageLogRepoSuite/TestCreate(Persists|Rejects)GovernanceTargetPlatform|TestModelGovernanceInventory' \
  -count=1

SUB2API_TEST_POSTGRES_IMAGE=postgres:14-alpine \
go test -tags=integration ./internal/repository \
  -run 'TestMigrationsRunner_ModelGovernanceFoundation|TestUsageLogRepoSuite/TestCreate(Persists|Rejects)GovernanceTargetPlatform|TestModelGovernanceInventory' \
  -count=1

go test -tags=unit ./internal/service \
  -run 'TestGatewayServiceRecordUsage_PersistsForcedTargetFromRequestContext|TestResolveGenericUsageQuotaPlatform_PreservesBaselineFallbacks' \
  -count=1

go test ./internal/service -count=1
git diff --check
```

Result: PASS. The default and PostgreSQL 14 repository matrices passed. The
final full default service package passed in 96.701 seconds. One earlier broad
repository attempt was interrupted before test execution by a transient Docker
Desktop testcontainer readiness timeout; an immediate clean rerun passed in
5.073 seconds, and PostgreSQL 14 passed in 4.302 seconds. `git diff --check`
produced no output.

No commit was created.

## P2-R2-002: OpenAI Passthrough Runtime Resolution

### RED

Shared runtime seam:

```bash
go test ./internal/service \
  -run TestResolveOpenAIForwardModelForEndpoint -count=1
```

Result: FAIL as expected with `undefined:
resolveOpenAIForwardModelForEndpoint`. Forwarding had no reusable endpoint-aware
model-resolution seam for projection to call.

Passthrough projection:

```bash
go test ./internal/service \
  -run TestProjectInventoryAccountMappingsMatchesOpenAIPassthroughReachability \
  -count=1
```

Result: FAIL as expected. For the required conflicting configuration, inventory
returned only `normal-upstream`; runtime witnesses returned `public` for ordinary
passthrough and `compact-upstream` for compact passthrough.

Scheduling/channel-restriction runtime consumer:

```bash
go test ./internal/service \
  -run TestResolveOpenAIAccountUpstreamModelForRequestUsesEndpointResolution \
  -count=1
```

Result: FAIL as expected. The resolver returned `normal-upstream` for ordinary
passthrough instead of the actual forwarded model `public`.

Finite wildcard precedence review:

```bash
go test ./internal/service \
  -run TestProjectInventoryAccountMappingsFindsReachableWildcardTargetBeyondFixedWitnessSet \
  -count=1
```

Result: FAIL as expected after the adversarial fixture was constrained to the
actual wildcard request domain. A fixed 256-probe implementation omitted a
reachable fallback compact target when 256 longer-precedence rules occupied
those witnesses. The final implementation derives the finite witness bound from
the number of configured normal and compact rules, guaranteeing an unoccupied
one-rune suffix when the fallback rule is reachable.

The first forwarding harness run was invalid because a minimal API-key fixture
had no gateway URL-validation config and panicked before forwarding. The test
was corrected to the established OAuth recorder path before interpreting its
behavioral result. The corrected forwarding characterization passed before the
refactor, proving the existing ordinary/compact request behavior.

### GREEN

- `resolveOpenAIForwardModelForEndpoint` is the shared endpoint-aware seam used
  by `/responses` forwarding, passthrough forwarding, scheduling/channel
  restriction checks, and inventory projection.
- Non-passthrough normal resolution applies normal mapping and then OpenAI/OAuth
  normalization.
- Non-passthrough compact resolution applies normal mapping and compact mapping
  in sequence; normalization runs only when compact mapping leaves the model
  unchanged.
- Passthrough normal resolution preserves the original requested model exactly.
- Passthrough compact resolution applies compact mapping directly to the
  original request and otherwise preserves it, without ordinary OAuth model
  normalization.
- Projection branches through `Account.IsOpenAIPassthroughEnabled()`, covering
  both `openai_passthrough` and legacy `openai_oauth_passthrough` flags.
- Ordinary passthrough projects only concrete exact configured request keys; it
  does not project wildcard request namespaces, allow-all catalogs, or normal
  mapping target values.
- Compact passthrough evaluates exact request witnesses and finite wildcard
  intersections through the shared seam. Concrete reachable wildcard targets
  are retained while `*`, wildcard targets, and precedence losers are excluded.
- The required example projects `public` and `compact-upstream` and excludes
  `normal-upstream`.
- The actual forwarding test captures both ordinary and compact upstream request
  bodies for the conflicting passthrough configuration.

Focused and neighboring verification:

```bash
go test ./internal/service \
  -run 'TestResolveOpenAIForwardModelForEndpoint|TestProjectInventoryAccountMappingsMatchesOpenAIPassthroughReachability|TestProjectInventoryAccountMappingsMatchesSequentialOpenAICompactReachability|TestProjectInventoryAccountMappingsFindsReachableWildcardTargetBeyondFixedWitnessSet|TestOpenAIGatewayService_PassthroughConflictingMappingsUseOriginalRequestDomain' \
  -count=1

go test ./internal/service \
  -run 'OpenAI.*(ModelMapping|Compact|Passthrough)|ResolveOpenAIForwardModel|ProjectInventoryAccountMappings' \
  -count=1

go test ./internal/service \
  -run 'TestResolveOpenAIAccountUpstreamModelForRequestUsesEndpointResolution|OpenAI.*(Scheduler|Scheduling|ChannelRestriction|Compact)' \
  -count=1

go test ./internal/service -count=1
git diff --check
```

Result: PASS. The final full service package completed in 97.258 seconds in the
recorded verification run, and `git diff --check` produced no output.

### P2-R2-002 Files

- `backend/internal/service/openai_model_mapping.go`
- `backend/internal/service/openai_model_mapping_test.go`
- `backend/internal/service/openai_gateway_forward.go`
- `backend/internal/service/openai_gateway_passthrough.go`
- `backend/internal/service/openai_gateway_scheduling.go`
- `backend/internal/service/openai_compact_model_mapping_test.go`
- `backend/internal/service/model_governance_inventory.go`
- `backend/internal/service/model_governance_inventory_projection_test.go`
- `docs/model-governance-phase-2-second-review-fix-report.md` (ignored)

No acceptance report, review source, plan, migration, or repository SQL file was
edited for P2-R2-002. No commit was created.

## P2-R2-002 Independent Review Follow-up

### RED

Bounded witness API and adversarial precedence:

```bash
go test ./internal/service \
  -run TestProjectInventoryAccountMappingsFindsReachableWildcardTargetBeyondFixedWitnessSet \
  -count=1
```

Result: first failed to compile with `undefined:
openAIPassthroughCompactWitnesses`, proving the projection exposed no bounded,
inspectable witness construction seam. After the first structural version, the
operation-bound variant similarly failed to compile with `undefined:
openAIPassthroughCompactWitnessesWithComparisons`.

Malformed rule safety:

```bash
go test ./internal/service \
  -run TestOpenAIPassthroughCompactWitnessesRejectMalformedRules -count=1
```

Result: FAIL as expected. A reachable malformed rule produced the control-
character witness `"bad\t"`.

Exact blocker branch construction:

```bash
go test ./internal/service \
  -run TestOpenAIPassthroughCompactWitnessesExactBlockerDoesNotReserveBranch \
  -count=1
```

Result: FAIL as expected after both the wildcard base and first suffix were
occupied by exact rules. The first implementation moved to `gpt-\"`, treating
an exact blocker as if it reserved the entire `!` branch, instead of producing
the deterministic structural witness `gpt-!!`.

### GREEN

- Normal and compact mappings are parsed once into printable exact rules and
  trailing-`*` prefix rules. Compact targets must be non-empty, printable, and
  concrete.
- Exact rules precede wildcard rules. Wildcard rules are sorted by descending
  prefix length with lexical tie-breaking, matching runtime exact/longest
  precedence.
- Normal exact request keys are classified once against compact precedence.
- Compact exact rules are checked once against the normal support domain.
- Compact wildcard rules intersect only with allow-all or normal prefix
  domains. Higher wildcard prefixes are precompiled into a prefix set per rule;
  exact blockers occupy only exact strings.
- Witnesses use printable deterministic suffixes only. No NUL, tab, newline, or
  other control/non-printable rune can be emitted.
- At most one witness is emitted per reachable compact rule, and every unique
  witness is resolved through `resolveOpenAIForwardModelForEndpoint` once by
  projection.
- The adversarial fixture contains more than 300 compact rules and asserts the
  fallback winner, first and last longest-prefix winners, exact winner,
  exclusions, printability, witness-count bound, and a deterministic quadratic
  comparison bound rather than wall-clock timing.
- Actual forwarding tests independently assert API-key/new-flag and
  OAuth/legacy-flag request bodies, ordinary original models, compact rewritten
  models, OAuth compact exact non-normalization, and exact/longest wildcard
  precedence.
- Real `ChannelService` restriction tests observe ordinary and compact
  passthrough upstream models. Scheduler coverage rejects compact-disabled
  passthrough accounts, while projection emits only the reachable ordinary exact
  request model.
- Malformed mappings and malformed/non-boolean passthrough data remain safe and
  non-enumerating through existing typed account accessors plus parser tests.

Complexity, with `N` normal rules and `M` compact rules:

- Parse and sort: `O((N + M) log(N + M))`.
- Exact classification and compact-rule reachability: `O((N + M)^2)` worst case.
- Witness storage and runtime resolution calls: `O(M)`.
- Total: `O((N + M)^2 + (N + M) log(N + M))`, excluding the linear byte length
  needed to inspect configured strings. No per-pair nested full-rule probe loop
  remains.

Independent-review verification commands:

```bash
go test ./internal/service \
  -run 'TestProjectInventoryAccountMappingsFindsReachableWildcardTargetBeyondFixedWitnessSet|TestOpenAIPassthroughCompactWitnessesRejectMalformedRules|TestOpenAIPassthroughCompactWitnessesExactBlockerDoesNotReserveBranch|TestProjectInventoryAccountMappingsPassthroughCompactDisabled|TestOpenAIGatewayService_(APIKeyPassthroughConflictingMappingsUseOriginalRequestDomain|OAuthLegacyPassthroughCompactExactIsNotNormalized|PassthroughCompactWildcardPrecedenceInForwardedBody)|TestIsUpstreamModelRestrictedByChannel_(OpenAIPassthroughUsesEndpointModel|CompactDisabledAccountUsesOrdinaryModel)|TestOpenAIGatewayService_SelectAccountWithScheduler_RejectsCompactDisabledPassthrough' \
  -count=1

go test ./internal/service \
  -run 'OpenAI.*(ModelMapping|Compact|Passthrough|Scheduler|Scheduling|ChannelRestriction)|ResolveOpenAIForwardModel|ProjectInventoryAccountMappings|IsUpstreamModelRestrictedByChannel' \
  -count=1

go test ./internal/service -count=1
git diff --check
```

Final result: PASS. The focused independent-review suite completed in 1.868
seconds, neighboring OpenAI/restriction coverage in 4.406 seconds, and the full
service package in 96.700 seconds. `git diff --check` produced no output.

No commit was created.

## P2-R2-002 Unconstrained Passthrough Domain Follow-up

This follow-up supersedes the constrained-domain projection and total-complexity
statements in the preceding independent-review section. Historical RED/GREEN
evidence above is retained unchanged.

### RED

Required unconstrained projection:

```bash
go test ./internal/service \
  -run TestProjectInventoryAccountMappingsPassthroughDomainIgnoresNormalMappingRestriction \
  -count=1
```

Result: FAIL as expected. With ordinary `public`, compact exact `other`, and
compact wildcard `gpt-*`, projection returned only `public` instead of
`compact-gpt`, `compact-other`, `other`, and `public`. This proved projection
incorrectly treated `model_mapping` as a passthrough request-domain restriction.

UTF-8 validation:

```bash
go test ./internal/service \
  -run TestOpenAIProjectionPrintableRejectsInvalidUTF8 -count=1
```

Result: FAIL as expected because `openAIProjectionPrintable` accepted a string
containing invalid UTF-8.

Scheduler consumer:

```bash
go test ./internal/service \
  -run TestOpenAIGatewayService_SelectAccountWithScheduler_PassthroughCompactOutsideNormalMappingDomain \
  -count=1
```

Result: FAIL as expected with `no available account`. Scheduler eligibility
still rejected compact request `other` because it was absent from the ordinary
mapping, even though passthrough runtime accepts the unconstrained request.

### GREEN

- OpenAI passthrough projection always models an unconstrained runtime request
  domain, regardless of whether `model_mapping` is populated.
- Ordinary finite projection includes printable concrete exact request keys
  from both normal and compact mappings. It excludes wildcard keys and mapping
  target values.
- Every compact target with a structurally reachable exact or longest-prefix
  rule domain is projected. An exact winner for one request does not make a
  broader wildcard target unreachable for all other requests in that prefix
  domain.
- The required ordinary `public`, compact exact `other`, compact wildcard
  `gpt-*` configuration projects `compact-gpt`, `compact-other`, `other`, and
  `public`, and excludes `normal-upstream`.
- Compact eligibility covers unknown, supported, unsupported, force-on, and
  force-off account states. Passthrough scheduling no longer applies ordinary
  `model_mapping` support filtering.
- `openAIProjectionPrintable` requires `utf8.ValidString` before rune-level
  printable/control checks.
- The real channel-restriction consumer covers compact exact and longest-prefix
  precedence. Scheduler coverage proves compact exact requests outside the
  ordinary mapping domain remain eligible.
- Witness allocation and count remain `O(M)` and at most the number of compact
  rules. Witness construction does not expand every normal rule against every
  compact witness.

Complexity, with `M` compact rules (and configured-string byte inspection stated
separately):

- Parse/sort and bounded witness construction remain within the existing
  quadratic comparison bound; witness storage is `O(M)`.
- Projection calls `resolveOpenAIForwardModelForEndpoint` once per witness.
  Runtime mapping resolution may sort `M` compact rules per call, costing
  `O(M log M)` per witness.
- Honest end-to-end worst-case compact projection cost is therefore
  `O(M^2 log M)`, plus configured-string byte inspection. The test-only
  instrumentation is named
  `openAIPassthroughCompactWitnessesWithConstructionComparisons` because it
  measures witness construction only, not resolver cost.

Final verification:

```bash
go test ./internal/service \
  -run 'Test(ProjectInventoryAccountMappings|OpenAIProjectionPrintable|OpenAIGatewayService_SelectAccountWithScheduler).*' \
  -count=1

go test -tags=unit ./internal/service \
  -run 'TestIsUpstreamModelRestrictedByChannel_(OpenAIPassthroughUsesEndpointModel|CompactDisabledAccountUsesOrdinaryModel|PassthroughCompactExactAndLongestPrecedence)' \
  -count=1

go test ./internal/service -count=1
git diff --check
```

Result: PASS. The focused default-tag projection, UTF-8, and scheduler suite
completed in 2.085 seconds; the focused unit-tag channel-restriction suite
completed in 1.372 seconds. The final complete default-tag service package
completed in 96.798 seconds. `git diff --check` produced no output.

An additional complete `go test -tags=unit ./internal/service -count=1` run was
attempted. It compiled and executed the new channel-restriction tests but the
package failed after 143.739 seconds at the unrelated existing
`TestSettingService_LoadForwardedClientIPSettingsWriteFailureUsesComputedMode/
compatibility_migration_remains_effective` assertion (`expected true`, `actual
false`). The focused unit-tag restriction command above passes and isolates the
task-owned consumer coverage.

No acceptance report, review source, plan, migration, repository implementation,
or SQL file was edited for this follow-up. No commit was created.

## P2-R2-001: Runtime Target Dimensions

### RED

Shared stable eligibility:

```bash
go test ./internal/service \
  -run TestStableRuntimePlatformEligibilityMatchesSchedulers -count=1
```

Result: FAIL as expected with `undefined:
IsStableRuntimePlatformEligible`. Mixed eligibility existed only inside scheduler
consumers and was not reusable by inventory projection.

Pure dimension projection:

```bash
go test ./internal/service \
  -run TestProjectInventoryRuntimeDimensionsApprovesOnlyStableRuntimePaths \
  -count=1
```

Result: FAIL as expected because the inventory projector accepted only accounts
and returned only account model IDs; it had no candidate/approved runtime
dimension payload.

Repository behavior:

```bash
go test -tags=integration ./internal/repository \
  -run 'TestModelGovernanceInventory(ProjectsMixedAndForcedRuntimeTargetDimensions|ExcludesUnsupportedCurrentGroupDimensions|CompositeTargetsUseNativeAndMixedEligibility)' \
  -count=1
```

Result: FAIL as expected. Anthropic mixed channel targets were absent, an OpenAI
account bound to an Anthropic group contributed impossible account and
observation rows, and a composite group emitted only Antigravity-native channel
and route artifacts.

Explicit force identity:

```bash
go test -tags=integration ./internal/repository \
  -run TestModelGovernanceInventoryProjectsMixedAndForcedRuntimeTargetDimensions \
  -count=1
```

Result: first failed to compile because `InventoryItem` had no
`TargetPlatform`. Carrying target platform only inside SQL could not represent
ordinary mixed and forced-native dimensions distinctly when all other dimension
fields and a model ID coincide.

### GREEN

- `IsStableRuntimePlatformEligible` is the pure configuration-only eligibility
  seam shared by inventory, `GatewayService`, and
  `GeminiMessagesCompatService`. Native target equality is always eligible;
  ordinary non-forced Anthropic/Gemini additionally permits mixed-enabled
  Antigravity; forced and unsupported mismatches are rejected. Cooldown, quota,
  overload, and other transient scheduler state are intentionally absent.
- The read-only repeatable-read transaction loads active accounts, active
  account/group/channel candidate bindings, and enabled finite composite route
  target platforms before the aggregate query. The interleaving test mutates
  account, group binding, channel binding, and route data between account and
  dimension reads and proves all reads use the original snapshot.
- SQL supplies candidate dimensions, not an eligibility matrix. Go approves
  them and returns one payload containing account model IDs plus
  account/group/channel/target dimensions to the single aggregate SQL
  statement.
- Ungrouped active/schedulable accounts remain native dimensions. Ordinary
  groups use `group.platform`. Composite groups use only enabled persisted route
  target platforms; exact pass-through and concrete prefix targets remain
  finite, while dynamic prefix pass-through remains excluded. The built-in
  detector fabricates no catalog.
- Mixed Antigravity channel mapping and pricing lookups use the Anthropic or
  Gemini runtime target. Mixed-disabled and OpenAI-to-Anthropic mismatches do not
  contribute current account mappings, channel capabilities, routes, or
  observations.
- Force-platform is a request-context artifact and has no dedicated persistent
  HTTP-route row. A bound Antigravity account is nevertheless a finite,
  persistent path: forced scheduling retains the requested group, requires the
  account-native target, and `channelLookupPlatform` uses forced Antigravity.
  Inventory therefore adds an Antigravity-native target dimension for every
  active bound group/channel containing an active/schedulable Antigravity
  account, separate from any ordinary mixed target dimension.
- `InventoryItem.TargetPlatform` makes ordinary mixed, composite target, and
  forced-native dimensions explicit. Classification follows target platform.
  Historical usage retains its recorded account/group/channel/model facts and
  is not filtered by current eligibility. Because usage has no separate target
  column, inventory follows the repository's established effective-platform
  rule: ordinary groups use their group target, composite groups and ungrouped
  rows use account native platform. Request-context `ForcePlatform` is not
  persisted, so historical forced versus ordinary requests cannot be separated
  when their stored dimensions otherwise coincide; current finite force
  dimensions remain explicit from persistent account/group/channel bindings.
- No routing, scheduler, channel, mapping, pricing, observation, or eligibility
  state is mutated.

### Verification

```bash
go test ./internal/service \
  -run 'ModelGovernance|ProjectInventory|StableRuntimePlatformEligibility|OpenAI.*(Scheduler|Scheduling)|GatewayService_selectAccountWithMixedScheduling|ForcePlatform' \
  -count=1

go test ./internal/handler/admin ./internal/server/middleware ./internal/server/routes \
  -run 'ModelGovernance|AccountHandlerSyncUpstreamModels' -count=1

go test ./internal/service -count=1

go test -tags=integration ./internal/repository \
  -run 'TestMigrationsRunner_ModelGovernanceFoundation|TestModelGovernanceRepository_|TestModelGovernanceInventory' \
  -count=1

SUB2API_TEST_POSTGRES_IMAGE=postgres:14-alpine \
go test -tags=integration ./internal/repository \
  -run 'TestMigrationsRunner_ModelGovernanceFoundation|TestModelGovernanceRepository_|TestModelGovernanceInventory' \
  -count=1
```

Result: PASS. The full default-tag service package completed in 99.870 seconds.
The default disposable PostgreSQL suite completed in 7.361 seconds and the
PostgreSQL 14 suite in 6.888 seconds.

### P2-R2-001 Files

- `backend/internal/service/account.go`
- `backend/internal/service/gateway_scheduling.go`
- `backend/internal/service/gemini_messages_compat_service.go`
- `backend/internal/service/model_governance_inventory.go`
- `backend/internal/service/model_governance_inventory_test.go`
- `backend/internal/service/model_governance_inventory_projection_test.go`
- `backend/internal/repository/model_governance_inventory_repo.go`
- `backend/internal/repository/model_governance_inventory_repo_integration_test.go`
- `docs/model-governance-phase-2-second-review-fix-report.md` (ignored)

No commit was created.

## P2-R2-002 Canonical Projection Key Follow-up

### RED

```bash
go test ./internal/service \
  -run TestProjectInventoryAccountMappingsPassthroughRejectsPaddedConfiguredKeys \
  -count=1
```

Result: FAIL as expected. Projection emitted padded normal and compact exact
identities, fabricated the trimmed compact exact identity `compact-padded`, and
fabricated the trimmed padded-wildcard prefix `padded-`. The runtime compact
resolver trims the requested model before lookup, so those configured keys are
unreachable and must not participate in finite projection.

### GREEN

- `compileOpenAIPassthroughProjectionRules` accepts a normal or compact key only
  when `key == strings.TrimSpace(key)` and the existing printable/UTF-8 checks
  pass.
- The guard applies before normal finite exact collection, compact finite exact
  collection, and compact exact/wildcard witness-rule construction.
- Runtime model-mapping behavior is unchanged; this is a projection compiler
  correction only.
- Regressions exclude padded normal exact identity, padded compact exact target
  and fabricated padded/original identities, and padded compact wildcard target
  and fabricated prefix identities.
- Canonical normal exact, compact exact, and compact wildcard rules retain their
  previous projected identities and targets.

Focused verification:

```bash
go test ./internal/service \
  -run TestProjectInventoryAccountMappingsPassthroughRejectsPaddedConfiguredKeys \
  -count=1

go test ./internal/service \
  -run 'TestProjectInventoryAccountMappingsPassthroughDomainIgnoresNormalMappingRestriction|TestProjectInventoryAccountMappingsPassthroughCompactEligibilityMatrix|TestOpenAIPassthroughCompactWitnessesRejectMalformedRules|TestProjectInventoryAccountMappingsFindsReachableWildcardTargetBeyondFixedWitnessSet' \
  -count=1
```

Result: PASS in 1.818 seconds and 2.766 seconds respectively.

Complexity, with `M` compact rules:

- Retained witness storage is `O(M)`: at most one witness is retained per
  reachable compact rule.
- Each witness resolution may allocate and sort temporary compact-rule data of
  `O(M)`. Across `O(M)` resolver calls, cumulative temporary allocations are
  `O(M^2)` even though peak retained witness storage remains `O(M)`.
- End-to-end worst-case time remains `O(M^2 log M)`, plus configured-string byte
  inspection.

Final default-tag service verification completed in 96.371 seconds and
`git diff --check` produced no output. No commit was created.

## P2-R2-001 Durable Effective Target Follow-up

### RED

Gateway propagation:

```bash
go test ./internal/service \
  -run 'TestGatewayServiceRecordUsage_(PersistsResolvedGovernanceTargetPlatform|PersistsForcedTargetFromRequestContext)|TestOpenAIGatewayServiceRecordUsage_PersistsResolvedGovernanceTargetPlatform' \
  -count=1
```

Result: FAIL as expected. Generic and OpenAI billing did not persist the
resolved request target. The context-only forced-platform case additionally
proved that consulting only the precomputed quota snapshot loses a valid
`ctxkey.ForcePlatform` override.

Repository and migration contracts:

```bash
go test ./internal/repository \
  -run 'TestBuildUsageLogBestEffortInsertQuery_IncludesGovernanceTargetPlatform|TestUsageLogEffectivePlatformExprPrefersPersistedGovernanceTarget' \
  -count=1

go test ./migrations -run ModelGovernanceFoundation -count=1
```

Result: FAIL as expected. Usage inserts had no typed target column, inventory
derived target platform only from mutable account/group state, and migration
200 did not include target platform in inventory identity.

### GREEN

- `usage_logs.governance_target_platform` is a nullable typed field. Generic
  and OpenAI gateway billing persist the effective target used by the request.
- Target resolution reuses the precomputed `QuotaPlatform` snapshot when one
  exists. Otherwise it resolves from live request context, preserving a
  context-only forced platform, and conservatively falls back to the account
  platform when no stronger evidence exists.
- Single, batch, and best-effort usage inserts, prepared arguments, selects,
  scans, and Ent-generated create/update/query support carry the field.
- Inventory aggregation prefers persisted request evidence. Legacy `NULL`
  rows use the documented group/account fallback; that fallback cannot exactly
  reconstruct historical forced or composite routing after mutable routing
  configuration changes.
- `model_inventory_items.target_platform VARCHAR(32) NOT NULL` is part of the
  hash-bucket uniqueness dimensions, advisory-lock material, exact trigger
  comparison, identity update columns, and immutable inventory identity.
- Integration coverage proves typed usage persistence/readback, historical
  target stability after group-platform changes, and distinct inventory rows
  for the same model under different targets.
- Admin handler coverage verifies a non-empty target survives the JSON response
  envelope.
- Non-gateway legacy usage writers intentionally leave the new field `NULL`.
  No compatibility reconstruction was added because migration 200 is unshipped.

Ent code was regenerated with:

```bash
go generate ./ent
```

Focused verification:

```bash
go test ./internal/service \
  -run 'TestGatewayServiceRecordUsage_(PersistsResolvedGovernanceTargetPlatform|PersistsForcedTargetFromRequestContext)|TestOpenAIGatewayServiceRecordUsage_PersistsResolvedGovernanceTargetPlatform' \
  -count=1
go test ./internal/repository -count=1
go test ./migrations -count=1
go test ./internal/handler/admin -count=1
go test ./internal/handler -count=1
go build ./...
git diff --check
```

Disposable PostgreSQL compatibility gates:

```bash
go test -tags=integration ./internal/repository \
  -run 'TestUsageLogRepoSuite/TestCreatePersistsGovernanceTargetPlatform|TestModelGovernanceInventoryUsesPersistedTargetWhenCurrentDimensionIsIneligible|TestMigrationsRunner_ModelGovernanceFoundation(Schema|InventoryUniquenessTreatsNullsAsEqual)$' \
  -count=1

SUB2API_TEST_POSTGRES_IMAGE=postgres:14-alpine \
go test -tags=integration ./internal/repository \
  -run 'TestUsageLogRepoSuite/TestCreatePersistsGovernanceTargetPlatform|TestModelGovernanceInventoryUsesPersistedTargetWhenCurrentDimensionIsIneligible|TestMigrationsRunner_ModelGovernanceFoundation(Schema|InventoryUniquenessTreatsNullsAsEqual)$' \
  -count=1
```

Result: PASS on the focused unit, repository, migration, handler, build, and
default/PostgreSQL 14 integration gates. `git diff --check` produced no output.

### Verification Boundary

- `go test -tags=unit ./internal/service` has one reproducible unrelated
  failure:
  `TestSettingService_LoadForwardedClientIPSettingsWriteFailureUsesComputedMode/compatibility_migration_remains_effective`
  expects `true` but receives `false` at
  `backend/internal/service/setting_service_update_test.go:812`. It reproduces
  in isolation, and no production file changed by this follow-up touches that
  path.
- Broad legacy usage integration selectors include existing fixtures that omit
  already-required currency or exchange-rate snapshots and fail with
  `invalid usage-log currency snapshot` or
  `incomplete usage-log exchange-rate snapshot`. The task-specific migration,
  inventory, and usage round-trip gates pass on both tested PostgreSQL versions.

### P2-R2-001 Durable Target Files

- `backend/ent/schema/usage_log.go`
- `backend/ent/migrate/schema.go`
- `backend/ent/mutation.go`
- `backend/ent/runtime/runtime.go`
- `backend/ent/usagelog.go`
- `backend/ent/usagelog/usagelog.go`
- `backend/ent/usagelog/where.go`
- `backend/ent/usagelog_create.go`
- `backend/ent/usagelog_update.go`
- `backend/internal/service/usage_log.go`
- `backend/internal/service/gateway_usage_billing.go`
- `backend/internal/service/openai_gateway_usage.go`
- `backend/internal/service/gateway_record_usage_test.go`
- `backend/internal/service/openai_gateway_record_usage_test.go`
- `backend/internal/repository/usage_log_repo.go`
- `backend/internal/repository/usage_log_repo_insert.go`
- `backend/internal/repository/usage_log_repo_query.go`
- `backend/internal/repository/usage_log_effective_platform_test.go`
- `backend/internal/repository/usage_log_repo_request_type_test.go`
- `backend/internal/repository/usage_log_repo_integration_test.go`
- `backend/internal/repository/model_governance_inventory_repo.go`
- `backend/internal/repository/model_governance_inventory_repo_integration_test.go`
- `backend/migrations/200_model_governance_foundation.sql`
- `backend/migrations/model_governance_foundation_migration_test.go`
- `backend/internal/repository/migrations_schema_integration_test.go`
- `backend/internal/handler/admin/model_governance_inventory_handler_test.go`
- `docs/model-governance-phase-2-second-review-fix-report.md` (ignored)

No acceptance report, second-review source, plan, or `.gitignore` file was
modified for this follow-up. No commit was created.

## Latest Review: Baseline Scheduler Domain, Isolation Safety, and Durable Target Validation

This section supersedes the earlier statement that OpenAI passthrough's
requested domain was unconstrained by normal mapping. Passthrough forwarding
still preserves the original requested model and uses the shared endpoint-aware
resolver, but scheduler eligibility retains the baseline
`account.IsModelSupported(requestedModel)` requirement. The forwarding behavior
never authorized an expansion of the scheduler's requested-model domain.

### RED

Passthrough scheduler and projection:

```bash
go test ./internal/service \
  -run 'TestOpenAIGatewayService_SelectAccountWithScheduler_RejectsPassthroughCompactOutsideNormalMappingDomain|TestProjectInventoryAccountMappings(PassthroughRespectsNormalMappingSupportDomain|PassthroughCompactEligibilityMatrix|MatchesOpenAIPassthroughReachability)|TestOpenAIPassthroughCompactWitnessesRejectMalformedRules' \
  -count=1
```

Result: FAIL as expected. The scheduler selected a passthrough account for
`other` even though its non-empty normal mapping supported only `public`.
Projection emitted `other`, compact exact targets, and compact wildcard targets
outside that normal support domain.

Durable target validation:

```bash
go test -tags=unit ./internal/service \
  -run 'TestResolvedUsageTargetPlatform_ValidatesRuntimeDecisionAgainstSelectedAccount' \
  -count=1
```

Result: FAIL as expected. An ordinary Anthropic group with a mixed-scheduling
Antigravity account persisted a conflicting Gemini resolved-context target.
Earlier RED coverage also proved arbitrary quota snapshots and mismatched force
targets could persist impossible account/target tuples, and Live usage omitted
its OpenAI target.

Exact-trigger isolation:

```bash
go test ./migrations -run TestModelGovernanceFoundationMigrationContract -count=1

go test -tags=integration ./internal/repository \
  -run 'TestMigrationsRunner_ModelGovernanceExactIdentityWritesRequireReadCommitted|TestMigrationsRunner_ModelGovernanceInventoryConcurrentNullableDuplicateAtReadCommitted|TestMigrationsRunner_ModelGovernanceRegistryAndAliasConcurrentDuplicatesAtReadCommitted' \
  -count=1
```

Result: the static contract and explicit Repeatable Read writes failed as
expected before the guards existed. READ COMMITTED concurrent inventory,
registry, and alias races provided the one-success/one-`23505` baseline.

### GREEN

- `isOpenAICompatibleAccountEligibleForRequest` once again applies
  `requestedModel != "" && !account.IsModelSupported(requestedModel)` to every
  account, including passthrough accounts.
- Passthrough forwarding is unchanged: ordinary requests preserve the original
  requested model, compact requests resolve compact mapping against that
  original request, and all forwarding/scheduling/channel consumers retain the
  shared endpoint-aware resolver.
- Finite passthrough projection derives its support domain only from normal
  exact/wildcard mapping keys. Empty normal mapping remains allow-all and
  finite projection remains conservative; with non-empty normal mapping,
  compact exact/wildcard rules outside the normal domain are unreachable.
- Compact-rule witness construction remains deterministic and worst-case
  quadratic. No cubic probe loop or fixed witness cap was introduced.
- `resolvedUsageTargetPlatform` accepts only the five concrete runtime
  platforms and validates every candidate with
  `IsStableRuntimePlatformEligible` against the selected account.
- Force context remains authoritative but uses forced/native eligibility.
  Invalid force or snapshot tuples fall back to the account's native platform.
- Resolved-target context is authoritative only for composite groups. Ordinary
  groups derive the durable target from their concrete group platform, so a
  conflicting resolved context cannot change the persisted target. Composite
  detached snapshots remain usable when request context is unavailable.
- Generic ordinary mixed Antigravity, forced Antigravity, composite mixed and
  native, invalid OpenAI force fallback, and detached handler-style snapshot
  flows are covered. OpenAI Live usage persists `PlatformOpenAI`.
- Every exact-identity write trigger rejects transaction isolation other than
  READ COMMITTED with SQLSTATE `0A000` before advisory locking or exact queries.
  Production governance writes use default READ COMMITTED. Inventory's
  Repeatable Read transaction is read-only and does not invoke write triggers.
- Explicit Repeatable Read rejection covers registry, alias, observation INSERT
  and identity UPDATE, and inventory. A read-only Repeatable Read transaction
  remains allowed.
- READ COMMITTED concurrency covers registry canonical IDs, aliases,
  observations, and inventory identities with nullable group/channel dimensions.

No routing expansion was retained. This follow-up restores the baseline
scheduler support predicate and makes inventory describe that existing runtime
domain; it does not alter passthrough request-body forwarding semantics.

### Final Evidence

```bash
go test ./internal/service \
  -run 'TestOpenAIGatewayService_SelectAccountWithScheduler_(RejectsCompactDisabledPassthrough|RejectsPassthroughCompactOutsideNormalMappingDomain)|TestProjectInventoryAccountMappings(PassthroughRespectsNormalMappingSupportDomain|PassthroughCompactEligibilityMatrix|MatchesOpenAIPassthroughReachability|PassthroughRejectsPaddedConfiguredKeys)|TestOpenAIPassthroughCompactWitnessesRejectMalformedRules|TestProjectInventoryAccountMappingsFindsReachableWildcardTargetBeyondFixedWitnessSet' \
  -count=1

go test -tags=unit ./internal/service \
  -run 'TestResolvedUsageTargetPlatform_ValidatesRuntimeDecisionAgainstSelectedAccount|TestGatewayServiceRecordUsage_(PersistsResolvedGovernanceTargetPlatform|PersistsForcedTargetFromRequestContext|PersistsHandlerResolvedTargetAfterContextDetach)' \
  -count=1

go test ./internal/service \
  -run 'TestOpenAIGatewayServiceRecordUsage_(PersistsResolvedGovernanceTargetPlatform|InvalidForcedTargetFallsBackToNativePlatform)|TestFinalizeLiveCallAggregatesTokensAndBillsOnce' \
  -count=1

go test ./internal/service -count=1
go test ./internal/repository -count=1
go test ./migrations -count=1
go test ./internal/handler -count=1
go test ./internal/handler/admin -count=1
go build ./...
```

Result: PASS. The full default service package completed in 97.981 seconds.

Default PostgreSQL:

```bash
go test -tags=integration ./internal/repository \
  -run 'TestMigrationsRunner_ModelGovernance(Foundation(Schema|InventoryUniquenessTreatsNullsAsEqual|UnboundedExactIdentityUniqueness)|ExactIdentityWritesRequireReadCommitted|InventoryConcurrentNullableDuplicateAtReadCommitted|RegistryAndAliasConcurrentDuplicatesAtReadCommitted)|TestUsageLogRepoSuite/TestCreatePersistsGovernanceTargetPlatform|TestModelGovernanceInventoryUsesPersistedTargetWhenCurrentDimensionIsIneligible' \
  -count=1
```

Result: PASS in 3.045 seconds on the final rerun.

PostgreSQL 14:

```bash
SUB2API_TEST_POSTGRES_IMAGE=postgres:14-alpine \
go test -tags=integration ./internal/repository \
  -run 'TestMigrationsRunner_ModelGovernance(Foundation(Schema|InventoryUniquenessTreatsNullsAsEqual|UnboundedExactIdentityUniqueness)|ExactIdentityWritesRequireReadCommitted|InventoryConcurrentNullableDuplicateAtReadCommitted|RegistryAndAliasConcurrentDuplicatesAtReadCommitted)|TestUsageLogRepoSuite/TestCreatePersistsGovernanceTargetPlatform|TestModelGovernanceInventoryUsesPersistedTargetWhenCurrentDimensionIsIneligible' \
  -count=1
```

Result: PASS in 3.284 seconds on the final rerun.

### Latest Review Files

- `backend/migrations/200_model_governance_foundation.sql`
- `backend/migrations/model_governance_foundation_migration_test.go`
- `backend/internal/repository/migrations_schema_integration_test.go`
- `backend/internal/service/openai_gateway_scheduling.go`
- `backend/internal/service/openai_account_scheduler_compact_test.go`
- `backend/internal/service/model_governance_inventory.go`
- `backend/internal/service/model_governance_inventory_projection_test.go`
- `backend/internal/service/gateway_usage_billing.go`
- `backend/internal/service/gateway_record_usage_test.go`
- `backend/internal/service/openai_gateway_record_usage_test.go`
- `backend/internal/service/openai_live.go`
- `backend/internal/service/openai_live_lifecycle_test.go`
- `docs/model-governance-phase-2-second-review-fix-report.md` (ignored)

Ent schema was not touched, so no regeneration was required. No acceptance
report, second-review source, harness documentation, plan, or `.gitignore` file
was modified for this follow-up. No commit was created.

## P2-R2-007: Isolated Usage Boundary Assertions

Strict test-only hardening was applied to the inventory repository integration
coverage. Production code was not changed.

### Isolated Boundaries

- The exact 30-day lower-bound row uses API key 1.
- The exact 7-day lower-bound row and immediately-before-end row use API key 2.
- The retained literal aggregate asserts two 7-day requests, three 30-day
  requests, `6,000,000` and `7,000,000` billing micros, one distinct 7-day API
  key, and two distinct 30-day API keys.
- Exact-end and after-end rows remain excluded, immediately-before-end remains
  included, and the future-only model remains absent.
- Future mixed currency now has its own fresh fixture. Its in-range USD row and
  excluded CNY row both have positive revenue `1`, so negative-source
  validation cannot mask mixed-currency leakage. The query returns no error and
  only the in-range request, revenue, currency, and API-key count.
- Future negative revenue now has its own fresh fixture and uses USD for both
  the in-range positive row and excluded negative row. The query returns no
  error and only the in-range request, revenue, currency, and API-key count.
- Future overflow remains isolated in a third fresh fixture and its future-only
  model remains absent without error.
- Subtests are sequential and use no `t.Parallel`, sleeps, or shared fixture.

### Mutation RED

The production `usage_rows` upper bound was temporarily replaced with a typed
but non-filtering `$3::timestamptz IS NOT NULL` predicate, preserving query
parameter validity while admitting exact-end and future rows. The focused suite
failed as intended:

- Half-open coverage exposed `future-only-model` and changed bounded-model
  requests/revenue from `2/3` and `6,000,000/7,000,000` to `4/5` and
  `30,000,000/31,000,000`.
- The positive future mixed-currency subtest returned
  `MODEL_GOVERNANCE_INVENTORY_MIXED_CURRENCY`.
- The same-currency future negative subtest returned
  `MODEL_GOVERNANCE_INVENTORY_NEGATIVE_REVENUE`.
- The future overflow subtest returned
  `MODEL_GOVERNANCE_INVENTORY_REVENUE_OVERFLOW`.

The production predicate was restored exactly to
`ul.created_at >= $2 AND ul.created_at < $3` before final verification.

### Final Evidence

```bash
go test -tags=integration ./internal/repository \
  -run 'TestModelGovernanceInventory(UsesHalfOpenUsageWindows|FutureRowsCannotAffectValidationOrAggregates)$' \
  -count=1

SUB2API_TEST_POSTGRES_IMAGE=postgres:14-alpine \
go test -tags=integration ./internal/repository \
  -run 'TestModelGovernanceInventory(UsesHalfOpenUsageWindows|FutureRowsCannotAffectValidationOrAggregates)$' \
  -count=1
```

Result: PASS. The final default disposable PostgreSQL run completed in 3.829
seconds; the final PostgreSQL 14 run completed in 3.259 seconds.

### P2-R2-007 Files

- `backend/internal/repository/model_governance_inventory_repo_integration_test.go`
- `docs/model-governance-phase-2-second-review-fix-report.md` (ignored)

No production code, acceptance report, second-review source, final report, or
`.gitignore` file was edited for P2-R2-007. No commit was created.

## P2-R2-001 Follow-up: Ungrouped Mixed Runtime Targets

### Root Cause

`listModelGovernanceInventoryDimensionCandidates` emitted only the account's
native target for ungrouped accounts and marked that candidate forced. A forced
candidate cannot represent ordinary mixed Antigravity routing, so an active,
schedulable, mixed-enabled ungrouped Antigravity account never reached the
existing stable eligibility check for Anthropic or Gemini.

### RED

Added `TestModelGovernanceInventoryProjectsUngroupedOrdinaryRuntimeTargets`.
Before the production change, the focused integration test failed with:

```text
mixed-enabled ungrouped account missing target "anthropic"
```

The test also fixes the required boundaries: mixed-enabled Antigravity emits
native, Anthropic, and Gemini identities; mixed-disabled Antigravity and OpenAI
remain native-only; every ungrouped identity has nil group/channel; and the
same account/model remains distinct by target platform. Real ungrouped usage
rows with persisted Anthropic and Gemini targets also assert isolated request,
revenue, currency, and affected-API-key aggregates under null dimensions.

### GREEN

Ungrouped SQL candidate generation now emits the account-native, Anthropic,
and Gemini targets as ordinary (`force_platform = FALSE`) candidates. The SQL
is intentionally broad: `ProjectInventoryRuntimeDimensions` remains the single
stable runtime eligibility gate and rejects unsupported targets.

The service projection test now also covers ordinary ungrouped native and
mixed candidates, including rejection of mixed targets for mixed-disabled
Antigravity and OpenAI accounts.

### Final Evidence

```bash
go test ./internal/service \
  -run TestProjectInventoryRuntimeDimensionsApprovesOnlyStableRuntimePaths \
  -count=1

go test -tags=integration ./internal/repository \
  -run TestModelGovernanceInventoryProjectsUngroupedOrdinaryRuntimeTargets \
  -count=1

go test ./internal/service -count=1
go test ./internal/repository -count=1
go test ./migrations -count=1
go test ./internal/handler ./internal/handler/admin -count=1
go build ./...

go test -tags=integration ./internal/repository \
  -run ModelGovernanceInventory -count=1

SUB2API_TEST_POSTGRES_IMAGE=postgres:14-alpine \
go test -tags=integration ./internal/repository \
  -run ModelGovernanceInventory -count=1
```

Result: PASS. The full service package completed in 97.041 seconds. The default
inventory integration suite completed in 4.194 seconds and the PostgreSQL 14
suite completed in 4.825 seconds on the final rerun after adding persisted
ungrouped-target usage attribution coverage.

### Follow-up Files

- `backend/internal/repository/model_governance_inventory_repo.go`
- `backend/internal/repository/model_governance_inventory_repo_integration_test.go`
- `backend/internal/service/model_governance_inventory_projection_test.go`
- `docs/model-governance-phase-2-second-review-fix-report.md` (ignored)

No acceptance report, second-review source, `.gitignore`, or generated Ent file
was edited for this follow-up. No commit was created.

## Third-Review RED/GREEN Resolution Evidence

Date: 2026-08-20

### RED

The third review identified eight remaining gaps in the mutable overlay:

- `P2-R3-001`: Anthropic OAuth/SetupToken inventory used mapping semantics that
  Messages forwarding ignored.
- `P2-R3-002`: stable request-shape and endpoint account capabilities were absent.
- `P2-R3-003`: Grok media mapping ran without prior endpoint/input normalization.
- `P2-R3-004`: grouped dimensions omitted reachable Antigravity thinking targets.
- `P2-R3-005`: row triggers did not reject statement-level TRUNCATE.
- `P2-R3-006`: detached cyber usage lost concrete target-platform evidence.
- `P2-R3-007`: governance evidence changed existing dashboard/stats attribution.
- `P2-R3-008`: BEFORE INSERT ledger claims could create ghost rows on conflict.

### GREEN

- Shared account-type-aware Anthropic resolution now matches Messages forwarding.
  API-key accounts map; other account types follow Claude normalization where
  runtime does. `count_tokens` intentionally uses its separate API-key-map versus
  non-API-key-normalize contract.
- Stable endpoint/account gates cover embeddings, OpenAI Images, and positive
  Grok media eligibility without transient cooldown/quota/overload gates. Every
  OpenAI Responses witness, including text, requires
  `OpenAIEndpointCapabilityResponses`.
- Grok image and video text/image-input outcomes normalize before support and
  mapping.
- Antigravity thinking is restricted to Claude-compatible endpoints and is
  consistent across account-only, grouped, mixed, forced, and composite paths.
- All four append-only tables reject direct, CASCADE, and multi-table TRUNCATE;
  rerun and PostgreSQL 14 coverage pass.
- Cyber handling snapshots `GovernanceTargetPlatform` before goroutine detach,
  performs no goroutine `gin.Context` access, and preserves billing/quota inputs.
- Option 1 restores dashboard/stats attribution to `fb280639`; governance target
  is inventory/evidence-only. Behavioral integration covers native, composite,
  forced-style, ungrouped, and legacy NULL fallback.
- Account incarnation claim is `AFTER INSERT`; account ID updates are immutable;
  DO NOTHING/UPDATE, rollback, concurrency, rerun, and PG14 tests create no ghost
  claims.

The usage-inclusive selector then exposed stale integration fixtures that lacked
mandatory currency snapshots and failed before intended assertions. An
integration-only repository adapter adds `USD/USD/1/identity/as-of` only when
the snapshot is completely absent. Explicit partial/invalid fixtures are not
completed, so production validation and rejection coverage are unchanged.

### Historical Fresh Gate

The timings in this subsection are retained as the coordinator checkpoint that
immediately followed the fixture remediation. They are historical and are
superseded for current-overlay status by the latest coordinator run below.

```bash
go test -tags=integration ./internal/repository \
  -run 'MigrationsRunner|ModelGovernanceRepository|ModelGovernanceInventory|UsageLog' \
  -count=1
```

Result: PASS in 15.094s. The formal PostgreSQL 14 integration selector passed in
13.360s. Focused config/migration/service/handler/routes/unit selectors passed;
`go test ./... -count=1` passed with service in 104.031s; `make generate`,
`make build`, and root diff/baseline checks passed. No production migration or
deploy occurred.

Documentation correction: the source no-diff baseline check covers only
`backend/internal/service/openai_gateway_service.go` and
`backend/internal/service/openai_gateway_scheduling.go`.
`backend/internal/service/gateway_forward.go` intentionally differs because
Messages forwarding delegates to shared `ResolveAnthropicFinalModel`; its parity
evidence is behavioral resolver/forwarding coverage, not a source no-diff check.
This intermediate status is superseded by the latest current-status update
below.

Round 1 integration refinement keeps `ProjectInventoryAccountMappings` as the
reference for finite provider reachability without directly approving its final
IDs. For true account-only dimensions, eligible compact-only mapping keys and
Bedrock default aliases are replayed as request witnesses through finite endpoint
compatibility, scheduler support, and account forwarding. This restores compact
and regional Bedrock inventory rows while preserving endpoint-specific Anthropic
resolution, Antigravity `count_tokens` rejection, and Gemini-target thinking
exclusion.

At that historical checkpoint, an independent scoped review of the then-current
post-fixture mutable worktree reported Critical 0, Important 0, Minor 0 and
returned **ACCEPTED**. It then considered all known final-review findings
addressed; the later `P2-R3-002` revalidation superseded that conclusion.

### Historical Latest Coordinator Current-Overlay Gate

The exact last coordinator run passed the focused config, migration, handlers,
routes, and unit selectors. The default usage-inclusive formal
integration selector passed in 13.956s, and the PostgreSQL 14 formal integration
selector passed in 12.367s. `go test ./... -count=1` passed with
`internal/service` completing in 102.197s. `make generate`, `make build`, and
`git diff --check` passed.

Round-2 remediation explicitly excludes `count_tokens` for a mixed Antigravity
account under an Anthropic target, in addition to the provider-specific
exclusions already recorded above. The fresh reviewer focused service, full
service, focused database, default formal integration, and PostgreSQL 14 formal
integration checks passed in 2.526s, 96.337s, 4.114s, 9.203s, and 8.005s;
diff and scheduler baseline checks passed. At that historical checkpoint this did
not replace the complete coordinator gate at 13.956s / 12.367s / service
102.197s. Both old records are superseded for current-overlay status by the gate
at the top of this document.

At that historical checkpoint, mutable-worktree technical status was
**ACCEPTED**, while final merge status remained **BLOCKED / CHANGES REQUIRED**
because the overlay was uncommitted, review documents were not tracked or were
ignored, and a committed-state full gate and independent review were still
required. The user-owned `.gitignore` change was excluded, and no final PASS was
claimed at that checkpoint.

The unrestricted `go test -tags=integration ./internal/repository -count=1` is
outside this gate and has unrelated failures in API-key `routing_mode` fixtures,
usage-billing wallet currency fixtures, and shared-row group counts. This report
does not characterize that unrestricted command as passing or those failures as
task regressions. No commit had been created at that checkpoint.

## Final Committed-State Acceptance Addendum

Historical final status: **FINAL PASS (superseded)**.

- Formal base: `cad5bc5606bfc059e2da91a54db9479f312b9dd2`.
- Accepted implementation HEAD: `557507a18ab31bcf410bc132fb2487cd189440b7`.
- Accepted implementation range:
  `cad5bc5606bfc059e2da91a54db9479f312b9dd2..557507a18ab31bcf410bc132fb2487cd189440b7`.
- The accepted commit contains the intended implementation, generated sources,
  migration, tests, and all five review documents. `.gitignore` was excluded and
  remains a user-owned uncommitted change.
- Committed coordinator gate: default usage-inclusive integration PASS 18.952s;
  PostgreSQL 14 PASS 17.489s; full suite PASS with `internal/service` 106.031s;
  focused suites, generation, build, diff, and generated-no-diff checks PASS.
- Independent committed-state review: Critical 0, Important 0, Minor 0; default
  integration PASS 17.162s; PostgreSQL 14 PASS 12.094s; full suite PASS with
  `internal/service` 105.369s; build, generation, and diff checks PASS; all five
  review documents tracked.
- No production operation was performed.

Historical reviewer verdict: **FINAL PASS (superseded)**. Current acceptance is
**CHANGES REQUIRED** because the newly validated `P2-R3-002` conflict lacked an
approved deviation. The strict-contract projection fix leaves runtime fallback
unchanged. Commit plus committed-state gate and independent review remain due.
