# Model Governance Phase 2 Third Review

Date: 2026-08-20

Status: **CHANGES REQUIRED**

The prior committed-state **FINAL PASS is superseded**. A newly validated
`P2-R3-002` contract violation showed that text-model Responses projection used
the Chat Completions capability and therefore included API-key accounts with
persisted `openai_responses_supported=false`. No approved plan deviation permits
that behavior. The strict contract governs inventory acceptance: every OpenAI
Responses projection requires `OpenAIEndpointCapabilityResponses`, independent
of request shape. Runtime text `/responses` fallback through Chat Completions is
intentionally unchanged. Current status remains **CHANGES REQUIRED** until this
fix and the five updated review documents are committed and the committed-state
gate and independent review are rerun. A fresh full gate passed only against the
current mutable-worktree overlay and is recorded in Section 16.1; it is not
committed-state acceptance.

## 1. Purpose

This document is the remediation handoff for the third independent review of
Model Governance Phase 2. It verifies the fixes described in:

- `docs/model-governance-phase-2-second-review.md`
- `docs/model-governance-phase-2-review-handoff-2026-08-20.md`
- `docs/model-governance-phase-2-acceptance-report.md`

This document retains the historical committed-state acceptance and third-review
defects, but that acceptance is now superseded by the current status above. The
current verdict is the reopened decision in Sections 3 and 17.

The fixing agent must treat the formal plan as authoritative:

`/Users/mac/project/all-model-router/docs/superpowers/plans/2026-08-18-model-governance-foundation-inventory.md`

## 2. Reviewed Scope

- Repository: `/Users/mac/project/sub2api-unirouter-model-governance-foundation-inventory`
- Branch: `model-governance-foundation-inventory`
- Formal base: `cad5bc5606bfc059e2da91a54db9479f312b9dd2`
- Historical committed HEAD at third review: `fb28063969782037ec7f5a9e2200bc73ecfc7f3a`
- Accepted implementation HEAD: `557507a18ab31bcf410bc132fb2487cd189440b7`
- Accepted implementation range:
  `cad5bc5606bfc059e2da91a54db9479f312b9dd2..557507a18ab31bcf410bc132fb2487cd189440b7`
- Excluded from implementation approval: the user-owned `.gitignore` change

The historical third review was performed against a mutable worktree. Final
acceptance was subsequently performed against the committed range above.

## 3. Verdict

**Phase 2 is CHANGES REQUIRED.**

The historical FINAL PASS for accepted HEAD `557507a18` is superseded by the
newly validated `P2-R3-002` contract violation. The projection fix has scoped
test evidence, but it is uncommitted and has not received a fresh committed-state
gate or independent review.

Historically, `P2-R3-001` through `P2-R3-008` were recorded as resolved in the
accepted committed range, and the committed-state coordinator gate and
independent review passed. That historical acceptance is superseded because
`P2-R3-002` did not actually satisfy its explicit Responses-disabled regression
contract.

The prior remediation resolved the originally reported defects in these areas:

- Mixed Anthropic/Gemini plus Antigravity target-platform selection.
- OpenAI normal and compact passthrough forwarding order.
- PostgreSQL microsecond timestamp canonicalization.
- Arbitrary-length exact model IDs.
- Single-copy batch-owned raw snapshots and snapshot references.
- Normal account-deletion evidence retention.
- Half-open usage windows and isolated boundary tests.

The remediation restored runtime parity for the finite inventory, completed the
database evidence boundary, preserved detached target-platform evidence, and
kept existing dashboard/statistics attribution at the `fb280639` baseline.

Historical prerequisites were recorded as satisfied at `557507a18`, with the
user-owned `.gitignore` excluded. They do not satisfy the current reopened gate.

## 4. Required Fixes

### P2-R3-001: Anthropic OAuth and SetupToken Projection Differs from Forwarding

Severity: High

Priority: P1 before Phase 2 acceptance

Status: **Accepted in committed range**

#### Contract

Every current inventory ID must be a finite model ID that the corresponding
configuration-enabled account/group/channel path can actually send upstream.
The inventory must not treat a configured mapping value as authoritative when
runtime forwarding ignores that mapping for the account type.

#### Actual Behavior

`ProjectInventoryAccountMappings` treats every non-Bedrock, non-OpenAI, and
non-Antigravity mapping value as an outbound upstream ID:

- `backend/internal/service/model_governance_inventory.go:727-750`

For an Anthropic OAuth or SetupToken account containing:

```json
{
  "model_mapping": {
    "public": "configured-upstream"
  }
}
```

the projector emits `configured-upstream`.

Native Anthropic Messages forwarding does not universally apply that mapping:

- API-key accounts apply ordinary account mapping:
  `backend/internal/service/gateway_forward.go:261-271`
- Service accounts use a separate explicit mapping path:
  `backend/internal/service/gateway_forward.go:272-282`
- OAuth and SetupToken accounts use Claude model normalization:
  `backend/internal/service/gateway_forward.go:284-289`

Scheduling also normalizes Anthropic non-API-key request IDs before support
evaluation:

- `backend/internal/service/gateway_scheduling.go:2489-2498`

The inventory can therefore include an unreachable mapping value and omit the
actual normalized model sent upstream.

#### Reproduction

1. Create an active, schedulable Anthropic OAuth account with a non-empty custom
   `model_mapping`.
2. Use a finite requested model whose configured mapping target differs from
   `claude.NormalizeModelID(requested)`.
3. Evaluate the actual Messages forwarding model.
4. Run the account-only and grouped/channel inventory projections.
5. Observe that inventory reports the configured mapping target even though
   runtime sends the normalized model.
6. Repeat for SetupToken.

#### Required Implementation Properties

- Introduce or reuse an account-type-aware Anthropic forwarding resolution seam.
- Match API-key, Service, OAuth, and SetupToken runtime behavior separately.
- Do not change runtime forwarding or account validation as part of this fix.
- Apply the same behavior in account-only and full route/channel projections.
- Continue preserving exact observation and historical usage identities.

#### Required Regression Tests

- Anthropic API-key custom mapping matches runtime.
- Anthropic Service account mapping matches its dedicated runtime path.
- Anthropic OAuth with a non-empty mapping emits the runtime-normalized model,
  not the ignored mapping value.
- Anthropic SetupToken behaves like its runtime forwarding path.
- Grouped/channel and account-only dimensions produce the same final model for
  equivalent routes.
- Tests use the actual runtime resolver or a shared helper, not duplicated
  projector logic as the oracle.

#### Acceptance Criteria

- Every projected Anthropic ID is a model the account type can actually send.
- No non-API-key mapping value is included merely because it appears in
  `credentials.model_mapping`.

### P2-R3-002: Account Endpoint Capability Gates Are Missing from Projection

Severity: High

Priority: P1 before Phase 2 acceptance

Status: **Reopened; fix pending committed-state acceptance**

#### Contract

An account/group/channel dimension is current only when stable runtime
configuration permits that account to serve the endpoint and target platform.
Platform-level compatibility alone is insufficient.

#### Actual Behavior

The projector checks broad endpoint-to-platform compatibility:

- call site: `backend/internal/service/model_governance_inventory.go:186-189`
- helper: `backend/internal/service/model_governance_inventory.go:388-401`

It then applies generic model support and forwarding:

- `backend/internal/service/model_governance_inventory.go:190-199`

Runtime has additional stable account-specific endpoint gates:

- OpenAI embeddings require an API-key account:
  `backend/internal/service/account.go:1463-1466`
- Responses capability depends on account type and persisted capability state:
  `backend/internal/service/account.go:1444-1454`
- Grok media depends on stable eligibility/override state:
  `backend/internal/service/account.go:1422-1435`
- Scheduler applies these account capability gates:
  `backend/internal/service/openai_gateway_scheduling.go:290-301`

An OpenAI OAuth account behind an embeddings route can pass the projector's
platform check even though runtime scheduling cannot select it for embeddings.

#### Required Implementation Properties

- Map each finite endpoint witness to the corresponding runtime capability.
- Apply the same stable account capability helper used by scheduling.
- Cover at least:
  - OpenAI embeddings
  - OpenAI Responses
  - OpenAI image generation where applicable
  - Grok media generation
- Do not use transient cooldown, overload, rate-limit, or quota exhaustion to
  hide a configuration-enabled account.
- Preserve forced, mixed, simple, standard, and composite dimension semantics.

#### Required Regression Tests

- OpenAI OAuth is excluded from embeddings dimensions.
- OpenAI API-key embeddings remains included when otherwise configured.
- A persisted Responses-disabled API-key account is excluded from Responses.
- Grok media explicit-disable and deterministic ineligible states are excluded.
- Stable enabled capability states remain included.
- Generic chat/messages endpoint behavior remains unchanged.
- Projection acceptance is compared against the scheduler capability seam.

#### Acceptance Criteria

- No projected endpoint dimension contains an account that runtime cannot select
  because of stable endpoint capability state.
- Platform compatibility and account capability are both required.

#### 2026-08-20 Revalidation

The accepted implementation contradicted the required regression at line 212:
text Responses inventory selected `OpenAIEndpointCapabilityChatCompletions`, so
a persisted Responses-disabled API-key remained projected. Runtime text
`/responses` can genuinely fall back through Chat Completions, but no approved
plan deviation changed the inventory contract. Strict-contract resolution keeps
that runtime behavior unchanged and requires
`OpenAIEndpointCapabilityResponses` for all OpenAI Responses projections,
including text models. Focused RED/GREEN and neighboring service tests cover the
correction; commit plus committed-state gate and independent review remain due.

### P2-R3-003: Grok Media Normalization Is Missing Before Mapping

Severity: High

Priority: P1 before Phase 2 acceptance

Status: **Accepted in committed range**

#### Contract

The finite inventory must apply model transformations in runtime order. For Grok
media, endpoint-specific normalization occurs before scheduler support checks and
account mapping.

#### Actual Behavior

Runtime normalizes Grok media aliases before scheduling:

- handler normalization: `backend/internal/handler/grok_media.go:100-104`
- normalized model passed to the scheduler:
  `backend/internal/handler/grok_media.go:192-211`

Forwarding also normalizes before account mapping:

- normalization and mapping order:
  `backend/internal/service/grok_media.go:347-366`
- concrete alias rules:
  `backend/internal/service/grok_media.go:766-780`

Examples include:

```text
grok-imagine -> grok-imagine-image-quality
grok-imagine-video-1.5 -> endpoint/input-dependent concrete video model
```

The full inventory chain currently sends the routed/channel-mapped model directly
to generic support and forwarding resolution:

- `backend/internal/service/model_governance_inventory.go:190-199`
- `backend/internal/service/model_governance_inventory.go:661-693`

For a route exposing `grok-imagine` and an account mapping
`grok-imagine-image-quality -> vendor-image-model`, runtime sends
`vendor-image-model`; the projector can reject the path or emit a pre-normalized
identity.

#### Required Implementation Properties

- Reuse runtime Grok media normalization for images and video endpoints.
- Normalize before account support and account mapping.
- For input-dependent finite outcomes that cannot be known from model ID alone,
  conservatively enumerate all runtime-reachable finite normalized outcomes.
- Apply stable Grok media capability gates from `P2-R3-002`.
- Do not change Grok runtime routing as part of inventory remediation.

#### Required Regression Tests

- Image generation alias normalization before mapping.
- Image edit alias normalization before mapping.
- Video text-only normalization.
- Video image-input normalization.
- Explicit account mapping after normalization.
- Disabled Grok media capability excludes all media dimensions.
- Inventory and runtime helper produce the same final finite targets.

#### Acceptance Criteria

- Every projected Grok media target follows runtime normalization-before-mapping.
- No reachable normalized target is omitted and no pre-normalized alias is
  reported as final upstream identity unless runtime can actually send it.

### P2-R3-004: Full Runtime Dimensions Omit Antigravity Thinking Targets

Severity: High

Priority: P1 before Phase 2 acceptance

Status: **Accepted in committed range**

#### Contract

For Antigravity mapping values, inventory must include the non-thinking target
and include a changed post-mapping thinking target when the actual runtime
support gate permits it.

#### Actual Behavior

The account-only helper applies thinking transformation:

- `backend/internal/service/model_governance_inventory.go:741-747`

The full grouped/channel/composite projection calls:

- `inventoryResolveAccountForwardModels`:
  `backend/internal/service/model_governance_inventory.go:198`
- generic Antigravity resolution:
  `backend/internal/service/model_governance_inventory.go:661-693`

That full path does not enumerate `applyThinkingModelSuffix` results.

Runtime applies the thinking transform after Antigravity mapping:

- native Claude path:
  `backend/internal/service/antigravity_gateway_claude.go:50-59`
- compatibility path:
  `backend/internal/service/antigravity_gateway_compat.go:212-220`
- scheduler post-transform support check:
  `backend/internal/service/gateway_scheduling.go:2448-2468`

Therefore the same account can expose a thinking target in an account-only
dimension while omitting it from grouped, mixed, forced, composite, or priced
dimensions.

#### Required Implementation Properties

- Apply Antigravity post-mapping transforms in the complete candidate chain.
- Use the actual runtime final-model support gate.
- Limit thinking enumeration to endpoints/paths that can carry the relevant
  thinking signal, or conservatively enumerate only when runtime reachability is
  demonstrable.
- Deduplicate after effective transformation.
- Preserve mixed target-platform and channel mapping semantics.

#### Required Regression Tests

- Native grouped Antigravity channel includes supported thinking target.
- Mixed Anthropic/Gemini plus Antigravity includes supported thinking target.
- Forced Antigravity target includes the correct transformed targets.
- Composite route dimension includes supported thinking target.
- Unsupported final thinking target remains absent.
- Non-thinking target remains present.
- Account-only and equivalent grouped projections agree.

#### Acceptance Criteria

- Thinking target reachability no longer depends on whether the dimension took
  the account-only shortcut.

### P2-R3-005: Append-Only Evidence Tables Permit TRUNCATE

Severity: High

Priority: P1 before Phase 2 acceptance

Status: **Accepted in committed range**

#### Contract

Classification batches and governance events are immutable, append-only,
durable evidence. The batch is now the sole owner of the complete raw snapshot.

#### Actual Behavior

Migration 200 installs row-level `UPDATE OR DELETE` rejection triggers:

- registry events:
  `backend/migrations/200_model_governance_foundation.sql:383-387`
- classification batches:
  `backend/migrations/200_model_governance_foundation.sql:389-393`
- observation events:
  `backend/migrations/200_model_governance_foundation.sql:395-399`

PostgreSQL does not fire row-level DELETE triggers for `TRUNCATE`.

The incarnation ledger correctly has a separate statement-level truncate
trigger:

- `backend/migrations/200_model_governance_foundation.sql:377-381`

A privileged `TRUNCATE model_classification_batches CASCADE` can erase the only
complete snapshots and dependent event history.

#### Required Implementation Properties

- Install `BEFORE TRUNCATE FOR EACH STATEMENT` rejection triggers for every
  append-only evidence table.
- At minimum cover:
  - `account_incarnation_ids`
  - `model_registry_events`
  - `model_classification_batches`
  - `model_observation_events`
- Keep triggers idempotently rerunnable in migration 200.
- Preserve ordinary inserts and all existing append-only UPDATE/DELETE behavior.

#### Required Regression Tests

- Direct TRUNCATE of each append-only table is rejected.
- `TRUNCATE ... CASCADE` beginning from classification batches is rejected.
- Existing rows remain after each rejected statement.
- PostgreSQL 14 executes the same contract.
- Direct migration rerun retains all truncate guards.

#### Acceptance Criteria

- No ordinary SQL mutation operation covered by the database contract can erase
  append-only evidence while triggers are enabled.

### P2-R3-006: Cyber-Policy Usage Loses Concrete Target Platform

Severity: High

Priority: P1 before Phase 2 acceptance

Status: **Accepted in committed range**

#### Contract

Historical usage inventory should prefer durable request-level target-platform
evidence. Detached/background persistence must snapshot required request context
before the original context is discarded.

#### Actual Behavior

The original request context is available before asynchronous cyber handling:

- `backend/internal/handler/openai_gateway_handler.go:3092-3096`

The goroutine then starts from a new background context:

- `backend/internal/handler/openai_gateway_handler.go:3126-3128`

`CyberPolicyUsageInput` is built without a concrete target/quota platform:

- `backend/internal/handler/openai_gateway_handler.go:3147-3165`

`RecordCyberPolicyUsageLog` forwards no `QuotaPlatform` into ordinary usage
recording:

- `backend/internal/service/openai_gateway_usage.go:70-98`

For forced or composite routes whose selected account native platform differs
from the request target, fallback attribution can store the wrong
`governance_target_platform`. Billing may remain correct, but governance history
is misclassified.

#### Required Implementation Properties

- Resolve and snapshot the concrete target platform before launching the
  goroutine.
- Add the snapshot to `CyberPolicyUsageInput`.
- Forward it through `OpenAIRecordUsageInput.QuotaPlatform` or the established
  shared usage seam.
- Do not retain or access `gin.Context` from the goroutine.
- Do not use governance evidence to alter billing, quota, rate limit, or
  scheduler decisions.

#### Required Regression Tests

- Composite request target differs from selected account native platform.
- Forced target differs from selected account native platform.
- Cyber usage stores the concrete target platform.
- Native requests remain unchanged.
- Billing, request type, tokens, and cost remain unchanged.
- Handler-level test proves target snapshot survives context detachment.

#### Acceptance Criteria

- Every new cyber usage row carries the same concrete target evidence that the
  corresponding normal usage path would persist.

### P2-R3-007: Governance Evidence Changes Existing Analytics Without Approval

Severity: High

Priority: P1 before Phase 2 acceptance

Status: **Accepted in committed range**

#### Contract

Phase 2 is additive and non-authoritative. Any behavior differing from the
formal plan requires a traced deviation containing original behavior, new
behavior, reason/evidence, scope/risk, and explicit approval.

#### Actual Behavior

The shared platform attribution expression now prefers governance target
evidence:

- `backend/internal/repository/usage_log_repo.go:32-37`

This expression is used outside the Phase 2 inventory by existing dashboard and
statistics queries:

- `backend/internal/repository/usage_log_repo_dashboard.go:475-496`
- `backend/internal/repository/usage_log_repo_stats.go:495-510`

As a result, existing API/reporting platform buckets can change solely because
the new governance field is populated. Mixed, forced, and composite requests can
move from prior account/group-derived buckets to request-target buckets.

The owner-decision matrix in the handoff covers single-copy snapshots,
arbitrary-length identity, and permanent account incarnations, but not analytics
attribution:

- `docs/model-governance-phase-2-review-handoff-2026-08-20.md:204-214`

The existing SQL-text test does not establish approved external behavior.

#### Required Resolution

Choose one option:

1. Keep `governance_target_platform` confined to the model-governance inventory
   query and restore existing dashboard/statistics attribution.
2. Retain the analytics change only after creating a durable owner-approved plan
   deviation and adding behavioral regression coverage.

#### Required Tests if Retained

- Existing dashboard consumer behavior for native requests.
- Mixed Antigravity request attribution.
- Forced target attribution.
- Composite target attribution.
- Legacy NULL fallback behavior.
- Batch/statistics consumer behavior, not only SQL substring order.
- Compatibility impact on existing API response grouping.

#### Acceptance Criteria

- Existing analytics behavior is unchanged, or its change is explicitly
  approved, documented, tested, and included in committed decision history.
- Governance evidence remains non-authoritative for billing and runtime
  enforcement.

## 5. Database Invariant Follow-Up

### P2-R3-008: Account Incarnation Claim and ON CONFLICT

Severity: Medium

Priority: P1 before claiming the unconditional database invariant

Status: **Accepted in committed range**

#### Current Behavior

The account `BEFORE INSERT` trigger claims an incarnation ID immediately:

- `backend/migrations/200_model_governance_foundation.sql:324-335`
- `backend/migrations/200_model_governance_foundation.sql:359-362`

The migration comment claims only successfully inserted account incarnations
consume IDs. Existing coverage proves rollback after a failed constraint insert,
but not `INSERT ... ON CONFLICT DO NOTHING`.

PostgreSQL runs row-level BEFORE INSERT triggers before conflict resolution. A
row skipped due to a different unique conflict can therefore claim a permanent
ledger ID without creating an account. Conflict-enabled account creation exists
in generated Ent APIs:

- `backend/ent/account_create.go:909-922`
- `backend/ent/account_create.go:1456-1459`

The ordinary production repository does not currently use this mode, which
reduces immediate reachability but does not satisfy the unconditional database
invariant.

#### Required Resolution

- Add integration coverage for `ON CONFLICT DO NOTHING` and conflict-update
  account inserts.
- Ensure no ledger ID is consumed unless an account incarnation is actually
  created, or explicitly forbid and database-enforce conflict/upsert account
  insertion.
- Preserve permanent no-reuse semantics for successfully created accounts.
- Preserve rollback behavior for failed inserts and concurrent reuse rejection.

#### Acceptance Criteria

- The documented invariant exactly matches all permitted account insertion
  forms.
- No ghost incarnation ledger entries are created by conflict-skipped inserts.

## 6. Original Second-Round Finding Status

| Finding | Third-review status |
| --- | --- |
| `P2-R2-001` mixed routing target platform | Original defect resolved |
| `P2-R2-002` OpenAI passthrough mapping order | Resolved |
| `P2-R2-003` PostgreSQL timestamp precision | Resolved |
| `P2-R2-004` arbitrary-length exact model ID | Resolved for active paths |
| `P2-R2-005` account deletion evidence retention | Normal deletion resolved; TRUNCATE and conflict-ledger gaps remain |
| `P2-R2-006` single-copy raw snapshot | Storage normalization and rollback resolved; TRUNCATE durability remains |
| `P2-R2-007` usage boundary tests | Resolved |

## 7. Confirmed Correct Areas

The fixing agent should preserve these behaviors and avoid unnecessary rewrites.

### 7.1 Mixed Target Platform

- Account platform, group platform, and effective target platform are separated.
- Mixed-enabled Antigravity can contribute to Anthropic and Gemini target
  dimensions.
- Mixed-disabled accounts are excluded from those dimensions.
- Channel mapping and pricing use the target platform.

### 7.2 OpenAI Passthrough

- Projection branches on the shared passthrough helper.
- Normal passthrough retains the original request model.
- Compact passthrough resolves compact mapping against the original request
  domain.
- New and legacy passthrough flags are covered.
- Non-passthrough normal-map-then-compact behavior remains intact.

### 7.3 Current Inventory Ownership

- Service projection is the sole semantic owner of current IDs.
- Aggregate SQL no longer independently expands current route, mapping, pricing,
  or observation IDs.
- SQL independently adds only bounded historical usage, as required.

### 7.4 Equal-Watermark Time

- One UTC microsecond canonical timestamp is used for idempotency, digest,
  persistence, comparison, rollback, projection, and event payloads.
- Same-stored-microsecond opposite-order tests exist.
- Distinct persisted microseconds remain ordered by time.

### 7.5 Exact Identity

- Exact model identity columns use `TEXT`.
- Hashes are lookup buckets/advisory-lock inputs only.
- Full `TEXT` equality decides identity.
- Long-ID service, repository, concurrency, update, inventory, and PostgreSQL 14
  tests exist.

### 7.6 Snapshot Normalization

- Complete raw snapshot is stored once at immutable batch scope.
- Observations/events retain snapshot references rather than payload copies.
- Missing transitions retain the previous evidence owner.
- Equal-watermark rollback restores snapshot references.
- Large-catalog structural coverage exists.

### 7.7 Usage Windows

- One frozen UTC `windowEnd` is used.
- Windows are half-open.
- Keys, aggregates, currency, negative-source, overflow, and API-key counts share
  one bounded source.
- Lower-bound API-key and future validation cases are independently tested.

### 7.8 Security and Authorization

- Inventory remains a signed `gateway:admin` GET.
- Replay protection and actor context remain active.
- Sensitive-read audit allowlisting remains present.
- No governance mutation route is registered.
- Effective model authorization remains `off`.

## 7.9 Third-Review Remediation Addendum

| Finding | Current-worktree resolution |
| --- | --- |
| `P2-R3-001` | A shared account-type-aware Anthropic resolver now matches Messages forwarding. API-key accounts apply model mapping; Service, OAuth, and SetupToken paths apply their runtime behavior, including Claude normalization where applicable. `count_tokens` intentionally remains separate: API-key mapping is applied, while other account types receive Claude normalization only. |
| `P2-R3-002` | Projection applies stable endpoint/account capabilities for embeddings, OpenAI Images, and positive Grok media eligibility. Every OpenAI Responses witness, including text, requires `OpenAIEndpointCapabilityResponses`. Cooldown, quota, overload, and other transient state are not gates. |
| `P2-R3-003` | Grok media aliases normalize before support checks and account mapping. Finite projection covers image generation/edit plus video text-only and image-input outcomes. |
| `P2-R3-004` | Antigravity thinking expansion is limited to Claude-compatible endpoints and is absent from Gemini-native paths. Account-only, grouped, mixed, forced, and composite dimensions use the same complete transform chain. |
| `P2-R3-005` | Statement-level `BEFORE TRUNCATE` guards protect all four append-only tables. Disposable default and PostgreSQL 14 tests cover direct, `CASCADE`, multi-table, row preservation, and migration-rerun behavior. |
| `P2-R3-006` | `GovernanceTargetPlatform` is snapped before the detached cyber goroutine and carried as evidence only. The goroutine does not access `gin.Context`; billing and quota inputs retain baseline behavior. |
| `P2-R3-007` | Option 1 was selected. Dashboard and statistics attribution retain `fb280639` behavior; governance target evidence is consumed only by inventory/evidence paths. Behavioral integration tests cover native, composite, forced-style, ungrouped, and legacy `NULL` fallback attribution. |
| `P2-R3-008` | Account incarnation claims now run `AFTER INSERT`; account ID updates are immutable. `DO NOTHING` and conflict-update paths create no ghost claims. Rollback, concurrency, rerun, and PostgreSQL 14 coverage remain green. |

The usage-inclusive gate initially exposed stale integration fixtures that omitted
mandatory currency snapshots and failed before their intended assertions. A new
integration-only repository adapter supplies `USD`, `USD`, rate `1`, source
`identity`, and an as-of timestamp only when the entire snapshot is absent.
Explicitly partial or invalid snapshots remain untouched and continue to test
production rejection. No production validation was weakened.

## 8. Non-Blocking or Deferred Items

Do not expand this remediation into these areas without explicit approval.

### 8.1 Asynchronous Audit

Durable synchronous audit persistence remains explicitly deferred.

### 8.2 Registry Mutation Concurrency

Registry deletion/discovery lock ordering and multi-row exact-identity advisory
lock ordering should be resolved with the first active registry mutation
workflow, not speculatively in Phase 2.

### 8.3 Usage Index

Do not add an index without production-shaped PostgreSQL 14
`EXPLAIN (ANALYZE, BUFFERS)` evidence.

### 8.4 Inventory Resource Budget

The signed inventory endpoint loads all configuration inputs and scans the
bounded 30-day usage source without pagination. This remains an operational
risk, but not a new Phase 2 merge blocker while access remains restricted. Before
production activation, define payload/cardinality/concurrency budgets and obtain
a production-shaped query plan.

### 8.5 Legacy Event Compatibility

Migration 200 remains unshipped. Do not add compatibility bridges for
pre-remediation event formats unless deployment evidence changes.

## 9. Owner Decision and Traceability Requirements

The handoff records owner approval for:

- Single-copy raw snapshots.
- Arbitrary-length exact identity.
- Permanent account incarnation IDs.

These explanations are technically adequate as working evidence but are not yet
durable merge provenance because the relevant documents remain ignored and the
overlay remains uncommitted.

Before final PASS:

1. Move each approved deviation into a tracked, committable decision or
   acceptance artifact.
2. Include original plan behavior, implemented behavior, reason/evidence,
   scope/risk, and approval source.
3. Add a separate approval only if the analytics attribution change in
   `P2-R3-007` is retained.
4. Record the final committed SHA and exact review range.

## 10. Current Mechanical State

At the time of review:

- Committed HEAD remained `fb28063969782037ec7f5a9e2200bc73ecfc7f3a`.
- The complete remediation remained an uncommitted overlay across implementation,
  migration, generated Ent/Wire source, and tests.
- `docs/model-governance-phase-2-review-handoff-2026-08-20.md` was ignored by
  `.gitignore:134`.
- `docs/model-governance-phase-2-second-review-fix-report.md` was also ignored.
- The user-owned `.gitignore` modification must not be included without separate
  approval.

Technical acceptance and merge readiness are separate. Even after all code
findings are resolved, final PASS requires a committed review range and fresh
committed-state verification.

## 11. Historical Pre-Remediation Verification Evidence

The following commands passed during the third review before the R3 remediation.
Section 16.1 supersedes this checkpoint with the fresh current-overlay gate.

### Focused Configuration and Migration

```bash
go test ./internal/config -run ModelGovernance -count=1
go test ./migrations -run ModelGovernanceFoundation -count=1
```

Result: PASS.

### Focused Service and Route Tests

```bash
go test ./internal/service \
  -run 'ModelGovernance|ProjectInventoryAccountMappings|PersistUpstreamModelDiscovery|PersistDiscoveredModels' \
  -count=1

go test ./internal/handler/admin ./internal/server/middleware ./internal/server/routes \
  -run 'ModelGovernance|AccountHandlerSyncUpstreamModels' \
  -count=1

go test -tags=unit ./internal/service \
  -run '^(TestBillingModelForRestriction|TestIsUpstreamModelRestrictedByChannel)' \
  -count=1
```

Result: PASS.

### Default PostgreSQL Integration

```bash
go test -tags=integration ./internal/repository \
  -run 'MigrationsRunner|ModelGovernanceRepository|ModelGovernanceInventory' \
  -count=1
```

Result: PASS.

### PostgreSQL 14 Integration

```bash
SUB2API_TEST_POSTGRES_IMAGE=postgres:14-alpine \
go test -tags=integration ./internal/repository \
  -run 'MigrationsRunner_ModelGovernanceFoundation|ModelGovernanceRepository|ModelGovernanceInventory' \
  -count=1
```

Result: PASS.

### Full Test, Generation, and Build

```bash
go test ./... -count=1
make generate
make build
```

Result: PASS.

### Diff and Baseline Checks

```bash
git diff --check cad5bc5606bfc059e2da91a54db9479f312b9dd2..HEAD
git diff --check

git diff fb28063969782037ec7f5a9e2200bc73ecfc7f3a --exit-code -- \
  backend/internal/service/openai_gateway_service.go \
  backend/internal/service/openai_gateway_scheduling.go
```

Result: PASS. Generation did not add unexpected paths beyond the intended dirty
overlay.

The no-diff baseline check intentionally excludes
`backend/internal/service/gateway_forward.go`. That file refactors Messages
forwarding through shared `ResolveAnthropicFinalModel` and therefore has an
expected source diff from `fb280639`; behavioral parity is established by the
focused resolver/forwarding tests, including the distinct `count_tokens` path.

At that checkpoint, the green gate did not invalidate the runtime-semantic
findings because the missing cases were not covered. Those cases are now covered
and resolved as recorded in Sections 7.9 and 16.1.

## 12. Historical Fixing-Agent Instructions

The fixing agent completed this sequence. It is retained as remediation history;
the current next steps are commit, committed-state gate, and independent review.

Use strict test-first remediation and preserve already-correct behavior.

1. Read the formal plan and this document before editing.
2. Reproduce `P2-R3-001` with an Anthropic OAuth/SetupToken runtime-parity test.
3. Fix and verify it before moving to `P2-R3-002`.
4. Repeat sequentially through `P2-R3-007`.
5. Resolve `P2-R3-008` with a focused database invariant test or an explicitly
   narrowed contract.
6. Do not rewrite the already-correct OpenAI passthrough, timestamp, long-ID,
   snapshot-reference, and usage-window foundations unless a required fix truly
   shares that seam.
7. Run focused neighboring tests after each change.
8. Run the full gate in Section 16 after all changes.
9. Produce a new fix report mapping every finding to code, tests, and fresh
   command output.
10. Commit intended implementation, migration, generated source, tests, and
    durable approval artifacts before requesting final review.

For any new deviation, record:

- Original plan behavior.
- Implemented behavior.
- Reason and evidence.
- Scope and risk.
- Explicit owner approval.

## 13. Prohibited Scope Expansion

Do not:

- Activate Phase 3 registry/classification writes.
- Activate Phase 5 `shadow` or `enforce` authorization.
- Change effective authorization from `off`.
- Let governance target evidence alter billing, quota, rate-limit, scheduler, or
  routing decisions.
- Apply migration 200 to production.
- Deploy or mutate production data.
- Add speculative compatibility for unshipped migration data.
- Redesign audit persistence.
- Add a usage index without approved PostgreSQL 14 evidence.
- Include the user-owned `.gitignore` change in an implementation commit without
  separate authorization.

## 14. Required Focused Tests

At minimum, the fixing agent should add tests whose names make these behaviors
discoverable:

- Anthropic OAuth/SetupToken forwarding parity.
- Endpoint-capability parity for embeddings, Responses, and Grok media.
- Grok media normalization-before-mapping.
- Antigravity thinking in grouped/mixed/composite dimensions.
- Append-only evidence TRUNCATE rejection.
- Cyber-policy detached target-platform persistence.
- Dashboard/statistics behavior or inventory-only governance target isolation.
- Account incarnation `ON CONFLICT` behavior.

Avoid tests that use the projector itself as the only runtime oracle.

## 15. Re-Acceptance Checklist

- [x] `P2-R3-001` Anthropic account-type forwarding parity is fixed.
- [ ] `P2-R3-002` strict Responses endpoint account capability is applied in a
      committed range and re-accepted.
- [x] `P2-R3-003` Grok media normalization and mapping order match runtime.
- [x] `P2-R3-004` Antigravity thinking is included only in runtime-compatible dimensions.
- [x] `P2-R3-005` all append-only evidence tables reject TRUNCATE.
- [x] `P2-R3-006` cyber usage retains concrete target evidence.
- [x] `P2-R3-007` existing analytics behavior is restored and tested.
- [x] `P2-R3-008` account incarnation behavior matches its documented invariant.
- [x] Previously resolved R2 findings remain green.
- [x] Inventory projector remains the sole current-ID semantic owner.
- [x] Historical usage remains bounded and additive.
- [x] Effective authorization remains `off`.
- [x] Inventory remains signed, audited through the accepted mechanism, and
      read-only.
- [x] No governance evidence changes runtime routing, scheduling, billing, quota,
      or rate limiting.
- [x] Migration 200 was tested only on disposable PostgreSQL.
- [ ] Required default and PostgreSQL 14 integration selectors pass on the new
      committed range.
- [ ] Full Go suite, generation, build, and diff checks pass on the new committed
      range.
- [x] Owner-approved deviations exist in tracked repository history.
- [ ] All intended implementation and review-document changes are committed and
      the SHA is recorded.
- [ ] Final review is performed against the new committed range.

## 16. Required Re-Verification Gate

Run from `backend/` unless noted otherwise:

```bash
go test ./internal/config -run ModelGovernance -count=1
go test ./migrations -run ModelGovernanceFoundation -count=1

go test ./internal/service \
  -run 'ModelGovernance|ProjectInventoryAccountMappings|PersistUpstreamModelDiscovery|PersistDiscoveredModels|CyberPolicy' \
  -count=1

go test ./internal/handler ./internal/handler/admin \
  ./internal/server/middleware ./internal/server/routes \
  -run 'ModelGovernance|AccountHandlerSyncUpstreamModels|CyberPolicy' \
  -count=1

go test -tags=unit ./internal/service \
  -run '^(TestBillingModelForRestriction|TestIsUpstreamModelRestrictedByChannel)' \
  -count=1

go test -tags=integration ./internal/repository \
  -run 'MigrationsRunner|ModelGovernanceRepository|ModelGovernanceInventory|UsageLog' \
  -count=1

SUB2API_TEST_POSTGRES_IMAGE=postgres:14-alpine \
go test -tags=integration ./internal/repository \
  -run 'MigrationsRunner_ModelGovernanceFoundation|ModelGovernanceRepository|ModelGovernanceInventory' \
  -count=1

go test ./... -count=1
make generate
make build
```

Then run from the repository root:

```bash
git diff --check cad5bc5606bfc059e2da91a54db9479f312b9dd2..HEAD
git diff --check
git status --short --branch
git status --short --ignored
```

The following subsection records the latest mutable-worktree gate. Historical
committed-state evidence is retained in Section 17 but is superseded and does not
accept the current overlay.

### 16.1 Fresh Current-Overlay Outcome

The coordinator reran this gate from `backend/` on 2026-08-20 against the current
mutable-worktree overlay containing the strict-contract `P2-R3-002` correction:

- Focused config, migration, service, handler, admin, middleware, routes, and
  unit selectors: **PASS**.
- Default usage-inclusive formal integration selector: **PASS in 20.834s**.
- PostgreSQL 14 formal integration selector: **PASS in 19.145s**.
- `go test ./... -count=1`: **PASS**; `internal/service` completed in 105.001s.
- `make generate`: **PASS**.
- `make build`: **PASS**.
- Runtime no-diff check: **PASS**.
- Generated no-diff check: **PASS**.
- `git diff --check`: **PASS**.
- Production operations: **not performed**.

This is mutable-worktree evidence only. Current status remains **CHANGES
REQUIRED** pending commit, committed-state gate, and independent review.

At that historical checkpoint, an independent scoped review of the then-current
post-fixture mutable worktree reported Critical 0, Important 0, Minor 0 and
returned **ACCEPTED**. It then considered all known final-review findings
addressed; the later `P2-R3-002` revalidation superseded that conclusion.
Historical focused service, full service, focused database, default formal
integration, and PostgreSQL 14 formal integration checks passed in 2.526s,
96.337s, 4.114s, 9.203s, and 8.005s respectively; diff and scheduler baseline
checks also passed. At that historical checkpoint, this scoped evidence
supplemented rather than replaced the complete coordinator gate at 13.956s /
12.367s / 102.197s. Both records are superseded for current-overlay status by
the gate above.

The unrestricted command
`go test -tags=integration ./internal/repository -count=1` is not a Section 16
gate and currently has unrelated failures in API-key `routing_mode` fixtures,
usage-billing wallet currency fixtures, and shared-row group counts. They are
disclosed as out-of-gate repository-suite state, not reported as passing and not
classified here as task regressions.

## 17. Final Review Decision

Current verdict: **CHANGES REQUIRED**.

The FINAL PASS recorded below is historical evidence for accepted HEAD
`557507a18` and is **superseded**. The newly validated `P2-R3-002` conflict had no
approved deviation: inventory acceptance must enforce Responses capability for
all OpenAI Responses request shapes even though runtime text `/responses`
fallback through Chat Completions remains unchanged. The projection correction
and review-document updates must be committed, then the committed-state gate and
independent review must be rerun before FINAL PASS can be reconsidered. The fresh
full gate in Section 16.1 applies only to the mutable overlay and is not
committed-state acceptance.

- Formal base: `cad5bc5606bfc059e2da91a54db9479f312b9dd2`.
- Accepted implementation HEAD: `557507a18ab31bcf410bc132fb2487cd189440b7`.
- Accepted technical range:
  `cad5bc5606bfc059e2da91a54db9479f312b9dd2..557507a18ab31bcf410bc132fb2487cd189440b7`.
- The accepted commit contains the intended implementation, generated sources,
  migration, tests, and all five review documents. `.gitignore` is excluded and
  remains user-owned and uncommitted.
- Committed coordinator gate: default usage-inclusive integration PASS 18.952s;
  PostgreSQL 14 PASS 17.489s; full suite PASS with `internal/service` 106.031s;
  focused suites, generation, build, diff, and generated-no-diff checks PASS.
- Independent committed-state review: Critical 0, Important 0, Minor 0; default
  integration PASS 17.162s; PostgreSQL 14 PASS 12.094s; full suite PASS with
  `internal/service` 105.369s; build, generation, and diff checks PASS; all five
  review documents tracked.
- No production operation was performed.

Historical outcome, now superseded: the technical range was accepted and this
recording review was administrative traceability. That conclusion no longer
describes current acceptance status.

Round 1 integration follow-up preserves provider-specific finite reachability
without restoring the account-only final-model shortcut: eligible OpenAI
compact-only mapping keys and Bedrock default aliases seed request witnesses,
which traverse endpoint compatibility and account forwarding. Bedrock
`count_tokens` remains excluded, as do Antigravity `count_tokens` and Gemini
target thinking outcomes. Round 2 additionally proves that `count_tokens` is
excluded for a mixed Antigravity account under an Anthropic target. Those
provider-specific boundaries remain accepted in the final committed state.
