# Task 3 Report: Persist Discovery Observations Without Authority

## Status

DONE_WITH_CONCERNS

Implementation commit: `cd3a8e506` (`feat: persist non-authoritative model observations`)

## Summary

- Added the service-layer discovery evidence contract and PostgreSQL repository.
- `PersistDiscoveredModels` now records the accepted parsed upstream payload before the existing credential discovery update can return success.
- Observation persistence is additive evidence only. It does not update model mappings, routes, groups, prices, scheduler eligibility, or authorization mode.
- Exact non-empty upstream model ID strings are preserved byte-for-byte; only all-whitespace IDs are rejected and exact duplicate strings are collapsed.
- Governed platform attribution is limited to `anthropic|openai|gemini|grok`. `antigravity`, `composite`, and unknown routing-only platforms use `AccountProvider=nil` and create `ignored/unsupported_routing_platform` projections.

## RED Evidence

Tests were written before production implementation.

1. `go -C backend test ./internal/service -run PersistDiscoveredModels -count=1`
   - RED: compile failed because `DiscoveryBatchInput`, `GovernanceProvider`, and the `AccountTestService` observation dependency did not exist.
2. `go -C backend test -tags=integration ./internal/repository -run ModelGovernanceRepository -count=1`
   - RED: compile failed because `NewModelObservationRepository`, `DiscoveryBatchInput`, and `GovernanceProvider` did not exist.
3. `go test ./internal/service -run PersistDiscoveredModelsPreservesExactNonEmptyUpstreamModelIDs -count=1`
   - RED: expected `" Vendor/Model:Latest "` to remain distinct, but existing code trimmed it to `"Vendor/Model:Latest"`.

All RED failures were caused by the missing or incorrect target behavior, not fixture errors.

## GREEN Evidence

- PASS: `go -C backend test ./internal/service -run PersistDiscoveredModels -count=1`
- PASS: `go -C backend test -tags=integration ./internal/repository -run ModelGovernanceRepository -count=1`
- PASS: `go -C backend test ./internal/service -count=1`
- PASS: `go -C backend test ./internal/handler/admin -run AccountHandlerSyncUpstreamModels -count=1`
- PASS: `go -C backend test ./... -count=1`
- PASS: `git diff --check`
- CONCERN: `go -C backend test -tags=integration ./internal/repository -count=1` fails in pre-existing unrelated repository integration fixtures. A representative failure reproduces independently with `go -C backend test -tags=integration ./internal/repository -run 'TestAPIKeyRepoSuite/TestCreate$' -count=1`: Ent inserts an empty/default routing mode rejected by `chk_api_keys_routing_mode`. The full suite also reports existing usage-currency fixture failures and shared data-count pollution. Task 3's targeted Testcontainers repository tests pass.

## Transaction and Idempotency Semantics

- `RecordDiscovery` starts one SQL transaction for the classification batch, current projections, and append-only events.
- The batch insert uses unique `idempotency_key` with `ON CONFLICT DO NOTHING` as the replay gate.
- Replay returns the original `batch_id`, commits no projection changes, and appends no events even if the replay payload differs.
- A projection/event failure rolls back the batch row, projections, and events together; the integration test forces an oversized model ID to prove rollback.
- An `accounts` row lock serializes accepted batches for the same account before observation projection rows are loaded and locked.
- New governed IDs start `discovered/awaiting_registry_classification`; new routing-only IDs start `ignored/unsupported_routing_platform`.
- Present IDs update presence, reset `miss_streak`, advance `last_seen_at` monotonically, and preserve classification/reason.
- Omitted IDs only become/remain `missing` and increment `miss_streak`; classification, reason, registry resolution, first/last seen, and raw snapshot remain unchanged.
- Reappearance resets `miss_streak` to zero while preserving classification/reason.

## Event Contract

For each non-replayed accepted batch, exactly one immutable event is appended per affected observation projection:

- `discovered`: a new projection was created.
- `observed`: an already-present projection was observed again.
- `missing`: a prior projection was omitted; one event is appended for every miss-streak increment.
- `reappeared`: a missing projection was observed and reset to present/zero misses.

Every event snapshots classification, classification reason, presence, miss streak, batch ID, routing platform, nullable account provider, and observed time. Database triggers from Task 2 reject event updates/deletes.

## Existing Call Flow

Before Task 3:

`SyncUpstreamModels` fetched and parsed model IDs, called `PersistDiscoveredModels`, updated `credentials.model_mapping` plus `model_discovery`, then returned HTTP success.

After Task 3:

`SyncUpstreamModels` still follows the same external flow. Inside `PersistDiscoveredModels`, the accepted parsed payload is encoded and recorded as governance evidence first. Only after that succeeds does the pre-existing `AccountModelDiscoveryStore.UpdateModelDiscovery` behavior run. Evidence failure prevents sync success and prevents the later credential update. The existing credential update remains separate legacy behavior; the new observation repository never mutates credentials or runtime routing state.

## Files

- Added `backend/internal/service/model_governance_types.go`
- Added `backend/internal/repository/model_governance_repo.go`
- Added `backend/internal/repository/model_governance_repo_integration_test.go`
- Modified `backend/internal/service/upstream_models.go`
- Modified `backend/internal/service/upstream_models_test.go`
- Modified `backend/internal/repository/wire.go`
- Modified `backend/internal/service/wire.go`
- Modified `backend/internal/service/account_test_service.go` (approved deviation)
- Modified generated `backend/cmd/server/wire_gen.go` (approved deviation)
- Modified `backend/internal/handler/admin/account_handler_available_models_test.go` (approved deviation)

## Production Database Safety

- No production database command or migration was run.
- Integration tests used only the repository test harness, which creates disposable Testcontainers PostgreSQL and Redis containers and terminates them after the test process.
- No production database URL was read or used.

## Self-Review

- Confirmed the observation repository references only `model_classification_batches`, `model_observations`, `model_observation_events`, and a read-only `accounts` row lock.
- Confirmed no observation SQL mutates credentials, mappings, routes, groups, prices, scheduler state, or authorization mode.
- Confirmed provider inference is an explicit four-value allowlist; every other routing platform remains unattributed and ignored.
- Confirmed exact model ID preservation with case, punctuation, slash, colon, and whitespace-sensitive test data.
- Confirmed omitted IDs preserve administrator classification and reason.
- Confirmed replay performs no projection/event writes and returns the original batch ID.
- Confirmed production Wire injects the repository and full non-integration backend tests pass.

## Plan Deviations

### Plan deviation 1: AccountTestService dependency field

- Original plan text: Task 3 file list modifies `backend/internal/service/upstream_models.go` and Wire files, but does not list `backend/internal/service/account_test_service.go`.
- Implemented behavior: added one unexported `ModelObservationRepository` dependency field and a focused setter to the file that actually declares `AccountTestService`.
- Evidence/reason: clean injection cannot be implemented in `upstream_models.go` because the target struct is declared in `account_test_service.go`.
- Scope/risk: dependency storage and test injection only; no routing, mapping, pricing, scheduling, or authorization behavior change. Low risk.
- Approval status: explicitly approved by the human during Task 3 review after independent review classified this deviation as necessary/minimal.

### Plan deviation 2: Generated Wire graph

- Original plan text: Task 3 requires modifying provider sets under `backend/internal/{repository,service}/wire.go`, but its file list and `git add` command omit `backend/cmd/server/wire_gen.go`.
- Implemented behavior: ran `make -C backend generate` and committed the generated constructor call that creates and injects `ModelObservationRepository`.
- Evidence/reason: the checked-in generated file retained the old `ProvideAccountTestService` signature and production compilation/initialization required regeneration.
- Scope/risk: generated dependency-injection call only. Low risk.
- Approval status: explicitly approved by the human during Task 3 review after independent review classified this deviation as necessary/minimal.

### Plan deviation 3: Handler success fixture

- Original plan text: Task 3 file list does not include `backend/internal/handler/admin/account_handler_available_models_test.go`.
- Implemented behavior: injected a recording observation repository into the handler sync-success test and asserted one accepted observation input.
- Evidence/reason: `go -C backend test ./... -count=1` showed the manually constructed service correctly failed closed because the new required evidence repository was absent; production Wire was already correct.
- Scope/risk: test fixture and assertion only; no production behavior change. No production risk.
- Approval status: explicitly approved by the human during Task 3 review after independent review classified this deviation as necessary but reducible; the fixture and assertion must remain narrowly scoped.

No other plan deviations.

## Fix Round 1/5: Preserve Discovery Evidence Boundaries

### Status

DONE

Commit subject: `fix: preserve discovery evidence boundaries`

### Findings Addressed

- Production sync no longer reconstructs governance evidence from the normalized runtime model list. `SyncUpstreamModels` carries `UpstreamModelDiscovery` from fetch through persistence while returning the same normalized `models` response and applying the same legacy mapping behavior.
- HTTP, Codex manifest, and Antigravity OAuth discovery preserve the accepted original JSON payload in the evidence envelope together with source metadata. PostgreSQL stores the envelope as `jsonb`, so insignificant source formatting is canonicalized at rest; JSON values, array order, exact string values, duplicates in arrays, and response metadata are retained.
- Evidence model IDs preserve exact non-empty upstream strings and first-seen order, while `credentials.model_mapping` continues to trim, deduplicate, and sort IDs as before.
- Empty accepted catalogs now record batch/raw evidence and apply missing transitions before returning the existing `Upstream returned no supported models` failure without changing runtime mapping.
- Stale batches are persisted as batch/raw evidence only. Under the account row lock, observations older than the latest accepted batch do not alter projections or append events.
- Repository input now rejects non-object snapshots and non-nil providers outside `anthropic|openai|gemini|grok`.
- Idempotency replay is accepted only for the same account and equivalent JSON payload. Cross-account or payload-mismatched reuse returns `discovery idempotency key collision`.
- Missing events are appended in sorted model-ID order for deterministic history.

### RED Evidence

1. Service boundary tests initially failed to compile because `UpstreamModelDiscovery`, `FetchUpstreamModelDiscovery`, and `PersistUpstreamModelDiscovery` did not exist.
2. Repository integration tests reproduced stale projection mutation, acceptance of invalid providers/non-object snapshots, cross-account idempotency replay, and nondeterministic missing-event order.
3. Handler tests reproduced normalized IDs reaching evidence and empty catalogs returning before evidence persistence.
4. `go test -tags=unit ./internal/pkg/antigravity -run TestClient_FetchAvailableModels_Success_RealCall -count=1` initially failed because no API exposed accepted response bytes.
5. `go test ./internal/service -run TestFetchUpstreamModelDiscoveryPreservesAntigravityOAuthPayload -count=1` first showed whitespace-sensitive IDs were trimmed, then showed the payload was re-encoded rather than carried from the accepted response.

### GREEN Evidence

- PASS: `go test ./internal/service -run 'FetchUpstream(ModelDiscovery|SupportedModels)|Persist(UpstreamModelDiscovery|DiscoveredModels)' -count=1`
- PASS: `go test ./internal/handler/admin -run AccountHandlerSyncUpstreamModels -count=1`
- PASS: `go test -tags=integration ./internal/repository -run ModelGovernanceRepository -count=1`
- PASS: `go test -tags=unit ./internal/pkg/antigravity -run FetchAvailableModels -count=1`
- PASS: `go test ./... -count=1`
- PASS: `git diff --check`

Integration tests used disposable Testcontainers PostgreSQL/Redis only. No production database command, migration, or production database URL was used.

### Approved Deviations

#### Handler lossless-discovery handoff

- Modified `backend/internal/handler/admin/account_handler.go` only within `SyncUpstreamModels` so production carries the lossless discovery object into persistence.
- Existing success response, normalized runtime model list, mapping update, and safe error classification remain unchanged.
- Explicitly approved by the human before implementation.

#### Antigravity accepted-response bytes

- Added `FetchAvailableModelsWithRawBytes` in `backend/internal/pkg/antigravity/client.go` and direct tests so Antigravity OAuth governance evidence does not reconstruct payloads from a decoded map.
- Existing `FetchAvailableModels` signature and typed/map return behavior remain intact through a compatibility wrapper; quota and other callers are unchanged.
- Explicitly approved by the human before implementation.

### Self-Review

- Confirmed governance repository writes remain limited to classification batches, observation projections, and immutable observation events.
- Confirmed stale batches and valid idempotent replays do not mutate projections/events.
- Confirmed the observation repository cannot mutate credentials, mappings, routes, groups, prices, scheduler eligibility, or authorization mode.
- Confirmed exact evidence IDs and normalized runtime IDs are separate values with independent tests.
- Confirmed empty catalogs do not change runtime mapping and retain the legacy 502 handler response.
- Confirmed no schema or migration changes were required.
