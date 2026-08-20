//go:build integration

package repository

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

type observationProjection struct {
	ModelID        string
	Classification string
	Reason         string
	Presence       string
	MissStreak     int
	FirstSeenAt    time.Time
	LastSeenAt     time.Time
}

type observationProjectionDetails struct {
	observationProjection
	ConnectionID       sql.NullInt64
	ResolvedRegistryID sql.NullInt64
	SnapshotBatchID    string
}

func TestModelGovernanceRepository_RecordDiscoveryPreservesIDsAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	accountID := createGovernanceObservationAccount(t, "openai", `{"model_mapping":{"admin-alias":"upstream-target"}}`)
	repo := NewModelObservationRepository(integrationDB)
	provider := service.GovernanceProvider("openai")
	firstObservedAt := time.Date(2026, time.August, 18, 8, 0, 0, 0, time.UTC)
	firstInput := service.DiscoveryBatchInput{
		IdempotencyKey:  "governance-first-" + fmt.Sprint(accountID),
		AccountID:       accountID,
		AccountProvider: &provider,
		RoutingPlatform: "openai",
		ModelIDs:        []string{"Vendor/Model:Latest", "gpt-5.6-sol"},
		RawSnapshot:     []byte(`{"models":["Vendor/Model:Latest","gpt-5.6-sol"]}`),
		ObservedAt:      firstObservedAt,
	}

	batchID, err := repo.RecordDiscovery(ctx, firstInput)
	require.NoError(t, err)
	require.NotEmpty(t, batchID)
	require.Equal(t, []observationProjection{
		{ModelID: "Vendor/Model:Latest", Classification: "discovered", Reason: "awaiting_registry_classification", Presence: "present", MissStreak: 0, FirstSeenAt: firstObservedAt, LastSeenAt: firstObservedAt},
		{ModelID: "gpt-5.6-sol", Classification: "discovered", Reason: "awaiting_registry_classification", Presence: "present", MissStreak: 0, FirstSeenAt: firstObservedAt, LastSeenAt: firstObservedAt},
	}, loadObservationProjections(t, accountID))
	require.Equal(t, 2, countObservationEvents(t, accountID))

	replay := firstInput
	replayedBatchID, err := repo.RecordDiscovery(ctx, replay)
	require.NoError(t, err)
	require.Equal(t, batchID, replayedBatchID)
	require.Len(t, loadObservationProjections(t, accountID), 2)
	require.Equal(t, 2, countObservationEvents(t, accountID), "replay must append no events")

	var mappingText string
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT credentials->'model_mapping' FROM accounts WHERE id = $1`, accountID).Scan(&mappingText))
	require.JSONEq(t, `{"admin-alias":"upstream-target"}`, mappingText)
}

func TestModelGovernanceRepository_RecordDiscoveryPreservesPayloadBytesThroughJSONBRoundTrip(t *testing.T) {
	ctx := context.Background()
	accountID := createGovernanceObservationAccount(t, "openai", `{}`)
	repo := NewModelObservationRepository(integrationDB)
	provider := service.GovernanceProvider("openai")
	payload := []byte("{\n  \"duplicate\": 1, \"duplicate\": 2, \"number\": 1.00e+02\n}")
	snapshot, err := json.Marshal(map[string]any{
		"payload_base64": base64.StdEncoding.EncodeToString(payload),
		"response":       map[string]any{"source": "test"},
	})
	require.NoError(t, err)

	batchID, err := repo.RecordDiscovery(ctx, service.DiscoveryBatchInput{
		IdempotencyKey: "lossless-payload-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: []string{"model"},
		RawSnapshot: snapshot, ObservedAt: time.Date(2026, time.August, 18, 9, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)

	var stored struct {
		Evidence struct {
			PayloadBase64 string `json:"payload_base64"`
		} `json:"evidence"`
	}
	require.NoError(t, json.Unmarshal([]byte(loadBatchRawSnapshot(t, batchID)), &stored))
	roundTripped, err := base64.StdEncoding.DecodeString(stored.Evidence.PayloadBase64)
	require.NoError(t, err)
	require.Equal(t, payload, roundTripped)
}

func TestModelGovernanceRepository_RecordDiscoveryPersistsLongExactModelID(t *testing.T) {
	ctx := context.Background()
	accountID := createGovernanceObservationAccount(t, "openai", `{}`)
	provider := service.GovernanceProvider("openai")
	modelID := "vendor/" + strings.Repeat("model-segment-", 24)

	batchID, err := NewModelObservationRepository(integrationDB).RecordDiscovery(ctx, service.DiscoveryBatchInput{
		IdempotencyKey: "long-model-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: []string{modelID},
		RawSnapshot: []byte(fmt.Sprintf(`{"models":[%q]}`, modelID)), ObservedAt: time.Date(2026, time.August, 19, 14, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	require.Greater(t, len(modelID), 255)
	require.Equal(t, modelID, loadObservationProjection(t, accountID, modelID).ModelID)
	require.Equal(t, []string{modelID}, loadBatchEventModelIDs(t, batchID))
}

func TestModelGovernanceRepository_SnapshotStorageIsNormalizedAndReferencesResolve(t *testing.T) {
	ctx := context.Background()
	accountID := createGovernanceObservationAccount(t, "openai", `{}`)
	provider := service.GovernanceProvider("openai")
	base := time.Date(2026, time.August, 19, 14, 15, 0, 0, time.UTC)
	models := []string{"model-a", "model-b", "model-c"}
	firstSnapshot := []byte(`{"models":["model-a","model-b","model-c"],"metadata":{"representative":"complete"}}`)
	firstBatchID, err := NewModelObservationRepository(integrationDB).RecordDiscovery(ctx, service.DiscoveryBatchInput{
		IdempotencyKey: "normalized-first-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: models,
		RawSnapshot: firstSnapshot, ObservedAt: base,
	})
	require.NoError(t, err)
	secondBatchID, err := NewModelObservationRepository(integrationDB).RecordDiscovery(ctx, service.DiscoveryBatchInput{
		IdempotencyKey: "normalized-second-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: []string{"model-a", "model-c"},
		RawSnapshot: []byte(`{"models":["model-a","model-c"]}`), ObservedAt: base.Add(time.Hour),
	})
	require.NoError(t, err)

	var batchEvidenceCopies, projectionRawColumns, eventRawPayloads, unresolvedReferences int
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM model_classification_batches
		WHERE batch_id = $1 AND raw_snapshot->'evidence' = $2::jsonb
	`, firstBatchID, string(firstSnapshot)).Scan(&batchEvidenceCopies))
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'model_observations' AND column_name = 'raw_snapshot'
	`).Scan(&projectionRawColumns))
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM model_observation_events WHERE payload ? 'raw_snapshot'
		  AND account_id = $1
	`, accountID).Scan(&eventRawPayloads))
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM model_observation_events e
		LEFT JOIN model_classification_batches causal ON causal.batch_id = e.batch_id
		LEFT JOIN model_classification_batches snapshot ON snapshot.batch_id = e.snapshot_batch_id
		WHERE e.account_id = $1 AND (causal.batch_id IS NULL OR snapshot.batch_id IS NULL)
	`, accountID).Scan(&unresolvedReferences))
	require.Equal(t, 1, batchEvidenceCopies, "the complete raw snapshot has one durable owner")
	require.Zero(t, projectionRawColumns)
	require.Zero(t, eventRawPayloads)
	require.Zero(t, unresolvedReferences)
	require.Equal(t, secondBatchID, loadObservationProjectionDetails(t, accountID, "model-a").SnapshotBatchID)
	require.Equal(t, firstBatchID, loadObservationProjectionDetails(t, accountID, "model-b").SnapshotBatchID,
		"omission preserves the last snapshot that contained the model")
	missing := loadDirectBatchEventPostState(t, secondBatchID, "model-b")
	require.Equal(t, secondBatchID, missing.BatchID)
	require.Equal(t, firstBatchID, missing.SnapshotBatchID)
}

func TestModelGovernanceRepository_LargeCatalogStoresOneSnapshotAndBoundedReferences(t *testing.T) {
	ctx := context.Background()
	accountID := createGovernanceObservationAccount(t, "openai", `{}`)
	provider := service.GovernanceProvider("openai")
	models := make([]string, 1000)
	for index := range models {
		models[index] = fmt.Sprintf("capacity-model-%04d", index)
	}
	snapshot, err := json.Marshal(map[string]any{
		"models":  models,
		"padding": strings.Repeat("x", 2*1024*1024),
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(snapshot), 2*1024*1024)

	batchID, err := NewModelObservationRepository(integrationDB).RecordDiscovery(ctx, service.DiscoveryBatchInput{
		IdempotencyKey: "capacity-1000-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: models,
		RawSnapshot: snapshot, ObservedAt: time.Date(2026, time.August, 19, 15, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)

	var batches, observations, events, projectionRawColumns, unresolvedReferences, eventSnapshotCopies, maxEventPayloadBytes int
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM model_classification_batches WHERE account_id = $1
	`, accountID).Scan(&batches))
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM model_observations WHERE account_id = $1
	`, accountID).Scan(&observations))
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'model_observations' AND column_name = 'raw_snapshot'
	`).Scan(&projectionRawColumns))
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(MAX(octet_length(payload::text)), 0),
		       COUNT(*) FILTER (WHERE payload ? 'raw_snapshot' OR payload ? 'evidence')
		FROM model_observation_events WHERE account_id = $1
	`, accountID).Scan(&events, &maxEventPayloadBytes, &eventSnapshotCopies))
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM model_observations o
		LEFT JOIN model_classification_batches snapshot ON snapshot.batch_id = o.snapshot_batch_id
		WHERE o.account_id = $1 AND snapshot.batch_id IS NULL
	`, accountID).Scan(&unresolvedReferences))
	var unresolvedEventReferences int
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM model_observation_events e
		LEFT JOIN model_classification_batches causal ON causal.batch_id = e.batch_id
		LEFT JOIN model_classification_batches snapshot ON snapshot.batch_id = e.snapshot_batch_id
		WHERE e.account_id = $1 AND (causal.batch_id IS NULL OR snapshot.batch_id IS NULL)
	`, accountID).Scan(&unresolvedEventReferences))

	require.Equal(t, 1, batches, "the raw catalog has one durable batch owner")
	require.Equal(t, 1000, observations)
	require.Equal(t, 1000, events)
	require.Zero(t, projectionRawColumns)
	require.Zero(t, eventSnapshotCopies)
	require.Zero(t, unresolvedReferences)
	require.Zero(t, unresolvedEventReferences)
	require.Less(t, maxEventPayloadBytes, 4096, "event payloads contain bounded metadata, not the 2 MiB snapshot")
	assertBatchSemanticOwner(t, batchID, service.DiscoveryBatchInput{
		AccountID: accountID, AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: models,
		RawSnapshot: snapshot, ObservedAt: time.Date(2026, time.August, 19, 15, 0, 0, 0, time.UTC),
	})
}

func TestModelGovernanceRepository_RecordDiscoveryReplayRequiresEquivalentProvenance(t *testing.T) {
	ctx := context.Background()
	accountID := createGovernanceObservationAccount(t, "openai", `{}`)
	repo := NewModelObservationRepository(integrationDB)
	provider := service.GovernanceProvider("openai")
	connectionID := int64(101)
	observedAt := time.Date(2026, time.August, 18, 10, 0, 0, 0, time.UTC)
	input := service.DiscoveryBatchInput{
		IdempotencyKey: "complete-provenance-" + fmt.Sprint(accountID), AccountID: accountID,
		ConnectionID: &connectionID, AccountProvider: &provider, RoutingPlatform: "openai",
		ModelIDs:    []string{"z-model", "a-model", "z-model"},
		RawSnapshot: []byte(`{"payload_base64":"e30=","response":{"source":"test"}}`), ObservedAt: observedAt,
	}
	firstBatchID, err := repo.RecordDiscovery(ctx, input)
	require.NoError(t, err)

	equivalent := input
	equivalent.ModelIDs = []string{"z-model", "a-model"}
	replayedBatchID, err := repo.RecordDiscovery(ctx, equivalent)
	require.NoError(t, err)
	require.Equal(t, firstBatchID, replayedBatchID)

	otherConnection := int64(102)
	otherProvider := service.GovernanceProvider("gemini")
	tests := []struct {
		name   string
		mutate func(*service.DiscoveryBatchInput)
	}{
		{name: "connection", mutate: func(got *service.DiscoveryBatchInput) { got.ConnectionID = &otherConnection }},
		{name: "provider", mutate: func(got *service.DiscoveryBatchInput) { got.AccountProvider = &otherProvider }},
		{name: "routing platform", mutate: func(got *service.DiscoveryBatchInput) { got.RoutingPlatform = "gemini" }},
		{name: "model order", mutate: func(got *service.DiscoveryBatchInput) { got.ModelIDs = []string{"a-model", "z-model"} }},
		{name: "model set", mutate: func(got *service.DiscoveryBatchInput) { got.ModelIDs = []string{"z-model"} }},
		{name: "observed at", mutate: func(got *service.DiscoveryBatchInput) { got.ObservedAt = observedAt.Add(time.Second) }},
		{name: "raw payload", mutate: func(got *service.DiscoveryBatchInput) {
			got.RawSnapshot = []byte(`{"payload_base64":"eyJ4IjoxfQ==","response":{"source":"test"}}`)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mismatch := input
			tt.mutate(&mismatch)
			_, err := repo.RecordDiscovery(ctx, mismatch)
			require.ErrorContains(t, err, "idempotency key collision")
		})
	}
}

func TestModelGovernanceRepository_RecordDiscoveryTracksMissingAndReappearanceWithoutReclassification(t *testing.T) {
	ctx := context.Background()
	accountID := createGovernanceObservationAccount(t, "anthropic", `{}`)
	repo := NewModelObservationRepository(integrationDB)
	provider := service.GovernanceProvider("anthropic")
	base := time.Date(2026, time.August, 18, 8, 0, 0, 0, time.UTC)

	record := func(key string, observedAt time.Time, ids ...string) {
		t.Helper()
		_, err := repo.RecordDiscovery(ctx, service.DiscoveryBatchInput{
			IdempotencyKey:  key + fmt.Sprint(accountID),
			AccountID:       accountID,
			AccountProvider: &provider,
			RoutingPlatform: "anthropic",
			ModelIDs:        ids,
			RawSnapshot:     []byte(`{"source":"test"}`),
			ObservedAt:      observedAt,
		})
		require.NoError(t, err)
	}

	record("present-", base, "claude-a", "claude-b")
	require.NoError(t, setObservationClassification(t, accountID, "claude-a", "approved", "admin_reviewed"))
	record("missing-one-", base.Add(time.Hour), "claude-b")
	record("missing-two-", base.Add(2*time.Hour), "claude-b")

	missing := loadObservationProjection(t, accountID, "claude-a")
	require.Equal(t, "approved", missing.Classification)
	require.Equal(t, "admin_reviewed", missing.Reason)
	require.Equal(t, "missing", missing.Presence)
	require.Equal(t, 2, missing.MissStreak)
	require.Equal(t, base, missing.LastSeenAt, "omission must not fabricate a last-seen time")

	record("reappeared-", base.Add(3*time.Hour), "claude-a", "claude-b")
	reappeared := loadObservationProjection(t, accountID, "claude-a")
	require.Equal(t, "approved", reappeared.Classification)
	require.Equal(t, "admin_reviewed", reappeared.Reason)
	require.Equal(t, "present", reappeared.Presence)
	require.Zero(t, reappeared.MissStreak)
	require.Equal(t, base.Add(3*time.Hour), reappeared.LastSeenAt)
	require.Equal(t, 8, countObservationEvents(t, accountID), "every affected projection gets one event per accepted batch")
	require.Equal(t, []string{"discovered", "missing", "missing", "reappeared"}, loadObservationEventTypes(t, accountID, "claude-a"))
}

func TestModelGovernanceRepository_RecordDiscoveryRollsBackBatchWhenProjectionFails(t *testing.T) {
	ctx := context.Background()
	accountID := createGovernanceObservationAccount(t, "openai", `{}`)
	repo := NewModelObservationRepository(integrationDB)
	provider := service.GovernanceProvider("openai")
	idempotencyKey := "governance-rollback-" + fmt.Sprint(accountID)

	_, err := repo.RecordDiscovery(ctx, service.DiscoveryBatchInput{
		IdempotencyKey:  idempotencyKey,
		AccountID:       accountID,
		AccountProvider: &provider,
		RoutingPlatform: "openai",
		ModelIDs:        []string{string(make([]byte, 256))},
		RawSnapshot:     []byte(`{"source":"rollback-test"}`),
		ObservedAt:      time.Date(2026, time.August, 18, 11, 0, 0, 0, time.UTC),
	})
	require.Error(t, err)

	var batchCount int
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM model_classification_batches WHERE idempotency_key = $1
	`, idempotencyKey).Scan(&batchCount))
	require.Zero(t, batchCount)
	require.Empty(t, loadObservationProjections(t, accountID))
}

func TestModelGovernanceRepository_RecordDiscoveryKeepsRoutingOnlyPlatformsIgnored(t *testing.T) {
	ctx := context.Background()
	for _, platform := range []string{"antigravity", "composite", "future-router"} {
		platform := platform
		t.Run(platform, func(t *testing.T) {
			accountID := createGovernanceObservationAccount(t, platform, `{}`)
			repo := NewModelObservationRepository(integrationDB)
			_, err := repo.RecordDiscovery(ctx, service.DiscoveryBatchInput{
				IdempotencyKey:  "routing-only-" + fmt.Sprint(accountID),
				AccountID:       accountID,
				AccountProvider: nil,
				RoutingPlatform: platform,
				ModelIDs:        []string{"unattributed-model"},
				RawSnapshot:     []byte(`{"models":["unattributed-model"]}`),
				ObservedAt:      time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC),
			})
			require.NoError(t, err)
			projection := loadObservationProjection(t, accountID, "unattributed-model")
			require.Equal(t, "ignored", projection.Classification)
			require.Equal(t, "unsupported_routing_platform", projection.Reason)
		})
	}
}

func TestModelGovernanceRepository_RecordDiscoveryStaleBatchPersistsEvidenceOnly(t *testing.T) {
	ctx := context.Background()
	accountID := createGovernanceObservationAccount(t, "openai", `{}`)
	repo := NewModelObservationRepository(integrationDB)
	provider := service.GovernanceProvider("openai")
	newerAt := time.Date(2026, time.August, 18, 15, 0, 0, 0, time.UTC)
	olderAt := newerAt.Add(-time.Hour)
	connectionNew := int64(901)
	connectionOld := int64(900)

	_, err := repo.RecordDiscovery(ctx, service.DiscoveryBatchInput{
		IdempotencyKey: "newer-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ConnectionID: &connectionNew,
		ModelIDs: []string{"new-model"}, RawSnapshot: []byte(`{"payload":{"data":[{"id":"new-model"}]}}`), ObservedAt: newerAt,
	})
	require.NoError(t, err)
	before := loadObservationProjectionDetails(t, accountID, "new-model")
	eventsBefore := countObservationEvents(t, accountID)

	staleSnapshot := []byte(`{"payload":{"data":[{"id":"old-model"}]},"metadata":{"stale":true}}`)
	staleBatchID, err := repo.RecordDiscovery(ctx, service.DiscoveryBatchInput{
		IdempotencyKey: "older-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ConnectionID: &connectionOld,
		ModelIDs: []string{"old-model"}, RawSnapshot: staleSnapshot, ObservedAt: olderAt,
	})
	require.NoError(t, err)
	require.NotEmpty(t, staleBatchID)
	require.Equal(t, before, loadObservationProjectionDetails(t, accountID, "new-model"))
	require.Empty(t, loadObservationProjectionsByID(t, accountID, "old-model"))
	require.Equal(t, eventsBefore, countObservationEvents(t, accountID))
	var stored struct {
		Evidence json.RawMessage `json:"evidence"`
	}
	require.NoError(t, json.Unmarshal([]byte(loadBatchRawSnapshot(t, staleBatchID)), &stored))
	require.JSONEq(t, string(staleSnapshot), string(stored.Evidence))
}

func TestModelGovernanceRepository_RecordDiscoveryEqualWatermarkUsesHighestProvenanceDigest(t *testing.T) {
	ctx := context.Background()
	repo := NewModelObservationRepository(integrationDB)
	provider := service.GovernanceProvider("openai")
	observedAt := time.Date(2026, time.August, 18, 15, 30, 0, 0, time.UTC)
	connectionA, connectionB := int64(910), int64(920)
	accountID := createGovernanceObservationAccount(t, "openai", `{}`)
	secondAccountID := createGovernanceObservationAccount(t, "openai", `{}`)
	inputA := func(accountID int64) service.DiscoveryBatchInput {
		return service.DiscoveryBatchInput{
			AccountID: accountID, AccountProvider: &provider, RoutingPlatform: "openai", ConnectionID: &connectionA,
			ModelIDs: []string{"model-a"}, RawSnapshot: []byte(`{"catalog":"a"}`), ObservedAt: observedAt,
		}
	}
	inputB := func(accountID int64, rawSnapshot []byte) service.DiscoveryBatchInput {
		return service.DiscoveryBatchInput{
			AccountID: accountID, AccountProvider: &provider, RoutingPlatform: "openai", ConnectionID: &connectionB,
			ModelIDs: []string{"model-b"}, RawSnapshot: rawSnapshot, ObservedAt: observedAt,
		}
	}
	var winningSnapshot []byte
	for nonce := 0; ; nonce++ {
		candidate := []byte(fmt.Sprintf(`{"catalog":"b","nonce":%d}`, nonce))
		if discoveryInputDigest(t, inputB(accountID, candidate)) > discoveryInputDigest(t, inputA(accountID)) &&
			discoveryInputDigest(t, inputB(secondAccountID, candidate)) > discoveryInputDigest(t, inputA(secondAccountID)) {
			winningSnapshot = candidate
			break
		}
	}

	run := func(label string, accountID int64, first, second service.DiscoveryBatchInput) ([]observationProjectionDetails, map[string][]string) {
		t.Helper()
		first.IdempotencyKey = fmt.Sprintf("equal-watermark-%s-first-%d", label, accountID)
		second.IdempotencyKey = fmt.Sprintf("equal-watermark-%s-second-%d", label, accountID)
		firstBatchID, err := repo.RecordDiscovery(ctx, first)
		require.NoError(t, err)
		secondBatchID, err := repo.RecordDiscovery(ctx, second)
		require.NoError(t, err)
		require.Equal(t, 2, countDiscoveryBatches(t, accountID), "both conflicting batches remain durable evidence")
		return loadObservationProjectionDetailsForAccount(t, accountID), map[string][]string{
			"first":  loadBatchEventTypes(t, firstBatchID),
			"second": loadBatchEventTypes(t, secondBatchID),
		}
	}

	lowThenHighProjection, lowThenHighEvents := run(
		"low-high", accountID, inputA(accountID), inputB(accountID, winningSnapshot),
	)
	assertProjectionSnapshotOwner(t, accountID, "model-b", inputB(accountID, winningSnapshot))
	require.NotEmpty(t, lowThenHighEvents["first"], "the initial lower digest truthfully projects on arrival")
	require.Contains(t, lowThenHighEvents["second"], "projection_replaced", "the later winner records replacement history")
	highThenLowProjection, highThenLowEvents := run(
		"high-low", secondAccountID, inputB(secondAccountID, winningSnapshot), inputA(secondAccountID),
	)
	assertProjectionSnapshotOwner(t, secondAccountID, "model-b", inputB(secondAccountID, winningSnapshot))
	require.NotEmpty(t, highThenLowEvents["first"])
	require.Empty(t, highThenLowEvents["second"], "a losing batch arriving after the winner is evidence-only")
	require.Equal(t, withoutSnapshotBatchIDs(highThenLowProjection), withoutSnapshotBatchIDs(lowThenHighProjection),
		"presence, miss streak, timestamps, snapshot references, and connections must follow the tuple winner")
}

func TestModelGovernanceRepository_RecordDiscoveryCanonicalMicrosecondConvergesAcrossArrivalOrders(t *testing.T) {
	ctx := context.Background()
	repo := NewModelObservationRepository(integrationDB)
	provider := service.GovernanceProvider("openai")
	base := time.Date(2026, time.August, 19, 13, 0, 0, 0, time.UTC)
	canonicalAt := base.Truncate(time.Microsecond)
	connectionA, connectionB := int64(931), int64(932)
	inputA := service.DiscoveryBatchInput{
		AccountProvider: &provider, RoutingPlatform: "openai", ConnectionID: &connectionA,
		ModelIDs: []string{"model-a"}, RawSnapshot: []byte(`{"catalog":"sub-microsecond-a"}`), ObservedAt: base.Add(100 * time.Nanosecond),
	}
	inputB := service.DiscoveryBatchInput{
		AccountProvider: &provider, RoutingPlatform: "openai", ConnectionID: &connectionB,
		ModelIDs: []string{"model-b"}, RawSnapshot: []byte(`{"catalog":"sub-microsecond-b"}`), ObservedAt: base.Add(200 * time.Nanosecond),
	}
	firstAccountID := createGovernanceObservationAccount(t, "openai", `{}`)
	secondAccountID := createGovernanceObservationAccount(t, "openai", `{}`)
	for nonce := 0; ; nonce++ {
		inputB.RawSnapshot = []byte(fmt.Sprintf(`{"catalog":"sub-microsecond-b","nonce":%d}`, nonce))
		firstA, firstB := inputA, inputB
		firstA.AccountID, firstB.AccountID = firstAccountID, firstAccountID
		secondA, secondB := inputA, inputB
		secondA.AccountID, secondB.AccountID = secondAccountID, secondAccountID
		firstA.ObservedAt, firstB.ObservedAt = canonicalAt, canonicalAt
		secondA.ObservedAt, secondB.ObservedAt = canonicalAt, canonicalAt
		if discoveryInputDigest(t, firstB) > discoveryInputDigest(t, firstA) &&
			discoveryInputDigest(t, secondB) > discoveryInputDigest(t, secondA) {
			break
		}
	}
	run := func(label string, accountID int64, first, second service.DiscoveryBatchInput) ([]observationProjectionDetails, string) {
		t.Helper()
		first.AccountID, second.AccountID = accountID, accountID
		canonicalFirst, canonicalSecond := first, second
		canonicalFirst.ObservedAt, canonicalSecond.ObservedAt = canonicalAt, canonicalAt
		winnerModelID := first.ModelIDs[0]
		if discoveryInputDigest(t, canonicalSecond) > discoveryInputDigest(t, canonicalFirst) {
			winnerModelID = second.ModelIDs[0]
		}
		first.IdempotencyKey = fmt.Sprintf("canonical-microsecond-%s-first-%d", label, accountID)
		second.IdempotencyKey = fmt.Sprintf("canonical-microsecond-%s-second-%d", label, accountID)
		firstBatchID, err := repo.RecordDiscovery(ctx, first)
		require.NoError(t, err)
		secondBatchID, err := repo.RecordDiscovery(ctx, second)
		require.NoError(t, err)
		require.Equal(t, canonicalAt, loadBatchObservedAt(t, firstBatchID))
		require.Equal(t, canonicalAt, loadBatchObservedAt(t, secondBatchID))
		assertBatchEventTimesCanonical(t, firstBatchID)
		assertBatchEventTimesCanonical(t, secondBatchID)
		for _, projection := range loadObservationProjectionDetailsForAccount(t, accountID) {
			require.Equal(t, canonicalAt, projection.FirstSeenAt)
			require.Equal(t, canonicalAt, projection.LastSeenAt)
		}
		return loadObservationProjectionDetailsForAccount(t, accountID), winnerModelID
	}

	firstProjection, firstWinner := run("a-b", firstAccountID, inputA, inputB)
	require.Equal(t, []string{firstWinner}, loadPresentObservationModelIDs(t, firstAccountID))
	firstWinnerInput := inputA
	if firstWinner == inputB.ModelIDs[0] {
		firstWinnerInput = inputB
	}
	firstWinnerInput.AccountID = firstAccountID
	firstWinnerInput.ObservedAt = canonicalAt
	assertProjectionSnapshotOwner(t, firstAccountID, firstWinner, firstWinnerInput)
	secondProjection, secondWinner := run("b-a", secondAccountID, inputB, inputA)
	require.Equal(t, withoutSnapshotBatchIDs(firstProjection), withoutSnapshotBatchIDs(secondProjection))
	require.Equal(t, []string{secondWinner}, loadPresentObservationModelIDs(t, secondAccountID))
	secondWinnerInput := inputA
	if secondWinner == inputB.ModelIDs[0] {
		secondWinnerInput = inputB
	}
	secondWinnerInput.AccountID = secondAccountID
	secondWinnerInput.ObservedAt = canonicalAt
	assertProjectionSnapshotOwner(t, secondAccountID, secondWinner, secondWinnerInput)
}

func TestModelGovernanceRepository_RecordDiscoveryDifferentCanonicalMicrosecondsRemainTimeOrdered(t *testing.T) {
	ctx := context.Background()
	repo := NewModelObservationRepository(integrationDB)
	provider := service.GovernanceProvider("openai")
	accountID := createGovernanceObservationAccount(t, "openai", `{}`)
	base := time.Date(2026, time.August, 19, 13, 30, 0, 0, time.UTC)
	older := service.DiscoveryBatchInput{
		IdempotencyKey: "canonical-order-older-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: []string{"older-model"},
		RawSnapshot: []byte(`{"catalog":"older"}`), ObservedAt: base.Add(900 * time.Nanosecond),
	}
	newer := service.DiscoveryBatchInput{
		IdempotencyKey: "canonical-order-newer-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: []string{"newer-model"},
		RawSnapshot: []byte(`{"catalog":"newer"}`), ObservedAt: base.Add(1100 * time.Nanosecond),
	}

	_, err := repo.RecordDiscovery(ctx, newer)
	require.NoError(t, err)
	olderBatchID, err := repo.RecordDiscovery(ctx, older)
	require.NoError(t, err)
	require.Empty(t, loadBatchEventTypes(t, olderBatchID))
	require.Equal(t, []string{"newer-model"}, loadPresentObservationModelIDs(t, accountID))
}

func TestModelGovernanceRepository_RecordDiscoveryEqualWatermarkEmptyWinnerClearsProjection(t *testing.T) {
	ctx := context.Background()
	accountID := createGovernanceObservationAccount(t, "openai", `{}`)
	repo := NewModelObservationRepository(integrationDB)
	provider := service.GovernanceProvider("openai")
	observedAt := time.Date(2026, time.August, 18, 15, 45, 0, 0, time.UTC)
	populated := service.DiscoveryBatchInput{
		IdempotencyKey: "equal-empty-populated-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: []string{"temporary-model"},
		RawSnapshot: []byte(`{"catalog":"populated"}`), ObservedAt: observedAt,
	}
	empty := service.DiscoveryBatchInput{
		IdempotencyKey: "equal-empty-winner-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", RawSnapshot: []byte(`{"catalog":"empty"}`), ObservedAt: observedAt,
	}
	for nonce := 0; ; nonce++ {
		_, populatedDigest, err := discoveryPersistenceEnvelope(populated)
		require.NoError(t, err)
		_, emptyDigest, err := discoveryPersistenceEnvelope(empty)
		require.NoError(t, err)
		if emptyDigest > populatedDigest {
			break
		}
		empty.RawSnapshot = []byte(fmt.Sprintf(`{"catalog":"empty","nonce":%d}`, nonce))
	}

	populatedBatchID, err := repo.RecordDiscovery(ctx, populated)
	require.NoError(t, err)
	emptyBatchID, err := repo.RecordDiscovery(ctx, empty)
	require.NoError(t, err)
	require.Empty(t, loadObservationProjections(t, accountID), "an empty higher-digest winner owns an empty projection")
	require.Contains(t, loadBatchEventTypes(t, emptyBatchID), "projection_replaced")
	require.Equal(t, 2, countDiscoveryBatches(t, accountID))
	assertBatchSemanticOwner(t, populatedBatchID, populated)
	assertBatchSemanticOwner(t, emptyBatchID, empty)
}

func TestModelGovernanceRepository_RecordDiscoveryEqualWatermarkReplacementAlwaysRestoresPreWatermarkBaseline(t *testing.T) {
	ctx := context.Background()
	accountID := createGovernanceObservationAccount(t, "openai", `{}`)
	repo := NewModelObservationRepository(integrationDB)
	provider := service.GovernanceProvider("openai")
	baselineAt := time.Date(2026, time.August, 18, 15, 50, 0, 0, time.UTC)
	watermark := baselineAt.Add(time.Hour)
	_, err := repo.RecordDiscovery(ctx, service.DiscoveryBatchInput{
		IdempotencyKey: "equal-chain-baseline-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: []string{"baseline-model"},
		RawSnapshot: []byte(`{"catalog":"baseline"}`), ObservedAt: baselineAt,
	})
	require.NoError(t, err)

	inputs := make([]service.DiscoveryBatchInput, 3)
	for index := range inputs {
		inputs[index] = service.DiscoveryBatchInput{
			IdempotencyKey: fmt.Sprintf("equal-chain-%d-%d", index, accountID), AccountID: accountID,
			AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: []string{fmt.Sprintf("winner-%d", index)},
			RawSnapshot: []byte(fmt.Sprintf(`{"catalog":"winner-%d"}`, index)), ObservedAt: watermark,
		}
	}
	for nonce := 0; ; nonce++ {
		inputs[1].RawSnapshot = []byte(fmt.Sprintf(`{"catalog":"winner-1","nonce":%d}`, nonce))
		if discoveryInputDigest(t, inputs[1]) > discoveryInputDigest(t, inputs[0]) {
			break
		}
	}
	for nonce := 0; ; nonce++ {
		inputs[2].RawSnapshot = []byte(fmt.Sprintf(`{"catalog":"winner-2","nonce":%d}`, nonce))
		if discoveryInputDigest(t, inputs[2]) > discoveryInputDigest(t, inputs[1]) {
			break
		}
	}
	var winningBatchID string
	for _, input := range inputs {
		winningBatchID, err = repo.RecordDiscovery(ctx, input)
		require.NoError(t, err)
	}

	baseline := loadObservationProjection(t, accountID, "baseline-model")
	require.Equal(t, "missing", baseline.Presence)
	require.Equal(t, 1, baseline.MissStreak, "only the final winning complete catalog may advance the baseline miss streak")
	require.Equal(t, []string{"winner-2"}, loadPresentObservationModelIDs(t, accountID))
	assertBatchSemanticOwner(t, winningBatchID, inputs[2])
	assertProjectionSnapshotOwner(t, accountID, "winner-2", inputs[2])
}

func TestModelGovernanceRepository_RecordDiscoveryEqualTupleDuplicateIsEvidenceOnly(t *testing.T) {
	ctx := context.Background()
	accountID := createGovernanceObservationAccount(t, "openai", `{}`)
	repo := NewModelObservationRepository(integrationDB)
	provider := service.GovernanceProvider("openai")
	input := service.DiscoveryBatchInput{
		IdempotencyKey: "equal-tuple-first-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: []string{"same-model"},
		RawSnapshot: []byte(`{"catalog":"identical"}`),
		ObservedAt:  time.Date(2026, time.August, 18, 17, 0, 0, 0, time.UTC),
	}
	_, err := repo.RecordDiscovery(ctx, input)
	require.NoError(t, err)
	eventsBefore := countObservationEvents(t, accountID)

	input.IdempotencyKey = "equal-tuple-second-" + fmt.Sprint(accountID)
	duplicateBatchID, err := repo.RecordDiscovery(ctx, input)
	require.NoError(t, err)
	require.Equal(t, 2, countDiscoveryBatches(t, accountID))
	require.Equal(t, eventsBefore, countObservationEvents(t, accountID))
	require.Empty(t, loadBatchEventTypes(t, duplicateBatchID))
	require.Zero(t, loadObservationProjection(t, accountID, "same-model").MissStreak)
}

func TestModelGovernanceRepository_RecordDiscoveryEqualTupleDuplicateCannotOwnLaterReplacement(t *testing.T) {
	ctx := context.Background()
	accountID := createGovernanceObservationAccount(t, "openai", `{}`)
	repo := NewModelObservationRepository(integrationDB)
	provider := service.GovernanceProvider("openai")
	baselineAt := time.Date(2026, time.August, 18, 17, 5, 0, 0, time.UTC)
	watermark := baselineAt.Add(time.Hour)
	baseline := service.DiscoveryBatchInput{
		AccountID: accountID, AccountProvider: &provider, RoutingPlatform: "openai",
		ModelIDs: []string{"baseline-model"}, RawSnapshot: []byte(`{"catalog":"baseline"}`), ObservedAt: baselineAt,
	}
	projecting := service.DiscoveryBatchInput{
		AccountID: accountID, AccountProvider: &provider, RoutingPlatform: "openai",
		ModelIDs: []string{"projecting-model"}, RawSnapshot: []byte(`{"catalog":"projecting"}`), ObservedAt: watermark,
	}
	higher := service.DiscoveryBatchInput{
		AccountID: accountID, AccountProvider: &provider, RoutingPlatform: "openai",
		ModelIDs: []string{"final-model"}, ObservedAt: watermark,
	}
	higher = forceHigherDigest(t, projecting, higher, "final")

	run := func(label string, accountID int64, withDuplicate bool) []observationProjectionDetails {
		t.Helper()
		initial := baseline
		initial.AccountID = accountID
		initial.IdempotencyKey = fmt.Sprintf("duplicate-owner-%s-baseline-%d", label, accountID)
		owner := projecting
		owner.AccountID = accountID
		owner.IdempotencyKey = fmt.Sprintf("duplicate-owner-%s-projecting-%d", label, accountID)
		winner := higher
		winner.AccountID = accountID
		winner = forceHigherDigest(t, owner, winner, "final")
		winner.IdempotencyKey = fmt.Sprintf("duplicate-owner-%s-final-%d", label, accountID)

		_, err := repo.RecordDiscovery(ctx, initial)
		require.NoError(t, err)
		ownerBatchID, err := repo.RecordDiscovery(ctx, owner)
		require.NoError(t, err)
		if withDuplicate {
			duplicate := owner
			duplicate.IdempotencyKey = fmt.Sprintf("duplicate-owner-%s-evidence-%d", label, accountID)
			duplicateBatchID, duplicateErr := repo.RecordDiscovery(ctx, duplicate)
			require.NoError(t, duplicateErr)
			require.Empty(t, loadBatchEventTypes(t, duplicateBatchID))
			require.NotEmpty(t, ownerBatchID)
		}
		_, err = repo.RecordDiscovery(ctx, winner)
		require.NoError(t, err)
		return loadObservationProjectionDetailsForAccount(t, accountID)
	}

	withoutDuplicate := run("without", accountID, false)
	secondAccountID := createGovernanceObservationAccount(t, "openai", `{}`)
	withDuplicate := run("with", secondAccountID, true)
	require.Equal(t, withoutSnapshotBatchIDs(withoutDuplicate), withoutSnapshotBatchIDs(withDuplicate),
		"an evidence-only equal-tuple duplicate must not replace the batch that owns projection events")
	require.Equal(t, 1, loadObservationProjection(t, secondAccountID, "baseline-model").MissStreak)
}

func TestModelGovernanceRepository_RecordDiscoveryReplacementEventsDescribeCommittedStateChange(t *testing.T) {
	ctx := context.Background()
	accountID := createGovernanceObservationAccount(t, "openai", `{}`)
	repo := NewModelObservationRepository(integrationDB)
	provider := service.GovernanceProvider("openai")
	observedAt := time.Date(2026, time.August, 18, 17, 15, 0, 0, time.UTC)
	lower := service.DiscoveryBatchInput{
		IdempotencyKey: "truthful-lower-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: []string{"shared", "lower-only"},
		RawSnapshot: []byte(`{"catalog":"lower"}`), ObservedAt: observedAt,
	}
	higher := service.DiscoveryBatchInput{
		IdempotencyKey: "truthful-higher-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: []string{"shared", "higher-only"},
		ObservedAt: observedAt,
	}
	higher = forceHigherDigest(t, lower, higher, "truthful-higher")

	_, err := repo.RecordDiscovery(ctx, lower)
	require.NoError(t, err)
	higherBatchID, err := repo.RecordDiscovery(ctx, higher)
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"higher-only": "discovered",
		"lower-only":  "removed",
		"shared":      "observed",
	}, loadBatchReplacementTransitions(t, higherBatchID))
}

func TestModelGovernanceRepository_RecordDiscoveryHistoricalRemovedBaselineStaysAbsent(t *testing.T) {
	ctx := context.Background()
	accountID := createGovernanceObservationAccount(t, "openai", `{}`)
	repo := NewModelObservationRepository(integrationDB)
	provider := service.GovernanceProvider("openai")
	firstWatermark := time.Date(2026, time.August, 18, 17, 30, 0, 0, time.UTC)
	introduced := service.DiscoveryBatchInput{
		IdempotencyKey: "removed-introduced-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: []string{"removed-model"},
		RawSnapshot: []byte(`{"catalog":"introduced"}`), ObservedAt: firstWatermark,
	}
	removed := service.DiscoveryBatchInput{
		IdempotencyKey: "removed-winner-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ObservedAt: firstWatermark,
	}
	removed = forceHigherDigest(t, introduced, removed, "removed-winner")
	_, err := repo.RecordDiscovery(ctx, introduced)
	require.NoError(t, err)
	_, err = repo.RecordDiscovery(ctx, removed)
	require.NoError(t, err)
	require.Empty(t, loadObservationProjectionsByID(t, accountID, "removed-model"))

	secondWatermark := firstWatermark.Add(time.Hour)
	reintroduced := service.DiscoveryBatchInput{
		IdempotencyKey: "removed-reintroduced-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: []string{"removed-model"},
		RawSnapshot: []byte(`{"catalog":"reintroduced"}`), ObservedAt: secondWatermark,
	}
	finalWinner := service.DiscoveryBatchInput{
		IdempotencyKey: "removed-final-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ObservedAt: secondWatermark,
	}
	finalWinner = forceHigherDigest(t, reintroduced, finalWinner, "removed-final")
	_, err = repo.RecordDiscovery(ctx, reintroduced)
	require.NoError(t, err)
	_, err = repo.RecordDiscovery(ctx, finalWinner)
	require.NoError(t, err)
	require.Empty(t, loadObservationProjectionsByID(t, accountID, "removed-model"),
		"a historical removed transition represents row absence, not a missing baseline row")
}

func TestModelGovernanceRepository_RecordDiscoveryRemovalPreservesAdminStateForFutureReappearance(t *testing.T) {
	ctx := context.Background()
	accountID := createGovernanceObservationAccount(t, "openai", `{}`)
	repo := NewModelObservationRepository(integrationDB)
	provider := service.GovernanceProvider("openai")
	watermark := time.Date(2026, time.August, 18, 17, 45, 0, 0, time.UTC)
	introduced := service.DiscoveryBatchInput{
		IdempotencyKey: "admin-introduced-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: []string{"admin-model"},
		RawSnapshot: []byte(`{"catalog":"admin-introduced"}`), ObservedAt: watermark,
	}
	removed := service.DiscoveryBatchInput{
		IdempotencyKey: "admin-removed-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ObservedAt: watermark,
	}
	removed = forceHigherDigest(t, introduced, removed, "admin-removed")
	_, err := repo.RecordDiscovery(ctx, introduced)
	require.NoError(t, err)
	registryID := createGovernanceRegistryModel(t, "admin-model")
	require.NoError(t, setObservationAdminState(t, accountID, "admin-model", "approved", "admin_reviewed", registryID))
	_, err = repo.RecordDiscovery(ctx, removed)
	require.NoError(t, err)
	require.Empty(t, loadObservationProjectionsByID(t, accountID, "admin-model"))

	_, err = repo.RecordDiscovery(ctx, service.DiscoveryBatchInput{
		IdempotencyKey: "admin-reappeared-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: []string{"admin-model"},
		RawSnapshot: []byte(`{"catalog":"admin-reappeared"}`), ObservedAt: watermark.Add(time.Hour),
	})
	require.NoError(t, err)
	reappeared := loadObservationProjectionDetails(t, accountID, "admin-model")
	require.Equal(t, "approved", reappeared.Classification)
	require.Equal(t, "admin_reviewed", reappeared.Reason)
	require.Equal(t, sql.NullInt64{Int64: registryID, Valid: true}, reappeared.ResolvedRegistryID)
}

func TestModelGovernanceRepository_RecordDiscoveryReplacementPreservesAdminStateWhenWinnerKeepsTransientModel(t *testing.T) {
	ctx := context.Background()
	accountID := createGovernanceObservationAccount(t, "openai", `{}`)
	repo := NewModelObservationRepository(integrationDB)
	provider := service.GovernanceProvider("openai")
	watermark := time.Date(2026, time.August, 18, 17, 50, 0, 0, time.UTC)
	lower := service.DiscoveryBatchInput{
		IdempotencyKey: "admin-shared-lower-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: []string{"admin-shared"},
		RawSnapshot: []byte(`{"catalog":"admin-shared-lower"}`), ObservedAt: watermark,
	}
	higher := service.DiscoveryBatchInput{
		IdempotencyKey: "admin-shared-higher-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: []string{"admin-shared", "higher-only"},
		ObservedAt: watermark,
	}
	higher = forceHigherDigest(t, lower, higher, "admin-shared-higher")
	_, err := repo.RecordDiscovery(ctx, lower)
	require.NoError(t, err)
	registryID := createGovernanceRegistryModel(t, "admin-shared")
	require.NoError(t, setObservationAdminState(t, accountID, "admin-shared", "approved", "admin_reviewed", registryID))
	_, err = repo.RecordDiscovery(ctx, higher)
	require.NoError(t, err)

	shared := loadObservationProjectionDetails(t, accountID, "admin-shared")
	require.Equal(t, "approved", shared.Classification)
	require.Equal(t, "admin_reviewed", shared.Reason)
	require.Equal(t, sql.NullInt64{Int64: registryID, Valid: true}, shared.ResolvedRegistryID)
}

func TestModelGovernanceRepository_RecordDiscoveryReplacementPreservesCurrentAdminStateForPreWatermarkRows(t *testing.T) {
	ctx := context.Background()
	repo := NewModelObservationRepository(integrationDB)
	provider := service.GovernanceProvider("openai")
	for _, tt := range []struct {
		name            string
		winningModelIDs []string
		wantPresence    string
	}{
		{name: "winner keeps row", winningModelIDs: []string{"shared-model", "higher-only"}, wantPresence: "present"},
		{name: "winner omits row", winningModelIDs: []string{"higher-only"}, wantPresence: "missing"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			accountID := createGovernanceObservationAccount(t, "openai", `{}`)
			baselineAt := time.Date(2026, time.August, 18, 17, 55, 0, 0, time.UTC)
			watermark := baselineAt.Add(time.Hour)
			_, err := repo.RecordDiscovery(ctx, service.DiscoveryBatchInput{
				IdempotencyKey: "admin-prewatermark-baseline-" + fmt.Sprint(accountID), AccountID: accountID,
				AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: []string{"shared-model"},
				RawSnapshot: []byte(`{"catalog":"baseline"}`), ObservedAt: baselineAt,
			})
			require.NoError(t, err)
			lower := service.DiscoveryBatchInput{
				IdempotencyKey: "admin-prewatermark-lower-" + fmt.Sprint(accountID), AccountID: accountID,
				AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: []string{"shared-model", "lower-only"},
				RawSnapshot: []byte(`{"catalog":"lower"}`), ObservedAt: watermark,
			}
			_, err = repo.RecordDiscovery(ctx, lower)
			require.NoError(t, err)
			registryID := createGovernanceRegistryModel(t, "admin-prewatermark-"+tt.name)
			require.NoError(t, setObservationAdminState(t, accountID, "shared-model", "approved", "admin_reviewed", registryID))

			higher := service.DiscoveryBatchInput{
				IdempotencyKey: "admin-prewatermark-higher-" + fmt.Sprint(accountID), AccountID: accountID,
				AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: tt.winningModelIDs, ObservedAt: watermark,
			}
			higher = forceHigherDigest(t, lower, higher, "admin-prewatermark-higher-"+tt.name)
			_, err = repo.RecordDiscovery(ctx, higher)
			require.NoError(t, err)

			shared := loadObservationProjectionDetails(t, accountID, "shared-model")
			require.Equal(t, "approved", shared.Classification)
			require.Equal(t, "admin_reviewed", shared.Reason)
			require.Equal(t, sql.NullInt64{Int64: registryID, Valid: true}, shared.ResolvedRegistryID)
			require.Equal(t, tt.wantPresence, shared.Presence)
		})
	}
}

func TestModelGovernanceRepository_RecordDiscoveryStrictlyOlderAfterNewerNeverReplays(t *testing.T) {
	ctx := context.Background()
	accountID := createGovernanceObservationAccount(t, "openai", `{}`)
	repo := NewModelObservationRepository(integrationDB)
	provider := service.GovernanceProvider("openai")
	newerAt := time.Date(2026, time.August, 18, 18, 0, 0, 0, time.UTC)
	_, err := repo.RecordDiscovery(ctx, service.DiscoveryBatchInput{
		IdempotencyKey: "strict-newer-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: []string{"newer-model"},
		RawSnapshot: []byte(`{"catalog":"newer"}`), ObservedAt: newerAt,
	})
	require.NoError(t, err)
	projectionBefore := loadObservationProjectionDetailsForAccount(t, accountID)
	eventsBefore := countObservationEvents(t, accountID)
	olderBatchID, err := repo.RecordDiscovery(ctx, service.DiscoveryBatchInput{
		IdempotencyKey: "strict-older-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: []string{"older-model"},
		RawSnapshot: []byte(`{"catalog":"older"}`), ObservedAt: newerAt.Add(-time.Hour),
	})
	require.NoError(t, err)
	require.Equal(t, projectionBefore, loadObservationProjectionDetailsForAccount(t, accountID))
	require.Equal(t, eventsBefore, countObservationEvents(t, accountID))
	require.Empty(t, loadBatchEventTypes(t, olderBatchID))
	require.Equal(t, 2, countDiscoveryBatches(t, accountID), "strictly older input remains durable evidence without retroactive replay")
}

func TestModelGovernanceRepository_ObservationEventsSnapshotCompleteCommittedPostState(t *testing.T) {
	ctx := context.Background()
	accountID := createGovernanceObservationAccount(t, "openai", `{}`)
	repo := NewModelObservationRepository(integrationDB)
	provider := service.GovernanceProvider("openai")
	firstAt := time.Date(2026, time.August, 18, 18, 15, 0, 0, time.UTC)
	connectionID := int64(701)
	first := service.DiscoveryBatchInput{
		IdempotencyKey: "post-state-first-" + fmt.Sprint(accountID), AccountID: accountID,
		ConnectionID: &connectionID, AccountProvider: &provider, RoutingPlatform: "openai",
		ModelIDs: []string{"retained-model"}, RawSnapshot: []byte(`{"catalog":"post-state-first"}`), ObservedAt: firstAt,
	}
	_, err := repo.RecordDiscovery(ctx, first)
	require.NoError(t, err)

	secondAt := firstAt.Add(time.Hour)
	second := service.DiscoveryBatchInput{
		IdempotencyKey: "post-state-second-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: []string{"retained-model", "removed-model"},
		RawSnapshot: []byte(`{"catalog":"post-state-second"}`), ObservedAt: secondAt,
	}
	_, err = repo.RecordDiscovery(ctx, second)
	require.NoError(t, err)
	registryID := createGovernanceRegistryModel(t, "removed-post-state")
	require.NoError(t, setObservationAdminState(t, accountID, "removed-model", "approved", "admin_reviewed", registryID))
	third := service.DiscoveryBatchInput{
		IdempotencyKey: "post-state-third-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: []string{"retained-model"}, ObservedAt: secondAt,
	}
	third = forceHigherDigest(t, second, third, "post-state-third")
	thirdBatchID, err := repo.RecordDiscovery(ctx, third)
	require.NoError(t, err)
	require.Empty(t, loadObservationProjectionsByID(t, accountID, "removed-model"))

	events := loadDirectObservationEventPostStates(t, accountID)
	require.NotEmpty(t, events)
	for _, event := range events {
		require.True(t, event.HasConnectionID, "%s/%s must snapshot connection_id, including null", event.ModelID, event.Transition)
		require.True(t, event.HasResolvedRegistryID, "%s/%s must snapshot resolved_registry_id, including null", event.ModelID, event.Transition)
		require.True(t, event.FirstSeenAt.Valid)
		require.True(t, event.LastSeenAt.Valid)
		require.NotEmpty(t, event.BatchID)
		require.NotEmpty(t, event.SnapshotBatchID)
		require.False(t, event.HasRawSnapshot)
		require.False(t, event.LastSeenAt.Time.Before(event.FirstSeenAt.Time))
	}
	removed := loadDirectBatchEventPostState(t, thirdBatchID, "removed-model")
	require.Equal(t, "removed", removed.Transition)
	require.Equal(t, "approved", removed.Classification)
	require.Equal(t, "admin_reviewed", removed.Reason)
	require.Equal(t, sql.NullInt64{Int64: registryID, Valid: true}, removed.ResolvedRegistryID)
}

func TestModelGovernanceRepository_RecordDiscoveryLaterReplacementConvergesMetadataAcrossPriorWatermarkOrders(t *testing.T) {
	ctx := context.Background()
	accountID := createGovernanceObservationAccount(t, "openai", `{}`)
	repo := NewModelObservationRepository(integrationDB)
	provider := service.GovernanceProvider("openai")
	firstAt := time.Date(2026, time.August, 18, 18, 30, 0, 0, time.UTC)
	priorAt := firstAt.Add(time.Hour)
	laterAt := priorAt.Add(time.Hour)
	baseConnection, lowConnection := int64(800), int64(801)
	base := service.DiscoveryBatchInput{
		AccountID: accountID, ConnectionID: &baseConnection, AccountProvider: &provider, RoutingPlatform: "openai",
		ModelIDs: []string{"shared-model"}, RawSnapshot: []byte(`{"catalog":"base"}`), ObservedAt: firstAt,
	}
	priorLow := service.DiscoveryBatchInput{
		AccountID: accountID, ConnectionID: &lowConnection, AccountProvider: &provider, RoutingPlatform: "openai",
		ModelIDs: []string{"shared-model"}, RawSnapshot: []byte(`{"catalog":"prior-low"}`), ObservedAt: priorAt,
	}
	priorHigh := service.DiscoveryBatchInput{
		AccountID: accountID, AccountProvider: &provider, RoutingPlatform: "openai", ObservedAt: priorAt,
	}
	priorHigh = forceHigherDigest(t, priorLow, priorHigh, "prior-high")
	laterLow := service.DiscoveryBatchInput{
		AccountID: accountID, AccountProvider: &provider, RoutingPlatform: "openai",
		ModelIDs: []string{"shared-model", "later-low-only"}, RawSnapshot: []byte(`{"catalog":"later-low"}`), ObservedAt: laterAt,
	}
	laterHigh := service.DiscoveryBatchInput{
		AccountID: accountID, AccountProvider: &provider, RoutingPlatform: "openai",
		ModelIDs: []string{"later-high-only"}, ObservedAt: laterAt,
	}
	laterHigh = forceHigherDigest(t, laterLow, laterHigh, "later-high")

	run := func(label string, firstPrior, secondPrior service.DiscoveryBatchInput) observationProjectionDetails {
		t.Helper()
		initial := base
		initial.AccountID = accountID
		firstPrior.AccountID, secondPrior.AccountID = accountID, accountID
		if firstPrior.ModelIDs == nil {
			firstPrior = forceHigherDigest(t, secondPrior, firstPrior, "prior-high")
		} else if secondPrior.ModelIDs == nil {
			secondPrior = forceHigherDigest(t, firstPrior, secondPrior, "prior-high")
		}
		initial.IdempotencyKey = fmt.Sprintf("metadata-%s-base-%d", label, accountID)
		firstPrior.IdempotencyKey = fmt.Sprintf("metadata-%s-prior-first-%d", label, accountID)
		secondPrior.IdempotencyKey = fmt.Sprintf("metadata-%s-prior-second-%d", label, accountID)
		low := laterLow
		low.AccountID = accountID
		low.IdempotencyKey = fmt.Sprintf("metadata-%s-later-low-%d", label, accountID)
		high := laterHigh
		high.AccountID = accountID
		high = forceHigherDigest(t, low, high, "later-high")
		high.IdempotencyKey = fmt.Sprintf("metadata-%s-later-high-%d", label, accountID)
		_, err := repo.RecordDiscovery(ctx, initial)
		require.NoError(t, err)
		_, err = repo.RecordDiscovery(ctx, firstPrior)
		require.NoError(t, err)
		_, err = repo.RecordDiscovery(ctx, secondPrior)
		require.NoError(t, err)
		_, err = repo.RecordDiscovery(ctx, low)
		require.NoError(t, err)
		_, err = repo.RecordDiscovery(ctx, high)
		require.NoError(t, err)
		return loadObservationProjectionDetails(t, accountID, "shared-model")
	}

	lowThenHigh := run("low-high", priorLow, priorHigh)
	firstAccountID := accountID
	secondAccountID := createGovernanceObservationAccount(t, "openai", `{}`)
	for _, input := range []*service.DiscoveryBatchInput{&base, &priorLow, &priorHigh, &laterLow, &laterHigh} {
		input.AccountID = secondAccountID
	}
	accountID = secondAccountID
	highThenLow := run("high-low", priorHigh, priorLow)
	firstBase := base
	firstBase.AccountID = firstAccountID
	secondBase := base
	secondBase.AccountID = secondAccountID
	assertProjectionSnapshotOwner(t, firstAccountID, "shared-model", firstBase)
	assertProjectionSnapshotOwner(t, secondAccountID, "shared-model", secondBase)
	highThenLow.SnapshotBatchID = ""
	lowThenHigh.SnapshotBatchID = ""
	require.Equal(t, highThenLow, lowThenHigh,
		"connection, first/last seen, snapshot reference, registry, classification, presence, and miss state must use the prior canonical final event")
	require.Equal(t, "missing", lowThenHigh.Presence)
	require.Equal(t, sql.NullInt64{Int64: baseConnection, Valid: true}, lowThenHigh.ConnectionID)
}

func TestModelGovernanceRepository_RecordDiscoveryConcurrentConflictingCatalogsConvergeToDigestWinner(t *testing.T) {
	ctx := context.Background()
	accountID := createGovernanceObservationAccount(t, "openai", `{}`)
	provider := service.GovernanceProvider("openai")
	observedAt := time.Date(2026, time.August, 18, 20, 0, 0, 0, time.UTC)
	lower := service.DiscoveryBatchInput{
		IdempotencyKey: "concurrent-conflict-low-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: []string{"low-model"},
		RawSnapshot: []byte(`{"catalog":"concurrent-low"}`), ObservedAt: observedAt,
	}
	higher := service.DiscoveryBatchInput{
		IdempotencyKey: "concurrent-conflict-high-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: []string{"high-model"}, ObservedAt: observedAt,
	}
	higher = forceHigherDigest(t, lower, higher, "concurrent-high")
	start := make(chan struct{})
	errs := make(chan error, 2)
	for _, input := range []service.DiscoveryBatchInput{lower, higher} {
		input := input
		go func() {
			<-start
			_, err := NewModelObservationRepository(integrationDB).RecordDiscovery(ctx, input)
			errs <- err
		}()
	}
	close(start)
	for range 2 {
		require.NoError(t, <-errs)
	}
	require.Equal(t, []string{"high-model"}, loadPresentObservationModelIDs(t, accountID))
	require.Equal(t, 2, countDiscoveryBatches(t, accountID))
}

func TestModelGovernanceRepository_RecordDiscoveryRejectsInvalidProviderAndNonObjectSnapshot(t *testing.T) {
	ctx := context.Background()
	accountID := createGovernanceObservationAccount(t, "openai", `{}`)
	repo := NewModelObservationRepository(integrationDB)
	invalidProvider := service.GovernanceProvider("azure-openai")

	_, err := repo.RecordDiscovery(ctx, service.DiscoveryBatchInput{
		IdempotencyKey: "invalid-provider-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &invalidProvider, RoutingPlatform: "openai", ModelIDs: []string{"model"},
		RawSnapshot: []byte(`{"payload":{}}`), ObservedAt: time.Now().UTC(),
	})
	require.ErrorContains(t, err, "account provider")

	provider := service.GovernanceProvider("openai")
	_, err = repo.RecordDiscovery(ctx, service.DiscoveryBatchInput{
		IdempotencyKey: "array-snapshot-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: []string{"model"},
		RawSnapshot: []byte(`[{"id":"model"}]`), ObservedAt: time.Now().UTC(),
	})
	require.ErrorContains(t, err, "JSON object")
}

func TestModelGovernanceRepository_RecordDiscoveryRejectsIdempotencyCollisionAcrossOwners(t *testing.T) {
	ctx := context.Background()
	firstAccountID := createGovernanceObservationAccount(t, "openai", `{}`)
	secondAccountID := createGovernanceObservationAccount(t, "openai", `{}`)
	repo := NewModelObservationRepository(integrationDB)
	provider := service.GovernanceProvider("openai")
	key := fmt.Sprintf("collision-%d-%d", firstAccountID, secondAccountID)
	observedAt := time.Date(2026, time.August, 18, 16, 0, 0, 0, time.UTC)

	firstBatchID, err := repo.RecordDiscovery(ctx, service.DiscoveryBatchInput{
		IdempotencyKey: key, AccountID: firstAccountID, AccountProvider: &provider,
		RoutingPlatform: "openai", ModelIDs: []string{"first"},
		RawSnapshot: []byte(`{"payload":{"data":[{"id":"first"}]}}`), ObservedAt: observedAt,
	})
	require.NoError(t, err)

	_, err = repo.RecordDiscovery(ctx, service.DiscoveryBatchInput{
		IdempotencyKey: key, AccountID: secondAccountID, AccountProvider: &provider,
		RoutingPlatform: "openai", ModelIDs: []string{"second"},
		RawSnapshot: []byte(`{"payload":{"data":[{"id":"second"}]}}`), ObservedAt: observedAt,
	})
	require.ErrorContains(t, err, "idempotency key collision")
	require.Empty(t, loadObservationProjections(t, secondAccountID))
	require.Equal(t, firstAccountID, loadBatchAccountID(t, firstBatchID))
}

func TestModelGovernanceRepository_RecordDiscoveryAppendsMissingEventsDeterministically(t *testing.T) {
	ctx := context.Background()
	accountID := createGovernanceObservationAccount(t, "openai", `{}`)
	repo := NewModelObservationRepository(integrationDB)
	provider := service.GovernanceProvider("openai")
	base := time.Date(2026, time.August, 18, 17, 0, 0, 0, time.UTC)

	_, err := repo.RecordDiscovery(ctx, service.DiscoveryBatchInput{
		IdempotencyKey: "deterministic-present-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: []string{"z-model", "a-model", "m-model"},
		RawSnapshot: []byte(`{"payload":{"data":[]}}`), ObservedAt: base,
	})
	require.NoError(t, err)
	batchID, err := repo.RecordDiscovery(ctx, service.DiscoveryBatchInput{
		IdempotencyKey: "deterministic-missing-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: nil,
		RawSnapshot: []byte(`{"payload":{"data":[]}}`), ObservedAt: base.Add(time.Hour),
	})
	require.NoError(t, err)
	require.Equal(t, []string{"a-model", "m-model", "z-model"}, loadBatchEventModelIDs(t, batchID))
}

func TestModelGovernanceRepository_RecordDiscoveryConcurrentSameAccountCompletesDeterministically(t *testing.T) {
	ctx := context.Background()
	accountID := createGovernanceObservationAccount(t, "openai", `{}`)
	provider := service.GovernanceProvider("openai")
	observedAt := time.Date(2026, time.August, 18, 18, 0, 0, 0, time.UTC)

	_, err := integrationDB.ExecContext(ctx, `
		CREATE OR REPLACE FUNCTION delay_governance_batch_insert() RETURNS TRIGGER AS $$
		BEGIN
			PERFORM pg_sleep(0.5);
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER trg_delay_governance_batch_insert
		AFTER INSERT ON model_classification_batches
		FOR EACH ROW EXECUTE FUNCTION delay_governance_batch_insert();
	`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(context.Background(), `DROP TRIGGER IF EXISTS trg_delay_governance_batch_insert ON model_classification_batches`)
		_, _ = integrationDB.ExecContext(context.Background(), `DROP FUNCTION IF EXISTS delay_governance_batch_insert()`)
	})

	start := make(chan struct{})
	errs := make(chan error, 2)
	batchIDs := make(chan string, 2)
	var wg sync.WaitGroup
	for index := 0; index < 2; index++ {
		index := index
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			batchID, recordErr := NewModelObservationRepository(integrationDB).RecordDiscovery(callCtx, service.DiscoveryBatchInput{
				IdempotencyKey: fmt.Sprintf("concurrent-%d-%d", accountID, index), AccountID: accountID,
				AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: []string{"shared-model"},
				RawSnapshot: []byte(`{"source":"concurrent-test"}`), ObservedAt: observedAt,
			})
			if recordErr == nil {
				batchIDs <- batchID
			}
			errs <- recordErr
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	close(batchIDs)

	for recordErr := range errs {
		require.NoError(t, recordErr)
	}
	require.Len(t, batchIDs, 2)
	require.Equal(t, observationProjection{
		ModelID: "shared-model", Classification: "discovered", Reason: "awaiting_registry_classification",
		Presence: "present", FirstSeenAt: observedAt, LastSeenAt: observedAt,
	}, loadObservationProjection(t, accountID, "shared-model"))
	require.Equal(t, []string{"discovered"}, loadObservationEventTypes(t, accountID, "shared-model"),
		"the equal-tuple duplicate is durable evidence but does not project twice")
	require.Equal(t, 2, countDiscoveryBatches(t, accountID))
}

func TestModelGovernanceRepository_ObservationEventsRetainIdentityAfterAccountDeletion(t *testing.T) {
	ctx := context.Background()
	accountID := createGovernanceObservationAccount(t, "openai", `{}`)
	provider := service.GovernanceProvider("openai")
	modelID := "reconstructable-model"

	batchID, err := NewModelObservationRepository(integrationDB).RecordDiscovery(ctx, service.DiscoveryBatchInput{
		IdempotencyKey: "reconstructable-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: []string{modelID},
		RawSnapshot: []byte(`{"source":"deletion-test"}`), ObservedAt: time.Date(2026, time.August, 18, 19, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)

	require.NoError(t, NewAccountRepository(integrationEntClient, integrationDB, nil).Delete(ctx, accountID))

	var storedAccountID int64
	var storedModelID, eventType, storedBatchID, snapshotBatchID, rawSnapshot, idempotencyKey string
	var storedConnectionID sql.NullInt64
	var storedObservedAt time.Time
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		SELECT e.account_id, e.upstream_model_id, e.event_type, e.batch_id, e.snapshot_batch_id,
		       b.connection_id, b.idempotency_key, b.observed_at, b.raw_snapshot::text
		FROM model_observation_events e
		JOIN model_classification_batches b ON b.batch_id = e.snapshot_batch_id
		WHERE e.account_id = $1 AND e.upstream_model_id = $2
	`, accountID, modelID).Scan(&storedAccountID, &storedModelID, &eventType, &storedBatchID, &snapshotBatchID,
		&storedConnectionID, &idempotencyKey, &storedObservedAt, &rawSnapshot))
	require.Equal(t, accountID, storedAccountID)
	require.Equal(t, modelID, storedModelID)
	require.Equal(t, "discovered", eventType)
	require.Equal(t, batchID, storedBatchID)
	require.Equal(t, batchID, snapshotBatchID)
	require.Equal(t, "reconstructable-"+fmt.Sprint(accountID), idempotencyKey)
	require.Contains(t, rawSnapshot, "deletion-test")
	require.False(t, storedObservedAt.IsZero())
	var batchCount, observationCount int
	var accountDeletedAt sql.NullTime
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM model_classification_batches WHERE batch_id = $1`, batchID).Scan(&batchCount))
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM model_observations WHERE account_id = $1`, accountID).Scan(&observationCount))
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT deleted_at FROM accounts WHERE id = $1`, accountID).Scan(&accountDeletedAt))
	require.Equal(t, 1, batchCount)
	require.Equal(t, 1, observationCount, "soft deletion preserves the current projection alongside immutable evidence")
	require.True(t, accountDeletedAt.Valid, "accountRepository.Delete must exercise the production soft-delete workflow")
}

func TestModelGovernanceRepository_AllBatchShapesSurviveAccountDeletion(t *testing.T) {
	ctx := context.Background()
	accountID := createGovernanceObservationAccount(t, "openai", `{}`)
	provider := service.GovernanceProvider("openai")
	repo := NewModelObservationRepository(integrationDB)
	base := time.Date(2026, time.August, 19, 15, 0, 0, 0, time.UTC)
	connectionID := int64(951)

	record := func(key string, modelIDs []string, raw string, observedAt time.Time) string {
		t.Helper()
		batchID, err := repo.RecordDiscovery(ctx, service.DiscoveryBatchInput{
			IdempotencyKey: key + fmt.Sprint(accountID), AccountID: accountID,
			ConnectionID: &connectionID, AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: modelIDs,
			RawSnapshot: []byte(raw), ObservedAt: observedAt,
		})
		require.NoError(t, err)
		return batchID
	}
	nonEmptyBatchID := record("survive-non-empty-", []string{"model"}, `{"shape":"non-empty"}`, base)
	emptyBatchID := record("survive-empty-", nil, `{"shape":"empty"}`, base.Add(time.Hour))
	staleBatchID := record("survive-stale-", []string{"stale"}, `{"shape":"stale"}`, base.Add(-time.Hour))
	equalLoserBatchID := record("survive-equal-loser-", nil, `{"shape":"empty"}`, base.Add(time.Hour))
	require.Empty(t, loadBatchEventTypes(t, staleBatchID))
	require.Empty(t, loadBatchEventTypes(t, equalLoserBatchID))

	_, err := integrationDB.ExecContext(ctx, `DELETE FROM accounts WHERE id = $1`, accountID)
	require.NoError(t, err)
	for batchID, shape := range map[string]string{
		nonEmptyBatchID:   "non-empty",
		emptyBatchID:      "empty",
		staleBatchID:      "stale",
		equalLoserBatchID: "empty",
	} {
		var storedAccountID int64
		var storedConnectionID int64
		var idempotencyKey, rawSnapshot, provenanceDigest string
		var observedAt time.Time
		require.NoError(t, integrationDB.QueryRowContext(ctx, `
			SELECT account_id, connection_id, idempotency_key, raw_snapshot::text,
			       raw_snapshot->'provenance'->>'digest', observed_at
			FROM model_classification_batches WHERE batch_id = $1
		`, batchID).Scan(&storedAccountID, &storedConnectionID, &idempotencyKey, &rawSnapshot, &provenanceDigest, &observedAt))
		require.Equal(t, accountID, storedAccountID)
		require.Equal(t, connectionID, storedConnectionID)
		require.NotEmpty(t, idempotencyKey)
		require.Contains(t, rawSnapshot, shape)
		require.NotEmpty(t, provenanceDigest)
		require.False(t, observedAt.IsZero())
	}
	require.NotEmpty(t, loadBatchEventTypes(t, nonEmptyBatchID))
	require.NotEmpty(t, loadBatchEventTypes(t, emptyBatchID))
}

func TestModelGovernanceRepository_EmptyZeroEventBatchSurvivesAccountDeletion(t *testing.T) {
	ctx := context.Background()
	accountID := createGovernanceObservationAccount(t, "openai", `{}`)
	provider := service.GovernanceProvider("openai")
	connectionID := int64(952)
	batchID, err := NewModelObservationRepository(integrationDB).RecordDiscovery(ctx, service.DiscoveryBatchInput{
		IdempotencyKey: "survive-empty-zero-events-" + fmt.Sprint(accountID), AccountID: accountID,
		ConnectionID: &connectionID, AccountProvider: &provider, RoutingPlatform: "openai",
		RawSnapshot: []byte(`{"shape":"empty-zero-events"}`), ObservedAt: time.Date(2026, time.August, 19, 16, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	require.Empty(t, loadBatchEventTypes(t, batchID))

	_, err = integrationDB.ExecContext(ctx, `DELETE FROM accounts WHERE id = $1`, accountID)
	require.NoError(t, err)
	var storedAccountID, storedConnectionID int64
	var rawSnapshot string
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		SELECT account_id, connection_id, raw_snapshot::text
		FROM model_classification_batches WHERE batch_id = $1
	`, batchID).Scan(&storedAccountID, &storedConnectionID, &rawSnapshot))
	require.Equal(t, accountID, storedAccountID)
	require.Equal(t, connectionID, storedConnectionID)
	require.Contains(t, rawSnapshot, "empty-zero-events")
}

func createGovernanceObservationAccount(t *testing.T, platform, credentials string) int64 {
	t.Helper()
	var accountID int64
	require.NoError(t, integrationDB.QueryRowContext(context.Background(), `
		INSERT INTO accounts (name, platform, type, credentials)
		VALUES ($1, $2, 'apikey', $3::jsonb)
		RETURNING id
	`, fmt.Sprintf("governance-observation-%s-%d", platform, time.Now().UnixNano()), platform, credentials).Scan(&accountID))
	return accountID
}

func loadObservationProjections(t *testing.T, accountID int64) []observationProjection {
	t.Helper()
	rows, err := integrationDB.QueryContext(context.Background(), `
		SELECT upstream_model_id, classification, classification_reason, upstream_presence,
		       miss_streak, first_seen_at, last_seen_at
		FROM model_observations WHERE account_id = $1 ORDER BY upstream_model_id
	`, accountID)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var result []observationProjection
	for rows.Next() {
		var item observationProjection
		require.NoError(t, rows.Scan(&item.ModelID, &item.Classification, &item.Reason, &item.Presence, &item.MissStreak, &item.FirstSeenAt, &item.LastSeenAt))
		result = append(result, item)
	}
	require.NoError(t, rows.Err())
	return result
}

func loadObservationProjection(t *testing.T, accountID int64, modelID string) observationProjection {
	t.Helper()
	var item observationProjection
	require.NoError(t, integrationDB.QueryRowContext(context.Background(), `
		SELECT upstream_model_id, classification, classification_reason, upstream_presence,
		       miss_streak, first_seen_at, last_seen_at
		FROM model_observations WHERE account_id = $1 AND upstream_model_id = $2
	`, accountID, modelID).Scan(&item.ModelID, &item.Classification, &item.Reason, &item.Presence, &item.MissStreak, &item.FirstSeenAt, &item.LastSeenAt))
	return item
}

func loadObservationProjectionDetails(t *testing.T, accountID int64, modelID string) observationProjectionDetails {
	t.Helper()
	var item observationProjectionDetails
	require.NoError(t, integrationDB.QueryRowContext(context.Background(), `
		SELECT upstream_model_id, classification, classification_reason, upstream_presence,
		       miss_streak, first_seen_at, last_seen_at, connection_id, resolved_registry_id, snapshot_batch_id
		FROM model_observations WHERE account_id = $1 AND upstream_model_id = $2
	`, accountID, modelID).Scan(
		&item.ModelID, &item.Classification, &item.Reason, &item.Presence,
		&item.MissStreak, &item.FirstSeenAt, &item.LastSeenAt, &item.ConnectionID, &item.ResolvedRegistryID, &item.SnapshotBatchID,
	))
	return item
}

func loadObservationProjectionDetailsForAccount(t *testing.T, accountID int64) []observationProjectionDetails {
	t.Helper()
	rows, err := integrationDB.QueryContext(context.Background(), `
		SELECT upstream_model_id, classification, classification_reason, upstream_presence,
		       miss_streak, first_seen_at, last_seen_at, connection_id, resolved_registry_id, snapshot_batch_id
		FROM model_observations WHERE account_id = $1 ORDER BY upstream_model_id
	`, accountID)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var result []observationProjectionDetails
	for rows.Next() {
		var item observationProjectionDetails
		require.NoError(t, rows.Scan(
			&item.ModelID, &item.Classification, &item.Reason, &item.Presence,
			&item.MissStreak, &item.FirstSeenAt, &item.LastSeenAt, &item.ConnectionID, &item.ResolvedRegistryID, &item.SnapshotBatchID,
		))
		result = append(result, item)
	}
	require.NoError(t, rows.Err())
	return result
}

func loadObservationProjectionsByID(t *testing.T, accountID int64, modelID string) []observationProjection {
	t.Helper()
	rows, err := integrationDB.QueryContext(context.Background(), `
		SELECT upstream_model_id, classification, classification_reason, upstream_presence,
		       miss_streak, first_seen_at, last_seen_at
		FROM model_observations WHERE account_id = $1 AND upstream_model_id = $2
	`, accountID, modelID)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var result []observationProjection
	for rows.Next() {
		var item observationProjection
		require.NoError(t, rows.Scan(&item.ModelID, &item.Classification, &item.Reason, &item.Presence, &item.MissStreak, &item.FirstSeenAt, &item.LastSeenAt))
		result = append(result, item)
	}
	require.NoError(t, rows.Err())
	return result
}

func loadPresentObservationModelIDs(t *testing.T, accountID int64) []string {
	t.Helper()
	rows, err := integrationDB.QueryContext(context.Background(), `
		SELECT upstream_model_id FROM model_observations
		WHERE account_id = $1 AND upstream_presence = 'present'
		ORDER BY upstream_model_id
	`, accountID)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var result []string
	for rows.Next() {
		var modelID string
		require.NoError(t, rows.Scan(&modelID))
		result = append(result, modelID)
	}
	require.NoError(t, rows.Err())
	return result
}

func loadBatchRawSnapshot(t *testing.T, batchID string) string {
	t.Helper()
	var snapshot string
	require.NoError(t, integrationDB.QueryRowContext(context.Background(), `
		SELECT raw_snapshot::text FROM model_classification_batches WHERE batch_id = $1
	`, batchID).Scan(&snapshot))
	return snapshot
}

func assertProjectionSnapshotOwner(t *testing.T, accountID int64, modelID string, expected service.DiscoveryBatchInput) {
	t.Helper()
	projection := loadObservationProjectionDetails(t, accountID, modelID)
	require.NotEmpty(t, projection.SnapshotBatchID)
	expected.AccountID = accountID
	assertBatchSemanticOwner(t, projection.SnapshotBatchID, expected)
}

func assertBatchSemanticOwner(t *testing.T, batchID string, expected service.DiscoveryBatchInput) {
	t.Helper()
	var digest, evidence string
	require.NoError(t, integrationDB.QueryRowContext(context.Background(), `
		SELECT raw_snapshot->'provenance'->>'digest', raw_snapshot->'evidence'
		FROM model_classification_batches WHERE batch_id = $1
	`, batchID).Scan(&digest, &evidence))
	require.Equal(t, discoveryInputDigest(t, expected), digest)
	require.JSONEq(t, string(expected.RawSnapshot), evidence)
}

func loadBatchObservedAt(t *testing.T, batchID string) time.Time {
	t.Helper()
	var observedAt time.Time
	require.NoError(t, integrationDB.QueryRowContext(context.Background(), `
		SELECT observed_at FROM model_classification_batches WHERE batch_id = $1
	`, batchID).Scan(&observedAt))
	return observedAt
}

func assertBatchEventTimesCanonical(t *testing.T, batchID string) {
	t.Helper()
	rows, err := integrationDB.QueryContext(context.Background(), `
		SELECT (payload->>'observed_at')::timestamptz,
		       (payload->>'first_seen_at')::timestamptz,
		       (payload->>'last_seen_at')::timestamptz
		FROM model_observation_events WHERE batch_id = $1
	`, batchID)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	for rows.Next() {
		var observedAt, firstSeenAt, lastSeenAt time.Time
		require.NoError(t, rows.Scan(&observedAt, &firstSeenAt, &lastSeenAt))
		for _, value := range []time.Time{observedAt, firstSeenAt, lastSeenAt} {
			require.Equal(t, value.Truncate(time.Microsecond), value)
			require.Equal(t, time.UTC, value.Location())
		}
	}
	require.NoError(t, rows.Err())
}

func loadBatchAccountID(t *testing.T, batchID string) int64 {
	t.Helper()
	var accountID int64
	require.NoError(t, integrationDB.QueryRowContext(context.Background(), `
		SELECT account_id FROM model_classification_batches WHERE batch_id = $1
	`, batchID).Scan(&accountID))
	return accountID
}

func loadBatchEventModelIDs(t *testing.T, batchID string) []string {
	t.Helper()
	rows, err := integrationDB.QueryContext(context.Background(), `
		SELECT e.upstream_model_id
		FROM model_observation_events e
		WHERE e.batch_id = $1
		ORDER BY e.id
	`, batchID)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var result []string
	for rows.Next() {
		var modelID string
		require.NoError(t, rows.Scan(&modelID))
		result = append(result, modelID)
	}
	require.NoError(t, rows.Err())
	return result
}

func loadBatchEventTypes(t *testing.T, batchID string) []string {
	t.Helper()
	rows, err := integrationDB.QueryContext(context.Background(), `
		SELECT event_type FROM model_observation_events WHERE batch_id = $1 ORDER BY id
	`, batchID)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var result []string
	for rows.Next() {
		var eventType string
		require.NoError(t, rows.Scan(&eventType))
		result = append(result, eventType)
	}
	require.NoError(t, rows.Err())
	return result
}

func countDiscoveryBatches(t *testing.T, accountID int64) int {
	t.Helper()
	var count int
	require.NoError(t, integrationDB.QueryRowContext(context.Background(), `
		SELECT COUNT(*) FROM model_classification_batches WHERE account_id = $1
	`, accountID).Scan(&count))
	return count
}

func withoutSnapshotBatchIDs(items []observationProjectionDetails) []observationProjectionDetails {
	result := append([]observationProjectionDetails(nil), items...)
	for index := range result {
		result[index].SnapshotBatchID = ""
	}
	return result
}

func discoveryInputDigest(t *testing.T, input service.DiscoveryBatchInput) string {
	t.Helper()
	_, digest, err := discoveryPersistenceEnvelope(input)
	require.NoError(t, err)
	return digest
}

func forceHigherDigest(t *testing.T, lower, higher service.DiscoveryBatchInput, catalog string) service.DiscoveryBatchInput {
	t.Helper()
	for nonce := 0; ; nonce++ {
		higher.RawSnapshot = []byte(fmt.Sprintf(`{"catalog":%q,"nonce":%d}`, catalog, nonce))
		if discoveryInputDigest(t, higher) > discoveryInputDigest(t, lower) {
			return higher
		}
	}
}

func loadBatchReplacementTransitions(t *testing.T, batchID string) map[string]string {
	t.Helper()
	rows, err := integrationDB.QueryContext(context.Background(), `
		SELECT upstream_model_id, payload->>'transition'
		FROM model_observation_events
		WHERE batch_id = $1 AND event_type = 'projection_replaced'
		ORDER BY upstream_model_id
	`, batchID)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	result := make(map[string]string)
	for rows.Next() {
		var modelID, transition string
		require.NoError(t, rows.Scan(&modelID, &transition))
		result[modelID] = transition
	}
	require.NoError(t, rows.Err())
	return result
}

func createGovernanceRegistryModel(t *testing.T, canonicalID string) int64 {
	t.Helper()
	var registryID int64
	require.NoError(t, integrationDB.QueryRowContext(context.Background(), `
		INSERT INTO model_registry (canonical_id, provider, modality, lifecycle, decided_by)
		VALUES ($1, 'openai', 'text', 'active', 'integration-test')
		RETURNING id
	`, fmt.Sprintf("%s-%d", canonicalID, time.Now().UnixNano())).Scan(&registryID))
	return registryID
}

func setObservationAdminState(t *testing.T, accountID int64, modelID, classification, reason string, registryID int64) error {
	t.Helper()
	_, err := integrationDB.ExecContext(context.Background(), `
		UPDATE model_observations
		SET classification = $3, classification_reason = $4, resolved_registry_id = $5
		WHERE account_id = $1 AND upstream_model_id = $2
	`, accountID, modelID, classification, reason, registryID)
	return err
}

type directObservationEventPostState struct {
	ModelID               string
	Transition            string
	BatchID               string
	SnapshotBatchID       string
	Classification        string
	Reason                string
	ConnectionID          sql.NullInt64
	ResolvedRegistryID    sql.NullInt64
	FirstSeenAt           sql.NullTime
	LastSeenAt            sql.NullTime
	HasRawSnapshot        bool
	HasConnectionID       bool
	HasResolvedRegistryID bool
}

func loadDirectObservationEventPostStates(t *testing.T, accountID int64) []directObservationEventPostState {
	t.Helper()
	rows, err := integrationDB.QueryContext(context.Background(), `
		SELECT upstream_model_id, COALESCE(payload->>'transition', event_type), batch_id, snapshot_batch_id,
		       classification, classification_reason,
		       (payload ? 'connection_id'), NULLIF(payload->>'connection_id', '')::bigint,
		       (payload ? 'resolved_registry_id'), NULLIF(payload->>'resolved_registry_id', '')::bigint,
		       (payload->>'first_seen_at')::timestamptz, (payload->>'last_seen_at')::timestamptz,
		       (payload ? 'raw_snapshot')
		FROM model_observation_events
		WHERE account_id = $1
		ORDER BY id
	`, accountID)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var result []directObservationEventPostState
	for rows.Next() {
		var event directObservationEventPostState
		require.NoError(t, rows.Scan(
			&event.ModelID, &event.Transition, &event.BatchID, &event.SnapshotBatchID,
			&event.Classification, &event.Reason,
			&event.HasConnectionID, &event.ConnectionID, &event.HasResolvedRegistryID, &event.ResolvedRegistryID,
			&event.FirstSeenAt, &event.LastSeenAt, &event.HasRawSnapshot,
		))
		result = append(result, event)
	}
	require.NoError(t, rows.Err())
	return result
}

func loadDirectBatchEventPostState(t *testing.T, batchID, modelID string) directObservationEventPostState {
	t.Helper()
	var event directObservationEventPostState
	require.NoError(t, integrationDB.QueryRowContext(context.Background(), `
		SELECT upstream_model_id, COALESCE(payload->>'transition', event_type), batch_id, snapshot_batch_id,
		       classification, classification_reason,
		       (payload ? 'connection_id'), NULLIF(payload->>'connection_id', '')::bigint,
		       (payload ? 'resolved_registry_id'), NULLIF(payload->>'resolved_registry_id', '')::bigint,
		       (payload->>'first_seen_at')::timestamptz, (payload->>'last_seen_at')::timestamptz,
		       (payload ? 'raw_snapshot')
		FROM model_observation_events
		WHERE batch_id = $1 AND upstream_model_id = $2
	`, batchID, modelID).Scan(
		&event.ModelID, &event.Transition, &event.BatchID, &event.SnapshotBatchID,
		&event.Classification, &event.Reason,
		&event.HasConnectionID, &event.ConnectionID, &event.HasResolvedRegistryID, &event.ResolvedRegistryID,
		&event.FirstSeenAt, &event.LastSeenAt, &event.HasRawSnapshot,
	))
	return event
}

func countObservationEvents(t *testing.T, accountID int64) int {
	t.Helper()
	var count int
	require.NoError(t, integrationDB.QueryRowContext(context.Background(), `
		SELECT COUNT(*) FROM model_observation_events WHERE account_id = $1
	`, accountID).Scan(&count))
	return count
}

func setObservationClassification(t *testing.T, accountID int64, modelID, classification, reason string) error {
	t.Helper()
	_, err := integrationDB.ExecContext(context.Background(), `
		UPDATE model_observations SET classification = $3, classification_reason = $4
		WHERE account_id = $1 AND upstream_model_id = $2
	`, accountID, modelID, classification, reason)
	return err
}

func loadObservationEventTypes(t *testing.T, accountID int64, modelID string) []string {
	t.Helper()
	rows, err := integrationDB.QueryContext(context.Background(), `
		SELECT e.event_type
		FROM model_observation_events e
		WHERE e.account_id = $1 AND e.upstream_model_id = $2
		ORDER BY e.id
	`, accountID, modelID)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var result []string
	for rows.Next() {
		var eventType string
		require.NoError(t, rows.Scan(&eventType))
		result = append(result, eventType)
	}
	require.NoError(t, rows.Err())
	return result
}
