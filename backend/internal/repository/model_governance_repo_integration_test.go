//go:build integration

package repository

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
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
	ConnectionID sql.NullInt64
	RawSnapshot  string
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
	require.Equal(t, []string{"discovered", "observed"}, loadObservationEventTypes(t, accountID, "shared-model"))
}

func TestModelGovernanceRepository_ObservationEventsRetainIdentityAfterAccountDeletion(t *testing.T) {
	ctx := context.Background()
	accountID := createGovernanceObservationAccount(t, "openai", `{}`)
	provider := service.GovernanceProvider("openai")
	modelID := "reconstructable-model"

	_, err := NewModelObservationRepository(integrationDB).RecordDiscovery(ctx, service.DiscoveryBatchInput{
		IdempotencyKey: "reconstructable-" + fmt.Sprint(accountID), AccountID: accountID,
		AccountProvider: &provider, RoutingPlatform: "openai", ModelIDs: []string{modelID},
		RawSnapshot: []byte(`{"source":"deletion-test"}`), ObservedAt: time.Date(2026, time.August, 18, 19, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)

	_, err = integrationDB.ExecContext(ctx, `DELETE FROM accounts WHERE id = $1`, accountID)
	require.NoError(t, err)

	var storedAccountID int64
	var storedModelID, eventType string
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		SELECT account_id, upstream_model_id, event_type
		FROM model_observation_events
		WHERE account_id = $1 AND upstream_model_id = $2
	`, accountID, modelID).Scan(&storedAccountID, &storedModelID, &eventType))
	require.Equal(t, accountID, storedAccountID)
	require.Equal(t, modelID, storedModelID)
	require.Equal(t, "discovered", eventType)
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
		       miss_streak, first_seen_at, last_seen_at, connection_id, raw_snapshot::text
		FROM model_observations WHERE account_id = $1 AND upstream_model_id = $2
	`, accountID, modelID).Scan(
		&item.ModelID, &item.Classification, &item.Reason, &item.Presence,
		&item.MissStreak, &item.FirstSeenAt, &item.LastSeenAt, &item.ConnectionID, &item.RawSnapshot,
	))
	return item
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

func loadBatchRawSnapshot(t *testing.T, batchID string) string {
	t.Helper()
	var snapshot string
	require.NoError(t, integrationDB.QueryRowContext(context.Background(), `
		SELECT raw_snapshot::text FROM model_classification_batches WHERE batch_id = $1
	`, batchID).Scan(&snapshot))
	return snapshot
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
		SELECT o.upstream_model_id
		FROM model_observation_events e
		JOIN model_observations o ON o.id = e.observation_id
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

func countObservationEvents(t *testing.T, accountID int64) int {
	t.Helper()
	var count int
	require.NoError(t, integrationDB.QueryRowContext(context.Background(), `
		SELECT COUNT(*) FROM model_observation_events e
		JOIN model_observations o ON o.id = e.observation_id
		WHERE o.account_id = $1
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
		JOIN model_observations o ON o.id = e.observation_id
		WHERE o.account_id = $1 AND o.upstream_model_id = $2
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
