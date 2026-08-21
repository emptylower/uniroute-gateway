//go:build integration

package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestShadowDecisions_PersistAndAtomicity(t *testing.T) {
	registryIntegrationMu.Lock()
	defer registryIntegrationMu.Unlock()
	ctx := context.Background()
	// Setup registry with one entry for classification
	registryRepo := NewModelRegistryRepository(integrationDB)
	snapshot, err := registryRepo.GetSnapshot(ctx)
	require.NoError(t, err)
	baseVersion := snapshot.Version
	canonical := fmt.Sprintf("shadow-model-%d", time.Now().UnixNano())
	_, _, err = registryRepo.CreateDecision(ctx, service.RegistryDecisionInput{
		IdempotencyKey:  fmt.Sprintf("shadow-reg-%d", time.Now().UnixNano()),
		ExpectedVersion: baseVersion,
		CanonicalID:     canonical,
		Provider:        service.GovernanceProviderAnthropic,
		Modality:        service.ModelModalityText,
		Status:          service.ModelLifecycleActive,
		Aliases:         nil,
		ActorID:         "tester",
	})
	require.NoError(t, err)

	// Create governance service with real DB repos
	observationRepo := NewModelObservationRepository(integrationDB)
	shadowRepo := NewShadowDecisionRepository(integrationDB)
	classifier := service.NewModelClassifier()
	evaluator := service.NewPublicationEvaluator()
	// Need registry service for snapshot
	registryService := service.NewModelRegistryService(registryRepo)
	governanceService := service.NewModelGovernanceService(service.ModelGovernanceServiceConfig{
		RegistryService:    registryService,
		ObservationRepo:    observationRepo,
		Classifier:         classifier,
		Evaluator:          evaluator,
		ShadowDecisionRepo: shadowRepo,
	})

	accountID := createGovernanceObservationAccount(t, "anthropic", `{}`)
	provider := service.GovernanceProviderAnthropic
	input := service.DiscoveryBatchInput{
		IdempotencyKey:  fmt.Sprintf("shadow-batch-%d", time.Now().UnixNano()),
		AccountID:       accountID,
		AccountProvider: &provider,
		RoutingPlatform: "anthropic",
		ModelIDs:        []string{canonical, "unknown-model-xyz", "gpt-*"},
		RawSnapshot:     []byte(fmt.Sprintf(`{"models":[%q,"unknown-model-xyz","gpt-*"]}`, canonical)),
		ObservedAt:      time.Now().UTC(),
	}

	summary, err := governanceService.ClassifyAndPersistShadow(ctx, input)
	require.NoError(t, err)
	require.Equal(t, 3, summary.Discovered)
	require.Equal(t, 1, summary.Publishable) // only canonical approved
	require.Equal(t, 0, summary.CrossProvider)
	require.Equal(t, 1, summary.Unknown)
	require.Equal(t, 1, summary.Ignored)

	// Verify batch and shadow decisions persisted
	var batchCount, shadowCount, observationCount int
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM model_classification_batches WHERE batch_id = $1`, summary.BatchID).Scan(&batchCount))
	require.Equal(t, 1, batchCount)
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM model_shadow_decisions WHERE batch_id = $1`, summary.BatchID).Scan(&shadowCount))
	require.Equal(t, 3, shadowCount)
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM model_observations WHERE account_id = $1`, accountID).Scan(&observationCount))
	require.Equal(t, 3, observationCount)

	// Verify append-only triggers: update/delete/truncate rejected
	_, err = integrationDB.ExecContext(ctx, `UPDATE model_shadow_decisions SET reason = 'tampered' WHERE batch_id = $1`, summary.BatchID)
	require.Error(t, err)
	_, err = integrationDB.ExecContext(ctx, `DELETE FROM model_shadow_decisions WHERE batch_id = $1`, summary.BatchID)
	require.Error(t, err)
	_, err = integrationDB.ExecContext(ctx, `TRUNCATE model_shadow_decisions`)
	require.Error(t, err)

	// Verify atomicity: use atomic recorder where shadow insert fails and whole transaction rolls back.
	// The atomic recorder does observation + shadow in one transaction, so no batch should remain.
	atomicRepo := NewModelObservationRepository(integrationDB).(*modelObservationRepository)
	// Wrap to make shadow fail: we will call the atomic method directly with invalid decisions
	input2 := service.DiscoveryBatchInput{
		IdempotencyKey:  fmt.Sprintf("shadow-fail-%d", time.Now().UnixNano()),
		AccountID:       accountID,
		AccountProvider: &provider,
		RoutingPlatform: "anthropic",
		ModelIDs:        []string{canonical},
		RawSnapshot:     []byte(fmt.Sprintf(`{"models":[%q]}`, canonical)),
		ObservedAt:      time.Now().UTC().Add(time.Second),
	}
	invalidDecisions := []service.ShadowDecision{
		{UpstreamModelID: canonical, Classification: "invalid_classification", Eligibility: "eligible", Reason: "test"},
	}
	_, err = atomicRepo.RecordDiscoveryWithShadow(ctx, input2, invalidDecisions)
	require.Error(t, err)
	var failedBatchCount int
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM model_classification_batches WHERE idempotency_key = $1`, input2.IdempotencyKey).Scan(&failedBatchCount))
	require.Equal(t, 0, failedBatchCount, "failed shadow should have rolled back the whole transaction")
}
