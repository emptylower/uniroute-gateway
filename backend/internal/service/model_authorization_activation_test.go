package service

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func newMockActivationService(t *testing.T) (*ModelAuthorizationActivationService, sqlmock.Sqlmock, func()) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	svc := NewModelAuthorizationActivationService(db)
	cleanup := func() { db.Close() }
	return svc, mock, cleanup
}

func TestActivation_RequiresInventoryAndVersions(t *testing.T) {
	svc, _, cleanup := newMockActivationService(t)
	defer cleanup()
	err := svc.Activate(context.Background(), ActivationInput{
		InventoryHash: "", RegistryVersion: 1, ChannelVersions: map[int64]int64{1: 1},
		ProjectedBatchID: "batch-1", AcknowledgedBy: "tester", IdempotencyKey: "idem-1", ActorID: "tester",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "inventory_hash")
}

func TestActivation_AcknowledgmentRequired(t *testing.T) {
	svc, _, cleanup := newMockActivationService(t)
	defer cleanup()
	err := svc.Activate(context.Background(), ActivationInput{
		InventoryHash: "hash-1", RegistryVersion: 5, ChannelVersions: map[int64]int64{10: 3},
		ProjectedBatchID: "batch-1", AcknowledgedBy: "", IdempotencyKey: "idem-1", ActorID: "tester",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "acknowledged_by")
}

func TestActivation_StaleVersionConflict(t *testing.T) {
	svc, mock, cleanup := newMockActivationService(t)
	defer cleanup()
	input := ActivationInput{
		InventoryHash: "hash-1", RegistryVersion: 3, ChannelVersions: map[int64]int64{10: 3},
		ProjectedBatchID: "batch-1", AcknowledgedBy: "tester", IdempotencyKey: "idem-1", ActorID: "tester",
	}
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT inventory_hash FROM model_authorization_activations`).WithArgs("idem-1").WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(`SELECT COALESCE\(MAX\(registry_version\)`).WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(int64(5)))
	mock.ExpectRollback()
	err := svc.Activate(context.Background(), input)
	require.Error(t, err)
	require.Contains(t, err.Error(), "stale registry")
	require.NoError(t, mock.ExpectationsWereMet())
}

// expectReadinessHappyPath stubs the exact read sequence of
// EvaluateReadiness for a fully-ready gateway.
func expectReadinessHappyPath(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT COALESCE\(MAX\(registry_version\),0\) FROM model_registry_events`).
		WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(int64(12)))
	mock.ExpectQuery(`SELECT COALESCE\(inventory_hash, ''\) FROM model_inventory_runs`).
		WillReturnRows(sqlmock.NewRows([]string{"inventory_hash"}).AddRow("inv-hash-9"))
	mock.ExpectQuery(`SELECT id, governance_version FROM channels`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "governance_version"}).
			AddRow(int64(3), int64(4)).AddRow(int64(5), int64(2)))
	mock.ExpectQuery(`SELECT batch_id FROM model_publication_events`).
		WillReturnRows(sqlmock.NewRows([]string{"batch_id"}).AddRow("proj-batch-7"))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM accounts WHERE platform IN \('anthropic','openai','gemini','grok'\) AND status = 'active'`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
	mock.ExpectQuery(`SELECT COUNT\(DISTINCT batch.account_id\)`).
		WillReturnRows(sqlmock.NewRows([]string{"covered"}).AddRow(2))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM accounts WHERE platform IN \(.*\) AND status = 'active' AND connection_id IS NULL`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
}

func TestEvaluateReadiness_ReadyGatewayPrefillsEverything(t *testing.T) {
	svc, mock, cleanup := newMockActivationService(t)
	defer cleanup()
	expectReadinessHappyPath(mock)

	readiness, err := svc.EvaluateReadiness(context.Background())
	require.NoError(t, err)
	require.True(t, readiness.Ready)
	require.Empty(t, readiness.UnmetPrerequisites)
	require.Equal(t, int64(12), readiness.CurrentRegistryVersion)
	require.Equal(t, "inv-hash-9", readiness.LatestCompletedInventoryHash)
	require.Equal(t, map[int64]int64{3: 4, 5: 2}, readiness.ChannelVersions)
	require.Equal(t, "proj-batch-7", readiness.ProjectedBatchID)
	require.Equal(t, 2, readiness.EnabledGovernedAccounts)
	require.Equal(t, 2, readiness.AccountsWithShadowDecision)
	require.Equal(t, 0, readiness.UnresolvedImpactAccounts)
	require.Equal(t, "shadow", readiness.CurrentMode)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEvaluateReadiness_EmptyRegistryReportsPrerequisite(t *testing.T) {
	svc, mock, cleanup := newMockActivationService(t)
	defer cleanup()
	mock.ExpectQuery(`SELECT COALESCE\(MAX\(registry_version\),0\) FROM model_registry_events`).
		WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(int64(0)))
	mock.ExpectQuery(`SELECT COALESCE\(inventory_hash, ''\) FROM model_inventory_runs`).
		WillReturnRows(sqlmock.NewRows([]string{"inventory_hash"}))
	mock.ExpectQuery(`SELECT id, governance_version FROM channels`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "governance_version"}))
	mock.ExpectQuery(`SELECT batch_id FROM model_publication_events`).WillReturnError(sql.ErrNoRows)
	// No enabled accounts: shadow coverage check skipped.
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM accounts WHERE platform IN \('anthropic','openai','gemini','grok'\) AND status = 'active'`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM accounts WHERE platform IN \(.*\) AND status = 'active' AND connection_id IS NULL`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	readiness, err := svc.EvaluateReadiness(context.Background())
	require.NoError(t, err)
	require.False(t, readiness.Ready)
	require.Contains(t, readiness.UnmetPrerequisites, ReadinessPrerequisiteRegistryEmpty)
	require.Contains(t, readiness.UnmetPrerequisites, ReadinessPrerequisiteInventoryNotCompleted)
	require.NotContains(t, readiness.UnmetPrerequisites, ReadinessPrerequisiteShadowCoverage)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEvaluateReadiness_IncompleteShadowCoverageAndUnresolvedImpacts(t *testing.T) {
	svc, mock, cleanup := newMockActivationService(t)
	defer cleanup()
	mock.ExpectQuery(`SELECT COALESCE\(MAX\(registry_version\),0\) FROM model_registry_events`).
		WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(int64(12)))
	mock.ExpectQuery(`SELECT COALESCE\(inventory_hash, ''\) FROM model_inventory_runs`).
		WillReturnRows(sqlmock.NewRows([]string{"inventory_hash"}).AddRow("inv-hash-9"))
	mock.ExpectQuery(`SELECT id, governance_version FROM channels`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "governance_version"}).AddRow(int64(3), int64(4)))
	mock.ExpectQuery(`SELECT batch_id FROM model_publication_events`).WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM accounts WHERE platform IN \('anthropic','openai','gemini','grok'\) AND status = 'active'`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(4))
	mock.ExpectQuery(`SELECT COUNT\(DISTINCT batch.account_id\)`).
		WillReturnRows(sqlmock.NewRows([]string{"covered"}).AddRow(1))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM accounts WHERE platform IN \(.*\) AND status = 'active' AND connection_id IS NULL`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))

	readiness, err := svc.EvaluateReadiness(context.Background())
	require.NoError(t, err)
	require.False(t, readiness.Ready)
	require.Contains(t, readiness.UnmetPrerequisites, ReadinessPrerequisiteShadowCoverage)
	require.Contains(t, readiness.UnmetPrerequisites, ReadinessPrerequisiteUnresolvedImpacts)
	require.Equal(t, 4, readiness.EnabledGovernedAccounts)
	require.Equal(t, 1, readiness.AccountsWithShadowDecision)
	require.Equal(t, 2, readiness.UnresolvedImpactAccounts)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEvaluateReadiness_NotConfiguredErrors(t *testing.T) {
	svc := NewModelAuthorizationActivationService(nil)
	_, err := svc.EvaluateReadiness(context.Background())
	require.Error(t, err)

	var nilSvc *ModelAuthorizationActivationService
	_, err = nilSvc.EvaluateReadiness(context.Background())
	require.Error(t, err)
}
