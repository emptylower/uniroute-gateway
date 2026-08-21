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
