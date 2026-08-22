package repository

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

func newGovernanceReadRepo(t *testing.T) (*modelGovernanceReadRepository, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	repo := &modelGovernanceReadRepository{db: db}
	return repo, mock, func() { _ = db.Close() }
}

func TestModelGovernanceReadRepository_ListQuarantinePool(t *testing.T) {
	repo, mock, cleanup := newGovernanceReadRepo(t)
	defer cleanup()

	now := time.Date(2026, 8, 22, 9, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM model_publication_eligibility WHERE eligibility = 'quarantined'`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(1)))
	mock.ExpectQuery(`FROM model_publication_eligibility e`).
		WithArgs(50, 0).
		WillReturnRows(sqlmock.NewRows([]string{
			"account_id", "account_name", "channel_id", "channel_name",
			"canonical_model_id", "reason", "registry_version", "channel_version",
			"quarantine_batch_id", "batch_id", "created_at", "updated_at",
		}).AddRow(int64(7), "agg-account", int64(3), "agg-channel",
			"gpt-5.2", "quarantined_by_admin", int64(12), int64(4),
			"qb-1", nil, now, now))

	result, err := repo.ListQuarantinePool(context.Background(), 1, 50)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.Total)
	require.Len(t, result.Items, 1)
	item := result.Items[0]
	require.Equal(t, "agg-account", item.AccountName)
	require.Equal(t, "gpt-5.2", item.CanonicalModelID)
	require.NotNil(t, item.QuarantineBatchID)
	require.Equal(t, "qb-1", *item.QuarantineBatchID)
	require.Nil(t, item.BatchID)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestModelGovernanceReadRepository_ListQuarantinePool_ClampsPagination(t *testing.T) {
	repo, mock, cleanup := newGovernanceReadRepo(t)
	defer cleanup()

	mock.ExpectQuery(`SELECT COUNT\(\*\)`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(0)))
	mock.ExpectQuery(`FROM model_publication_eligibility e`).WithArgs(200, 400).
		WillReturnRows(sqlmock.NewRows([]string{
			"account_id", "account_name", "channel_id", "channel_name",
			"canonical_model_id", "reason", "registry_version", "channel_version",
			"quarantine_batch_id", "batch_id", "created_at", "updated_at",
		}))

	// page=3 page_size=1000 -> offset (3-1)*200=400; page_size clamped to 200.
	result, err := repo.ListQuarantinePool(context.Background(), 3, 1000)
	require.NoError(t, err)
	require.Equal(t, 200, result.PageSize)
	require.Empty(t, result.Items)

	// page=0 -> clamped to 1 with offset 0.
	mock.ExpectQuery(`SELECT COUNT\(\*\)`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(0)))
	mock.ExpectQuery(`FROM model_publication_eligibility e`).WithArgs(50, 0).
		WillReturnRows(sqlmock.NewRows([]string{
			"account_id", "account_name", "channel_id", "channel_name",
			"canonical_model_id", "reason", "registry_version", "channel_version",
			"quarantine_batch_id", "batch_id", "created_at", "updated_at",
		}))
	result, err = repo.ListQuarantinePool(context.Background(), 0, 0)
	require.NoError(t, err)
	require.Equal(t, 1, result.Page)
	require.Equal(t, 50, result.PageSize)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestModelGovernanceReadRepository_ListEvents_PublicationStreamExcludesPayload(t *testing.T) {
	repo, mock, cleanup := newGovernanceReadRepo(t)
	defer cleanup()

	now := time.Date(2026, 8, 22, 9, 30, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM model_publication_events`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(1)))
	mock.ExpectQuery(`FROM model_publication_events`).WithArgs(50, 0).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "event_type", "actor_id", "registry_version", "created_at",
			"account_id", "canonical_model_id", "channel_id", "eligibility", "reason",
			"channel_version", "batch_id", "quarantine_batch_id",
		}).AddRow(int64(9), "restore", "admin-1", int64(12), now,
			int64(7), "gpt-5.2", int64(3), "eligible", "restored_after_probe",
			int64(4), "b-2", "qb-1"))

	result, err := repo.ListGovernanceEvents(context.Background(), service.GovernanceEventStreamPublication, 1, 50)
	require.NoError(t, err)
	require.Len(t, result.Items, 1)
	item := result.Items[0]
	require.Equal(t, service.GovernanceEventStreamPublication, item.Stream)
	require.Equal(t, "restore", item.EventType)
	require.Equal(t, "admin-1", item.ActorID)
	require.Equal(t, int64(7), *item.AccountID)
	require.Equal(t, "gpt-5.2", item.CanonicalModelID)
	require.Equal(t, "qb-1", *item.QuarantineBatchID)
	require.Empty(t, item.CanonicalID)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestModelGovernanceReadRepository_ListEvents_RegistryStream(t *testing.T) {
	repo, mock, cleanup := newGovernanceReadRepo(t)
	defer cleanup()

	now := time.Date(2026, 8, 22, 8, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM model_registry_events`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(1)))
	mock.ExpectQuery(`FROM model_registry_events e`).WithArgs(50, 0).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "event_type", "actor_id", "registry_version", "created_at", "canonical_id",
		}).AddRow(int64(4), "decision", "admin-1", int64(12), now, "claude-opus-4-6"))

	result, err := repo.ListGovernanceEvents(context.Background(), service.GovernanceEventStreamRegistry, 1, 50)
	require.NoError(t, err)
	require.Len(t, result.Items, 1)
	item := result.Items[0]
	require.Equal(t, service.GovernanceEventStreamRegistry, item.Stream)
	require.Equal(t, "claude-opus-4-6", item.CanonicalID)
	require.Nil(t, item.AccountID)
	require.Empty(t, item.Eligibility)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestModelGovernanceReadRepository_ListEvents_RejectsUnknownStream(t *testing.T) {
	repo, _, cleanup := newGovernanceReadRepo(t)
	defer cleanup()

	_, err := repo.ListGovernanceEvents(context.Background(), "audit-log", 1, 50)
	require.Error(t, err)
}

func TestModelGovernanceReadRepository_NilDBIsError(t *testing.T) {
	repo := &modelGovernanceReadRepository{db: nil}
	_, err := repo.ListQuarantinePool(context.Background(), 1, 50)
	require.Error(t, err)
	_, err = repo.ListGovernanceEvents(context.Background(), "", 1, 50)
	require.Error(t, err)

	var nilRepo *modelGovernanceReadRepository
	_, err = nilRepo.ListQuarantinePool(context.Background(), 1, 50)
	require.Error(t, err)
}

func TestModelGovernanceReadRepository_ListConnections(t *testing.T) {
	repo, mock, cleanup := newGovernanceReadRepo(t)
	defer cleanup()

	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM upstream_connections`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(1)))
	mock.ExpectQuery(`FROM upstream_connections`).WithArgs(50, 0).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "kind", "provider", "base_url", "status", "credential_version",
		}).AddRow(int64(11), "aggregator", nil, "https://agg.example.com", "active", int64(4)))

	result, err := repo.ListConnections(context.Background(), 1, 50)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.Total)
	require.Len(t, result.Items, 1)
	item := result.Items[0]
	require.Equal(t, "aggregator", item.Kind)
	require.Nil(t, item.Provider)
	require.Equal(t, "https://agg.example.com", item.BaseURL)
	require.Equal(t, int64(4), item.CredentialVersion)
	require.NoError(t, mock.ExpectationsWereMet())
}
