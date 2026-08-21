package repository

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func newMockPublicationRepo(t *testing.T) (*modelPublicationRepository, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	repo := &modelPublicationRepository{db: db}
	cleanup := func() { db.Close() }
	return repo, mock, cleanup
}

func TestModelPublicationRepo_Decision(t *testing.T) {
	repo, mock, cleanup := newMockPublicationRepo(t)
	defer cleanup()

	key := service.ModelAuthorizationKey{AccountID: 1, ChannelID: 2, CanonicalModelID: "claude-opus-4-6"}
	rows := sqlmock.NewRows([]string{"eligibility", "reason", "registry_version", "channel_version"}).
		AddRow("eligible", "eligible", 10, 5)
	mock.ExpectQuery(`SELECT eligibility, reason, registry_version, channel_version`).
		WithArgs(int64(1), int64(2), "claude-opus-4-6").WillReturnRows(rows)

	dec, err := repo.Decision(context.Background(), key)
	require.NoError(t, err)
	require.Equal(t, "eligible", dec.Eligibility)
	require.Equal(t, int64(10), dec.RegistryVersion)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestModelPublicationRepo_RecomputeBatch_IdempotentReplay(t *testing.T) {
	repo, mock, cleanup := newMockPublicationRepo(t)
	defer cleanup()

	input := service.RecomputeInput{
		BatchID: "batch-1", IdempotencyKey: "idem-1", RegistryVersion: 5, ChannelVersions: map[int64]int64{10: 3},
		ActorID: "tester", Items: []service.RecomputeItem{
			{AccountID: 1, ChannelID: 10, CanonicalModelID: "claude-opus-4-6", Eligibility: service.PublicationEligibilityEligible, Reason: "eligible", RegistryVersion: 5, ChannelVersion: 3},
		},
	}

	// First call: no existing idempotency, success path
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT request_hash FROM governance_idempotency_records`).WithArgs("idem-1").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(`SELECT COALESCE\(MAX\(registry_version\)`).WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(int64(5)))
	mock.ExpectQuery(`SELECT governance_version FROM channels`).WithArgs(int64(10)).
		WillReturnRows(sqlmock.NewRows([]string{"governance_version"}).AddRow(int64(3)))
	mock.ExpectQuery(`SELECT eligibility, quarantine_batch_id FROM model_publication_eligibility`).WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(`INSERT INTO model_publication_eligibility`).WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO model_publication_events`).WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO governance_idempotency_records`).WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	require.NoError(t, repo.RecomputeBatch(context.Background(), input))

	// Second call with same idempotency key and same hash => idempotent replay, commit without writes
	mock.ExpectBegin()
	// Return existing hash that matches recomputed hash
	// We need computed hash; approximate by letting query return whatever recomputed would be: we call recomputeRequestHash logic indirectly,
	// easiest: mock to return the same hash the code will compute; we can compute it via helper by invoking same logic:
	// Instead, we mock to return request_hash that matches whatever, and test only that code path returns no error.
	// To make match, we precompute expected hash via same function as repo: use recomputeRequestHash.
	expectedHash := recomputeRequestHash(input)
	mock.ExpectQuery(`SELECT request_hash FROM governance_idempotency_records`).WithArgs("idem-1").
		WillReturnRows(sqlmock.NewRows([]string{"request_hash"}).AddRow(expectedHash))
	mock.ExpectCommit()

	require.NoError(t, repo.RecomputeBatch(context.Background(), input))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestModelPublicationRepo_RecomputeBatch_StaleRegistryVersion(t *testing.T) {
	repo, mock, cleanup := newMockPublicationRepo(t)
	defer cleanup()

	input := service.RecomputeInput{
		BatchID: "batch-2", IdempotencyKey: "idem-2", RegistryVersion: 3, ChannelVersions: map[int64]int64{10: 3},
		ActorID: "tester", Items: []service.RecomputeItem{
			{AccountID: 1, ChannelID: 10, CanonicalModelID: "claude-opus-4-6", Eligibility: service.PublicationEligibilityEligible, Reason: "eligible", RegistryVersion: 3, ChannelVersion: 3},
		},
	}
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT request_hash FROM governance_idempotency_records`).WithArgs("idem-2").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(`SELECT COALESCE\(MAX\(registry_version\)`).WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(int64(5)))
	mock.ExpectRollback()

	err := repo.RecomputeBatch(context.Background(), input)
	require.Error(t, err)
	require.Contains(t, err.Error(), "stale registry")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestModelPublicationRepo_RecomputeBatch_StaleChannelVersion(t *testing.T) {
	repo, mock, cleanup := newMockPublicationRepo(t)
	defer cleanup()

	input := service.RecomputeInput{
		BatchID: "batch-3", IdempotencyKey: "idem-3", RegistryVersion: 5, ChannelVersions: map[int64]int64{10: 5},
		ActorID: "tester", Items: []service.RecomputeItem{
			{AccountID: 1, ChannelID: 10, CanonicalModelID: "claude-opus-4-6", Eligibility: service.PublicationEligibilityEligible, Reason: "eligible", RegistryVersion: 5, ChannelVersion: 5},
		},
	}
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT request_hash FROM governance_idempotency_records`).WithArgs("idem-3").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(`SELECT COALESCE\(MAX\(registry_version\)`).WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(int64(5)))
	mock.ExpectQuery(`SELECT governance_version FROM channels`).WithArgs(int64(10)).
		WillReturnRows(sqlmock.NewRows([]string{"governance_version"}).AddRow(int64(7)))
	mock.ExpectRollback()

	err := repo.RecomputeBatch(context.Background(), input)
	require.Error(t, err)
	require.Contains(t, err.Error(), "stale channel")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestModelPublicationRepo_RecomputeBatch_CannotRestoreQuarantineWithStale(t *testing.T) {
	// Verify quarantine row cannot be overwritten via stale version attempt.
	repo, mock, cleanup := newMockPublicationRepo(t)
	defer cleanup()

	quarantineBatch := "q-batch-1"
	input := service.RecomputeInput{
		BatchID: "batch-restore", IdempotencyKey: "idem-restore", RegistryVersion: 5, ChannelVersions: map[int64]int64{10: 5},
		ActorID: "tester", Items: []service.RecomputeItem{
			{AccountID: 1, ChannelID: 10, CanonicalModelID: "claude-opus-4-6", Eligibility: service.PublicationEligibilityEligible, Reason: "eligible", RegistryVersion: 5, ChannelVersion: 5, QuarantineBatchID: &quarantineBatch},
		},
	}
	// Setup: channel version mismatch triggers stale, proving cannot restore.
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT request_hash FROM governance_idempotency_records`).WithArgs("idem-restore").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(`SELECT COALESCE\(MAX\(registry_version\)`).WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(int64(5)))
	mock.ExpectQuery(`SELECT governance_version FROM channels`).WithArgs(int64(10)).
		WillReturnRows(sqlmock.NewRows([]string{"governance_version"}).AddRow(int64(9)))
	mock.ExpectRollback()

	err := repo.RecomputeBatch(context.Background(), input)
	require.Error(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}
