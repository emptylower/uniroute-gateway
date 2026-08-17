package repository

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
)

func TestUpdateModelDiscoveryOnlyPatchesOwnedCredentialKeys(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
	t.Cleanup(func() { _ = client.Close() })

	mock.ExpectBegin()
	mock.ExpectExec(`(?s)UPDATE accounts.*jsonb_set\(.*credentials.*model_mapping.*model_discovery`).
		WithArgs(
			`{"gpt-5.6-sol":"gpt-5.6-sol"}`,
			`{"models":["gpt-5.6-sol"],"source":"upstream"}`,
			false,
			int64(27),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO scheduler_outbox")).
		WithArgs(service.SchedulerOutboxEventAccountChanged, int64(27), nil, nil, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	repo := newAccountRepositoryWithSQL(client, db, nil)
	err = repo.UpdateModelDiscovery(
		context.Background(),
		27,
		map[string]any{"gpt-5.6-sol": "gpt-5.6-sol"},
		map[string]any{"source": "upstream", "models": []string{"gpt-5.6-sol"}},
	)

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestUpdateModelDiscoveryRemovesMetadataForCredentialShadow(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
	t.Cleanup(func() { _ = client.Close() })

	mock.ExpectBegin()
	mock.ExpectExec(`(?s)UPDATE accounts.*model_mapping.*- 'model_discovery'`).
		WithArgs(
			`{"gpt-5.3-codex-spark":"gpt-5.3-codex-spark"}`,
			`{}`,
			true,
			int64(28),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO scheduler_outbox")).
		WithArgs(service.SchedulerOutboxEventAccountChanged, int64(28), nil, nil, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	repo := newAccountRepositoryWithSQL(client, db, nil)
	err = repo.UpdateModelDiscovery(
		context.Background(),
		28,
		map[string]any{"gpt-5.3-codex-spark": "gpt-5.3-codex-spark"},
		nil,
	)

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}
