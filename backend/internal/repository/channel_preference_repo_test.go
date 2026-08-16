package repository

import (
	"context"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/migrations"
	"github.com/stretchr/testify/require"
)

func TestChannelPreferenceRepository_GetAPIKeyChannelIDsScopesByOwner(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery(`(?s)SELECT akc.channel_id.*JOIN api_keys ak.*ak.user_id = \$2.*ORDER BY akc.channel_id`).
		WithArgs(int64(9), int64(42)).
		WillReturnRows(sqlmock.NewRows([]string{"channel_id"}).AddRow(int64(10)).AddRow(int64(20)))

	repo := NewChannelPreferenceRepository(db)
	ids, err := repo.GetAPIKeyChannelIDs(context.Background(), 9, 42)

	require.NoError(t, err)
	require.Equal(t, []int64{10, 20}, ids)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestChannelPreferenceRepository_ReplaceAPIKeyChannelsIsTransactional(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	mock.ExpectQuery(`(?s)SELECT key FROM api_keys.*user_id = \$2.*FOR UPDATE`).
		WithArgs(int64(9), int64(42)).
		WillReturnRows(sqlmock.NewRows([]string{"key"}).AddRow("sk-test"))
	mock.ExpectExec(`DELETE FROM api_key_channels WHERE api_key_id = \$1`).
		WithArgs(int64(9)).
		WillReturnResult(sqlmock.NewResult(0, 2))
	for _, channelID := range []int64{10, 20} {
		mock.ExpectExec(`(?s)INSERT INTO api_key_channels.*VALUES \(\$1, \$2\)`).
			WithArgs(int64(9), channelID).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectExec(`(?s)UPDATE api_keys.*routing_mode = 'channels'.*group_id = \$1.*WHERE id = \$2`).
		WithArgs(int64(3), int64(9)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	repo := NewChannelPreferenceRepository(db)
	key, err := repo.ReplaceAPIKeyChannels(context.Background(), 9, 42, 3, []int64{10, 20})

	require.NoError(t, err)
	require.Equal(t, "sk-test", key)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestChannelPreferenceRepository_ReplaceAPIKeyChannelsRejectsWrongOwner(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	mock.ExpectQuery(`(?s)SELECT key FROM api_keys.*user_id = \$2.*FOR UPDATE`).
		WithArgs(int64(9), int64(42)).
		WillReturnRows(sqlmock.NewRows([]string{"key"}))
	mock.ExpectRollback()

	repo := NewChannelPreferenceRepository(db)
	_, err = repo.ReplaceAPIKeyChannels(context.Background(), 9, 42, 3, []int64{10})

	require.ErrorIs(t, err, service.ErrAPIKeyNotFound)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestChannelRoutingMigrationPreservesLegacyDefaultAndCascades(t *testing.T) {
	raw, err := migrations.FS.ReadFile("191_api_key_channel_routing.sql")
	require.NoError(t, err)
	sql := strings.ToLower(string(raw))

	for _, fragment := range []string{
		"default 'legacy_group'",
		"check (routing_mode in ('legacy_group', 'channels'))",
		"primary key (api_key_id, channel_id)",
		"primary key (user_id, channel_id)",
		"references api_keys(id) on delete cascade",
		"references users(id) on delete cascade",
		"references channels(id) on delete cascade",
	} {
		require.Contains(t, sql, fragment)
	}
}

func TestAutomaticChannelRoutingMigrationExtendsConstraint(t *testing.T) {
	raw, err := migrations.FS.ReadFile("192_api_key_auto_channel_routing.sql")
	require.NoError(t, err)
	sql := strings.ToLower(string(raw))

	for _, fragment := range []string{
		"drop constraint if exists chk_api_keys_routing_mode",
		"check (routing_mode in ('legacy_group', 'channels', 'auto_channels'))",
	} {
		require.Contains(t, sql, fragment)
	}
}
