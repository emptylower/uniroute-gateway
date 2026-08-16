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

func TestChannelPreferenceRepository_PersistsAnchorScopesOwnerAndCascades(t *testing.T) {
	ctx := context.Background()
	suffix := time.Now().UnixNano()
	group := mustCreateGroup(t, integrationEntClient, &service.Group{
		Name:           fmt.Sprintf("channel-routing-group-%d", suffix),
		Platform:       service.PlatformOpenAI,
		Status:         service.StatusActive,
		RateMultiplier: 0.2,
	})
	user := mustCreateUser(t, integrationEntClient, &service.User{
		Email:  fmt.Sprintf("channel-routing-%d@example.com", suffix),
		Status: service.StatusActive,
	})
	key := mustCreateApiKey(t, integrationEntClient, &service.APIKey{
		UserID:  user.ID,
		Key:     fmt.Sprintf("sk-channel-routing-%d", suffix),
		Name:    "channel-routing",
		GroupID: &group.ID,
	})
	var channelID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO channels (name, description, status)
		VALUES ($1, 'integration', 'active') RETURNING id`,
		fmt.Sprintf("channel-routing-%d", suffix)).Scan(&channelID))

	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(context.Background(), "DELETE FROM users WHERE id = $1", user.ID)
		_, _ = integrationDB.ExecContext(context.Background(), "DELETE FROM channels WHERE id = $1", channelID)
		_, _ = integrationDB.ExecContext(context.Background(), "DELETE FROM groups WHERE id = $1", group.ID)
	})

	repo := NewChannelPreferenceRepository(integrationDB)
	keyValue, err := repo.ReplaceAPIKeyChannels(ctx, key.ID, user.ID, group.ID, []int64{channelID})
	require.NoError(t, err)
	require.Equal(t, key.Key, keyValue)

	ids, err := repo.GetAPIKeyChannelIDs(ctx, key.ID, user.ID)
	require.NoError(t, err)
	require.Equal(t, []int64{channelID}, ids)
	foreignIDs, err := repo.GetAPIKeyChannelIDs(ctx, key.ID, user.ID+1000)
	require.NoError(t, err)
	require.Empty(t, foreignIDs)

	var mode string
	var anchorID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx,
		"SELECT routing_mode, group_id FROM api_keys WHERE id = $1", key.ID,
	).Scan(&mode, &anchorID))
	require.Equal(t, service.APIKeyRoutingModeChannels, mode)
	require.Equal(t, group.ID, anchorID)

	require.NoError(t, repo.ReplaceUserDefaultChannels(ctx, user.ID, []int64{channelID}))
	require.NoError(t, integrationDB.QueryRowContext(ctx,
		"DELETE FROM channels WHERE id = $1 RETURNING id", channelID,
	).Scan(&channelID))

	var keyLinks, defaultLinks int
	require.NoError(t, integrationDB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM api_key_channels WHERE api_key_id = $1", key.ID,
	).Scan(&keyLinks))
	require.NoError(t, integrationDB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM user_default_channels WHERE user_id = $1", user.ID,
	).Scan(&defaultLinks))
	require.Zero(t, keyLinks)
	require.Zero(t, defaultLinks)
}
