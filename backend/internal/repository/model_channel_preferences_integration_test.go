//go:build integration

package repository

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestModelChannelPreferencesConcurrentPersistenceAndConstraints(t *testing.T) {
	ctx := context.Background()
	user := mustCreateUser(t, integrationEntClient, &service.User{Email: fmt.Sprintf("model-channel-%d@example.com", time.Now().UnixNano()), Status: service.StatusActive})
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(context.Background(), "DELETE FROM users WHERE id = $1", user.ID)
	})
	repo := NewChannelPreferenceRepository(integrationDB)
	var wg sync.WaitGroup
	errors := make(chan error, 24)
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errors <- repo.UpsertUserModelChannelPreference(ctx, user.ID, fmt.Sprintf("claude-%d", i), service.ModelChannelOfficial)
		}(i)
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	// A fresh repository reads every concurrently saved model, not just the
	// last request's replacement map.
	choices, err := NewChannelPreferenceRepository(integrationDB).GetUserModelChannelPreferences(ctx, user.ID)
	require.NoError(t, err)
	require.Len(t, choices, 24)
	require.NoError(t, repo.UpsertUserModelChannelPreference(ctx, user.ID, "claude-0", service.ModelChannelCloudVendor))
	choices, err = repo.GetUserModelChannelPreferences(ctx, user.ID)
	require.NoError(t, err)
	require.Len(t, choices, 24)
	require.Equal(t, service.ModelChannelCloudVendor, choices["claude-0"])
	require.Equal(t, service.ModelChannelOfficial, choices["claude-1"])
	foreign, err := repo.GetUserModelChannelPreferences(ctx, user.ID+100000)
	require.NoError(t, err)
	require.Empty(t, foreign)
	for _, invalid := range []struct{ model, channel string }{
		{"CLAUDE-A", "official"}, {" claude-a ", "official"}, {"claude-*", "official"}, {"claude-a", "unknown"},
	} {
		require.Error(t, repo.UpsertUserModelChannelPreference(ctx, user.ID, invalid.model, invalid.channel))
	}
	require.Error(t, repo.UpsertUserModelChannelPreference(ctx, user.ID+100000, "claude-a", "official"))
	_, err = integrationDB.ExecContext(ctx, "DELETE FROM users WHERE id = $1", user.ID)
	require.NoError(t, err)
	var remaining int
	require.NoError(t, integrationDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM user_model_channel_preferences WHERE user_id = $1", user.ID).Scan(&remaining))
	require.Zero(t, remaining)
}
