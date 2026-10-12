package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPublicMediaFeatureEnvironment(t *testing.T) {
	resetViperWithJWTSecret(t)
	cfg, err := Load()
	require.NoError(t, err)
	require.False(t, cfg.MediaTasks.PublicAPIEnabled)
	require.Empty(t, cfg.MediaTasks.CanaryKeyIDs)
	t.Setenv("MEDIA_TASKS_PUBLIC_API_ENABLED", "true")
	t.Setenv("MEDIA_TASKS_CANARY_KEY_IDS", "12,34")
	cfg, err = Load()
	require.NoError(t, err)
	require.True(t, cfg.MediaTasks.PublicAPIEnabled)
	require.Equal(t, []int64{12, 34}, cfg.MediaTasks.CanaryKeyIDs)
}
