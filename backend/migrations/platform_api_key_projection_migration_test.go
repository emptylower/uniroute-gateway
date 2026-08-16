package migrations

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlatformAPIKeyProjectionMigrationIsAdditiveVerifierOnlyAndImmutable(t *testing.T) {
	raw, err := os.ReadFile("197_platform_api_key_projection.sql")
	require.NoError(t, err)
	sql := strings.ToUpper(string(raw))

	require.Contains(t, sql, "ADD COLUMN IF NOT EXISTS PLATFORM_KEY_ID")
	require.Contains(t, sql, "ADD COLUMN IF NOT EXISTS KEY_SHA256")
	require.Contains(t, sql, "ADD COLUMN IF NOT EXISTS KEY_PREFIX")
	require.Contains(t, sql, "ADD COLUMN IF NOT EXISTS PLATFORM_KEY_VERSION")
	require.Contains(t, sql, "CREATE UNIQUE INDEX IF NOT EXISTS IDX_API_KEYS_PLATFORM_KEY_ID")
	require.Contains(t, sql, "CREATE UNIQUE INDEX IF NOT EXISTS IDX_API_KEYS_PROJECTED_SHA256")
	require.Contains(t, sql, "PLATFORM_KEY_ID IS IMMUTABLE ONCE ASSIGNED")
	require.NotContains(t, sql, "DROP COLUMN")
	require.NotContains(t, sql, "DELETE FROM API_KEYS")
}

func TestProjectedKeyCacheInvalidationMigrationQueuesVerifierHash(t *testing.T) {
	raw, err := os.ReadFile("198_projected_key_auth_cache_outbox.sql")
	require.NoError(t, err)
	sql := strings.ToUpper(string(raw))

	require.Contains(t, sql, "API_KEY_AUTH_CACHE_KEY(K.KEY_SHA256, K.KEY)")
	require.Contains(t, sql, "NEW.KEY_SHA256")
	require.Contains(t, sql, "OLD.KEY_SHA256")
	require.Contains(t, sql, "AFTER INSERT OR UPDATE OR DELETE ON API_KEYS")
	require.Contains(t, sql, "ENQUEUE_USER_AUTH_CACHE_INVALIDATION")
	require.Contains(t, sql, "ENQUEUE_GROUP_AUTH_CACHE_INVALIDATION")
	require.Contains(t, sql, "ENQUEUE_ALLOWED_GROUP_AUTH_CACHE_INVALIDATION")
	require.NotContains(t, sql, "SELECT ENCODE(SHA256(CONVERT_TO(K.KEY")
}
