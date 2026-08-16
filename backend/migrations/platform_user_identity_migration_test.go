package migrations

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlatformUserIdentityMigrationIsAdditiveUniqueAndImmutable(t *testing.T) {
	raw, err := os.ReadFile("196_platform_user_identity.sql")
	require.NoError(t, err)
	sql := strings.ToUpper(string(raw))

	require.Contains(t, sql, "ADD COLUMN IF NOT EXISTS PLATFORM_USER_ID")
	require.Contains(t, sql, "CREATE UNIQUE INDEX IF NOT EXISTS IDX_USERS_PLATFORM_USER_ID")
	require.Contains(t, sql, "WHERE PLATFORM_USER_ID IS NOT NULL")
	require.Contains(t, sql, "PLATFORM_USER_ID IS IMMUTABLE ONCE ASSIGNED")
	require.NotContains(t, sql, "DROP COLUMN")
	require.NotContains(t, sql, "DELETE FROM USERS")
}

func TestPlatformSignupSourceMigrationAlignsDatabaseConstraint(t *testing.T) {
	raw, err := os.ReadFile("199_allow_platform_signup_source.sql")
	require.NoError(t, err)
	sql := strings.ToUpper(string(raw))

	require.Contains(t, sql, "DROP CONSTRAINT IF EXISTS USERS_SIGNUP_SOURCE_CHECK")
	require.Contains(t, sql, "ADD CONSTRAINT USERS_SIGNUP_SOURCE_CHECK")
	for _, source := range []string{
		"'EMAIL'",
		"'LINUXDO'",
		"'WECHAT'",
		"'OIDC'",
		"'GITHUB'",
		"'GOOGLE'",
		"'DINGTALK'",
		"'PLATFORM'",
	} {
		require.Contains(t, sql, source)
	}
	require.NotContains(t, sql, "UPDATE USERS")
	require.NotContains(t, sql, "DELETE FROM USERS")
}
