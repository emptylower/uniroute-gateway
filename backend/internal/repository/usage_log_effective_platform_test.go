package repository

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUsageLogEffectivePlatformExprMatchesAnalyticsBaseline(t *testing.T) {
	require.Equal(t,
		"CASE WHEN g.platform = 'composite' THEN a.platform ELSE COALESCE(NULLIF(g.platform,''), a.platform) END",
		usageLogEffectivePlatformExpr,
	)
}
