package repository

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestUsageAggregatesUseUSDProjectionColumns(t *testing.T) {
	require.Equal(t, "COALESCE(SUM(ul.actual_cost_usd), 0)", usageActualCostDisplayExpr("CNY", "ul"))
	require.Equal(t, "COALESCE(SUM(base_cost_usd), 0)", usageStandardCostDisplayExpr("USD", ""))
	require.Equal(t, "USD", usageDisplayCurrency("EUR"))
}
func TestHistoricalFrozenCNYAuditConvertsWithoutMarketFX(t *testing.T) {
	require.Equal(t, float64(1), convertUsageActualCost(7.2, 7.2, "CNY", "frozen", "USD"))
	require.Equal(t, float64(2), convertUsageActualCost(2, 7.2, "USD", "identity", "USD"))
	require.Equal(t, float64(3), convertUsageActualCost(3, 1, "CNY", "legacy", "USD"))
	require.Equal(t, float64(1), convertUsageStandardCost(1, 999, 7.2, "frozen", "USD"))
}
