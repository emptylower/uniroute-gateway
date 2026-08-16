package repository

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUsageCurrencyExpressionsNormalizeMixedSettlementRowsBeforeSum(t *testing.T) {
	cny := usageActualCostDisplayExpr("CNY", "ul")
	usd := usageActualCostDisplayExpr("USD", "")
	standardCNY := usageStandardCostDisplayExpr("CNY", "")

	require.Contains(t, cny, "settlement_currency = 'CNY'")
	require.Contains(t, cny, "actual_cost * ul.exchange_rate")
	require.Contains(t, usd, "settlement_currency = 'USD'")
	require.Contains(t, usd, "actual_cost / NULLIF(exchange_rate, 0)")
	require.Contains(t, standardCNY, "source_cost * exchange_rate")
	require.Contains(t, cny, "exchange_rate_source = 'legacy'")
}

func TestUsageDisplayCurrencyRejectsUnknownValues(t *testing.T) {
	require.Equal(t, "CNY", usageDisplayCurrency("cny"))
	require.Equal(t, "USD", usageDisplayCurrency("USD"))
	require.Empty(t, usageDisplayCurrency("EUR"))
}

func TestConvertUsageActualCostMixedSettlementRows(t *testing.T) {
	require.InDelta(t, 14.4, convertUsageActualCost(7.2, 7.2, "CNY", "currencyapi", "CNY")+
		convertUsageActualCost(1, 7.2, "USD", "identity", "CNY"), 1e-12)
	require.InDelta(t, 2, convertUsageActualCost(7.2, 7.2, "CNY", "currencyapi", "USD")+
		convertUsageActualCost(1, 7.2, "USD", "identity", "USD"), 1e-12)
}

func TestConvertUsageStandardCostUsesSourceUSD(t *testing.T) {
	require.InDelta(t, 7.2, convertUsageStandardCost(1, 999, 7.2, "currencyapi", "CNY"), 1e-12)
	require.InDelta(t, 1, convertUsageStandardCost(1, 999, 7.2, "currencyapi", "USD"), 1e-12)
	require.InDelta(t, 3, convertUsageStandardCost(1, 3, 0, "legacy", "CNY"), 1e-12)
}
