package repository

import "strings"

// Historical rows retain their original amount and frozen rate. Projection
// columns are backfilled once by the USD migration, never from market FX.
func convertUsageActualCost(actualCost, exchangeRate float64, settlementCurrency, exchangeRateSource, _ string) float64 {
	if strings.EqualFold(exchangeRateSource, "legacy") || exchangeRate <= 0 {
		return actualCost
	}
	if strings.EqualFold(settlementCurrency, "CNY") {
		return actualCost / exchangeRate
	}
	return actualCost
}
func convertUsageStandardCost(sourceCost, totalCost, _ float64, exchangeRateSource, _ string) float64 {
	if strings.EqualFold(exchangeRateSource, "legacy") {
		return totalCost
	}
	return sourceCost
}
func usageDisplayCurrency(_ string) string { return "USD" }
func usageActualCostDisplayExpr(_, alias string) string {
	if alias != "" {
		alias += "."
	}
	return "COALESCE(SUM(" + alias + "actual_cost_usd), 0)"
}
func usageStandardCostDisplayExpr(_, alias string) string {
	if alias != "" {
		alias += "."
	}
	return "COALESCE(SUM(" + alias + "base_cost_usd), 0)"
}
