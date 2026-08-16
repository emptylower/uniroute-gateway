package repository

import "strings"

func convertUsageActualCost(actualCost, exchangeRate float64, settlementCurrency, exchangeRateSource, displayCurrency string) float64 {
	if strings.EqualFold(strings.TrimSpace(exchangeRateSource), "legacy") || exchangeRate <= 0 {
		return actualCost
	}
	settlement := strings.ToUpper(strings.TrimSpace(settlementCurrency))
	display := usageDisplayCurrency(displayCurrency)
	if settlement == display || display == "" {
		return actualCost
	}
	if settlement == "USD" && display == "CNY" {
		return actualCost * exchangeRate
	}
	if settlement == "CNY" && display == "USD" {
		return actualCost / exchangeRate
	}
	return actualCost
}

func convertUsageStandardCost(sourceCost, totalCost, exchangeRate float64, exchangeRateSource, displayCurrency string) float64 {
	if strings.EqualFold(strings.TrimSpace(exchangeRateSource), "legacy") || exchangeRate <= 0 {
		return totalCost
	}
	if usageDisplayCurrency(displayCurrency) == "CNY" {
		return sourceCost * exchangeRate
	}
	return sourceCost
}

func usageDisplayCurrency(currency string) string {
	if strings.EqualFold(strings.TrimSpace(currency), "CNY") {
		return "CNY"
	}
	if strings.EqualFold(strings.TrimSpace(currency), "USD") {
		return "USD"
	}
	return ""
}

func usageActualCostDisplayExpr(currency, alias string) string {
	prefix := ""
	if alias != "" {
		prefix = alias + "."
	}
	if usageDisplayCurrency(currency) == "CNY" {
		return "COALESCE(SUM(CASE WHEN " + prefix + "exchange_rate_source = 'legacy' THEN " + prefix + "actual_cost WHEN " + prefix + "settlement_currency = 'CNY' THEN " + prefix + "actual_cost ELSE " + prefix + "actual_cost * " + prefix + "exchange_rate END), 0)"
	}
	return "COALESCE(SUM(CASE WHEN " + prefix + "exchange_rate_source = 'legacy' THEN " + prefix + "actual_cost WHEN " + prefix + "settlement_currency = 'USD' THEN " + prefix + "actual_cost ELSE " + prefix + "actual_cost / NULLIF(" + prefix + "exchange_rate, 0) END), 0)"
}

func usageStandardCostDisplayExpr(currency, alias string) string {
	prefix := ""
	if alias != "" {
		prefix = alias + "."
	}
	if usageDisplayCurrency(currency) == "CNY" {
		return "COALESCE(SUM(CASE WHEN " + prefix + "exchange_rate_source = 'legacy' THEN " + prefix + "total_cost ELSE " + prefix + "source_cost * " + prefix + "exchange_rate END), 0)"
	}
	return "COALESCE(SUM(CASE WHEN " + prefix + "exchange_rate_source = 'legacy' THEN " + prefix + "total_cost ELSE " + prefix + "source_cost END), 0)"
}
