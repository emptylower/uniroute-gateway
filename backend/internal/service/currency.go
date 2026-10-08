package service

import (
	"context"
	"fmt"
	"strings"
)

const (
	CurrencyCNY = "CNY"
	CurrencyUSD = "USD"
)

type billingCurrencyUpdateContextKey struct{}

// ContextWithBillingCurrencyUpdate marks a user update as an explicit billing
// currency change. Generic profile updates must not persist a currency copied
// from a stale user snapshot.
func ContextWithBillingCurrencyUpdate(ctx context.Context) context.Context {
	return context.WithValue(ctx, billingCurrencyUpdateContextKey{}, true)
}

func BillingCurrencyUpdateRequested(ctx context.Context) bool {
	requested, _ := ctx.Value(billingCurrencyUpdateContextKey{}).(bool)
	return requested
}

func NormalizeBillingCurrency(value string) (string, error) {
	currency := strings.ToUpper(strings.TrimSpace(value))
	if currency == "" || currency == CurrencyUSD {
		return CurrencyUSD, nil
	}
	return "", fmt.Errorf("unsupported currency %q: only USD is supported", value)
}
func IsSupportedBillingCurrency(value string) bool {
	return strings.EqualFold(strings.TrimSpace(value), CurrencyUSD)
}
func NormalizeUserBillingCurrency(_ string) string { return CurrencyUSD }

// NormalizeHistoricalBillingCurrency is only for retired native ledger audit paths.
func NormalizeHistoricalBillingCurrency(raw string) string {
	currency := strings.ToUpper(strings.TrimSpace(raw))
	if currency == "" {
		return CurrencyCNY
	}
	return currency
}
