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
	if currency == "" {
		return CurrencyCNY, nil
	}
	switch currency {
	case CurrencyCNY, CurrencyUSD:
		return currency, nil
	default:
		return "", fmt.Errorf("unsupported currency %q: allowed values are CNY and USD", value)
	}
}

func IsSupportedBillingCurrency(value string) bool {
	value = strings.ToUpper(strings.TrimSpace(value))
	return value == CurrencyCNY || value == CurrencyUSD
}

func normalizeBillingCurrencyOrDefault(value string) string {
	currency, err := NormalizeBillingCurrency(value)
	if err != nil {
		return CurrencyCNY
	}
	return currency
}

func NormalizeUserBillingCurrency(value string) string {
	return normalizeBillingCurrencyOrDefault(value)
}
