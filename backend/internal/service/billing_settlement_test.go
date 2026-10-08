package service

import (
	"context"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestUSDSettlementPreservesAmountsAndQuotaCosts(t *testing.T) {
	for _, currency := range []string{"USD", "CNY", ""} {
		t.Run(currency, func(t *testing.T) {
			cost := &CostBreakdown{TotalCost: 2, ActualCost: 0.1}
			frozen := &BillingSnapshot{FX: ExchangeRateSnapshot{BaseCurrency: "USD", QuoteCurrency: "CNY", Rate: 7.2}}
			result, err := ResolveCostSettlement(SettlementContextFromSnapshot(context.Background(), frozen), cost, &User{BillingCurrency: currency}, false, nil, &config.Config{})
			require.NoError(t, err)
			require.Equal(t, "USD", result.SettlementCurrency)
			require.Equal(t, float64(1), result.ExchangeRate)
			require.Equal(t, float64(2), result.BaseCost)
			require.Equal(t, 0.1, cost.ActualCost)
			params := &postUsageBillingParams{Cost: cost, User: &User{ID: 1}, APIKey: &APIKey{ID: 2, Quota: 10, RateLimit5h: 5}, Account: &Account{ID: 3}, APIKeyService: &APIKeyService{}}
			cmd := buildUsageBillingCommand("usd-regression", nil, params)
			require.Zero(t, cmd.BalanceCost)
			require.Equal(t, 0.1, cmd.WalletCostUSD)
			require.Equal(t, 0.1, cmd.APIKeyQuotaCost)
			require.Equal(t, 0.1, cmd.APIKeyRateLimitCost)
		})
	}
}
