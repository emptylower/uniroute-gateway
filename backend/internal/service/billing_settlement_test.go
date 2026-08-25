package service

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"

	"github.com/stretchr/testify/require"
)

func newSettlementTestFX() *ExchangeRateService {
	return &ExchangeRateService{
		bootstrapRate: 7.2,
		ttl:           time.Minute,
		staleTTL:      time.Hour,
		cache:         make(map[string]ExchangeRateSnapshot),
	}
}

func settlementTestCost() *CostBreakdown {
	// Mirror production shape: official price 2.0 USD, group multiplier 0.05.
	return &CostBreakdown{TotalCost: 2.0, ActualCost: 0.1}
}

func TestNormalizeSettlementCurrencyMode(t *testing.T) {
	require.Equal(t, "", NormalizeSettlementCurrencyMode(""))
	require.Equal(t, "", NormalizeSettlementCurrencyMode("   "))
	require.Equal(t, "", NormalizeSettlementCurrencyMode("bogus"))
	require.Equal(t, SettlementCurrencyModeUser, NormalizeSettlementCurrencyMode("user"))
	require.Equal(t, SettlementCurrencyModeUser, NormalizeSettlementCurrencyMode(" USER "))
	require.Equal(t, SettlementCurrencyModeFixedUSD, NormalizeSettlementCurrencyMode("fixed_usd"))
	require.Equal(t, SettlementCurrencyModeFixedUSD, NormalizeSettlementCurrencyMode("Fixed_Usd"))
}

func TestResolveCostSettlement_NilConfigKeepsLegacyLiveSettlement(t *testing.T) {
	user := &User{BillingCurrency: CurrencyCNY}
	cost := settlementTestCost()

	settlement, err := ResolveCostSettlement(context.Background(), cost, user, false, newSettlementTestFX(), nil)
	require.NoError(t, err)
	require.Equal(t, CurrencyUSD, settlement.SourceCurrency)
	require.Equal(t, CurrencyCNY, settlement.SettlementCurrency)
	require.InDelta(t, 7.2, settlement.ExchangeRate, 1e-12)
	require.Equal(t, "bootstrap_config", settlement.ExchangeRateSource)
	require.InDelta(t, 2.0, settlement.SourceCost, 1e-12)
	require.InDelta(t, 14.4, settlement.BaseCost, 1e-9)
	// effective multiplier 0.05 preserved through conversion
	require.InDelta(t, 0.72, cost.ActualCost, 1e-9)
}

func TestResolveCostSettlement_SimpleRunModeDefaultsToFixedUSD(t *testing.T) {
	cfg := &config.Config{RunMode: config.RunModeSimple}
	user := &User{BillingCurrency: CurrencyCNY}
	cost := settlementTestCost()

	settlement, err := ResolveCostSettlement(context.Background(), cost, user, false, newSettlementTestFX(), cfg)
	require.NoError(t, err)
	require.Equal(t, CurrencyUSD, settlement.SettlementCurrency)
	require.InDelta(t, 1.0, settlement.ExchangeRate, 1e-12)
	require.Equal(t, "simple_mode", settlement.ExchangeRateSource)
	require.InDelta(t, 2.0, settlement.BaseCost, 1e-12)
	// fixed mode must not touch the computed cost
	require.InDelta(t, 0.1, cost.ActualCost, 1e-12)
}

func TestResolveCostSettlement_StandardRunModeDefaultsToLiveSettlement(t *testing.T) {
	cfg := &config.Config{RunMode: config.RunModeStandard}
	user := &User{BillingCurrency: CurrencyCNY}
	cost := settlementTestCost()

	settlement, err := ResolveCostSettlement(context.Background(), cost, user, false, newSettlementTestFX(), cfg)
	require.NoError(t, err)
	require.Equal(t, CurrencyCNY, settlement.SettlementCurrency)
	require.InDelta(t, 7.2, settlement.ExchangeRate, 1e-12)
	require.InDelta(t, 0.72, cost.ActualCost, 1e-9)
}

func TestResolveCostSettlement_ExplicitFixedUSDOverridesStandardRunMode(t *testing.T) {
	cfg := &config.Config{RunMode: config.RunModeStandard}
	cfg.Billing.Settlement.CurrencyMode = SettlementCurrencyModeFixedUSD
	user := &User{BillingCurrency: CurrencyCNY}
	cost := settlementTestCost()

	settlement, err := ResolveCostSettlement(context.Background(), cost, user, false, newSettlementTestFX(), cfg)
	require.NoError(t, err)
	require.Equal(t, CurrencyUSD, settlement.SettlementCurrency)
	require.InDelta(t, 1.0, settlement.ExchangeRate, 1e-12)
	require.Equal(t, "fixed_usd", settlement.ExchangeRateSource)
	require.InDelta(t, 0.1, cost.ActualCost, 1e-12)
}

// Production target: RunMode stays simple, currency_mode=user switches CNY users
// to live FX settlement without touching any other simple-mode behavior.
func TestResolveCostSettlement_ExplicitUserOverridesSimpleRunMode(t *testing.T) {
	cfg := &config.Config{RunMode: config.RunModeSimple}
	cfg.Billing.Settlement.CurrencyMode = SettlementCurrencyModeUser
	user := &User{BillingCurrency: CurrencyCNY}
	cost := settlementTestCost()

	settlement, err := ResolveCostSettlement(context.Background(), cost, user, false, newSettlementTestFX(), cfg)
	require.NoError(t, err)
	require.Equal(t, CurrencyCNY, settlement.SettlementCurrency)
	require.InDelta(t, 7.2, settlement.ExchangeRate, 1e-12)
	require.InDelta(t, 14.4, settlement.BaseCost, 1e-9)
	require.InDelta(t, 0.72, cost.ActualCost, 1e-9)
}

func TestResolveCostSettlement_UserModeRequiresExchangeRateService(t *testing.T) {
	cfg := &config.Config{RunMode: config.RunModeSimple}
	cfg.Billing.Settlement.CurrencyMode = SettlementCurrencyModeUser
	user := &User{BillingCurrency: CurrencyCNY}

	_, err := ResolveCostSettlement(context.Background(), settlementTestCost(), user, false, nil, cfg)
	require.Error(t, err)
}

func TestResolveCostSettlement_SubscriptionBillingStaysUSDInUserMode(t *testing.T) {
	cfg := &config.Config{RunMode: config.RunModeSimple}
	cfg.Billing.Settlement.CurrencyMode = SettlementCurrencyModeUser
	user := &User{BillingCurrency: CurrencyCNY}
	cost := settlementTestCost()

	settlement, err := ResolveCostSettlement(context.Background(), cost, user, true, newSettlementTestFX(), cfg)
	require.NoError(t, err)
	require.Equal(t, CurrencyUSD, settlement.SettlementCurrency)
	require.InDelta(t, 1.0, settlement.ExchangeRate, 1e-12)
	require.Equal(t, "identity", settlement.ExchangeRateSource)
	// identity rate keeps the multiplier-adjusted actual cost unchanged
	require.InDelta(t, 0.1, cost.ActualCost, 1e-12)
}

func TestResolveCostSettlement_UserModePrefersPinnedSnapshot(t *testing.T) {
	cfg := &config.Config{RunMode: config.RunModeSimple}
	cfg.Billing.Settlement.CurrencyMode = SettlementCurrencyModeUser
	user := &User{BillingCurrency: CurrencyCNY}
	cost := settlementTestCost()

	ctx := WithBillingSettlementContext(context.Background())
	storeBillingSettlementSnapshot(ctx, ExchangeRateSnapshot{
		BaseCurrency: CurrencyUSD, QuoteCurrency: CurrencyCNY,
		Rate: 7.5, Source: "live", AsOf: time.Now().UTC(),
	})

	settlement, err := ResolveCostSettlement(ctx, cost, user, false, newSettlementTestFX(), cfg)
	require.NoError(t, err)
	require.Equal(t, "live", settlement.ExchangeRateSource)
	require.InDelta(t, 7.5, settlement.ExchangeRate, 1e-12)
	require.InDelta(t, 2.0*7.5, settlement.BaseCost, 1e-9)
	require.InDelta(t, 0.75, cost.ActualCost, 1e-9)
}

func TestResolveCostSettlement_NilCostFixedMode(t *testing.T) {
	cfg := &config.Config{RunMode: config.RunModeSimple}

	settlement, err := ResolveCostSettlement(context.Background(), nil, nil, false, nil, cfg)
	require.NoError(t, err)
	require.Equal(t, CurrencyUSD, settlement.SettlementCurrency)
	require.Zero(t, settlement.SourceCost)
	require.Zero(t, settlement.BaseCost)
}

func TestResolveCostSettlement_NilCostUserMode(t *testing.T) {
	cfg := &config.Config{RunMode: config.RunModeSimple}
	cfg.Billing.Settlement.CurrencyMode = SettlementCurrencyModeUser
	user := &User{BillingCurrency: CurrencyCNY}

	settlement, err := ResolveCostSettlement(context.Background(), nil, user, false, newSettlementTestFX(), cfg)
	require.NoError(t, err)
	require.Equal(t, CurrencyCNY, settlement.SettlementCurrency)
	require.InDelta(t, 7.2, settlement.ExchangeRate, 1e-12)
	require.Zero(t, settlement.SourceCost)
}
