package service

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCurrencyAPIProviderParsesRateAndTimestamp(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v3/latest", r.URL.Path)
		require.Equal(t, CurrencyUSD, r.URL.Query().Get("base_currency"))
		require.Equal(t, CurrencyCNY, r.URL.Query().Get("currencies"))
		require.Equal(t, "secret", r.Header.Get("apikey"))
		_, _ = w.Write([]byte(`{"meta":{"last_updated_at":"2026-08-12T04:00:00Z"},"data":{"CNY":{"code":"CNY","value":7.21}}}`))
	}))
	defer server.Close()

	provider := &httpExchangeRateProvider{
		provider: "currencyapi", endpoint: server.URL + "/v3/latest", apiKey: "secret",
		client: server.Client(),
	}
	snapshot, err := provider.Fetch(context.Background(), CurrencyUSD, CurrencyCNY)

	require.NoError(t, err)
	require.InDelta(t, 7.21, snapshot.Rate, 1e-12)
	require.Equal(t, "currencyapi", snapshot.Source)
	require.Equal(t, time.Date(2026, time.August, 12, 4, 0, 0, 0, time.UTC), snapshot.AsOf)
}

func TestCurrencyAPIProviderRejectsMissingKeyBeforeNetwork(t *testing.T) {
	provider := &httpExchangeRateProvider{
		provider: "currencyapi", endpoint: "https://api.currencyapi.com/v3/latest", client: http.DefaultClient,
	}
	_, err := provider.Fetch(context.Background(), CurrencyUSD, CurrencyCNY)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "API key"))
}

type exchangeRateProviderFake struct {
	snapshot ExchangeRateSnapshot
	err      error
	calls    int
}

func (f *exchangeRateProviderFake) Fetch(_ context.Context, _, _ string) (ExchangeRateSnapshot, error) {
	f.calls++
	return f.snapshot, f.err
}

func TestExchangeRateServiceCachesLiveAndLabelsStaleSnapshot(t *testing.T) {
	provider := &exchangeRateProviderFake{snapshot: ExchangeRateSnapshot{Rate: 7.18, Source: "test_live", AsOf: time.Now().UTC()}}
	svc := &ExchangeRateService{provider: provider, ttl: time.Minute, staleTTL: time.Hour, cache: make(map[string]ExchangeRateSnapshot)}

	first, err := svc.Snapshot(context.Background(), CurrencyUSD, CurrencyCNY)
	require.NoError(t, err)
	second, err := svc.Snapshot(context.Background(), CurrencyUSD, CurrencyCNY)
	require.NoError(t, err)
	require.Equal(t, 1, provider.calls)
	require.Equal(t, first.Rate, second.Rate)
	require.False(t, second.Fallback)

	provider.err = errors.New("provider down")
	cached := svc.cache["USD/CNY"]
	cached.ExpiresAt = time.Now().Add(-time.Second)
	svc.cache["USD/CNY"] = cached
	stale, err := svc.Snapshot(context.Background(), CurrencyUSD, CurrencyCNY)
	require.NoError(t, err)
	require.Equal(t, "stale_test_live", stale.Source)
	require.True(t, stale.Fallback)
	_, err = svc.Snapshot(context.Background(), CurrencyUSD, CurrencyCNY)
	require.NoError(t, err)
	require.Equal(t, 2, provider.calls, "fresh stale snapshot should throttle provider retries")
}

func TestExchangeRateServiceUsesAuditableBootstrapFallback(t *testing.T) {
	svc := &ExchangeRateService{bootstrapRate: 7.2, ttl: time.Minute, staleTTL: time.Hour, cache: make(map[string]ExchangeRateSnapshot)}
	snapshot, err := svc.Snapshot(context.Background(), CurrencyUSD, CurrencyCNY)
	require.NoError(t, err)
	require.InDelta(t, 7.2, snapshot.Rate, 1e-12)
	require.Equal(t, "bootstrap_config", snapshot.Source)
	require.True(t, snapshot.Fallback)

	second, err := svc.Snapshot(context.Background(), CurrencyUSD, CurrencyCNY)
	require.NoError(t, err)
	require.Equal(t, snapshot.Rate, second.Rate)
	require.Equal(t, snapshot.Source, second.Source)
}

func TestExchangeRateServiceRejectsUnsupportedBaseCurrency(t *testing.T) {
	svc := &ExchangeRateService{bootstrapRate: 7.2, ttl: time.Minute, staleTTL: time.Hour, cache: make(map[string]ExchangeRateSnapshot)}

	_, err := svc.Snapshot(context.Background(), "EUR", CurrencyCNY)

	require.ErrorContains(t, err, "unsupported base currency")
}

func TestExchangeRateServiceRejectsImplausibleProviderRate(t *testing.T) {
	provider := &exchangeRateProviderFake{snapshot: ExchangeRateSnapshot{
		Rate: 99, Source: "test_live", AsOf: time.Now().UTC(),
	}}
	svc := &ExchangeRateService{provider: provider, ttl: time.Minute, staleTTL: time.Hour, cache: make(map[string]ExchangeRateSnapshot)}

	_, err := svc.Snapshot(context.Background(), CurrencyUSD, CurrencyCNY)

	require.ErrorContains(t, err, "no valid exchange rate")
}

func TestExchangeRateServiceRejectsStaleAndFutureProviderTimestamps(t *testing.T) {
	for name, asOf := range map[string]time.Time{
		"stale":  time.Now().UTC().Add(-72 * time.Hour),
		"future": time.Now().UTC().Add(time.Hour),
	} {
		t.Run(name, func(t *testing.T) {
			provider := &exchangeRateProviderFake{snapshot: ExchangeRateSnapshot{
				Rate: 7.2, Source: "test_live", AsOf: asOf,
			}}
			svc := &ExchangeRateService{provider: provider, ttl: time.Minute, staleTTL: time.Hour, cache: make(map[string]ExchangeRateSnapshot)}

			_, err := svc.Snapshot(context.Background(), CurrencyUSD, CurrencyCNY)

			require.ErrorContains(t, err, "no valid exchange rate")
		})
	}
}

func TestSettleUsageCostAppliesFXBeforeMultiplier(t *testing.T) {
	cost := &CostBreakdown{TotalCost: 10, ActualCost: 2}
	user := &User{BillingCurrency: CurrencyCNY}
	fx := &ExchangeRateService{bootstrapRate: 7.2, ttl: time.Minute, staleTTL: time.Hour, cache: make(map[string]ExchangeRateSnapshot)}

	snapshot, err := settleUsageCost(context.Background(), cost, user, false, fx)

	require.NoError(t, err)
	require.InDelta(t, 10, snapshot.SourceCost, 1e-12)
	require.InDelta(t, 72, snapshot.BaseCost, 1e-12)
	require.InDelta(t, 14.4, cost.ActualCost, 1e-12)
	require.Equal(t, CurrencyUSD, snapshot.SourceCurrency)
	require.Equal(t, CurrencyCNY, snapshot.SettlementCurrency)
}

func TestSettleUsageCostFailsClosedWithoutCrossCurrencyRate(t *testing.T) {
	cost := &CostBreakdown{TotalCost: 10, ActualCost: 2}
	user := &User{BillingCurrency: CurrencyCNY}
	fx := &ExchangeRateService{ttl: time.Minute, staleTTL: time.Hour, cache: make(map[string]ExchangeRateSnapshot)}

	_, err := settleUsageCost(context.Background(), cost, user, false, fx)

	require.ErrorContains(t, err, "no valid exchange rate")
	require.InDelta(t, 2, cost.ActualCost, 1e-12)
}

func TestGroupRateMultiplierForCurrencyFallsBackToLegacy(t *testing.T) {
	cny, usd := 0.3, 0.5
	group := &Group{RateMultiplier: 0.8, RateMultiplierCNY: &cny, RateMultiplierUSD: &usd}
	require.InDelta(t, 0.3, group.RateMultiplierForCurrency(CurrencyCNY), 1e-12)
	require.InDelta(t, 0.5, group.RateMultiplierForCurrency(CurrencyUSD), 1e-12)
	require.InDelta(t, 0.8, (&Group{RateMultiplier: 0.8}).RateMultiplierForCurrency(CurrencyUSD), 1e-12)
}
