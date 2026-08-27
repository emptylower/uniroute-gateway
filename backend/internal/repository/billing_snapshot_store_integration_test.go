//go:build integration

package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestBillingSnapshotStoreRoundTripAndIdempotentInsert(t *testing.T) {
	require.NoError(t, ApplyMigrations(context.Background(), integrationDB))
	store := NewBillingSnapshotStore(integrationDB)
	price := 2e-6
	snap := &service.BillingSnapshot{
		ID: "bsnap_test_" + t.Name(), Version: service.BillingSnapshotVersion, FrozenAt: time.Now().UTC().Truncate(time.Microsecond),
		Family: service.BillingFamilyOpenAI, UserID: 1, APIKeyID: 2, AccountID: 3, RequestedModel: "m", BillingModel: "m", Candidates: []string{"m"},
		Pricing: service.BillingSnapshotPricing{Mode: service.BillingModeToken, Source: service.PricingSourceLiteLLM, Base: &service.ModelPricing{InputPricePerToken: price, OutputPricePerToken: price * 5, MaxInputTokens: 128000}},
		Multipliers: service.BillingSnapshotMultipliers{Base: 1, Text: 1, Image: 1, Video: 1, WebSearch: 1, Account: 1, PeakAt: time.Now().UTC().Truncate(time.Microsecond)},
		FX:          service.ExchangeRateSnapshot{BaseCurrency: "USD", QuoteCurrency: "CNY", Rate: 7, Source: "test"},
		Flags:       service.BillingSnapshotFlags{BillingCurrency: "CNY", MultiplierCurrency: "CNY"},
	}
	require.NoError(t, store.InsertBillingSnapshot(context.Background(), snap))
	require.NoError(t, store.InsertBillingSnapshot(context.Background(), snap), "second insert is a no-op")
	back, err := store.GetBillingSnapshot(context.Background(), snap.ID)
	require.NoError(t, err)
	require.Equal(t, snap.Pricing.Base.MaxInputTokens, back.Pricing.Base.MaxInputTokens)
	require.Equal(t, snap.FX.Rate, back.FX.Rate)
	require.Equal(t, snap.Multipliers.Text, back.Multipliers.Text)
	_, err = store.GetBillingSnapshot(context.Background(), "bsnap_missing")
	require.ErrorIs(t, err, service.ErrBillingSnapshotNotFound)
	var n int
	require.NoError(t, integrationDB.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM wallet_billing_snapshot WHERE id = $1`, snap.ID).Scan(&n))
	require.Equal(t, 1, n)
}
