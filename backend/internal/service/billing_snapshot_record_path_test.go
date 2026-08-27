//go:build unit

package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

var recordModeDriftTestRan bool // read by TestMain (Task 8) so a -run subset never asserts a count it did not produce

func TestOpenAIUsageTokensAndSettlementInput(t *testing.T) {
	usage := OpenAIUsage{InputTokens: 1000, CacheReadInputTokens: 300, CacheCreationInputTokens: 200, OutputTokens: 50, ImageInputTokens: 7, ImageOutputTokens: 9}
	tokens := openAIUsageTokens(usage)
	require.Equal(t, UsageTokens{InputTokens: 500, ImageInputTokens: 7, OutputTokens: 50, CacheCreationTokens: 200, CacheReadTokens: 300, ImageOutputTokens: 9}, tokens, "the :146-159 subtraction, clamped at 0")
	require.Equal(t, 0, openAIUsageTokens(OpenAIUsage{InputTokens: 10, CacheReadInputTokens: 20}).InputTokens)
	in := snapshotSettlementInputFromOpenAIResult(&OpenAIForwardResult{Usage: usage, ImageCount: 2, ImageSize: "1K", VideoCount: 1, VideoResolution: "720p", VideoDurationSeconds: 5, WebSearchCalls: 3}, "priority", true)
	require.Equal(t, tokens, in.Tokens)
	require.Equal(t, "priority", in.ServiceTier)
	require.Equal(t, 2, in.ImageCount)
	require.Equal(t, 3, in.WebSearchCalls)
	require.True(t, in.GrokVideo)
	require.Equal(t, SnapshotSettlementInput{}, snapshotSettlementInputFromOpenAIResult(nil, "", false))
}

func TestApplyBillingSnapshotToSettlementModes(t *testing.T) {
	recordModeDriftTestRan = true
	svc, snap, _, _, _ := freezeForSettleTest(t, BillingFamilyGeneric, "claude-sonnet-4")
	settler := &billingSnapshotSettler{snapshots: svc, billing: svc.billing}
	in := SnapshotSettlementInput{Tokens: UsageTokens{InputTokens: 1000, OutputTokens: 100}}
	fromSnap, err := svc.billing.CalculateCostFromSnapshot(snap, in)
	require.NoError(t, err)
	live := &CostBreakdown{ActualCost: fromSnap.ActualCost * 2, TotalCost: fromSnap.TotalCost * 2}

	svc.cfg.CanonicalWallet.BillingSnapshotMode = "off"
	got, _ := settler.applyBillingSnapshotToSettlement(context.Background(), snap, live, in, "claude-sonnet-4")
	require.Same(t, live, got)

	svc.cfg.CanonicalWallet.BillingSnapshotMode = "record"
	drift0 := BillingSnapshotMetricsSnapshot().Drift
	got, _ = settler.applyBillingSnapshotToSettlement(context.Background(), snap, live, in, "claude-sonnet-4")
	require.Same(t, live, got, "record mode settles as today")
	require.Equal(t, drift0+1, BillingSnapshotMetricsSnapshot().Drift, "record mode counts the drift")
	got, _ = settler.applyBillingSnapshotToSettlement(context.Background(), snap, fromSnap, in, "claude-sonnet-4")
	require.Equal(t, drift0+1, BillingSnapshotMetricsSnapshot().Drift, "equal costs are not drift")

	svc.cfg.CanonicalWallet.BillingSnapshotMode = "settle"
	got, ctx := settler.applyBillingSnapshotToSettlement(context.Background(), snap, live, in, "claude-sonnet-4")
	require.Equal(t, fromSnap.ActualCost, got.ActualCost)
	pinned, ok := pinnedBillingSettlementSnapshot(ctx, CurrencyUSD, "CNY")
	require.True(t, ok)
	require.Equal(t, snap.FX.Rate, pinned.Rate)

	// a settled model the snapshot did not price → live, counted, no drift
	mismatch0 := BillingSnapshotMetricsSnapshot().ModelMismatch
	got, _ = settler.applyBillingSnapshotToSettlement(context.Background(), snap, live, in, "some-other-model")
	require.Same(t, live, got)
	require.Equal(t, mismatch0+1, BillingSnapshotMetricsSnapshot().ModelMismatch)
	require.Equal(t, drift0+1, BillingSnapshotMetricsSnapshot().Drift)

	got, _ = settler.applyBillingSnapshotToSettlement(context.Background(), nil, live, in, "claude-sonnet-4")
	require.Same(t, live, got, "no snapshot → live path, in every mode")
}

// The FX pin must never clobber a holder the request already carries.
func TestSettlementContextFromSnapshotUsesAFreshHolder(t *testing.T) {
	_, snap, _, _, _ := freezeForSettleTest(t, BillingFamilyGeneric, "claude-sonnet-4")
	outer := WithBillingSettlementContext(context.Background())
	storeBillingSettlementSnapshot(outer, ExchangeRateSnapshot{BaseCurrency: "USD", QuoteCurrency: "CNY", Rate: 6.5})
	snap.FX.Rate = 7.25
	inner := SettlementContextFromSnapshot(outer, snap)
	got, ok := pinnedBillingSettlementSnapshot(inner, CurrencyUSD, "CNY")
	require.True(t, ok)
	require.Equal(t, 7.25, got.Rate)
	still, ok := pinnedBillingSettlementSnapshot(outer, CurrencyUSD, "CNY")
	require.True(t, ok)
	require.Equal(t, 6.5, still.Rate, "the caller's holder is untouched")
}
