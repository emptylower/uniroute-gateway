//go:build unit

package service

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

// testing.TB (not *testing.T) so the benchmarks can reuse the fixture.
func newSnapshotTestFixture(t testing.TB) (*BillingSnapshotService, *APIKey, *User, *Account) {
	t.Helper()
	cfg := &config.Config{}
	cfg.Default.RateMultiplier = 1
	cfg.CanonicalWallet.BillingSnapshotMode = "record"
	// NewExchangeRateService(nil) has bootstrapRate 0 (exchange_rate.go:133),
	// and Snapshot() then fails validateLiveSnapshot's [4,12] band and returns
	// an error (:270-279) — so the fixture needs a real bootstrap rate.
	cfg.Billing.ExchangeRate.BootstrapUSDToCNY = 7.0 // config.go:877
	billing := newTestBillingService()               // fallback prices: claude-sonnet-4 = $3/$15 per MTok
	// A bare &ChannelService{} PANICS on Resolve with a non-nil GroupID:
	// GetChannelModelPricing → lookupGroupChannel → loadCache → fetchChannelData
	// → s.repo.ListAll on a nil repository (model_pricing_resolver.go:67,
	// channel_service.go:499/:481/:177/:293). Build it from mockChannelRepository
	// (channel_service_test.go:19) as newResolverWithChannel does
	// (model_pricing_resolver_test.go:216-236), bound to THIS fixture's group.
	cs := newChannelServiceForTest(7, nil)
	resolver := NewModelPricingResolver(cs, billing)
	fx := NewExchangeRateService(cfg)
	svc := NewBillingSnapshotService(cfg, resolver, billing, fx, nil)
	svc.now = func() time.Time { return time.Date(2026, 8, 27, 3, 0, 0, 0, time.UTC) }
	group := &Group{ID: 7, RateMultiplier: 1.5, ImageRateIndependent: true, ImageRateMultiplier: 2.5, WebSearchPricePerCall: floatPtr(0.02)}
	gid := int64(7)
	user := &User{ID: 42, BillingCurrency: "CNY"}
	apiKey := &APIKey{ID: 11, GroupID: &gid, Group: group, User: user} // APIKey.User (api_key.go:63) — the facades derive the user from it, and Freeze refuses a nil user before any pricing check; every RecordUsage site passes apiKey.User in production (api_key_repo.go:932 populates the edge)
	rate := 1.25
	account := &Account{ID: 99, RateMultiplier: &rate, Platform: PlatformOpenAI}
	return svc, apiKey, user, account
}

func floatPtr(v float64) *float64 { return &v }

// newChannelServiceForTest mirrors newResolverWithChannel
// (model_pricing_resolver_test.go:216-236) but takes the group id and the
// pricing slice, and returns the service so a test can invalidateCache()
// (channel_service.go:378) after mutating the seed — the channel table is
// cached for channelCacheTTL = 10 min (channel_service.go:147, :177-183).
func newChannelServiceForTest(groupID int64, pricing []ChannelModelPricing) *ChannelService {
	repo := &mockChannelRepository{
		listAllFn: func(_ context.Context) ([]Channel, error) {
			if len(pricing) == 0 {
				return nil, nil
			}
			return []Channel{{ID: 1, Name: "test-channel", Status: StatusActive, GroupIDs: []int64{groupID}, ModelPricing: pricing}}, nil
		},
		getGroupPlatformsFn: func(_ context.Context, _ []int64) (map[int64]string, error) {
			return map[int64]string{groupID: "anthropic"}, nil
		},
	}
	return NewChannelService(repo, nil, nil, nil)
}

func TestBillingSnapshotFreezeCapturesEveryPricingInput(t *testing.T) {
	svc, apiKey, user, account := newSnapshotTestFixture(t)
	snap, err := svc.Freeze(context.Background(), FreezeInput{
		APIKey: apiKey, User: user, Account: account,
		RequestedModel: "claude-sonnet-4", BillingModel: "claude-sonnet-4",
		Family:               BillingFamilyGeneric,
		ResolveUserGroupRate: func(_ context.Context, _, _ int64, def float64) float64 { return def },
	})
	require.NoError(t, err)
	require.NotEmpty(t, snap.ID)
	require.True(t, len(snap.ID) > 6 && snap.ID[:6] == "bsnap_")
	require.Equal(t, BillingSnapshotVersion, snap.Version)
	require.Equal(t, int64(42), snap.UserID)
	require.Equal(t, int64(11), snap.APIKeyID)
	require.Equal(t, int64(99), snap.AccountID)
	require.Equal(t, "claude-sonnet-4", snap.BillingModel)
	require.Equal(t, BillingModeToken, snap.Pricing.Mode)
	// resolveBasePricing reports PricingSourceLiteLLM whenever GetModelPricing
	// succeeds, hard-coded fallback included (model_pricing_resolver.go:108-117);
	// PricingSourceFallback only ever accompanies a nil BasePricing.
	require.Equal(t, PricingSourceLiteLLM, snap.Pricing.Source)
	require.NotNil(t, snap.Pricing.Base)
	require.InDelta(t, 3e-6, snap.Pricing.Base.InputPricePerToken, 1e-12)
	require.Equal(t, 1.5, snap.Multipliers.Base)
	require.Equal(t, 1.5, snap.Multipliers.Text, "no peak window configured")
	require.Equal(t, 2.5, snap.Multipliers.Image, "independent image multiplier is frozen")
	require.Equal(t, 1.5, snap.Multipliers.Video)
	require.Equal(t, 1.5, snap.Multipliers.WebSearch)
	require.Equal(t, 1.25, snap.Multipliers.Account)
	require.Equal(t, "CNY", snap.Flags.BillingCurrency)
	require.False(t, snap.Flags.SubscriptionBilling)
	require.False(t, snap.Flags.LongContextBillingEnabled, "not an OpenAI account with the flag")
	require.Equal(t, "USD", snap.FX.BaseCurrency)
	require.Equal(t, "CNY", snap.FX.QuoteCurrency)
	require.Equal(t, 7.0, snap.FX.Rate, "the fixture's bootstrap rate")
	require.NotNil(t, snap.Media.WebSearchPricePerCall)
	require.Equal(t, 0.02, *snap.Media.WebSearchPricePerCall)
	require.Equal(t, []string{"claude-sonnet-4"}, snap.Candidates)
}

// The nil-user guard is the one fail-closed branch nothing else exercises now
// that the fixture wires apiKey.User; in record mode a site with a nil user
// silently takes no snapshot (freezeOutcome), so the guard must stay visible.
func TestBillingSnapshotFreezeRefusesNilUser(t *testing.T) {
	svc, apiKey, _, account := newSnapshotTestFixture(t)
	// Freeze reads FreezeInput.User; the facades derive it from apiKey.User — the
	// facade-side derivation is pinned in TestFreezeBillingSnapshotModeRules.
	_, err := svc.Freeze(context.Background(), FreezeInput{APIKey: apiKey, Account: account, RequestedModel: "claude-sonnet-4", BillingModel: "claude-sonnet-4", Family: BillingFamilyGeneric})
	require.Error(t, err)
}

func TestBillingSnapshotFreezeRefusesUnpriceableModel(t *testing.T) {
	svc, apiKey, user, account := newSnapshotTestFixture(t)
	_, err := svc.Freeze(context.Background(), FreezeInput{APIKey: apiKey, User: user, Account: account, RequestedModel: "no-such-model", BillingModel: "no-such-model", Family: BillingFamilyGeneric,
		ResolveUserGroupRate: func(_ context.Context, _, _ int64, def float64) float64 { return def }})
	require.ErrorIs(t, err, ErrModelPricingUnavailable, "Freeze must refuse exactly what EnsureModelPricing refuses")
}

func TestBillingSnapshotFreezeUsesPinnedFXWhenPresent(t *testing.T) {
	svc, apiKey, user, account := newSnapshotTestFixture(t)
	pinned := ExchangeRateSnapshot{BaseCurrency: "USD", QuoteCurrency: "CNY", Rate: 6.5, Source: "test-pinned", AsOf: time.Unix(1, 0)}
	// storeBillingSettlementSnapshot returns nothing and is a no-op on a bare
	// context: it writes into the holder WithBillingSettlementContext installs
	// (billing_settlement.go:22, :49-56).
	ctx := WithBillingSettlementContext(context.Background())
	storeBillingSettlementSnapshot(ctx, pinned)
	snap, err := svc.Freeze(ctx, FreezeInput{APIKey: apiKey, User: user, Account: account, RequestedModel: "claude-sonnet-4", BillingModel: "claude-sonnet-4", Family: BillingFamilyGeneric,
		ResolveUserGroupRate: func(_ context.Context, _, _ int64, def float64) float64 { return def }})
	require.NoError(t, err)
	require.Equal(t, 6.5, snap.FX.Rate)
	require.Equal(t, "test-pinned", snap.FX.Source)
}

func TestBillingSnapshotFreezeSubscriptionBillingUsesUSDMultiplierAndNoFX(t *testing.T) {
	svc, apiKey, user, account := newSnapshotTestFixture(t)
	apiKey.Group.SubscriptionType = SubscriptionTypeSubscription // Group.SubscriptionType is a string (group.go:38); IsSubscriptionType() compares it (:134)
	sub := &UserSubscription{ID: 5}
	snap, err := svc.Freeze(context.Background(), FreezeInput{APIKey: apiKey, User: user, Account: account, Subscription: sub, RequestedModel: "claude-sonnet-4", BillingModel: "claude-sonnet-4", Family: BillingFamilyGeneric,
		ResolveUserGroupRate: func(_ context.Context, _, _ int64, def float64) float64 { return def }})
	require.NoError(t, err)
	require.True(t, snap.Flags.SubscriptionBilling)
	require.Equal(t, "USD", snap.Flags.MultiplierCurrency)
}

// An FX outage must not become a request-path failure in record mode: today
// FX is resolved only at settlement (gateway_usage_billing.go:796), after the
// response is served, so an outage costs a usage row, not a request. Freeze
// leaves FX zero and counts it; SettlementContextFromSnapshot pins nothing
// when Rate <= 0, so settlement degrades to today's live FX.
func TestBillingSnapshotFreezeSurvivesFXOutage(t *testing.T) {
	svc, apiKey, user, account := newSnapshotTestFixture(t)
	svc.exchangeRates = NewExchangeRateService(nil) // bootstrap rate 0 → Snapshot() errors
	before := BillingSnapshotMetricsSnapshot().FXUnavailable
	snap, err := svc.Freeze(context.Background(), FreezeInput{APIKey: apiKey, User: user, Account: account, RequestedModel: "claude-sonnet-4", BillingModel: "claude-sonnet-4", Family: BillingFamilyGeneric,
		ResolveUserGroupRate: func(_ context.Context, _, _ int64, def float64) float64 { return def }})
	require.NoError(t, err)
	require.Equal(t, 0.0, snap.FX.Rate)
	require.Equal(t, before+1, BillingSnapshotMetricsSnapshot().FXUnavailable)
}

// The OpenAI settlement path reads the long-context flag from the CREDENTIAL
// account when the selected account is a shadow (openai_gateway_usage.go:205-212
// via resolveCredentialAccount, credential_shadow.go:13) — the freeze must read
// it from that same account or the OpenAI family breaks the differential.
func TestBillingSnapshotFreezeReadsLongContextFlagFromBillingAccount(t *testing.T) {
	svc, apiKey, user, account := newSnapshotTestFixture(t)
	// Account.Extra is map[string]any (account.go:28); IsOpenAILongContextBillingEnabled
	// reads Extra[openAILongContextBillingEnabledKey].(bool) on an OpenAI account (:1245-1250).
	credential := &Account{ID: 100, Platform: PlatformOpenAI, Extra: map[string]any{openAILongContextBillingEnabledKey: true}}
	snap, err := svc.Freeze(context.Background(), FreezeInput{APIKey: apiKey, User: user, Account: account, BillingAccount: credential, RequestedModel: "claude-sonnet-4", BillingModel: "claude-sonnet-4", Family: BillingFamilyOpenAI,
		ResolveUserGroupRate: func(_ context.Context, _, _ int64, def float64) float64 { return def }})
	require.NoError(t, err)
	require.True(t, snap.Flags.LongContextBillingEnabled)
	require.Equal(t, int64(99), snap.AccountID, "AccountID stays the selected account; only the flags come from the billing account")
}

// "Immutable" means the snapshot shares no pointer with the group, the channel
// pricing or the intervals. A shallow copy would let a later group edit move a
// frozen price.
func TestBillingSnapshotIsDeepCopied(t *testing.T) {
	svc, apiKey, user, account := newSnapshotTestFixture(t)
	apiKey.Group.ImagePrice1K = floatPtr(0.5)
	snap, err := svc.Freeze(context.Background(), FreezeInput{APIKey: apiKey, User: user, Account: account, RequestedModel: "claude-sonnet-4", BillingModel: "claude-sonnet-4", Family: BillingFamilyGeneric,
		ResolveUserGroupRate: func(_ context.Context, _, _ int64, def float64) float64 { return def }})
	require.NoError(t, err)
	*apiKey.Group.ImagePrice1K = 99
	*apiKey.Group.WebSearchPricePerCall = 99
	require.Equal(t, 0.5, *snap.Media.ImagePrice.Price1K)
	require.Equal(t, 0.02, *snap.Media.WebSearchPricePerCall)
}

func TestBillingSnapshotJSONRoundTrip(t *testing.T) {
	svc, apiKey, user, account := newSnapshotTestFixture(t)
	snap, err := svc.Freeze(context.Background(), FreezeInput{APIKey: apiKey, User: user, Account: account, RequestedModel: "claude-sonnet-4", BillingModel: "claude-sonnet-4", Family: BillingFamilyGeneric,
		ResolveUserGroupRate: func(_ context.Context, _, _ int64, def float64) float64 { return def }})
	require.NoError(t, err)
	raw, err := snap.MarshalPayload()
	require.NoError(t, err)
	back, err := UnmarshalBillingSnapshotPayload(raw)
	require.NoError(t, err)
	require.Equal(t, snap, back) // four time.Time fields round-trip through RFC3339Nano in UTC; if this proves flaky compare with .Equal() field-wise — do not drop it
}
