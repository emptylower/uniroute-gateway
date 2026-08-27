//go:build unit

package service

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

// Coverage-gap tests for the Phase 3.2 gate (per-function ≥ 85% on every
// billing_snapshot row except the Persist/Load wrappers, which the
// integration suite exercises).

type fakeBillingSnapshotStore struct {
	insertErr error
	inserted  int
}

func (f *fakeBillingSnapshotStore) InsertBillingSnapshot(_ context.Context, _ *BillingSnapshot) error {
	f.inserted++
	return f.insertErr
}

func (f *fakeBillingSnapshotStore) GetBillingSnapshot(_ context.Context, id string) (*BillingSnapshot, error) {
	if id == "bsnap_hit" {
		return &BillingSnapshot{ID: id, Version: BillingSnapshotVersion}, nil
	}
	return nil, ErrBillingSnapshotNotFound
}

func TestBillingSnapshotPayloadAndHelperEdgeCases(t *testing.T) {
	var nilSnap *BillingSnapshot
	_, err := nilSnap.MarshalPayload()
	require.Error(t, err)

	_, err = UnmarshalBillingSnapshotPayload([]byte("{not json"))
	require.Error(t, err)
	_, err = UnmarshalBillingSnapshotPayload([]byte(`{"id":"x","version":99}`))
	require.Error(t, err)

	require.Equal(t, BillingSnapshotMedia{}, mediaPricingFromGroup(nil))

	require.Nil(t, clonePricing(nil))

	// Mode(): nil receiver, empty/unknown config value.
	var nilSvc *BillingSnapshotService
	require.Equal(t, BillingSnapshotModeOff, nilSvc.Mode())
	bogus := &config.Config{}
	bogus.CanonicalWallet.BillingSnapshotMode = "bogus"
	bogusSvc := NewBillingSnapshotService(bogus, nil, nil, nil, nil)
	require.Equal(t, BillingSnapshotModeOff, bogusSvc.Mode())
	// exercise the constructor's default clock (tests elsewhere override it)
	require.False(t, bogusSvc.now().IsZero())

	// snapshotCoversModel: empty settled model and the candidates chain.
	svc, apiKeyCov, _, _ := newSnapshotTestFixture(t)
	frozen, err := svc.Freeze(context.Background(), FreezeInput{APIKey: apiKeyCov, User: apiKeyCov.User, Account: &Account{ID: 99}, RequestedModel: "claude-sonnet-4", BillingModel: "claude-sonnet-4", Family: BillingFamilyGeneric,
		ResolveUserGroupRate: func(_ context.Context, _, _ int64, def float64) float64 { return def }})
	require.NoError(t, err)
	require.False(t, snapshotCoversModel(frozen, ""))
	require.False(t, snapshotCoversModel(frozen, "  "))
	frozen.Candidates = []string{"alias-a", "alias-b"}
	require.True(t, snapshotCoversModel(frozen, "alias-b"))
	require.False(t, snapshotCoversModel(frozen, "alias-c"))

	// snapshotHasConfiguredImagePrice / VideoPrice default tiers.
	require.False(t, snapshotHasConfiguredImagePrice(nil, "1K"))
	require.False(t, snapshotHasConfiguredImagePrice(&ImagePriceConfig{Price2K: floatPtr(1)}, "4K"))
	require.True(t, snapshotHasConfiguredImagePrice(&ImagePriceConfig{Price2K: floatPtr(1)}, "2K"))
	require.True(t, snapshotHasConfiguredImagePrice(&ImagePriceConfig{}, "3K") == false)
	require.False(t, snapshotHasConfiguredVideoPrice(nil, "720p"))
	require.True(t, snapshotHasConfiguredVideoPrice(&VideoPriceConfig{Price480P: floatPtr(1)}, "480p"))
	require.True(t, snapshotHasConfiguredVideoPrice(&VideoPriceConfig{Price1080P: floatPtr(1)}, "1080p"))

	// worstPerRequestResolved: max over tiers plus the default.
	p10, p50 := floatPtr(10), floatPtr(50)
	resolved := &ResolvedPricing{
		Source:                 PricingSourceChannel,
		DefaultPerRequestPrice: 5,
		RequestTiers: []PricingInterval{
			{TierLabel: "1K", PerRequestPrice: p10},
			{TierLabel: "4K", PerRequestPrice: p50},
		},
	}
	worst := worstPerRequestResolved(resolved)
	require.Equal(t, 50.0, worst.DefaultPerRequestPrice)
	require.Nil(t, worst.RequestTiers)
	require.Equal(t, 5.0, worstPerRequestResolved(&ResolvedPricing{DefaultPerRequestPrice: 5}).DefaultPerRequestPrice)

	// unitsRoundUp: zero, subscription, fail-closed, overflow.
	fxSnap, err := svc.Freeze(context.Background(), FreezeInput{APIKey: apiKeyCov, User: apiKeyCov.User, Account: &Account{ID: 99}, RequestedModel: "claude-sonnet-4", BillingModel: "claude-sonnet-4", Family: BillingFamilyGeneric,
		ResolveUserGroupRate: func(_ context.Context, _, _ int64, def float64) float64 { return def }})
	require.NoError(t, err)
	snap := fxSnap
	snap.FX.Rate = 7
	zero, err := unitsRoundUp(0, snap)
	require.NoError(t, err)
	require.Equal(t, int64(0), zero)
	sub := *snap
	sub.Flags.SubscriptionBilling = true
	subUnits, err := unitsRoundUp(0.25, &sub)
	require.NoError(t, err)
	require.Equal(t, int64(math.Ceil(0.25*canonicalWalletUnitsPerCNY)), subUnits)
	noFX := *snap
	noFX.FX.Rate = 0
	_, err = unitsRoundUp(1, &noFX)
	require.ErrorIs(t, err, ErrEstimateUnbounded)
	huge := *snap
	huge.FX.Rate = math.MaxFloat64 / 2
	_, err = unitsRoundUp(1e300, &huge)
	require.Error(t, err)

	// newBillingSnapshotID: the rand-failure branch via the injection seam.
	orig := billingSnapshotRandRead
	billingSnapshotRandRead = func(_ []byte) (int, error) { return 0, errors.New("no entropy") }
	_, err = newBillingSnapshotID()
	billingSnapshotRandRead = orig
	require.Error(t, err)
}

func TestImageAndVideoCostFromSnapshotChannelBranches(t *testing.T) {
	billing := newTestBillingService()
	svcFixture, _, _, _ := newSnapshotTestFixture(t)
	snap, err := svcFixture.Freeze(context.Background(), FreezeInput{
		APIKey: &APIKey{ID: 11, User: &User{ID: 42, BillingCurrency: "CNY"}, Group: &Group{ID: 7, RateMultiplier: 1.5}},
		User:   &User{ID: 42, BillingCurrency: "CNY"}, Account: &Account{ID: 99},
		RequestedModel: "claude-sonnet-4", BillingModel: "claude-sonnet-4", Family: BillingFamilyGeneric,
		ResolveUserGroupRate: func(_ context.Context, _, _ int64, def float64) float64 { return def },
	})
	require.NoError(t, err)
	in := SnapshotSettlementInput{ImageCount: 2, ImageSize: "1K", Tokens: UsageTokens{InputTokens: 100, OutputTokens: 10}}

	// Generic + channel + token-mode resolve: CalculateCostUnified cannot price
	// (no base, no intervals) and the generic helper PROPAGATES the error.
	broken := &ResolvedPricing{Source: PricingSourceChannel, Mode: BillingModeToken}
	_, err = billing.imageCostFromSnapshot(snap, broken, in)
	require.ErrorIs(t, err, ErrModelPricingUnavailable)

	// OpenAI family with the same broken resolve: the helper logs and falls
	// through to the default per-image price instead of propagating.
	snapOpenAI := *snap
	snapOpenAI.Family = BillingFamilyOpenAI
	fallback, err := billing.imageCostFromSnapshot(&snapOpenAI, broken, in)
	require.NoError(t, err)
	require.NotNil(t, fallback)

	// Video: a configured group price wins over everything else.
	svcVideo, snapVideo, _, _, _ := freezeForSettleTest(t, BillingFamilyOpenAI, "claude-sonnet-4")
	snapVideo.Media.VideoPrice = &VideoPriceConfig{Price720P: floatPtr(0.02)}
	configured := svcVideo.billing.videoCostFromSnapshot(snapVideo, snapVideo.resolvedPricing(), SnapshotSettlementInput{VideoCount: 2, VideoResolution: "720p", VideoDurationSeconds: 10, GrokVideo: true})
	want := svcVideo.billing.CalculateVideoCost(snapVideo.BillingModel, "720p", 2, 10, snapVideo.Media.VideoPrice, snapVideo.Multipliers.Video)
	require.Equal(t, want.ActualCost, configured.ActualCost)

	// Video: the zero-count clamp mirrors the live ≥1 rule on the direct path.
	clamped := svcVideo.billing.videoCostFromSnapshot(snapVideo, snapVideo.resolvedPricing(), SnapshotSettlementInput{VideoCount: 0, VideoResolution: "720p", VideoDurationSeconds: 10, GrokVideo: true})
	require.NotNil(t, clamped)

	// Video: channel per-request pricing goes through CalculateCostUnified and
	// is stamped BillingModeVideo. A fresh snapshot (no configured group video
	// price), or the configured-price branch above would take it first.
	_, snapChannel, _, _, _ := freezeForSettleTest(t, BillingFamilyOpenAI, "claude-sonnet-4")
	channel := &ResolvedPricing{Source: PricingSourceChannel, Mode: BillingModePerRequest, DefaultPerRequestPrice: 0.5}
	stamped := svcVideo.billing.videoCostFromSnapshot(snapChannel, channel, SnapshotSettlementInput{VideoCount: 1, VideoResolution: "720p", VideoDurationSeconds: 10, GrokVideo: true})
	require.Equal(t, string(BillingModeVideo), stamped.BillingMode)
}

func TestPersistBillingSnapshotBestEffortCountsAndPersists(t *testing.T) {
	svc, _, _, _ := newSnapshotTestFixture(t)
	store := &fakeBillingSnapshotStore{}
	svc.store = store
	snap := &BillingSnapshot{ID: "bsnap_x", Version: BillingSnapshotVersion, BillingModel: "m", Pricing: BillingSnapshotPricing{Mode: BillingModeToken}}

	// nils short-circuit.
	persistBillingSnapshotBestEffort(context.Background(), nil, snap)
	persistBillingSnapshotBestEffort(context.Background(), svc, nil)
	require.Equal(t, 0, store.inserted)

	// happy path: one insert on a detached context.
	persistBillingSnapshotBestEffort(context.Background(), svc, snap)
	require.Equal(t, 1, store.inserted)
	require.Equal(t, int64(0), BillingSnapshotMetricsSnapshot().PersistError)

	// error path: counted, not propagated.
	store.insertErr = errors.New("db down")
	persistBillingSnapshotBestEffort(context.Background(), svc, snap)
	require.Equal(t, 2, store.inserted)
	require.Equal(t, int64(1), BillingSnapshotMetricsSnapshot().PersistError)
}

func TestBillingSnapshotLoadDelegatesToStore(t *testing.T) {
	svc, _, _, _ := newSnapshotTestFixture(t)
	svc.store = &fakeBillingSnapshotStore{}
	got, err := svc.Load(context.Background(), "  bsnap_hit  ")
	require.NoError(t, err)
	require.Equal(t, "bsnap_hit", got.ID)
	_, err = svc.Load(context.Background(), "bsnap_missing")
	require.ErrorIs(t, err, ErrBillingSnapshotNotFound)

	// nil receiver / nil store guards.
	var nilSvc *BillingSnapshotService
	_, err = nilSvc.Load(context.Background(), "bsnap_hit")
	require.ErrorIs(t, err, ErrBillingSnapshotNotFound)
	emptySvc := &BillingSnapshotService{}
	require.NoError(t, emptySvc.Persist(context.Background(), &BillingSnapshot{ID: "bsnap_x"}))
	var nilSnap *BillingSnapshot
	require.NoError(t, emptySvc.Persist(context.Background(), nilSnap))
	require.NoError(t, nilSvc.Persist(context.Background(), nilSnap))
}

func TestBillingSnapshotIDPrefixShape(t *testing.T) {
	id, err := newBillingSnapshotID()
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(id, "bsnap_"))
}
