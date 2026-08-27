//go:build unit

package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// The Live record's multiplier is GroupRateMultiplier × PeakMultiplierAt(now)
// (internal/handler/openai_live.go:113-117); the snapshot's is
// RateMultiplierForCurrency × computePeakAwareMultipliers. They agree today by
// construction only — this test makes a future drift visible (3.7 reconciles them).
func TestLiveSnapshotMultiplierMatchesLiveRecordDerivation(t *testing.T) {
	svc, apiKey, user, account := newSnapshotTestFixture(t)
	// A non-unit peak factor, or the assertion is 1.5 == 1.5 and Freeze could drop
	// computePeakAwareMultipliers unnoticed: PeakMultiplierAt applies only to
	// subscription groups (group.go:271); a full-day window makes it independent of
	// timezone.Location(). FreezeInput.Subscription stays nil, so isSubscription
	// stays false and the multiplier currency stays CNY on both sides.
	apiKey.Group.SubscriptionType = SubscriptionTypeSubscription
	apiKey.Group.PeakRateEnabled, apiKey.Group.PeakStart, apiKey.Group.PeakEnd, apiKey.Group.PeakRateMultiplier = true, "00:00", "23:59", 3.0
	snap, err := svc.Freeze(context.Background(), FreezeInput{APIKey: apiKey, User: user, Account: account, RequestedModel: "claude-sonnet-4", BillingModel: "claude-sonnet-4", Family: BillingFamilyLive,
		ResolveUserGroupRate: func(_ context.Context, _, _ int64, def float64) float64 { return def }})
	require.NoError(t, err)
	groupRate := apiKey.Group.RateMultiplierForCurrency(NormalizeUserBillingCurrency(user.BillingCurrency))
	want := groupRate * apiKey.Group.PeakMultiplierAt(svc.now())
	require.Equal(t, 3.0, apiKey.Group.PeakMultiplierAt(svc.now()), "the peak factor must be in force, or this test has no teeth")
	require.Equal(t, want, snap.Multipliers.Text)
}
