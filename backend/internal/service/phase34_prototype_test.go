//go:build integration

package service

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Phase 3.4 prototype, Sub2API half — redesign §7 tests 14–20.
// Every lease expiry in these tests is decided by the bridge's injectable
// clock (bridge.now), never by waiting; Redis keys are given a real
// expires_at ten seconds ahead so Redis itself never evicts them mid-test.
// Server-side state on the fake is set under fake.mu (see the fake file).

const p34Margin = 100 * time.Millisecond

// p34Bridge builds a bridge over real Redis and the given control plane with
// a fixed, injectable clock. cfgMode is shadow or enforce.
func p34Bridge(t *testing.T, ctx context.Context, cfgMode string, store CanonicalWalletLeaseStore, control canonicalWalletControlPlane, now time.Time) *CanonicalWalletBridge {
	t.Helper()
	cfg := canonicalWalletTestConfig(cfgMode)
	cfg.ExpirySkewMarginMS = int(p34Margin / time.Millisecond)
	cfg.LeaseBudgetUnits = 500_000_000 // 5 CNY, the production default
	b := newCanonicalWalletBridge(cfg, store, control, nil, nil, 0)
	b.now = func() time.Time { return now }
	return b
}

// Test 15a (seal): after a seal, a NEW reservation on the lease is refused by
// the reserve script's budget guard; a retry carrying an existing marker still
// succeeds (duplicate check runs before the budget check — verified against
// reserveCanonicalWalletLeaseScript's order: EXISTS → currency → expiry →
// marker → budget); the current pointer is deleted; the pre-seal consumed is
// returned; a second seal is idempotent.
func TestPhase34Proto15aSealRefusesNewReservationsButHonoursMarkers(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	user := "shipany-user-" + uuid.NewString()
	expires := time.Now().UTC().Add(10 * time.Second)
	require.NoError(t, store.InstallCanonicalWalletLease(ctx, CanonicalWalletLease{
		LeaseID: "lease-seal", PlatformUserID: user, Currency: "CNY", BudgetUnits: 500_000_000, ExpiresAt: expires,
	}))
	_, err := store.ReserveCanonicalWalletLease(ctx, user, "lease-seal", "CNY", "evt-before", 30_000_000, time.Now().UTC())
	require.NoError(t, err)

	pre, err := store.SealCanonicalWalletLease(ctx, user, "lease-seal")
	require.NoError(t, err)
	require.Equal(t, int64(30_000_000), pre, "the seal returns the PRE-seal consumed")

	sealed, err := store.GetCanonicalWalletLeaseByID(ctx, user, "lease-seal")
	require.NoError(t, err)
	require.Equal(t, sealed.BudgetUnits, sealed.ConsumedUnits, "consumed == budget after the seal")
	_, err = store.GetCanonicalWalletLease(ctx, user)
	require.ErrorIs(t, err, ErrCanonicalWalletLeaseMissing, "the current pointer is deleted by the seal")

	_, err = store.ReserveCanonicalWalletLease(ctx, user, "lease-seal", "CNY", "evt-after", 1, time.Now().UTC())
	require.ErrorIs(t, err, ErrCanonicalWalletLeaseExhausted, "a new reservation on a sealed lease is refused")

	dup, err := store.ReserveCanonicalWalletLease(ctx, user, "lease-seal", "CNY", "evt-before", 30_000_000, time.Now().UTC())
	require.NoError(t, err)
	require.True(t, dup.Duplicate, "a retry carrying its marker still succeeds on the sealed lease")

	again, err := store.SealCanonicalWalletLease(ctx, user, "lease-seal")
	require.NoError(t, err)
	require.Equal(t, sealed.BudgetUnits, again, "a second seal is idempotent and returns the already-sealed consumed (== budget)")

	_, err = store.SealCanonicalWalletLease(ctx, user, "lease-absent")
	require.ErrorIs(t, err, ErrCanonicalWalletLeaseMissing)
}
