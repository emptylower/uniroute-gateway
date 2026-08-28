//go:build integration

package service

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
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

// Test 18 — skew margin: a lease inside the margin is treated as expired.
func TestPhase34Proto18SkewMarginTreatsNearExpiryAsExpired(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	user := "shipany-user-" + uuid.NewString()
	wall := time.Now().UTC()
	expires := wall.Add(10 * time.Second) // Redis keeps the key alive for the whole test
	require.NoError(t, store.InstallCanonicalWalletLease(ctx, CanonicalWalletLease{
		LeaseID: "lease-margin", PlatformUserID: user, Currency: "CNY", BudgetUnits: 500_000_000, ExpiresAt: expires,
	}))
	control := &canonicalWalletControlStub{} // records ensure calls; returns a fresh lease when asked

	// 50 ms before expiry, inside the 100 ms margin → expired → ensure is called.
	control.lease = CanonicalWalletLease{LeaseID: "lease-fresh", PlatformUserID: user, Currency: "CNY", BudgetUnits: 500_000_000, ExpiresAt: expires.Add(time.Minute)}
	b := p34Bridge(t, ctx, config.CanonicalWalletModeEnforce, store, control, expires.Add(-50*time.Millisecond))
	lease, err := b.ensureLease(ctx, user, "CNY", 1_000_000, canonicalWalletLeasePurposeAuthorize, "")
	require.NoError(t, err)
	require.Equal(t, "lease-fresh", lease.LeaseID)
	require.Equal(t, 1, control.ensureCalls)

	// 150 ms before expiry, outside the margin → cache hit, no ensure.
	control2 := &canonicalWalletControlStub{}
	require.NoError(t, store.InstallCanonicalWalletLease(ctx, CanonicalWalletLease{
		LeaseID: "lease-margin-2", PlatformUserID: user + "-b", Currency: "CNY", BudgetUnits: 500_000_000, ExpiresAt: expires,
	}))
	b2 := p34Bridge(t, ctx, config.CanonicalWalletModeEnforce, store, control2, expires.Add(-150*time.Millisecond))
	lease, err = b2.ensureLease(ctx, user+"-b", "CNY", 1_000_000, canonicalWalletLeasePurposeAuthorize, "")
	require.NoError(t, err)
	require.Equal(t, "lease-margin-2", lease.LeaseID)
	require.Equal(t, 0, control2.ensureCalls)
}
