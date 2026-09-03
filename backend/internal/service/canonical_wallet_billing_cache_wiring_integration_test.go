//go:build integration

// TestProvideBillingCacheServiceWiresTheRealBillingCacheUnderEnforce (Wallet
// Lease Phase 5-G, Task 1 Step 3): the third canonical-wallet construction
// site exercised against the PRODUCTION constructor — repository.NewBillingCache
// over the shared test Redis — under enforce. At baseline this leg panics
// inside ProvideBillingCacheService (requireCanonicalWalletStore: *billingCache
// does not implement CanonicalWalletLeaseStore); the fix embeds the shared
// canonicalWalletRedisStore so it does.
//
// DEVIATION NOTE (plan Task 1 Step 3): the plan places this test inside
// internal/service/canonical_wallet_admission_integration_test.go (package
// service, in-package). An in-package service test cannot import
// internal/repository — repository imports service, and the Go tool rejects
// the cycle ("import cycle not allowed in test", verified empirically) — so
// this leg lives in package service_test. The plan's `svc.canonicalWallet !=
// nil` assertion is unreachable from an external package (the field is
// unexported and BillingCacheService exposes no accessor); the wiring is
// asserted behaviorally instead, through the plan's own install-then-check
// shape: a lease installed through the SAME real cache admits via the real
// CheckBillingEligibility entry point, and an absent lease refuses — with the
// bridge unwired (the baseline shape) the zero-balance legacy path would deny
// the funded lease instead of admitting it.
package service_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestProvideBillingCacheServiceWiresTheRealBillingCacheUnderEnforce(t *testing.T) {
	ctx := context.Background()
	rdb := service.SharedTestRedisClientForTest(t)
	real := repository.NewBillingCache(rdb) // the production constructor, not a hand-assembled dual struct

	// canonicalWalletTestConfig(config.CanonicalWalletModeEnforce)'s values
	// (unexported in package service; replicated verbatim), on a *config.Config
	// like the existing enforce legs.
	cfg := &config.Config{RunMode: config.RunModeStandard}
	cfg.CanonicalWallet = config.CanonicalWalletConfig{
		Mode: config.CanonicalWalletModeEnforce, ControlPlaneURL: "https://control.example.test", Issuer: "gateway", Audience: "control",
		Secret: strings.Repeat("w", 32), Version: "v1", LeaseTTLSeconds: 300, LeaseBudgetUnits: 100000,
		RequestTimeoutMS: 300, ExpirySkewMarginMS: 100, SettlementQueueSize: 1, SettlementWorkers: 1, EnforceReady: true,
		BillingSnapshotMode: "record", LiveWindowMinSeconds: 20, LiveControllerTakeoverSeconds: 15,
		ReceivableRedriveIntervalSeconds: 60, ReceivableRedriveMaxAttempts: 2, RetentionDays: 45,
	}
	cfg.Billing.ExchangeRate.BootstrapUSDToCNY = 7.2 // keep the downstream currency step out of the way

	var svc *service.BillingCacheService
	require.NotPanics(t, func() {
		svc = service.ProvideBillingCacheService(real, nil, nil, nil, nil, nil, cfg, nil, nil, nil)
	}, "enforce must boot when the billing cache provides the wallet store — this is the startup panic the runbook walk found")
	require.NotNil(t, svc)
	t.Cleanup(svc.Stop)

	// The bridge was built: the real cache must satisfy the store interface
	// the provider type-asserts (the same check requireCanonicalWalletStore
	// makes, pinned from the outside).
	store, ok := any(real).(service.CanonicalWalletLeaseStore)
	require.True(t, ok, "the real billing cache must satisfy service.CanonicalWalletLeaseStore")

	// Install-then-check, the existing enforce leg's shape verbatim: a funded
	// lease installed through that same cache admits; an absent lease refuses.
	platformUserID := "shipany-user-" + uuid.NewString()
	require.NoError(t, store.InstallCanonicalWalletLease(ctx, service.CanonicalWalletLease{
		LeaseID: "lease-" + uuid.NewString(), PlatformUserID: platformUserID, Currency: "CNY",
		BudgetUnits: 100_000000, ExpiresAt: time.Now().UTC().Add(time.Minute),
	}))
	require.NoError(t, svc.CheckBillingEligibility(ctx, &service.User{ID: 1, PlatformUserID: platformUserID, BillingCurrency: "CNY"}, nil, nil, nil, ""),
		"a lease with real headroom must admit through the real entry point — the derived bridge actually gates")

	err := svc.CheckBillingEligibility(ctx, &service.User{ID: 2, PlatformUserID: "shipany-user-" + uuid.NewString(), BillingCurrency: "CNY"}, nil, nil, nil, "")
	require.Error(t, err, "an absent lease must refuse — fail closed, never fall through to the legacy balance path in enforce")
}
