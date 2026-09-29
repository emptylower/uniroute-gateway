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
// CheckBillingEligibility entry point — with the bridge unwired (the
// baseline shape) the zero-balance legacy path would deny the funded lease
// instead of admitting it.
//
// ABSENT-LEASE LEGS (revised by spec
// 2026-09-29-wallet-lease-enforce-eligibility-bootstrap §4 test 1, which
// supersedes the 5-G plan's "absent lease refuses" wording — an absent lease
// now bootstraps through ONE bounded synchronous ensure before any refusal):
// funded ensure admits, zero-balance ensure is a terminal 403, transport
// failure fails closed as a retryable 503. The control plane is a real
// httptest server speaking the v2 ensure wire, so the whole production HTTP
// client runs.
package service_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// canonicalWalletWiringCfg is canonicalWalletTestConfig's values (unexported
// in package service; replicated verbatim), on a *config.Config like the
// existing enforce legs, with the control plane pointed at serverURL.
func canonicalWalletWiringCfg(serverURL string) *config.Config {
	cfg := &config.Config{RunMode: config.RunModeStandard}
	cfg.CanonicalWallet = config.CanonicalWalletConfig{
		Mode: config.CanonicalWalletModeEnforce, ControlPlaneURL: serverURL, Issuer: "gateway", Audience: "control",
		Secret: strings.Repeat("w", 32), Version: "v1", LeaseTTLSeconds: 300, LeaseBudgetUnits: 100000,
		RequestTimeoutMS: 300, ExpirySkewMarginMS: 100, SettlementQueueSize: 1, SettlementWorkers: 1, EnforceReady: true,
		BillingSnapshotMode: "record", LiveWindowMinSeconds: 20, LiveControllerTakeoverSeconds: 15,
		ReceivableRedriveIntervalSeconds: 60, ReceivableRedriveMaxAttempts: 2, RetentionDays: 45,
	}
	cfg.Billing.ExchangeRate.BootstrapUSDToCNY = 7.2 // keep the downstream currency step out of the way
	return cfg
}

func TestProvideBillingCacheServiceWiresTheRealBillingCacheUnderEnforce(t *testing.T) {
	ctx := context.Background()
	rdb := service.SharedTestRedisClientForTest(t)
	real := repository.NewBillingCache(rdb) // the production constructor, not a hand-assembled dual struct
	cfg := canonicalWalletWiringCfg("https://control.example.test")

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
	// lease installed through that same cache admits.
	platformUserID := "shipany-user-" + uuid.NewString()
	require.NoError(t, store.InstallCanonicalWalletLease(ctx, service.CanonicalWalletLease{
		LeaseID: "lease-" + uuid.NewString(), PlatformUserID: platformUserID, Currency: "CNY",
		BudgetUnits: 100_000000, ExpiresAt: time.Now().UTC().Add(time.Minute),
	}))
	require.NoError(t, svc.CheckBillingEligibility(ctx, &service.User{ID: 1, PlatformUserID: platformUserID, BillingCurrency: "CNY"}, nil, nil, nil, ""),
		"a lease with real headroom must admit through the real entry point — the derived bridge actually gates")
}

// canonicalWalletEnsureServer answers the v2 ensure route over the
// production HTTP client's exact wire: a body-less GET gets 405 (the
// construction-time rollout probe's "route exists"), POST returns body (the
// 200 lease JSON) with status.
func canonicalWalletEnsureServer(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return server
}

// TestAbsentLeaseBootstrapsThroughTheRealWiring (spec
// 2026-09-29-wallet-lease-enforce-eligibility-bootstrap §4 test 1): the
// 5-G wiring's "an absent lease must refuse — fail closed, never fall
// through to the legacy balance path" leg, revised to the bootstrap
// semantics through the same production constructor + real cache. A NoError
// on the funded leg is itself the never-legacy discriminator: the legacy
// path over this cache (no installed balance) denies.
func TestAbsentLeaseBootstrapsThroughTheRealWiring(t *testing.T) {
	ctx := context.Background()

	t.Run("funded ensure bootstraps and admits", func(t *testing.T) {
		rdb := service.SharedTestRedisClientForTest(t)
		real := repository.NewBillingCache(rdb)
		platformUserID := "shipany-user-" + uuid.NewString()
		leaseJSON := fmt.Sprintf(`{"data":{"lease_id":"lease-%s","platform_user_id":%q,"currency":"CNY","unit_version":"cny-e8-v1","scale":8,`+
			`"budget":{"amount_units":"100000000","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},`+
			`"reserved":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},`+
			`"captured":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},`+
			`"released":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},`+
			`"headroom":{"amount_units":"100000000","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},`+
			`"capture_seq":0,"status":"active","expires_at":%q,"outcome":"issued","clamped_by":"none"}}`,
			uuid.NewString(), platformUserID, time.Now().UTC().Add(time.Minute).Format(time.RFC3339))
		server := canonicalWalletEnsureServer(t, http.StatusOK, leaseJSON)

		svc := service.ProvideBillingCacheService(real, nil, nil, nil, nil, nil, canonicalWalletWiringCfg(server.URL), nil, nil, nil)
		t.Cleanup(svc.Stop)
		require.NoError(t, svc.CheckBillingEligibility(ctx, &service.User{ID: 2, PlatformUserID: platformUserID, BillingCurrency: "CNY"}, nil, nil, nil, ""),
			"an absent lease with a funded control plane must bootstrap and admit — and never fall through to the legacy balance path")
	})

	t.Run("zero-balance ensure is a terminal 403", func(t *testing.T) {
		rdb := service.SharedTestRedisClientForTest(t)
		real := repository.NewBillingCache(rdb)
		server := canonicalWalletEnsureServer(t, http.StatusPaymentRequired, `{"data":{"reason":"insufficient_balance"}}`)

		svc := service.ProvideBillingCacheService(real, nil, nil, nil, nil, nil, canonicalWalletWiringCfg(server.URL), nil, nil, nil)
		t.Cleanup(svc.Stop)
		err := svc.CheckBillingEligibility(ctx, &service.User{ID: 3, PlatformUserID: "shipany-user-" + uuid.NewString(), BillingCurrency: "CNY"}, nil, nil, nil, "")
		require.ErrorIs(t, err, service.ErrInsufficientBalance, "the control plane's no-money answer is the real 403")
	})

	t.Run("transport failure fails closed as retryable 503", func(t *testing.T) {
		rdb := service.SharedTestRedisClientForTest(t)
		real := repository.NewBillingCache(rdb)
		svc := service.ProvideBillingCacheService(real, nil, nil, nil, nil, nil, canonicalWalletWiringCfg("https://control.invalid.test"), nil, nil, nil)
		t.Cleanup(svc.Stop)
		err := svc.CheckBillingEligibility(ctx, &service.User{ID: 4, PlatformUserID: "shipany-user-" + uuid.NewString(), BillingCurrency: "CNY"}, nil, nil, nil, "")
		require.ErrorIs(t, err, service.ErrBillingServiceUnavailable, "an unreachable control plane fails closed — 503, never admitted, never legacy")
	})
}
