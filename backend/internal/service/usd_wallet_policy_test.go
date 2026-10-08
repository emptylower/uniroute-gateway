//go:build unit

package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func usdWalletTestConfig() *config.Config {
	cfg := &config.Config{}
	cfg.CanonicalWallet.USDWalletEnabled = true
	cfg.CanonicalWallet.USDPolicyVersion = config.CanonicalUSDWalletPolicyVersion
	cfg.CanonicalWallet.Mode = config.CanonicalWalletModeEnforce
	cfg.CanonicalWallet.BillingSnapshotMode = "settle"
	return cfg
}

func TestUSDWalletAuthorizeChecksPolicyWithCachedLease(t *testing.T) {
	auth, snap, key, control, store := newAuthorizerFixture(t, config.CanonicalWalletModeEnforce)
	auth.cfg.CanonicalWallet.USDWalletEnabled = true
	auth.cfg.CanonicalWallet.USDPolicyVersion = config.CanonicalUSDWalletPolicyVersion
	snap.Flags.USDWalletPolicyVersion = config.CanonicalUSDWalletPolicyVersion
	snap.FX, _, _ = canonicalUSDWalletSnapshot(key.User, auth.cfg)
	control.policyVersion = config.CanonicalUSDWalletPolicyVersion
	store.lease = &control.lease
	h, err := auth.Authorize(context.Background(), AuthorizeInput{Snapshot: snap, User: key.User, FixedEstimateUnits: 100})
	require.NoError(t, err)
	require.Equal(t, "lease-1", h.LeaseID)
	require.Equal(t, 1, control.ensureCalls, "cached lease must still confirm policy")
	require.Equal(t, "lease-1", control.lastEnsure.PreferLeaseID)
	require.Empty(t, control.lastEnsure.Drained, "covering cached lease is not sealed")
	require.Equal(t, config.CanonicalUSDWalletPolicyVersion, control.lastEnsure.USDWalletPolicyVersion)
	control.policyVersion = ""
	_, err = auth.Authorize(context.Background(), AuthorizeInput{Snapshot: snap, User: key.User, FixedEstimateUnits: 100})
	require.ErrorIs(t, err, ErrCanonicalUSDWalletPolicy)
	auth.cfg.CanonicalWallet.USDPolicyVersion = ""
	_, err = auth.Authorize(context.Background(), AuthorizeInput{Snapshot: snap, User: key.User, FixedEstimateUnits: 100})
	require.ErrorIs(t, err, ErrCanonicalUSDWalletPolicy)
}

func TestUSDWalletHTTPEnsureRequiresMatchingPolicyEcho(t *testing.T) {
	for _, echo := range []string{config.CanonicalUSDWalletPolicyVersion, "", "usd-wallet-v2"} {
		t.Run(echo, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request canonicalWalletEnsureRequest
				require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
				require.Equal(t, config.CanonicalUSDWalletPolicyVersion, request.USDWalletPolicyVersion)
				require.Equal(t, "usd-e8-v1", request.MinHeadroom.UnitVersion)
				wire := canonicalWalletEnsureWireResponse{
					USDWalletPolicyVersion: echo, LeaseID: "lease-usd", PlatformUserID: "shipany-user", Currency: "USD", Scale: 8, UnitVersion: "usd-e8-v1",
					Budget: newCanonicalWalletAmountObject(1000), Reserved: newCanonicalWalletAmountObject(0), Captured: newCanonicalWalletAmountObject(0), Released: newCanonicalWalletAmountObject(0), Headroom: newCanonicalWalletAmountObject(1000),
					Status: "active", Outcome: "issued", ClampedBy: "none", ExpiresAt: time.Now().Add(time.Minute),
				}
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"data": wire}))
			}))
			defer server.Close()
			cfg := canonicalWalletTestConfig(config.CanonicalWalletModeEnforce)
			cfg.ControlPlaneURL = server.URL
			client := newCanonicalWalletHTTPClient(cfg, server.Client())
			result, err := client.EnsureLease(context.Background(), canonicalWalletEnsureRequest{
				PlatformUserID: "shipany-user", Currency: "USD", Purpose: "authorize", USDWalletPolicyVersion: config.CanonicalUSDWalletPolicyVersion,
				MinHeadroom: newCanonicalWalletAmountObject(1), RequestedBudget: newCanonicalWalletAmountObject(1000), RequestedTTLSeconds: 60,
			})
			if echo == config.CanonicalUSDWalletPolicyVersion {
				require.NoError(t, err)
				require.Equal(t, echo, result.USDWalletPolicyVersion)
			} else {
				require.ErrorIs(t, err, ErrCanonicalUSDWalletPolicy)
			}
		})
	}
}

func TestUSDWalletAuthorizerRejectsMissingFrozenPolicy(t *testing.T) {
	auth, snap, key, control, _ := newAuthorizerFixture(t, config.CanonicalWalletModeEnforce)
	auth.cfg.CanonicalWallet.USDWalletEnabled = true
	auth.cfg.CanonicalWallet.USDPolicyVersion = config.CanonicalUSDWalletPolicyVersion
	_, err := auth.Authorize(context.Background(), AuthorizeInput{Snapshot: snap, User: key.User, FixedEstimateUnits: 100})
	require.ErrorIs(t, err, ErrCanonicalUSDWalletPolicy)
	require.Zero(t, control.ensureCalls, "unversioned admission must not reach the lease control plane")
}

func TestUSDWalletFrozenUnlinkedUsersKeepLegacySimpleMode(t *testing.T) {
	cfg := usdWalletTestConfig()
	cfg.RunMode = config.RunModeSimple
	snap := &BillingSnapshot{FX: ExchangeRateSnapshot{BaseCurrency: "USD", QuoteCurrency: "USD", Rate: 6.4, Source: "historical"}}
	settlement, err := ResolveCostSettlement(SettlementContextFromSnapshot(context.Background(), snap), &CostBreakdown{TotalCost: 1}, &User{BillingCurrency: "USD"}, false, nil, cfg)
	require.NoError(t, err)
	require.Equal(t, "USD", settlement.SettlementCurrency)
	require.Equal(t, CanonicalWalletUnitVersion, settlement.ExchangeRateSource)
}

func TestUSDWalletColdAdmissionBootstrapsVersionedLease(t *testing.T) {
	cfg := usdWalletTestConfig()
	cfg.RunMode = config.RunModeStandard
	bridge, store, control := newBridgeForEnsureLeaseTest(t)
	bridge.cfg.Mode = config.CanonicalWalletModeEnforce
	bridge.cfg.USDWalletEnabled = true
	bridge.cfg.USDPolicyVersion = config.CanonicalUSDWalletPolicyVersion
	control.policyVersion = config.CanonicalUSDWalletPolicyVersion
	control.lease = CanonicalWalletLease{LeaseID: "lease-usd", Currency: "USD", BudgetUnits: 500_000_000, ExpiresAt: time.Now().Add(time.Minute)}
	cache := &BillingCacheService{cfg: cfg, canonicalWallet: bridge}
	user := &User{ID: 42, PlatformUserID: "shipany-user", BillingCurrency: "USD"}
	require.Nil(t, store.lease)
	ctx := WithBillingSettlementContext(context.Background())
	require.NoError(t, cache.CheckBillingEligibility(ctx, user, nil, nil, nil, ""))
	require.Equal(t, config.CanonicalUSDWalletPolicyVersion, control.lastEnsure.USDWalletPolicyVersion)
	require.Equal(t, 1, control.ensureCalls)
	require.Equal(t, "lease-usd", store.lease.LeaseID)
	snapshot, ok := pinnedBillingSettlementSnapshot(ctx, "USD", "USD")
	require.True(t, ok)
	require.Equal(t, float64(1), snapshot.Rate)
	store.lease = nil
	control.policyVersion = ""
	require.Error(t, cache.CheckBillingEligibility(WithBillingSettlementContext(context.Background()), user, nil, nil, nil, ""), "a cold lease with missing policy echo must fail closed")
}
