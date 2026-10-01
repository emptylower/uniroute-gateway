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

func TestUSDWalletFixedPolicyIgnoresLiveFXAndRunMode(t *testing.T) {
	cfg := usdWalletTestConfig()
	cfg.RunMode = config.RunModeSimple
	cfg.Billing.ExchangeRate.BootstrapUSDToCNY = 8.9
	user := &User{PlatformUserID: "shipany-user", BillingCurrency: "CNY"}
	snapshot, err := resolveBillingExchangeRate(context.Background(), user, NewExchangeRateService(cfg), cfg)
	require.NoError(t, err)
	require.Equal(t, 7.2, snapshot.Rate)
	require.Equal(t, config.CanonicalUSDWalletPolicyVersion, snapshot.Source)
	ctx := WithBillingSettlementContext(context.Background())
	storeBillingSettlementSnapshot(ctx, snapshot)
	cfg.Billing.ExchangeRate.BootstrapUSDToCNY = 6.1
	cost := &CostBreakdown{TotalCost: 1, ActualCost: 0.5}
	settlement, err := ResolveCostSettlement(ctx, cost, user, false, NewExchangeRateService(cfg), cfg)
	require.NoError(t, err)
	require.Equal(t, "CNY", settlement.SettlementCurrency)
	require.Equal(t, 7.2, settlement.ExchangeRate)
	require.Equal(t, 3.6, cost.ActualCost)
	units, err := canonicalWalletUnitsFromCNY(cost.ActualCost)
	require.NoError(t, err)
	require.Equal(t, int64(360_000_000), units, "$0.50 maps to 360 credits")
}

func TestUSDWalletMissingPolicyNeverFallsBackToLiveFX(t *testing.T) {
	user := &User{PlatformUserID: "shipany-user", BillingCurrency: "CNY"}
	for _, version := range []string{"", "usd-wallet-v2"} {
		t.Run(version, func(t *testing.T) {
			cfg := usdWalletTestConfig()
			cfg.CanonicalWallet.USDPolicyVersion = version
			cfg.Billing.ExchangeRate.BootstrapUSDToCNY = 7
			_, err := resolveBillingExchangeRate(context.Background(), user, NewExchangeRateService(cfg), cfg)
			require.ErrorIs(t, err, ErrCanonicalUSDWalletPolicy)
			_, err = ResolveCostSettlement(context.Background(), &CostBreakdown{TotalCost: 1}, user, false, NewExchangeRateService(cfg), cfg)
			require.ErrorIs(t, err, ErrCanonicalUSDWalletPolicy)
		})
	}
}

func TestUSDWalletPolicyKeepsUnlinkedAndNativeUSDUsersUnchanged(t *testing.T) {
	cfg := usdWalletTestConfig()
	cfg.Billing.ExchangeRate.BootstrapUSDToCNY = 8.9
	for _, user := range []*User{{BillingCurrency: "CNY"}, {PlatformUserID: "native-usd-user", BillingCurrency: "USD"}} {
		snapshot, err := resolveBillingExchangeRate(context.Background(), user, NewExchangeRateService(cfg), cfg)
		require.NoError(t, err)
		require.NotEqual(t, config.CanonicalUSDWalletPolicyVersion, snapshot.Source)
		if user.BillingCurrency == "CNY" {
			require.Equal(t, 8.9, snapshot.Rate)
		} else {
			require.Equal(t, 1.0, snapshot.Rate)
			require.Equal(t, "USD", snapshot.QuoteCurrency)
		}
	}
}

func TestUSDWalletPreflightPinsFixedPolicyWithoutLiveRateService(t *testing.T) {
	cfg := usdWalletTestConfig()
	cfg.RunMode = config.RunModeSimple
	svc := &BillingCacheService{cfg: cfg}
	user := &User{PlatformUserID: "shipany-user", BillingCurrency: "CNY"}
	ctx := WithBillingSettlementContext(context.Background())
	require.NoError(t, svc.CheckBillingEligibility(ctx, user, nil, nil, nil, ""))
	snapshot, ok := pinnedBillingSettlementSnapshot(ctx, "USD", "CNY")
	require.True(t, ok)
	require.Equal(t, 7.2, snapshot.Rate)
	cfg.CanonicalWallet.USDPolicyVersion = ""
	require.Error(t, svc.CheckBillingEligibility(WithBillingSettlementContext(context.Background()), user, nil, nil, nil, ""))
}

func TestUSDWalletFreezePinsSamePolicyAsPreflightAndRejectsMismatch(t *testing.T) {
	svc, key, user, account := newSnapshotTestFixture(t)
	svc.cfg.CanonicalWallet = usdWalletTestConfig().CanonicalWallet
	user.PlatformUserID = "shipany-user"
	svc.exchangeRates = nil
	ctx := WithBillingSettlementContext(context.Background())
	snapshot, err := resolveBillingExchangeRate(ctx, user, nil, svc.cfg)
	require.NoError(t, err)
	storeBillingSettlementSnapshot(ctx, snapshot)
	snap, err := svc.Freeze(ctx, FreezeInput{APIKey: key, User: user, Account: account, BillingModel: "claude-sonnet-4", Family: BillingFamilyGeneric})
	require.NoError(t, err)
	require.Equal(t, snapshot, snap.FX)
	require.Equal(t, config.CanonicalUSDWalletPolicyVersion, snap.Flags.USDWalletPolicyVersion)
	storeBillingSettlementSnapshot(ctx, ExchangeRateSnapshot{BaseCurrency: "USD", QuoteCurrency: "CNY", Rate: 6.7, Source: "live"})
	_, err = svc.Freeze(ctx, FreezeInput{APIKey: key, User: user, Account: account, BillingModel: "claude-sonnet-4"})
	require.ErrorIs(t, err, ErrCanonicalUSDWalletPolicy)
	svc.cfg.CanonicalWallet.USDPolicyVersion = ""
	_, err = svc.Freeze(context.Background(), FreezeInput{APIKey: key, User: user, Account: account, BillingModel: "claude-sonnet-4"})
	require.ErrorIs(t, err, ErrCanonicalUSDWalletPolicy)
}

func TestUSDWalletFrozenLegacyPolicySurvivesActivation(t *testing.T) {
	user := &User{PlatformUserID: "shipany-user", BillingCurrency: "CNY"}
	legacy := &BillingSnapshot{FX: ExchangeRateSnapshot{BaseCurrency: "USD", QuoteCurrency: "CNY", Rate: 6.4, Source: "historical", AsOf: time.Now()}}
	cost := &CostBreakdown{TotalCost: 1, ActualCost: 0.5}
	settlement, err := ResolveCostSettlement(SettlementContextFromSnapshot(context.Background(), legacy), cost, user, false, nil, usdWalletTestConfig())
	require.NoError(t, err)
	require.Equal(t, 6.4, settlement.ExchangeRate)
	require.Equal(t, "historical", settlement.ExchangeRateSource)
	require.Equal(t, 3.2, cost.ActualCost)
}

func TestUSDWalletFrozenNewPolicySurvivesConfigChangeAndRejectsCorruption(t *testing.T) {
	user := &User{PlatformUserID: "shipany-user", BillingCurrency: "CNY"}
	fx, _, err := canonicalUSDWalletSnapshot(user, usdWalletTestConfig())
	require.NoError(t, err)
	snap := &BillingSnapshot{FX: fx, Flags: BillingSnapshotFlags{USDWalletPolicyVersion: config.CanonicalUSDWalletPolicyVersion}}
	settlement, err := ResolveCostSettlement(SettlementContextFromSnapshot(context.Background(), snap), &CostBreakdown{TotalCost: 1}, user, false, nil, nil)
	require.NoError(t, err)
	require.Equal(t, 7.2, settlement.ExchangeRate)
	user.BillingCurrency = "USD"
	settlement, err = ResolveCostSettlement(SettlementContextFromSnapshot(context.Background(), snap), &CostBreakdown{TotalCost: 1}, user, false, nil, nil)
	require.NoError(t, err)
	require.Equal(t, "CNY", settlement.SettlementCurrency, "frozen currency must survive user edits")
	user.BillingCurrency = "CNY"
	for _, corrupt := range []ExchangeRateSnapshot{{}, {BaseCurrency: "USD", QuoteCurrency: "CNY", Rate: 6.4, Source: "live"}} {
		snap.FX = corrupt
		_, err = ResolveCostSettlement(SettlementContextFromSnapshot(context.Background(), snap), &CostBreakdown{TotalCost: 1}, user, false, newSettlementTestFX(), usdWalletTestConfig())
		require.ErrorIs(t, err, ErrCanonicalUSDWalletPolicy)
	}
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
				require.Equal(t, "cny-e8-v1", request.MinHeadroom.UnitVersion)
				wire := canonicalWalletEnsureWireResponse{
					USDWalletPolicyVersion: echo, LeaseID: "lease-usd", PlatformUserID: "shipany-user", Currency: "CNY", Scale: 8, UnitVersion: "cny-e8-v1",
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
				PlatformUserID: "shipany-user", Currency: "CNY", Purpose: "authorize", USDWalletPolicyVersion: config.CanonicalUSDWalletPolicyVersion,
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
	snap := &BillingSnapshot{FX: ExchangeRateSnapshot{BaseCurrency: "USD", QuoteCurrency: "CNY", Rate: 6.4, Source: "historical"}}
	settlement, err := ResolveCostSettlement(SettlementContextFromSnapshot(context.Background(), snap), &CostBreakdown{TotalCost: 1}, &User{BillingCurrency: "CNY"}, false, nil, cfg)
	require.NoError(t, err)
	require.Equal(t, "USD", settlement.SettlementCurrency)
	require.Equal(t, exchangeRateSourceSimpleMode, settlement.ExchangeRateSource)
}

func TestUSDWalletColdAdmissionBootstrapsVersionedLease(t *testing.T) {
	cfg := usdWalletTestConfig()
	cfg.RunMode = config.RunModeStandard
	bridge, store, control := newBridgeForEnsureLeaseTest(t)
	bridge.cfg.Mode = config.CanonicalWalletModeEnforce
	bridge.cfg.USDWalletEnabled = true
	bridge.cfg.USDPolicyVersion = config.CanonicalUSDWalletPolicyVersion
	control.policyVersion = config.CanonicalUSDWalletPolicyVersion
	control.lease = CanonicalWalletLease{LeaseID: "lease-usd", Currency: "CNY", BudgetUnits: 500_000_000, ExpiresAt: time.Now().Add(time.Minute)}
	cache := &BillingCacheService{cfg: cfg, canonicalWallet: bridge}
	user := &User{ID: 42, PlatformUserID: "shipany-user", BillingCurrency: "CNY"}
	require.Nil(t, store.lease)
	ctx := WithBillingSettlementContext(context.Background())
	require.NoError(t, cache.CheckBillingEligibility(ctx, user, nil, nil, nil, ""))
	require.Equal(t, config.CanonicalUSDWalletPolicyVersion, control.lastEnsure.USDWalletPolicyVersion)
	require.Equal(t, 1, control.ensureCalls)
	require.Equal(t, "lease-usd", store.lease.LeaseID)
	snapshot, ok := pinnedBillingSettlementSnapshot(ctx, "USD", "CNY")
	require.True(t, ok)
	require.Equal(t, 7.2, snapshot.Rate)
	store.lease = nil
	control.policyVersion = ""
	require.Error(t, cache.CheckBillingEligibility(WithBillingSettlementContext(context.Background()), user, nil, nil, nil, ""), "a cold lease with missing policy echo must fail closed")
}
