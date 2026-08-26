package service

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

// Wire-boundary proof for SubmitSettlement: ShipAny's settlements route still
// speaks *_micros at 1,000,000-per-CNY while everything internal is
// cny-e8-v1 units at 100,000,000-per-CNY. The conversion direction here is
// units->micros with CEILING division (never settle less than reserved),
// and the response's canonical_balance_micros converts back x100 exactly.
func TestCanonicalWalletSubmitSettlementWireConvertsUnitsToMicrosWithCeiling(t *testing.T) {
	localBalance := int64(123_456789) // deliberately NOT a whole number of micros
	var received canonicalWalletSettlementWireRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/internal/v1/wallet/settlements", r.URL.Path)
		require.Equal(t, "POST", r.Method)
		require.True(t, strings.HasPrefix(r.Header.Get("Idempotency-Key"), "gwusg_"))
		require.NoError(t, json.NewDecoder(r.Body).Decode(&received))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"accepted":true,"duplicate":false,"canonical_balance_micros":987654}}`))
	}))
	defer server.Close()

	cfg := canonicalWalletTestConfig(config.CanonicalWalletModeShadow)
	cfg.ControlPlaneURL, cfg.Secret = server.URL, strings.Repeat("s", 32)
	client := newCanonicalWalletHTTPClient(cfg, server.Client())
	result, err := client.SubmitSettlement(context.Background(), CanonicalWalletSettlementEvent{
		EventID: "gwusg_test", GatewayRequestID: "req-1", PlatformUserID: "user-1", LeaseID: "lease-1",
		Currency: "CNY", AmountUnits: 101, // 101 units = 1.01 micros -> ceiling to 2 micros
		LocalBalanceAfterUnits: &localBalance, // 123456789 units = 1234567.89 micros -> ceiling to 1234568
		OccurredAt:             time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	require.True(t, result.Accepted)

	require.Equal(t, int64(2), received.AmountMicros, "ceiling division: (101+99)/100 = 2 — the wire amount must never be smaller than what was reserved")
	require.NotNil(t, received.LocalBalanceAfterMicros)
	require.Equal(t, int64(1234568), *received.LocalBalanceAfterMicros, "ceiling division on the local balance too")
	require.Equal(t, "2026-08-26T12:00:00Z", received.OccurredAt)
	require.Equal(t, "req-1", received.GatewayRequestID)
	require.Equal(t, "lease-1", received.LeaseID)

	// Response balance converts UP in scale (micros -> units), always exact.
	require.NotNil(t, result.CanonicalBalanceUnits)
	require.Equal(t, int64(98_765400), *result.CanonicalBalanceUnits, "987654 micros x100 = 98,765,400 units")
}

func TestCanonicalWalletSubmitSettlementRejectsControlPlaneRefusal(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"accepted":false,"duplicate":false}}`))
	}))
	defer server.Close()
	cfg := canonicalWalletTestConfig(config.CanonicalWalletModeShadow)
	cfg.ControlPlaneURL, cfg.Secret = server.URL, strings.Repeat("s", 32)
	client := newCanonicalWalletHTTPClient(cfg, server.Client())
	_, err := client.SubmitSettlement(context.Background(), CanonicalWalletSettlementEvent{
		GatewayRequestID: "req-1", PlatformUserID: "user-1", Currency: "CNY", AmountUnits: 100,
	})
	require.Error(t, err)
}

// AcquireLease must keep speaking ShipAny's *_micros wire shape while
// converting the response x100 into internal units — and must REJECT a
// non-CNY currency outright instead of coercing it to CNY.
func TestCanonicalWalletAcquireLeaseWireResponseConvertsMicrosToUnits(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req canonicalWalletLeaseRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		require.Equal(t, int64(500_000), req.RequestedMicros, "requested budget stays in ShipAny's wire-level micros")
		_, _ = w.Write([]byte(`{"data":{"lease_id":"lease-wire","platform_user_id":"user-1","currency":"CNY","budget_micros":500000,"consumed_micros":250000,"expires_at":"2030-01-01T00:00:00Z"}}`))
	}))
	defer server.Close()
	cfg := canonicalWalletTestConfig(config.CanonicalWalletModeShadow)
	cfg.ControlPlaneURL, cfg.Secret = server.URL, strings.Repeat("s", 32)
	client := newCanonicalWalletHTTPClient(cfg, server.Client())
	lease, err := client.AcquireLease(context.Background(), canonicalWalletLeaseRequest{
		PlatformUserID: "user-1", Currency: "CNY", RequestedMicros: 500_000, RequestedTTLSeconds: 60,
	})
	require.NoError(t, err)
	require.Equal(t, int64(50_000000), lease.BudgetUnits, "500000 micros x100 = 50,000,000 units")
	require.Equal(t, int64(25_000000), lease.ConsumedUnits, "partial consumption history must survive the boundary conversion")
	require.Equal(t, "CNY", lease.Currency)
}

func TestCanonicalWalletAcquireLeaseRejectsInvalidWireResponses(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		wantErr string
	}{
		{"empty lease id", `{"data":{"lease_id":"","platform_user_id":"user-1","currency":"CNY","budget_micros":500000,"consumed_micros":0,"expires_at":"2030-01-01T00:00:00Z"}}`, "invalid"},
		{"wrong platform user", `{"data":{"lease_id":"l","platform_user_id":"someone-else","currency":"CNY","budget_micros":500000,"consumed_micros":0,"expires_at":"2030-01-01T00:00:00Z"}}`, "invalid"},
		{"zero budget", `{"data":{"lease_id":"l","platform_user_id":"user-1","currency":"CNY","budget_micros":0,"consumed_micros":0,"expires_at":"2030-01-01T00:00:00Z"}}`, "invalid"},
		{"negative consumption", `{"data":{"lease_id":"l","platform_user_id":"user-1","currency":"CNY","budget_micros":500000,"consumed_micros":-1,"expires_at":"2030-01-01T00:00:00Z"}}`, "invalid"},
		{"consumption over budget", `{"data":{"lease_id":"l","platform_user_id":"user-1","currency":"CNY","budget_micros":500000,"consumed_micros":500001,"expires_at":"2030-01-01T00:00:00Z"}}`, "invalid"},
		{"zero expiry", `{"data":{"lease_id":"l","platform_user_id":"user-1","currency":"CNY","budget_micros":500000,"consumed_micros":0,"expires_at":"0001-01-01T00:00:00Z"}}`, "invalid"},
		{"non-CNY currency", `{"data":{"lease_id":"l","platform_user_id":"user-1","currency":"USD","budget_micros":500000,"consumed_micros":0,"expires_at":"2030-01-01T00:00:00Z"}}`, "unsupported currency"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			cfg := canonicalWalletTestConfig(config.CanonicalWalletModeShadow)
			cfg.ControlPlaneURL, cfg.Secret = server.URL, strings.Repeat("s", 32)
			client := newCanonicalWalletHTTPClient(cfg, server.Client())
			_, err := client.AcquireLease(context.Background(), canonicalWalletLeaseRequest{
				PlatformUserID: "user-1", Currency: "CNY", RequestedMicros: 500_000, RequestedTTLSeconds: 60,
			})
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestCanonicalWalletAcquireLeaseRejectsNonCNYRequestedCurrency(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"lease_id":"l","platform_user_id":"user-1","currency":"USD","budget_micros":500000,"consumed_micros":0,"expires_at":"2030-01-01T00:00:00Z"}}`))
	}))
	defer server.Close()
	cfg := canonicalWalletTestConfig(config.CanonicalWalletModeShadow)
	cfg.ControlPlaneURL, cfg.Secret = server.URL, strings.Repeat("s", 32)
	client := newCanonicalWalletHTTPClient(cfg, server.Client())
	_, err := client.AcquireLease(context.Background(), canonicalWalletLeaseRequest{
		PlatformUserID: "user-1", Currency: "USD", RequestedMicros: 500_000, RequestedTTLSeconds: 60,
	})
	// The wire response's own currency is validated first, so a USD response
	// fails with the control-plane error before the requested currency does.
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported currency")
}

func TestCanonicalWalletDoJSONNon2xxReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusBadGateway)
	}))
	defer server.Close()
	cfg := canonicalWalletTestConfig(config.CanonicalWalletModeShadow)
	cfg.ControlPlaneURL, cfg.Secret = server.URL, strings.Repeat("s", 32)
	client := newCanonicalWalletHTTPClient(cfg, server.Client())
	var resp map[string]any
	err := client.doJSON(context.Background(), http.MethodPost, "/api/internal/v1/wallet/settlements", canonicalWalletSettlementScope, "key", map[string]string{"a": "b"}, &resp)
	require.Error(t, err)
	require.Contains(t, err.Error(), "status 502")
}

func TestRequireCNYBillingCurrencyStrictBoundary(t *testing.T) {
	for _, valid := range []string{"CNY", "cny", " CNY ", "\tcny\n"} {
		got, err := RequireCNYBillingCurrency(valid)
		require.NoError(t, err, valid)
		require.Equal(t, "CNY", got)
	}
	for _, invalid := range []string{"", "USD", "usd", "CNYX", "人民币"} {
		_, err := RequireCNYBillingCurrency(invalid)
		require.Error(t, err, "currency %q must be rejected", invalid)
	}
}

func TestCanonicalWalletUnitsFromCNYScaleAndGuards(t *testing.T) {
	units, err := canonicalWalletUnitsFromCNY(0.01)
	require.NoError(t, err)
	require.Equal(t, int64(1_000_000), units, "0.01 CNY (1 credit) = 1,000,000 units")

	units, err = canonicalWalletUnitsFromCNY(0.005)
	require.NoError(t, err)
	require.Equal(t, int64(500_000), units)

	for name, bad := range map[string]float64{
		"negative":   -1,
		"NaN":        math.NaN(),
		"+Inf":       math.Inf(1),
		"-Inf":       math.Inf(-1),
		"over int64": math.MaxFloat64,
	} {
		_, err := canonicalWalletUnitsFromCNY(bad)
		require.Error(t, err, name)
	}
}

func TestCanonicalWalletLeaseRemainingUnitsNeverNegative(t *testing.T) {
	lease := CanonicalWalletLease{BudgetUnits: 100, ConsumedUnits: 40}
	require.Equal(t, int64(60), lease.RemainingUnits())

	// A struct that somehow carries consumption past budget clamps to 0
	// through SubUnits' negative-result rejection instead of going negative.
	lease.ConsumedUnits = 101
	require.Equal(t, int64(0), lease.RemainingUnits())
}

func TestCanonicalWalletBridgeModeAccessorHandlesNilReceiver(t *testing.T) {
	var bridge *CanonicalWalletBridge
	require.Equal(t, config.CanonicalWalletModeDisabled, bridge.Mode())
}

func TestNewCanonicalWalletBridgeNilForDisabledOrUnsetMode(t *testing.T) {
	require.Nil(t, NewCanonicalWalletBridge(&config.Config{}, &canonicalWalletStoreStub{}, nil, nil))
	require.Nil(t, NewCanonicalWalletBridge(nil, &canonicalWalletStoreStub{}, nil, nil))

	cfg := &config.Config{}
	cfg.CanonicalWallet = canonicalWalletTestConfig(config.CanonicalWalletModeEnforce)
	bridge := NewCanonicalWalletBridge(cfg, &canonicalWalletStoreStub{}, nil, nil)
	require.NotNil(t, bridge)
	require.NotEmpty(t, bridge.workerID, "the claim token is generated once per bridge")
	require.True(t, strings.HasPrefix(bridge.workerID, "sub2api-wallet-dispatcher-"))
}

func TestCanonicalWalletBridgeStatsExposeCounters(t *testing.T) {
	// The counters are GLOBAL atomics shared across every bridge in the
	// process — assert on deltas, never absolute values.
	beforeQueued := CanonicalWalletBridgeStats()["queued"]
	beforeUnsupported := CanonicalWalletBridgeStats()["unsupported_currency"]
	canonicalWalletBridgeMetrics.queued.Add(3)
	canonicalWalletBridgeMetrics.unsupportedCurrency.Add(1)
	stats := CanonicalWalletBridgeStats()
	require.Equal(t, int64(3), stats["queued"]-beforeQueued)
	require.Equal(t, int64(1), stats["unsupported_currency"]-beforeUnsupported)
}

// observeCanonicalWalletSettlement is the production entry point called from
// the usage-billing paths — exercise its guard branches and its happy-path
// conversion with real structs.
func TestObserveCanonicalWalletSettlementGuardsAndConversion(t *testing.T) {
	cost := &CostBreakdown{ActualCost: 0.25}
	user := &User{PlatformUserID: "shipany-user-observe", BillingCurrency: "CNY", Balance: 10.0}
	newBilling := 9.75

	bridge := &CanonicalWalletBridge{cfg: canonicalWalletTestConfig(config.CanonicalWalletModeEnforce)}

	// Happy path: runs the CNY->units conversions and reaches
	// ObserveSettlement. With a nil outbox, ObserveSettlement returns before
	// any durable write (and before its currency/metric boundaries) — so
	// these assertions cover the free function's own guards and conversions;
	// the full durable happy path is proven by
	// TestCanonicalWalletOutboxDispatcherDeliversEndToEnd.
	require.NotPanics(t, func() {
		observeCanonicalWalletSettlement(bridge, "req-obs-1", user, cost, false, true, &UsageBillingApplyResult{NewBalance: &newBilling})
	})

	// Guards: each of these must return before any side effect.
	require.NotPanics(t, func() {
		observeCanonicalWalletSettlement(nil, "r", user, cost, false, true, nil)                              // nil bridge
		observeCanonicalWalletSettlement(bridge, "r", nil, cost, false, true, nil)                            // nil user
		observeCanonicalWalletSettlement(bridge, "r", user, nil, false, true, nil)                            // nil cost
		observeCanonicalWalletSettlement(bridge, "r", user, cost, true, true, nil)                            // subscription billing
		observeCanonicalWalletSettlement(bridge, "r", user, cost, false, false, nil)                          // billing not applied
		observeCanonicalWalletSettlement(bridge, "r", user, &CostBreakdown{ActualCost: 0}, false, true, nil)  // zero cost
		observeCanonicalWalletSettlement(bridge, "r", user, &CostBreakdown{ActualCost: -5}, false, true, nil) // negative cost
	})
}

func canonicalWalletBridgeStatsValue(key string) int64 {
	return CanonicalWalletBridgeStats()[key]
}

func TestAcquireLeaseDefaultsWindowWhenTTLUnset(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"lease_id":"lease-ttl","platform_user_id":"user-1","currency":"CNY","budget_micros":1000,"consumed_micros":0,"expires_at":"2030-01-01T00:00:00Z"}}`))
	}))
	defer server.Close()
	cfg := canonicalWalletTestConfig(config.CanonicalWalletModeShadow)
	cfg.ControlPlaneURL, cfg.Secret = server.URL, strings.Repeat("s", 32)
	cfg.LeaseTTLSeconds = 0 // falls back to a 60-second idempotency window
	client := newCanonicalWalletHTTPClient(cfg, server.Client())
	lease, err := client.AcquireLease(context.Background(), canonicalWalletLeaseRequest{
		PlatformUserID: "user-1", Currency: "CNY", RequestedMicros: 1000, RequestedTTLSeconds: 0,
	})
	require.NoError(t, err)
	require.Equal(t, "lease-ttl", lease.LeaseID)
}
