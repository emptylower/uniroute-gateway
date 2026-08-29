package service

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

// Wire-boundary proof for SubmitSettlement (Phase 3.5, §11.2 — retargeted
// from the v1 micros wire, same values on both sides of the boundary): the
// v2 settlements route is units-native — the event's cny-e8-v1 units cross
// the wire EXACTLY as the amount object's decimal string, no conversion in
// either direction, and local_balance_after is dropped from the request (the
// drift comparison happens client-side against the response).
func TestCanonicalWalletSubmitSettlementWireIsUnitsExact(t *testing.T) {
	var received canonicalWalletSettlementWireRequest
	var rawBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/internal/v2/wallet/settlements", r.URL.Path)
		require.Equal(t, "POST", r.Method)
		require.True(t, strings.HasPrefix(r.Header.Get("Idempotency-Key"), "gwusg_"))
		raw, readErr := io.ReadAll(r.Body)
		require.NoError(t, readErr)
		require.NoError(t, json.Unmarshal(raw, &received))
		require.NoError(t, json.Unmarshal(raw, &rawBody))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"accepted":true,"duplicate":false,"named_lease_id":null,"event":{"event_id":"gwusg_test","lease_id":"lease-1","amount":{"amount_units":"101","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"lease_capture_seq":1,"lease_captured_before":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"lease_captured_after":{"amount_units":"101","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"occurred_at":"2026-08-26T12:00:00Z"},"lease":{"lease_id":"lease-1","platform_user_id":"user-1","currency":"CNY","unit_version":"cny-e8-v1","scale":8,"budget":{"amount_units":"1000","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"reserved":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"captured":{"amount_units":"101","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"released":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"capture_seq":1,"status":"active","expires_at":"2030-01-01T00:00:00Z"},"canonical_balance":{"amount_units":"98765400","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"}}}`))
	}))
	defer server.Close()

	cfg := canonicalWalletTestConfig(config.CanonicalWalletModeShadow)
	cfg.ControlPlaneURL, cfg.Secret = server.URL, strings.Repeat("s", 32)
	client := newCanonicalWalletHTTPClient(cfg, server.Client())
	result, err := client.SubmitSettlement(context.Background(), CanonicalWalletSettlementEvent{
		EventID: "gwusg_test", GatewayRequestID: "req-1", PlatformUserID: "user-1", LeaseID: "lease-1",
		Currency: "CNY", AmountUnits: 101, // 101 units cross the wire as exactly "101"
		LocalBalanceAfterUnits: ptrOf(int64(123_456789)), // deliberately NOT a whole micro — never converted, never sent
		OccurredAt:             time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	require.True(t, result.Accepted)

	require.Equal(t, "101", received.Amount.AmountUnits, "units-native: the wire amount is the event's units exactly — never smaller, never larger")
	require.Equal(t, "CNY", received.Amount.Currency)
	require.Equal(t, 8, received.Amount.Scale)
	require.Equal(t, "cny-e8-v1", received.Amount.UnitVersion)
	require.Equal(t, "2026-08-26T12:00:00Z", received.OccurredAt)
	require.Equal(t, "req-1", received.GatewayRequestID)
	require.Equal(t, "lease-1", received.LeaseID)
	require.Nil(t, rawBody["local_balance_after"], "§11.2: local_balance_after is dropped from the request — the drift comparison happens client-side")
	require.Nil(t, rawBody["local_balance_after_units"])

	// The strict parser: the response's event amount and canonical balance
	// come back as int64 units, no ×100 anywhere.
	require.Equal(t, int64(101), result.CapturedUnits)
	require.Equal(t, int64(1), result.LeaseCaptureSeq)
	require.Equal(t, "", result.NamedLeaseID, "a null named_lease_id parses to the empty string")
	require.NotNil(t, result.CanonicalBalanceUnits)
	require.Equal(t, int64(98_765_400), *result.CanonicalBalanceUnits, "98,765,400 units on the wire = 98,765,400 units parsed")
}

func ptrOf(v int64) *int64 { return &v }

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

// EnsureLease speaks ShipAny's v2 amount-object (cny-e8-v1) wire shape —
// every amount a Phase 0 object, no bare integer crosses the wire (§9.2) —
// and must REJECT a non-CNY currency outright instead of coercing it to CNY.
func TestCanonicalWalletEnsureLeaseWireResponseIsUnitsNative(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req canonicalWalletEnsureRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		require.Equal(t, "500000000", req.RequestedBudget.AmountUnits, "requested budget stays in cny-e8-v1 units")
		require.Equal(t, "cny-e8-v1", req.RequestedBudget.UnitVersion)
		require.Equal(t, 8, req.RequestedBudget.Scale)
		require.Equal(t, "CNY", req.RequestedBudget.Currency)
		_, _ = w.Write([]byte(`{"data":{"lease_id":"lease-wire","platform_user_id":"user-1","currency":"CNY","unit_version":"cny-e8-v1","scale":8,
			"budget":{"amount_units":"500000000","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},
			"reserved":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},
			"captured":{"amount_units":"250000000","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},
			"released":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},
			"headroom":{"amount_units":"250000000","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},
			"capture_seq":1,"status":"active","expires_at":"2030-01-01T00:00:00Z","outcome":"reused","clamped_by":"none"}}`))
	}))
	defer server.Close()
	cfg := canonicalWalletTestConfig(config.CanonicalWalletModeShadow)
	cfg.ControlPlaneURL, cfg.Secret = server.URL, strings.Repeat("s", 32)
	client := newCanonicalWalletHTTPClient(cfg, server.Client())
	res, err := client.EnsureLease(context.Background(), canonicalWalletEnsureRequest{
		PlatformUserID: "user-1", Currency: "CNY", Purpose: "authorize", MinHeadroom: newCanonicalWalletAmountObject(1), RequestedBudget: newCanonicalWalletAmountObject(500_000_000), RequestedTTLSeconds: 60, CallerSlotTTLSeconds: 1800,
	})
	require.NoError(t, err)
	require.Equal(t, int64(500_000_000), res.Lease.BudgetUnits)
	require.Equal(t, int64(250_000_000), res.Lease.ConsumedUnits, "consumed = budget − headroom_units: partial consumption history must survive the boundary")
	require.Equal(t, "CNY", res.Lease.Currency)
	require.Equal(t, "reused", res.Outcome)
}

func TestCanonicalWalletEnsureLeaseRejectsInvalidWireResponses(t *testing.T) {
	amount := func(units string) string {
		return `{"amount_units":"` + units + `","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"}`
	}
	// amountFields renders the five amount fields of a v2 response around the
	// named objects; every case must fail for ITS OWN clause, so the unnamed
	// amounts are valid.
	amountFields := func(budget, captured, headroom string) string {
		return `"budget":` + amount(budget) + `,"reserved":` + amount("0") + `,"captured":` + amount(captured) + `,"released":` + amount("0") + `,"headroom":` + amount(headroom)
	}
	cases := []struct {
		name    string
		body    string
		wantErr string
	}{
		{"empty lease id", `{"data":{"lease_id":"","platform_user_id":"user-1","currency":"CNY",` + amountFields("500000000", "0", "500000000") + `,"expires_at":"2030-01-01T00:00:00Z","status":"active","outcome":"issued"}}`, "invalid"},
		{"wrong platform user", `{"data":{"lease_id":"l","platform_user_id":"someone-else","currency":"CNY",` + amountFields("500000000", "0", "500000000") + `,"expires_at":"2030-01-01T00:00:00Z","status":"active","outcome":"issued"}}`, "invalid"},
		{"zero budget", `{"data":{"lease_id":"l","platform_user_id":"user-1","currency":"CNY",` + amountFields("0", "0", "0") + `,"expires_at":"2030-01-01T00:00:00Z","status":"active","outcome":"issued"}}`, "invalid"},
		{"negative captured", `{"data":{"lease_id":"l","platform_user_id":"user-1","currency":"CNY",` + amountFields("500000000", "-1", "500000000") + `,"expires_at":"2030-01-01T00:00:00Z","status":"active","outcome":"issued"}}`, "not a decimal integer string"},
		{"headroom over budget", `{"data":{"lease_id":"l","platform_user_id":"user-1","currency":"CNY",` + amountFields("500000000", "0", "500000001") + `,"expires_at":"2030-01-01T00:00:00Z","status":"active","outcome":"issued"}}`, "invalid"},
		{"bad outcome", `{"data":{"lease_id":"l","platform_user_id":"user-1","currency":"CNY",` + amountFields("500000000", "0", "500000000") + `,"expires_at":"2030-01-01T00:00:00Z","status":"active","outcome":"maybe"}}`, "invalid"},
		{"zero expiry", `{"data":{"lease_id":"l","platform_user_id":"user-1","currency":"CNY",` + amountFields("500000000", "0", "500000000") + `,"expires_at":"0001-01-01T00:00:00Z","status":"active","outcome":"issued"}}`, "invalid"},
		{"non-CNY currency", `{"data":{"lease_id":"l","platform_user_id":"user-1","currency":"USD",` + amountFields("500000000", "0", "500000000") + `,"expires_at":"2030-01-01T00:00:00Z","status":"active","outcome":"issued"}}`, "unsupported currency"},
		// §9.2 strictness: the decimal string is the only carrier of the value.
		{"budget beyond int64", `{"data":{"lease_id":"l","platform_user_id":"user-1","currency":"CNY","budget":{"amount_units":"9223372036854775808","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"reserved":` + amount("0") + `,"captured":` + amount("0") + `,"released":` + amount("0") + `,"headroom":` + amount("500000000") + `,"expires_at":"2030-01-01T00:00:00Z","status":"active","outcome":"issued"}}`, "exceeds int64"},
		{"negative budget", `{"data":{"lease_id":"l","platform_user_id":"user-1","currency":"CNY","budget":{"amount_units":"-1","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"reserved":` + amount("0") + `,"captured":` + amount("0") + `,"released":` + amount("0") + `,"headroom":` + amount("500000000") + `,"expires_at":"2030-01-01T00:00:00Z","status":"active","outcome":"issued"}}`, "not a decimal integer string"},
		{"leading zero budget", `{"data":{"lease_id":"l","platform_user_id":"user-1","currency":"CNY","budget":{"amount_units":"01","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"reserved":` + amount("0") + `,"captured":` + amount("0") + `,"released":` + amount("0") + `,"headroom":` + amount("500000000") + `,"expires_at":"2030-01-01T00:00:00Z","status":"active","outcome":"issued"}}`, "not a decimal integer string"},
		{"non-CNY amount object", `{"data":{"lease_id":"l","platform_user_id":"user-1","currency":"CNY","budget":{"amount_units":"500000000","currency":"USD","scale":8,"unit_version":"cny-e8-v1"},"reserved":` + amount("0") + `,"captured":` + amount("0") + `,"released":` + amount("0") + `,"headroom":` + amount("500000000") + `,"expires_at":"2030-01-01T00:00:00Z","status":"active","outcome":"issued"}}`, "not a cny-e8-v1 amount object"},
		{"status closed", `{"data":{"lease_id":"l","platform_user_id":"user-1","currency":"CNY",` + amountFields("500000000", "0", "500000000") + `,"expires_at":"2030-01-01T00:00:00Z","status":"closed","outcome":"issued"}}`, "invalid"},
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
			_, err := client.EnsureLease(context.Background(), canonicalWalletEnsureRequest{
				PlatformUserID: "user-1", Currency: "CNY", Purpose: "authorize", MinHeadroom: newCanonicalWalletAmountObject(1), RequestedBudget: newCanonicalWalletAmountObject(500_000_000), RequestedTTLSeconds: 60, CallerSlotTTLSeconds: 1800,
			})
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestCanonicalWalletEnsureLeaseRejectsNonCNYRequestedCurrency(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"lease_id":"l","platform_user_id":"user-1","currency":"USD","budget":{"amount_units":"500000000","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"headroom":{"amount_units":"500000000","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"expires_at":"2030-01-01T00:00:00Z","status":"active","outcome":"issued"}}`))
	}))
	defer server.Close()
	cfg := canonicalWalletTestConfig(config.CanonicalWalletModeShadow)
	cfg.ControlPlaneURL, cfg.Secret = server.URL, strings.Repeat("s", 32)
	client := newCanonicalWalletHTTPClient(cfg, server.Client())
	_, err := client.EnsureLease(context.Background(), canonicalWalletEnsureRequest{
		PlatformUserID: "user-1", Currency: "USD", Purpose: "authorize", MinHeadroom: newCanonicalWalletAmountObject(1), RequestedBudget: newCanonicalWalletAmountObject(500_000_000), RequestedTTLSeconds: 60, CallerSlotTTLSeconds: 1800,
	})
	// The v2 client validates the REQUESTED currency before the call leaves
	// the gateway — a non-CNY request is rejected outright either way.
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
	err := client.doJSON(context.Background(), http.MethodPost, "/api/internal/v2/wallet/settlements", canonicalWalletSettlementScope, "key", map[string]string{"a": "b"}, &resp)
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
	t.Cleanup(bridge.Close)
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
		observeCanonicalWalletSettlement(bridge, "req-obs-1", user, cost, false, true, &UsageBillingApplyResult{NewBalance: &newBilling}, "", "")
	})

	// Guards: each of these must return before any side effect.
	require.NotPanics(t, func() {
		observeCanonicalWalletSettlement(nil, "r", user, cost, false, true, nil, "", "")                              // nil bridge
		observeCanonicalWalletSettlement(bridge, "r", nil, cost, false, true, nil, "", "")                            // nil user
		observeCanonicalWalletSettlement(bridge, "r", user, nil, false, true, nil, "", "")                            // nil cost
		observeCanonicalWalletSettlement(bridge, "r", user, cost, true, true, nil, "", "")                            // subscription billing
		observeCanonicalWalletSettlement(bridge, "r", user, cost, false, false, nil, "", "")                          // billing not applied
		observeCanonicalWalletSettlement(bridge, "r", user, &CostBreakdown{ActualCost: 0}, false, true, nil, "", "")  // zero cost
		observeCanonicalWalletSettlement(bridge, "r", user, &CostBreakdown{ActualCost: -5}, false, true, nil, "", "") // negative cost
	})
}

func canonicalWalletBridgeStatsValue(key string) int64 {
	return CanonicalWalletBridgeStats()[key]
}

func TestPhase34ProtoEnsureWireRoundTripAndRefusals(t *testing.T) {
	var gotHeader http.Header
	var gotBody canonicalWalletEnsureRequest
	refuse := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/internal/v2/wallet/leases/ensure", r.URL.Path)
		gotHeader = r.Header.Clone()
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
		if refuse != "" {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"code":-1,"message":"` + refuse + `","data":{"reason":"` + refuse + `"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"data":{"lease_id":"srv-1","platform_user_id":"u1","currency":"CNY","unit_version":"cny-e8-v1","scale":8,
			"budget":{"amount_units":"500000000","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},
			"reserved":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},
			"captured":{"amount_units":"100000000","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},
			"released":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},
			"headroom":{"amount_units":"400000000","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},
			"capture_seq":3,"status":"active","expires_at":"2030-01-01T00:00:00Z","outcome":"reused","clamped_by":"none"}}`))
	}))
	defer srv.Close()
	cfg := canonicalWalletTestConfig(config.CanonicalWalletModeEnforce)
	cfg.ControlPlaneURL, cfg.Secret = srv.URL, strings.Repeat("s", 32)
	client := newCanonicalWalletHTTPClient(cfg, srv.Client())
	req := canonicalWalletEnsureRequest{PlatformUserID: "u1", Currency: "CNY", Purpose: "authorize", MinHeadroom: newCanonicalWalletAmountObject(1), RequestedBudget: newCanonicalWalletAmountObject(500_000_000), RequestedTTLSeconds: 300, CallerSlotTTLSeconds: 1800}

	res, err := client.EnsureLease(context.Background(), req)
	require.NoError(t, err)
	require.Empty(t, gotHeader.Get("Idempotency-Key"), "ensure is idempotent by transaction: no gwlease_ window key")
	require.True(t, strings.HasPrefix(gotHeader.Get("Authorization"), "Bearer "))
	require.Equal(t, req, gotBody)
	require.Equal(t, "1", gotBody.MinHeadroom.AmountUnits)
	require.Equal(t, "500000000", gotBody.RequestedBudget.AmountUnits)
	require.Equal(t, "cny-e8-v1", gotBody.MinHeadroom.UnitVersion)
	require.Equal(t, "reused", res.Outcome)
	require.Equal(t, int64(500_000_000), res.Lease.BudgetUnits)
	require.Equal(t, int64(100_000_000), res.Lease.ConsumedUnits, "consumed = budget − headroom_units")

	for reason, want := range map[string]error{
		"lease_cap_reached": ErrCanonicalWalletLeaseCapReached, "insufficient_balance": ErrCanonicalWalletBalanceShortfall, "lease_contention": ErrCanonicalWalletLeaseContention,
	} {
		refuse = reason
		_, err := client.EnsureLease(context.Background(), req)
		require.ErrorIs(t, err, want, reason)
	}
}
