//go:build integration

package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestCanonicalWalletOutboxDispatcherDeliversEndToEnd drives the REAL
// delivery loop, every link real: ObserveSettlement inserts durably into a
// real Postgres outbox table; runOutboxDispatcher's ticker claims it;
// deliverOutboxEvent acquires the lease from a real HTTP control plane
// (speaking ShipAny's *_micros wire format), reserves against a real Redis
// lease store (the same gatewayCacheAdapterForTest used by the admission
// tests), submits the settlement back to the control plane, and marks the
// row delivered. A non-CNY event must leave NO durable row at all.
func TestCanonicalWalletOutboxDispatcherDeliversEndToEnd(t *testing.T) {
	ctx := context.Background()
	db := startCanonicalWalletTestPostgres(t, ctx)
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	outbox := &outboxStoreForTest{db: db}

	platformUserID := "shipany-user-" + uuid.NewString()

	var receivedSettlements []canonicalWalletSettlementWireRequest
	var receivedLeaseRequests []canonicalWalletLeaseRequest
	settlementSignal := make(chan struct{}, 4)
	controlPlane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/internal/v1/wallet/leases/acquire":
			var req canonicalWalletLeaseRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			receivedLeaseRequests = append(receivedLeaseRequests, req)
			require.Equal(t, "CNY", req.Currency)
			// ShipAny's route speaks *_micros: 5 CNY budget = 5,000,000 micros.
			_, _ = w.Write([]byte(`{"data":{"lease_id":"lease-e2e","platform_user_id":"` + req.PlatformUserID + `","currency":"CNY","budget_micros":5000000,"consumed_micros":0,"expires_at":"2030-01-01T00:00:00Z"}}`))
		case "/api/internal/v1/wallet/settlements":
			var req canonicalWalletSettlementWireRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			receivedSettlements = append(receivedSettlements, req)
			select {
			case settlementSignal <- struct{}{}:
			default:
			}
			_, _ = w.Write([]byte(`{"data":{"accepted":true,"duplicate":false,"canonical_balance_micros":4970000}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer controlPlane.Close()

	cfg := canonicalWalletTestConfig(config.CanonicalWalletModeEnforce)
	cfg.ControlPlaneURL, cfg.Secret = controlPlane.URL, strings.Repeat("s", 32)
	cfg.RequestTimeoutMS = 100 // dispatcher tick interval for this test

	client := newCanonicalWalletHTTPClient(cfg, controlPlane.Client())
	bridge := newCanonicalWalletBridge(cfg, store, client, db, outbox)
	require.NotNil(t, bridge)

	amountUnits := int64(30_000000) // 0.30 CNY in cny-e8-v1 units
	bridge.ObserveSettlement(CanonicalWalletSettlementEvent{
		GatewayRequestID: "req-e2e-1", PlatformUserID: platformUserID,
		Currency: "CNY", AmountUnits: amountUnits,
	})

	// A non-CNY event must be rejected at the strict boundary and never
	// reach the durable store.
	bridge.ObserveSettlement(CanonicalWalletSettlementEvent{
		GatewayRequestID: "req-e2e-usd", PlatformUserID: platformUserID,
		Currency: "USD", AmountUnits: 1_000000,
	})
	var usdRows int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM wallet_settlement_outbox WHERE gateway_request_id = 'req-e2e-usd'`).Scan(&usdRows))
	require.Equal(t, 0, usdRows, "a non-CNY settlement event must never be durably recorded")

	// Wait for the dispatcher to deliver (first tick fires one
	// RequestTimeoutMS after construction).
	deadline := time.Now().Add(10 * time.Second)
	delivered := false
	for time.Now().Before(deadline) && !delivered {
		select {
		case <-settlementSignal:
			delivered = true
		case <-time.After(100 * time.Millisecond):
		}
	}
	require.True(t, delivered, "the dispatcher must deliver the durably-recorded settlement to the control plane")

	// The wire request carried CEILING-converted micros and the lease
	// acquisition spoke micros too. ensureLease requests
	// max(cfg.LeaseBudgetUnits, amountUnits) — here the amount (30,000,000
	// units) exceeds the test config's budget (100,000 units), so the lease
	// is sized to the amount: ceiling(30,000,000 / 100) = 300,000 micros.
	require.Len(t, receivedLeaseRequests, 1)
	require.Equal(t, int64(300000), receivedLeaseRequests[0].RequestedMicros, "max(configured budget, amount) converted to wire-level micros with ceiling division")
	require.Len(t, receivedSettlements, 1)
	require.Equal(t, "lease-e2e", receivedSettlements[0].LeaseID, "the settlement is anchored to the lease the reservation actually landed on")
	require.Equal(t, int64(300000), receivedSettlements[0].AmountMicros, "30,000,000 units = 300,000 micros")

	// The reservation really consumed the Redis lease.
	leased, err := store.GetCanonicalWalletLeaseByID(ctx, platformUserID, "lease-e2e")
	require.NoError(t, err)
	require.Equal(t, amountUnits, leased.ConsumedUnits, "the real Redis lease shows the reserved consumption")

	// The outbox row is resolved as delivered once the dispatcher's own
	// mark lands (poll briefly — the signal fires before MarkDelivered).
	deadline = time.Now().Add(10 * time.Second)
	status := ""
	for time.Now().Before(deadline) {
		rows, err := db.QueryContext(ctx, `SELECT status FROM wallet_settlement_outbox WHERE gateway_request_id = 'req-e2e-1'`)
		require.NoError(t, err)
		for rows.Next() {
			var s string
			require.NoError(t, rows.Scan(&s))
			status = s
		}
		rows.Close()
		if status == "delivered" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.Equal(t, "delivered", status, "the dispatcher must resolve the claimed row as delivered under its own claim token")
}
