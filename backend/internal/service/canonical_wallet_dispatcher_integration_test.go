//go:build integration

package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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
// row delivered. A non-USD event must leave NO durable row at all.
func TestCanonicalWalletOutboxDispatcherDeliversEndToEnd(t *testing.T) {
	ctx := context.Background()
	db := startCanonicalWalletTestPostgres(t, ctx)
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	outbox := &outboxStoreForTest{db: db}

	platformUserID := "shipany-user-" + uuid.NewString()

	var receivedSettlements []canonicalWalletSettlementWireRequest
	var receivedEnsureRequests []canonicalWalletEnsureRequest
	settlementSignal := make(chan struct{}, 4)
	controlPlane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/internal/v2/wallet/leases/ensure":
			var req canonicalWalletEnsureRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			receivedEnsureRequests = append(receivedEnsureRequests, req)
			require.Equal(t, "USD", req.Currency)
			_, _ = w.Write([]byte(`{"data":{"lease_id":"lease-e2e","platform_user_id":"` + req.PlatformUserID + `","currency":"USD","unit_version":"usd-e8-v1","scale":8,"budget":{"amount_units":"500000000","currency":"USD","scale":8,"unit_version":"usd-e8-v1"},"reserved":{"amount_units":"0","currency":"USD","scale":8,"unit_version":"usd-e8-v1"},"captured":{"amount_units":"0","currency":"USD","scale":8,"unit_version":"usd-e8-v1"},"released":{"amount_units":"0","currency":"USD","scale":8,"unit_version":"usd-e8-v1"},"headroom":{"amount_units":"500000000","currency":"USD","scale":8,"unit_version":"usd-e8-v1"},"capture_seq":0,"status":"active","expires_at":"2030-01-01T00:00:00Z","outcome":"issued","clamped_by":"none"}}`))
		case "/api/internal/v2/wallet/settlements":
			var req canonicalWalletSettlementWireRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			receivedSettlements = append(receivedSettlements, req)
			select {
			case settlementSignal <- struct{}{}:
			default:
			}
			_, _ = w.Write([]byte(`{"data":{"accepted":true,"duplicate":false,"named_lease_id":null,"event":{"event_id":"` + req.EventID + `","lease_id":"lease-e2e","amount":{"amount_units":"30000000","currency":"USD","scale":8,"unit_version":"usd-e8-v1"},"lease_capture_seq":1,"lease_captured_before":{"amount_units":"0","currency":"USD","scale":8,"unit_version":"usd-e8-v1"},"lease_captured_after":{"amount_units":"30000000","currency":"USD","scale":8,"unit_version":"usd-e8-v1"},"occurred_at":"2026-01-01T00:00:00Z"},"lease":{"lease_id":"lease-e2e","platform_user_id":"user-1","currency":"USD","unit_version":"usd-e8-v1","scale":8,"budget":{"amount_units":"500000000","currency":"USD","scale":8,"unit_version":"usd-e8-v1"},"reserved":{"amount_units":"0","currency":"USD","scale":8,"unit_version":"usd-e8-v1"},"captured":{"amount_units":"30000000","currency":"USD","scale":8,"unit_version":"usd-e8-v1"},"released":{"amount_units":"0","currency":"USD","scale":8,"unit_version":"usd-e8-v1"},"capture_seq":1,"status":"active","expires_at":"2030-01-01T00:00:00Z"},"canonical_balance":{"amount_units":"497000000","currency":"USD","scale":8,"unit_version":"usd-e8-v1"}}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer controlPlane.Close()

	cfg := canonicalWalletTestConfig(config.CanonicalWalletModeEnforce)
	cfg.ControlPlaneURL, cfg.Secret = controlPlane.URL, strings.Repeat("s", 32)
	cfg.RequestTimeoutMS = 100 // dispatcher tick interval for this test

	client := newCanonicalWalletHTTPClient(cfg, controlPlane.Client())
	bridge := newCanonicalWalletBridge(cfg, store, client, db, outbox, 0, nil)
	t.Cleanup(bridge.Close)
	require.NotNil(t, bridge)

	amountUnits := int64(30_000000) // 0.30 USD in usd-e8-v1 units
	bridge.ObserveSettlement(CanonicalWalletSettlementEvent{
		GatewayRequestID: "req-e2e-1", PlatformUserID: platformUserID,
		Currency: "USD", AmountUnits: amountUnits,
	})

	// A non-USD event must be rejected at the strict boundary and never
	// reach the durable store.
	bridge.ObserveSettlement(CanonicalWalletSettlementEvent{
		GatewayRequestID: "req-e2e-usd", PlatformUserID: platformUserID,
		Currency: "CNY", AmountUnits: 1_000000,
	})
	var usdRows int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM wallet_settlement_outbox WHERE gateway_request_id = 'req-e2e-usd'`).Scan(&usdRows))
	require.Equal(t, 0, usdRows, "a non-USD settlement event must never be durably recorded")

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

	// The wire request was UNITS-NATIVE and the ensure call asked for the
	// event's amount as min_headroom_units and the CONFIGURED lease budget as
	// requested_budget_units — the server takes the max (§3 step 4), so the
	// client no longer computes it.
	require.Len(t, receivedEnsureRequests, 1)
	require.Equal(t, amountUnits, mustUnits(receivedEnsureRequests[0].MinHeadroom), "the amount is the min_headroom ask")
	require.Equal(t, cfg.LeaseBudgetUnits, mustUnits(receivedEnsureRequests[0].RequestedBudget), "requested_budget is the configured lease budget")
	require.Len(t, receivedSettlements, 1)
	require.Equal(t, "lease-e2e", receivedSettlements[0].LeaseID, "the settlement is anchored to the lease the reservation actually landed on")
	require.Equal(t, "30000000", receivedSettlements[0].Amount.AmountUnits, "30,000,000 units cross the v2 wire as exactly 30,000,000")

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

// budgetConsumingControlPlane (Phase 5-G, Task 4): the control plane whose
// SubmitSettlement consumes the attempt's ENTIRE per-attempt budget and then
// fails transiently — the production shape behind the 71 stale reclaims
// logged in three days (a slow or stalled control-plane call).
type budgetConsumingControlPlane struct {
	canonicalWalletControlStub
}

func (c *budgetConsumingControlPlane) SubmitSettlement(ctx context.Context, _ CanonicalWalletSettlementEvent) (*CanonicalWalletSettlementResult, error) {
	<-ctx.Done()
	return nil, errors.New("canonical wallet control plane: transport failure after the attempt budget")
}

// TestDeliverOutboxEventRecordsFailureWhenTheAttemptConsumedItsBudget (Wallet
// Lease Phase 5-G, Task 4): the failure record must never ride the attempt's
// own context. When the attempt itself consumed the budget, the mark on that
// expired context fails and is discarded — the row stays in_flight, is
// reclaimed ~6 s later by ReclaimStaleInFlightEvents (which does not touch
// attempt_count), and loops forever without aging toward dead-letter.
func TestDeliverOutboxEventRecordsFailureWhenTheAttemptConsumedItsBudget(t *testing.T) {
	ctx := context.Background()
	db := startCanonicalWalletTestPostgres(t, ctx)
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	outbox := &outboxStoreForTest{db: db}

	platformUserID := "shipany-user-" + uuid.NewString()
	control := &budgetConsumingControlPlane{canonicalWalletControlStub{lease: CanonicalWalletLease{
		LeaseID: "lease-budget-" + uuid.NewString(), PlatformUserID: platformUserID, Currency: "USD",
		BudgetUnits: 500_000000, ExpiresAt: time.Now().UTC().Add(time.Minute),
	}}}
	cfg := canonicalWalletTestConfig(config.CanonicalWalletModeEnforce)
	cfg.RequestTimeoutMS = 150
	bridge := newCanonicalWalletBridge(cfg, store, control, db, outbox, 0, nil)
	t.Cleanup(bridge.Close)

	require.True(t, bridge.ObserveSettlement(CanonicalWalletSettlementEvent{
		GatewayRequestID: "req-budget-1", PlatformUserID: platformUserID,
		Currency: "USD", AmountUnits: 10_000000,
	}))
	claimed, err := outbox.ClaimPendingOutboxEvents(ctx, bridge.workerID, 1)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	e := claimed[0]

	// The dispatcher gives each event exactly one per-attempt budget.
	eventCtx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.RequestTimeoutMS)*time.Millisecond)
	bridge.deliverOutboxEvent(eventCtx, e)
	cancel()

	var attemptCount int
	var status string
	var nextAttemptAt sql.NullTime
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT attempt_count, status, next_attempt_at FROM wallet_settlement_outbox WHERE id = $1`, e.ID,
	).Scan(&attemptCount, &status, &nextAttemptAt))
	require.Equal(t, 1, attemptCount, "the failure must be recorded even when the attempt consumed its whole budget — otherwise the row loops in_flight without aging toward dead-letter")
	require.Equal(t, "pending", status, "the row returns to pending for the backoff")
	require.True(t, nextAttemptAt.Valid, "next_attempt_at must be set")
}
