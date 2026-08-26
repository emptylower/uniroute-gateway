//go:build integration

package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestCanonicalWalletOutboxRetryReservesAgainstTheOriginalLease drives the
// real dispatcher through the exact sequence that used to dead-letter a
// settlement: reserve on lease A, fail the settlement, rotate the current
// lease to B, retry.
//
// The Redis reservation marker is keyed by (platform_user_id, event_id) and
// remembers that this event belongs to lease A. A retry that reserves
// against "whatever lease is current now" gets Lua code 6 (cross-lease
// conflict) on every attempt until the row dead-letters — the settlement is
// then never delivered, while the local balance was already debited.
//
// Every link here is real: real Postgres outbox, real Redis lease store with
// the real Lua scripts, real HTTP control plane, and the real
// runOutboxDispatcher goroutine.
func TestCanonicalWalletOutboxRetryReservesAgainstTheOriginalLease(t *testing.T) {
	ctx := context.Background()
	db := startCanonicalWalletTestPostgres(t, ctx)
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	outbox := &outboxStoreForTest{db: db}

	platformUserID := "shipany-user-" + uuid.NewString()

	var mu sync.Mutex
	var leaseAcquires int
	var settlementLeaseIDs []string
	settlementSignal := make(chan struct{}, 8)

	controlPlane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/internal/v1/wallet/leases/acquire":
			var req canonicalWalletLeaseRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			mu.Lock()
			leaseAcquires++
			mu.Unlock()
			// lease-a: 5 CNY of budget in ShipAny's wire micros, expiring
			// far in the future so it stays resolvable BY ID after it stops
			// being the user's current lease.
			_, _ = w.Write([]byte(`{"data":{"lease_id":"lease-a","platform_user_id":"` + req.PlatformUserID + `","currency":"CNY","budget_micros":5000000,"consumed_micros":0,"expires_at":"2030-01-01T00:00:00Z"}}`))
		case "/api/internal/v1/wallet/settlements":
			var req canonicalWalletSettlementWireRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			mu.Lock()
			settlementLeaseIDs = append(settlementLeaseIDs, req.LeaseID)
			attempt := len(settlementLeaseIDs)
			mu.Unlock()
			select {
			case settlementSignal <- struct{}{}:
			default:
			}
			if attempt == 1 {
				// First delivery fails — this is what sends the row back to
				// pending and creates the retry this test is about.
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"error":"control plane unavailable"}`))
				return
			}
			_, _ = w.Write([]byte(`{"data":{"accepted":true,"duplicate":false,"canonical_balance_micros":4970000}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer controlPlane.Close()

	cfg := canonicalWalletTestConfig(config.CanonicalWalletModeEnforce)
	cfg.ControlPlaneURL, cfg.Secret = controlPlane.URL, strings.Repeat("s", 32)
	cfg.RequestTimeoutMS = 100 // also the dispatcher's tick interval

	client := newCanonicalWalletHTTPClient(cfg, controlPlane.Client())
	bridge := newCanonicalWalletBridge(cfg, store, client, db, outbox)
	require.NotNil(t, bridge)

	gatewayRequestID := "req-rebind-" + uuid.NewString()
	amountUnits := int64(30_000000) // 0.30 CNY in cny-e8-v1 units
	bridge.ObserveSettlement(CanonicalWalletSettlementEvent{
		GatewayRequestID: gatewayRequestID, PlatformUserID: platformUserID,
		Currency: "CNY", AmountUnits: amountUnits,
	})

	// Wait for the first (failing) delivery attempt.
	select {
	case <-settlementSignal:
	case <-time.After(20 * time.Second):
		t.Fatal("the dispatcher never attempted the first delivery")
	}

	// The row must now durably record WHICH lease the reservation landed on.
	// Poll: the bind happens before the settlement call, but the row's
	// post-failure status transition happens after it.
	deadline := time.Now().Add(20 * time.Second)
	boundLeaseID, status := "", ""
	for time.Now().Before(deadline) {
		require.NoError(t, db.QueryRowContext(ctx,
			`SELECT COALESCE(lease_id, ''), status FROM wallet_settlement_outbox WHERE gateway_request_id = $1`,
			gatewayRequestID).Scan(&boundLeaseID, &status))
		if boundLeaseID == "lease-a" && status == "pending" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.Equal(t, "lease-a", boundLeaseID, "the outbox row must durably record the lease its reservation was anchored to")
	require.Equal(t, "pending", status, "a failed settlement must return the row to pending, not resolve it")

	// Rotate the user's CURRENT lease to lease-b, with a later expiry so the
	// pointer-regression guard lets the pointer advance. lease-a stays alive
	// in Redis and stays resolvable by id — exactly the production state
	// where the old code would reserve against b and conflict forever.
	require.NoError(t, store.InstallCanonicalWalletLease(ctx, CanonicalWalletLease{
		LeaseID: "lease-b", PlatformUserID: platformUserID, Currency: "CNY",
		BudgetUnits: 500_000000, ConsumedUnits: 0,
		ExpiresAt: time.Date(2031, 1, 1, 0, 0, 0, 0, time.UTC),
	}))

	// The retry must settle, and must do so on lease-a.
	deadline = time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		require.NoError(t, db.QueryRowContext(ctx,
			`SELECT status FROM wallet_settlement_outbox WHERE gateway_request_id = $1`,
			gatewayRequestID).Scan(&status))
		if status == "delivered" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.Equal(t, "delivered", status, "the retry must deliver, not dead-letter on a cross-lease reservation conflict")

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(settlementLeaseIDs), 2, "there must be a retry after the first failure")
	for i, leaseID := range settlementLeaseIDs {
		require.Equal(t, "lease-a", leaseID, "settlement attempt %d must stay anchored to the lease the reservation landed on", i+1)
	}
	require.Equal(t, 1, leaseAcquires, "the retry must resolve the bound lease by id, not acquire a new one")
}
