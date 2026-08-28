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
		case "/api/internal/v2/wallet/leases/ensure":
			var req canonicalWalletEnsureRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			mu.Lock()
			leaseAcquires++
			mu.Unlock()
			// lease-a: 5 CNY of budget, expiring far in the future so it
			// stays resolvable BY ID after it stops being the user's
			// current lease.
			_, _ = w.Write([]byte(`{"data":{"lease_id":"lease-a","platform_user_id":"` + req.PlatformUserID + `","currency":"CNY","unit_version":"cny-e8-v1","scale":8,"budget":{"amount_units":"500000000","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"reserved":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"captured":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"released":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"headroom":{"amount_units":"500000000","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"capture_seq":0,"status":"active","expires_at":"2030-01-01T00:00:00Z","outcome":"issued","clamped_by":"none"}}`))
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
	bridge := newCanonicalWalletBridge(cfg, store, client, db, outbox, 0, nil)
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

func TestCanonicalWalletDeliverOutboxEventClaimLost(t *testing.T) {
	ctx := context.Background()
	db := startCanonicalWalletTestPostgres(t, ctx)
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	outbox := &outboxStoreForTest{db: db}

	platformUserID := "shipany-user-" + uuid.NewString()
	controlPlane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/internal/v2/wallet/leases/ensure" {
			var req canonicalWalletEnsureRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			_, _ = w.Write([]byte(`{"data":{"lease_id":"lease-lost","platform_user_id":"` + req.PlatformUserID + `","currency":"CNY","unit_version":"cny-e8-v1","scale":8,"budget":{"amount_units":"500000000","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"reserved":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"captured":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"released":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"headroom":{"amount_units":"500000000","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"capture_seq":0,"status":"active","expires_at":"2030-01-01T00:00:00Z","outcome":"issued","clamped_by":"none"}}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer controlPlane.Close()

	cfg := canonicalWalletTestConfig(config.CanonicalWalletModeEnforce)
	cfg.ControlPlaneURL, cfg.Secret = controlPlane.URL, strings.Repeat("s", 32)
	client := newCanonicalWalletHTTPClient(cfg, controlPlane.Client())
	bridge := newCanonicalWalletBridge(cfg, store, client, db, outbox, 0, nil)

	event := CanonicalWalletSettlementEvent{
		EventID: "gwusg_" + uuid.NewString(), GatewayRequestID: "req-" + uuid.NewString(),
		PlatformUserID: platformUserID, Currency: "CNY", AmountUnits: 10_000000, OccurredAt: time.Now().UTC(),
	}
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, outbox.InsertOutboxEventTx(ctx, tx, event))
	require.NoError(t, tx.Commit())

	// Claim row under worker-other
	var id int64
	require.NoError(t, db.QueryRowContext(ctx, `UPDATE wallet_settlement_outbox SET status = 'in_flight', claimed_by = 'worker-other', claimed_at = now() WHERE event_id = $1 RETURNING id`, event.EventID).Scan(&id))

	outboxEvent := CanonicalWalletOutboxEvent{
		ID: id, EventID: event.EventID, GatewayRequestID: event.GatewayRequestID,
		PlatformUserID: event.PlatformUserID, Currency: "CNY", AmountUnits: event.AmountUnits,
		OccurredAt: event.OccurredAt,
	}

	// bridge.workerID is a generated uuid, different from worker-other.
	// deliverOutboxEvent should encounter ErrCanonicalWalletOutboxClaimLost when binding lease-lost,
	// and return early WITHOUT calling MarkOutboxEventFailed.
	bridge.deliverOutboxEvent(ctx, outboxEvent)

	var status, claimedBy string
	var attempts int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT status, claimed_by, attempt_count FROM wallet_settlement_outbox WHERE id = $1`, id).Scan(&status, &claimedBy, &attempts))
	require.Equal(t, "in_flight", status)
	require.Equal(t, "worker-other", claimedBy)
	require.Equal(t, 0, attempts)
}

func TestCanonicalWalletDeliverOutboxEventStaleBindingAndExpiredFallback(t *testing.T) {
	ctx := context.Background()
	db := startCanonicalWalletTestPostgres(t, ctx)
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	outbox := &outboxStoreForTest{db: db}

	platformUserID := "shipany-user-" + uuid.NewString()

	controlPlane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/internal/v2/wallet/leases/ensure":
			var req canonicalWalletEnsureRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			_, _ = w.Write([]byte(`{"data":{"lease_id":"lease-fresh","platform_user_id":"` + req.PlatformUserID + `","currency":"CNY","unit_version":"cny-e8-v1","scale":8,"budget":{"amount_units":"500000000","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"reserved":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"captured":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"released":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"headroom":{"amount_units":"500000000","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"capture_seq":0,"status":"active","expires_at":"2030-01-01T00:00:00Z","outcome":"issued","clamped_by":"none"}}`))
		case "/api/internal/v1/wallet/settlements":
			var req canonicalWalletSettlementWireRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			_, _ = w.Write([]byte(`{"data":{"accepted":true,"duplicate":false,"canonical_balance_micros":4000000}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer controlPlane.Close()

	cfg := canonicalWalletTestConfig(config.CanonicalWalletModeEnforce)
	cfg.ControlPlaneURL, cfg.Secret = controlPlane.URL, strings.Repeat("s", 32)
	client := newCanonicalWalletHTTPClient(cfg, controlPlane.Client())
	bridge := newCanonicalWalletBridge(cfg, store, client, db, outbox, 0, nil)

	// 1. Install an EXPIRED lease in Redis to test resolveOutboxEventLease fallback
	require.NoError(t, store.InstallCanonicalWalletLease(ctx, CanonicalWalletLease{
		LeaseID: "lease-expired", PlatformUserID: platformUserID, Currency: "CNY",
		BudgetUnits: 100_000000, ConsumedUnits: 0,
		ExpiresAt: time.Now().UTC().Add(-time.Hour),
	}))

	localBal := int64(350_000000)
	event := CanonicalWalletSettlementEvent{
		EventID: "gwusg_" + uuid.NewString(), GatewayRequestID: "req-fallback-" + uuid.NewString(),
		PlatformUserID: platformUserID, LeaseID: "lease-expired", Currency: "CNY",
		AmountUnits: 10_000000, LocalBalanceAfterUnits: &localBal, OccurredAt: time.Now().UTC(),
	}
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, outbox.InsertOutboxEventTx(ctx, tx, event))
	require.NoError(t, tx.Commit())

	var id int64
	require.NoError(t, db.QueryRowContext(ctx, `UPDATE wallet_settlement_outbox SET status = 'in_flight', claimed_by = $2, claimed_at = now() WHERE event_id = $1 RETURNING id`, event.EventID, bridge.workerID).Scan(&id))

	outboxEvent := CanonicalWalletOutboxEvent{
		ID: id, EventID: event.EventID, GatewayRequestID: event.GatewayRequestID,
		PlatformUserID: event.PlatformUserID, LeaseID: "lease-expired", Currency: "CNY",
		AmountUnits: event.AmountUnits, LocalBalanceAfterUnits: &localBal,
		OccurredAt: event.OccurredAt,
	}

	// resolveOutboxEventLease sees lease-expired is expired -> falls back to ensureLease -> gets lease-fresh.
	// rebinds outbox row to lease-fresh.
	// reserves lease-fresh.
	// submits settlement -> returns balance delta != 0 -> balanceMismatch incremented!
	bridge.deliverOutboxEvent(ctx, outboxEvent)

	var status, boundLease string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT status, lease_id FROM wallet_settlement_outbox WHERE id = $1`, id).Scan(&status, &boundLease))
	require.Equal(t, "delivered", status)
	require.Equal(t, "lease-fresh", boundLease)
	require.Greater(t, canonicalWalletBridgeMetrics.balanceMismatch.Load(), int64(0))

	// 2. Now test stale binding release:
	// Install lease-exhausted with 0 remaining budget
	require.NoError(t, store.InstallCanonicalWalletLease(ctx, CanonicalWalletLease{
		LeaseID: "lease-exhausted", PlatformUserID: platformUserID, Currency: "CNY",
		BudgetUnits: 10_000000, ConsumedUnits: 10_000000,
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	}))

	event2 := CanonicalWalletSettlementEvent{
		EventID: "gwusg_" + uuid.NewString(), GatewayRequestID: "req-exhausted-" + uuid.NewString(),
		PlatformUserID: platformUserID, LeaseID: "lease-exhausted", Currency: "CNY",
		AmountUnits: 10_000000, OccurredAt: time.Now().UTC(),
	}
	tx2, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, outbox.InsertOutboxEventTx(ctx, tx2, event2))
	require.NoError(t, tx2.Commit())

	var id2 int64
	require.NoError(t, db.QueryRowContext(ctx, `UPDATE wallet_settlement_outbox SET status = 'in_flight', claimed_by = $2, claimed_at = now() WHERE event_id = $1 RETURNING id`, event2.EventID, bridge.workerID).Scan(&id2))

	outboxEvent2 := CanonicalWalletOutboxEvent{
		ID: id2, EventID: event2.EventID, GatewayRequestID: event2.GatewayRequestID,
		PlatformUserID: event2.PlatformUserID, LeaseID: "lease-exhausted", Currency: "CNY",
		AmountUnits: event2.AmountUnits, OccurredAt: event2.OccurredAt,
	}

	// resolveOutboxEventLease returns lease-exhausted (valid, not expired).
	// leaseID == e.LeaseID, no rebind before reserve.
	// Reserve returns ErrCanonicalWalletLeaseExhausted.
	// canonicalWalletLeaseBindingIsStale is true -> rebinds to "" (clears binding)!
	// marks outbox event failed.
	bridge.deliverOutboxEvent(ctx, outboxEvent2)

	require.NoError(t, db.QueryRowContext(ctx, `SELECT status, COALESCE(lease_id, '') FROM wallet_settlement_outbox WHERE id = $1`, id2).Scan(&status, &boundLease))
	require.Equal(t, "pending", status)
	require.Equal(t, "", boundLease, "stale binding must be cleared on ErrCanonicalWalletLeaseExhausted")
}

func TestCanonicalWalletResolveOutboxEventLeaseRedisError(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	_ = rdb.Close()

	bridge := &CanonicalWalletBridge{store: store}
	_, err := bridge.resolveOutboxEventLease(ctx, CanonicalWalletOutboxEvent{
		PlatformUserID: "u", LeaseID: "some-lease", Currency: "CNY", AmountUnits: 10,
	})
	require.Error(t, err)
}
