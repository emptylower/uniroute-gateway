//go:build integration

package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestObserveSettlementSurfacesRealInsertFailures drives ObserveSettlement's
// durability-failure branch against a REAL Postgres failure: the outbox
// table is dropped after a successful insert, so the next insert genuinely
// fails, rolls its transaction back, and is counted in queue_dropped — the
// disclosed "Postgres itself is down" boundary.
func TestObserveSettlementSurfacesRealInsertFailures(t *testing.T) {
	ctx := context.Background()
	db := startCanonicalWalletTestPostgres(t, ctx)
	outbox := &outboxStoreForTest{db: db}
	bridge := &CanonicalWalletBridge{
		cfg: canonicalWalletTestConfig(config.CanonicalWalletModeEnforce), store: &canonicalWalletStoreStub{},
		control: &canonicalWalletControlStub{}, outboxDB: db, outbox: outbox, workerID: "test-insert-failure",
	}

	before := CanonicalWalletBridgeStats()["queue_dropped"]

	event := CanonicalWalletSettlementEvent{
		GatewayRequestID: "req-drop-" + uuid.NewString(), PlatformUserID: "shipany-user-" + uuid.NewString(),
		Currency: "CNY", AmountUnits: 1_000000,
	}
	bridge.ObserveSettlement(event) // succeeds — row durably present
	pending, err := outbox.ClaimPendingOutboxEvents(ctx, "probe", 10)
	require.NoError(t, err)
	require.Len(t, pending, 1)

	_, err = db.ExecContext(ctx, `DROP TABLE wallet_settlement_outbox`)
	require.NoError(t, err)

	bridge.ObserveSettlement(event) // insert now fails for real; must not panic, must count
	require.Greater(t, CanonicalWalletBridgeStats()["queue_dropped"], before,
		"a failed durable insert is counted in queue_dropped — the operator signal for lost events")
}

// claimOnlyRow drives the store's real atomic claim until the single
// expected event comes back claimed under the test's own worker id.
func claimOnlyRow(t *testing.T, ctx context.Context, outbox *outboxStoreForTest, workerID string) CanonicalWalletOutboxEvent {
	t.Helper()
	events, err := outbox.ClaimPendingOutboxEvents(ctx, workerID, 10)
	require.NoError(t, err)
	require.Len(t, events, 1, "exactly one durably-recorded event is expected")
	return events[0]
}

// TestDeliverOutboxEventRealFailurePaths drives deliverOutboxEvent's error
// branches end-to-end against real infrastructure. Every failed path must
// mark the claimed row FAILED (back to pending) so the dispatcher retries.
func TestDeliverOutboxEventRealFailurePaths(t *testing.T) {
	ctx := context.Background()

	t.Run("lease acquire fails when control plane is down", func(t *testing.T) {
		db := startCanonicalWalletTestPostgres(t, ctx)
		rdb := startCanonicalWalletTestRedis(t, ctx)
		store := &gatewayCacheAdapterForTest{rdb: rdb}
		outbox := &outboxStoreForTest{db: db}

		cfg := canonicalWalletTestConfig(config.CanonicalWalletModeEnforce)
		cfg.ControlPlaneURL, cfg.Secret = "http://127.0.0.1:1", strings.Repeat("s", 32) // nothing listens here
		client := newCanonicalWalletHTTPClient(cfg, nil)
		bridge := &CanonicalWalletBridge{cfg: cfg, store: store, control: client, outboxDB: db, outbox: outbox, workerID: "test-worker-acquire-fail"}

		bridge.ObserveSettlement(CanonicalWalletSettlementEvent{
			GatewayRequestID: "req-acq-fail", PlatformUserID: "shipany-user-" + uuid.NewString(),
			Currency: "CNY", AmountUnits: 30_000000,
		})
		e := claimOnlyRow(t, ctx, outbox, bridge.workerID)

		bridge.deliverOutboxEvent(ctx, e)

		status, err := outbox.OutboxEventStatus(ctx, e.ID)
		require.NoError(t, err)
		require.Equal(t, "pending", status, "a failed lease acquisition returns the row to pending for retry")
	})

	t.Run("reservation exhausted against a tiny real lease", func(t *testing.T) {
		db := startCanonicalWalletTestPostgres(t, ctx)
		rdb := startCanonicalWalletTestRedis(t, ctx)
		store := &gatewayCacheAdapterForTest{rdb: rdb}
		outbox := &outboxStoreForTest{db: db}
		platformUserID := "shipany-user-" + uuid.NewString()

		// The control plane only ever issues a 100-unit lease, far below
		// the event's amount, so the REAL Redis reserve rejects it.
		controlPlane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req canonicalWalletEnsureRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			_, _ = w.Write([]byte(`{"data":{"lease_id":"lease-tiny","platform_user_id":"` + req.PlatformUserID + `","currency":"CNY","unit_version":"cny-e8-v1","scale":8,"budget":{"amount_units":"100","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"reserved":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"captured":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"released":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"headroom":{"amount_units":"100","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"capture_seq":0,"status":"active","expires_at":"2030-01-01T00:00:00Z","outcome":"issued","clamped_by":"none"}}`))
		}))
		defer controlPlane.Close()
		cfg := canonicalWalletTestConfig(config.CanonicalWalletModeEnforce)
		cfg.ControlPlaneURL, cfg.Secret = controlPlane.URL, strings.Repeat("s", 32)
		client := newCanonicalWalletHTTPClient(cfg, controlPlane.Client())
		bridge := &CanonicalWalletBridge{cfg: cfg, store: store, control: client, outboxDB: db, outbox: outbox, workerID: "test-worker-reserve-fail"}

		bridge.ObserveSettlement(CanonicalWalletSettlementEvent{
			GatewayRequestID: "req-res-fail", PlatformUserID: platformUserID,
			Currency: "CNY", AmountUnits: 30_000000,
		})
		e := claimOnlyRow(t, ctx, outbox, bridge.workerID)

		bridge.deliverOutboxEvent(ctx, e)

		status, err := outbox.OutboxEventStatus(ctx, e.ID)
		require.NoError(t, err)
		// Phase 3.4a (redesign §9.3): the fake grants below min_headroom_units,
		// which a §3-conformant server never does — the local under-grant guard
		// is TRANSIENT (its own sentinel), so the row is retried on the backoff,
		// never dead-lettered. Only the server's insufficient_balance refusal is
		// terminal (balance_shortfall).
		require.Equal(t, "pending", status, "an under-grant delivery that does not resolve in one attempt is transient (§9.3/§11.3): retried on the backoff, never dead-lettered")
		var attempts int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT attempt_count FROM wallet_settlement_outbox WHERE id = $1`, e.ID).Scan(&attempts))
		require.Equal(t, 0, attempts, "§11.3: the split consumes no attempt on either row — the refusal that triggered it is not MarkOutboxEventFailed")
		var reason sql.NullString
		require.NoError(t, db.QueryRowContext(ctx, `SELECT dead_letter_reason FROM wallet_settlement_outbox WHERE id = $1`, e.ID).Scan(&reason))
		require.False(t, reason.Valid, "a retried row carries no dead-letter reason")
		// Phase 3.5 (§11.3, retargeted from 3.3a's never-installed rule): the
		// settle purpose INSTALLS the under-granted lease and the dispatcher
		// splits before reserving — the tiny lease exists, the row was split
		// (parent 100 units, a remainder carrying the rest), and the parent
		// is pending because this control plane has no settlements route at
		// all (a 404 — transient) rather than because of the under-grant.
		_, err = store.GetCanonicalWalletLeaseByID(ctx, platformUserID, "lease-tiny")
		require.NoError(t, err, "§11.3: the under-granted settle lease is installed")
		var parentAmount int64
		var remainderRows int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT amount_units FROM wallet_settlement_outbox WHERE id = $1`, e.ID).Scan(&parentAmount))
		require.Equal(t, int64(100), parentAmount, "the row was split to the lease's budget")
		require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM wallet_settlement_outbox WHERE parent_event_id = $1`, e.EventID).Scan(&remainderRows))
		require.Equal(t, 1, remainderRows, "the remainder row exists")
	})

	t.Run("settlement refused by control plane", func(t *testing.T) {
		db := startCanonicalWalletTestPostgres(t, ctx)
		rdb := startCanonicalWalletTestRedis(t, ctx)
		store := &gatewayCacheAdapterForTest{rdb: rdb}
		outbox := &outboxStoreForTest{db: db}
		platformUserID := "shipany-user-" + uuid.NewString()

		refused := false
		controlPlane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/api/internal/v2/wallet/leases/ensure":
				var req canonicalWalletEnsureRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				_, _ = w.Write([]byte(`{"data":{"lease_id":"lease-refuse","platform_user_id":"` + req.PlatformUserID + `","currency":"CNY","unit_version":"cny-e8-v1","scale":8,"budget":{"amount_units":"500000000","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"reserved":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"captured":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"released":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"headroom":{"amount_units":"500000000","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"capture_seq":0,"status":"active","expires_at":"2030-01-01T00:00:00Z","outcome":"issued","clamped_by":"none"}}`))
			case "/api/internal/v2/wallet/settlements":
				refused = true
				_, _ = w.Write([]byte(`{"data":{"accepted":false,"duplicate":false}}`))
			default:
				http.NotFound(w, r)
			}
		}))
		defer controlPlane.Close()
		cfg := canonicalWalletTestConfig(config.CanonicalWalletModeEnforce)
		cfg.ControlPlaneURL, cfg.Secret = controlPlane.URL, strings.Repeat("s", 32)
		client := newCanonicalWalletHTTPClient(cfg, controlPlane.Client())
		bridge := &CanonicalWalletBridge{cfg: cfg, store: store, control: client, outboxDB: db, outbox: outbox, workerID: "test-worker-submit-fail"}

		bridge.ObserveSettlement(CanonicalWalletSettlementEvent{
			GatewayRequestID: "req-submit-fail", PlatformUserID: platformUserID,
			Currency: "CNY", AmountUnits: 30_000000,
		})
		e := claimOnlyRow(t, ctx, outbox, bridge.workerID)

		bridge.deliverOutboxEvent(ctx, e)

		require.True(t, refused, "the settlement reached the control plane and was refused")
		status, err := outbox.OutboxEventStatus(ctx, e.ID)
		require.NoError(t, err)
		require.Equal(t, "pending", status, "a refused settlement returns the row to pending for retry")
	})
}

func TestEnsureCanonicalWalletHeadroomGuardsAndErrors(t *testing.T) {
	ctx := context.Background()

	// Nil receiver and disabled mode always allow — no panic, no reserve.
	var nilBridge *CanonicalWalletBridge
	ok, err := nilBridge.EnsureCanonicalWalletHeadroom(ctx, "r", "u", "l", "CNY", 1, 1)
	require.NoError(t, err)
	require.True(t, ok)

	disabled := &CanonicalWalletBridge{cfg: canonicalWalletTestConfig(config.CanonicalWalletModeDisabled)}
	ok, err = disabled.EnsureCanonicalWalletHeadroom(ctx, "r", "u", "l", "CNY", 1, 1)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = disabled.HasCanonicalWalletHeadroom(ctx, "u", "CNY")
	require.NoError(t, err)
	require.True(t, ok)

	// Enforce mode against a MISSING lease: a genuine store error (not the
	// ordinary exhaustion denial) propagates.
	rdb := startCanonicalWalletTestRedis(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	enforce := newCanonicalWalletBridge(canonicalWalletTestConfig(config.CanonicalWalletModeEnforce), store, &canonicalWalletControlStub{}, nil, nil, 0, nil)
	t.Cleanup(enforce.Close)
	ok, err = enforce.EnsureCanonicalWalletHeadroom(ctx, "req-g", "shipany-user-"+uuid.NewString(), "lease-missing", "CNY", 1, 100)
	require.ErrorIs(t, err, ErrCanonicalWalletLeaseMissing)
	require.False(t, ok)
}

func TestEnsureLeaseAndDoJSONRemainingRealPaths(t *testing.T) {
	ctx := context.Background()

	// Unreachable control plane -> real transport error from doJSON.
	cfg := canonicalWalletTestConfig(config.CanonicalWalletModeShadow)
	cfg.ControlPlaneURL, cfg.Secret = "http://127.0.0.1:1", strings.Repeat("s", 32)
	client := newCanonicalWalletHTTPClient(cfg, nil)
	_, err := client.EnsureLease(ctx, canonicalWalletEnsureRequest{
		PlatformUserID: "user-1", Currency: "CNY", Purpose: "authorize", MinHeadroom: newCanonicalWalletAmountObject(1), RequestedBudget: newCanonicalWalletAmountObject(1), RequestedTTLSeconds: 60, CallerSlotTTLSeconds: 1800,
	})
	require.Error(t, err)

	// Phase 3.4: the v2 lease wire is units-native — the x100 conversion the
	// old overflow leg exercised is gone (redesign §6). The same property in
	// the new wire: a wire figure beyond int64's range must be REJECTED (the
	// strict amount-object parser fails), never accepted or wrapped.
	overflowServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"lease_id":"l","platform_user_id":"user-1","currency":"CNY","unit_version":"cny-e8-v1","scale":8,"budget":{"amount_units":"92233720368547758080","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"reserved":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"captured":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"released":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"headroom":{"amount_units":"0","currency":"CNY","scale":8,"unit_version":"cny-e8-v1"},"expires_at":"2030-01-01T00:00:00Z","status":"active","outcome":"issued"}}`))
	}))
	defer overflowServer.Close()
	cfg2 := canonicalWalletTestConfig(config.CanonicalWalletModeShadow)
	cfg2.ControlPlaneURL, cfg2.Secret = overflowServer.URL, strings.Repeat("s", 32)
	client2 := newCanonicalWalletHTTPClient(cfg2, overflowServer.Client())
	_, err = client2.EnsureLease(ctx, canonicalWalletEnsureRequest{
		PlatformUserID: "user-1", Currency: "CNY", Purpose: "authorize", MinHeadroom: newCanonicalWalletAmountObject(1), RequestedBudget: newCanonicalWalletAmountObject(1), RequestedTTLSeconds: 60, CallerSlotTTLSeconds: 1800,
	})
	require.Error(t, err, "a wire budget beyond int64's range must be rejected, not accepted")

	badBody := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`not-json-at-all`))
	}))
	defer badBody.Close()
	cfg3 := canonicalWalletTestConfig(config.CanonicalWalletModeShadow)
	cfg3.ControlPlaneURL, cfg3.Secret = badBody.URL, strings.Repeat("s", 32)
	client3 := newCanonicalWalletHTTPClient(cfg3, badBody.Client())

	// Invalid JSON response body -> decode error from doJSON.
	var resp map[string]any
	err = client3.doJSON(ctx, http.MethodGet, "/x", canonicalWalletSettlementScope, "", nil, &resp)
	require.Error(t, err)

	// Unmarshalable request body -> encode error from doJSON.
	err = client3.doJSON(ctx, http.MethodGet, "/x", canonicalWalletSettlementScope, "", make(chan int), &resp)
	require.Error(t, err)

	// Invalid control-plane URL -> request-build error from doJSON.
	cfg4 := canonicalWalletTestConfig(config.CanonicalWalletModeShadow)
	cfg4.ControlPlaneURL, cfg4.Secret = "http://127.0.0.1:99999x", strings.Repeat("s", 32)
	client4 := newCanonicalWalletHTTPClient(cfg4, nil)
	err = client4.doJSON(ctx, http.MethodGet, "/x", canonicalWalletSettlementScope, "", map[string]string{"k": "v"}, &resp)
	require.Error(t, err)
}

func TestObserveSettlementAndCheckAndReserveGuardBranches(t *testing.T) {
	ctx := context.Background()
	db := startCanonicalWalletTestPostgres(t, ctx)
	outbox := &outboxStoreForTest{db: db}

	// Disabled mode and zero/negative amounts return before any effect.
	disabled := &CanonicalWalletBridge{cfg: canonicalWalletTestConfig(config.CanonicalWalletModeDisabled), outboxDB: db, outbox: outbox}
	disabled.ObserveSettlement(CanonicalWalletSettlementEvent{GatewayRequestID: "r", PlatformUserID: "u", Currency: "CNY", AmountUnits: 1})
	enabled := &CanonicalWalletBridge{cfg: canonicalWalletTestConfig(config.CanonicalWalletModeEnforce), store: &canonicalWalletStoreStub{}, control: &canonicalWalletControlStub{}, outboxDB: db, outbox: outbox, workerID: "w-guards"}
	enabled.ObserveSettlement(CanonicalWalletSettlementEvent{GatewayRequestID: "r", PlatformUserID: "u", Currency: "CNY", AmountUnits: 0})
	enabled.ObserveSettlement(CanonicalWalletSettlementEvent{GatewayRequestID: "r", PlatformUserID: "u", Currency: "CNY", AmountUnits: -5})

	// A blank platform user id is counted and dropped before any write.
	beforeMissing := CanonicalWalletBridgeStats()["missing_platform_user_id"]
	enabled.ObserveSettlement(CanonicalWalletSettlementEvent{GatewayRequestID: "r", PlatformUserID: "   ", Currency: "CNY", AmountUnits: 1})
	require.Greater(t, CanonicalWalletBridgeStats()["missing_platform_user_id"], beforeMissing)

	// An outbox without its DB half is refused at the guard.
	halfWired := &CanonicalWalletBridge{cfg: canonicalWalletTestConfig(config.CanonicalWalletModeEnforce), outbox: outbox}
	halfWired.ObserveSettlement(CanonicalWalletSettlementEvent{GatewayRequestID: "r", PlatformUserID: "u", Currency: "CNY", AmountUnits: 1})

	// Nothing above may have written a row.
	rows, err := outbox.ClaimPendingOutboxEvents(ctx, "guard-probe", 10)
	require.NoError(t, err)
	require.Empty(t, rows)

	// CheckAndReserve on a nil bridge / disabled mode allows without touching stores.
	var nilBridge *CanonicalWalletBridge
	allowed, err := nilBridge.CheckAndReserve(ctx, CanonicalWalletSettlementEvent{})
	require.NoError(t, err)
	require.True(t, allowed)
	allowed, err = disabled.CheckAndReserve(ctx, CanonicalWalletSettlementEvent{})
	require.NoError(t, err)
	require.True(t, allowed)

	// BeginTx failure against a genuinely closed database.
	closedDB := startCanonicalWalletTestPostgres(t, ctx)
	closedOutbox := &outboxStoreForTest{db: closedDB}
	bridgeClosed := &CanonicalWalletBridge{
		cfg: canonicalWalletTestConfig(config.CanonicalWalletModeEnforce), store: &canonicalWalletStoreStub{},
		control: &canonicalWalletControlStub{}, outboxDB: closedDB, outbox: closedOutbox, workerID: "w-closed",
	}
	require.NoError(t, closedDB.Close())
	beforeDropped := CanonicalWalletBridgeStats()["queue_dropped"]
	bridgeClosed.ObserveSettlement(CanonicalWalletSettlementEvent{
		GatewayRequestID: "req-closed-db", PlatformUserID: "shipany-user-closed", Currency: "CNY", AmountUnits: 1,
	})
	require.Greater(t, CanonicalWalletBridgeStats()["queue_dropped"], beforeDropped,
		"a failed BEGIN is counted in queue_dropped like every other durability loss")
}
