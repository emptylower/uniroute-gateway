//go:build integration

package service

import (
	"context"
	"database/sql"
	"math"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

// TestObserveSettlementCarriesSnapshot (Phase 4.2-G Task 1b): every surface
// that holds a billing snapshot at settlement time lands it on the outbox
// row's own column. Task 0 established the scope: the id is in reach at BOTH
// HTTP/WS callers (`input.BillingSnapshot`, frozen at FreezeBillingSnapshot
// in the authorize path and threaded through OpenAIRecordUsageInput / the
// generic gateway's record-usage input — openai_gateway_usage.go:315/404,
// gateway_usage_billing.go:816/835/889) and at both Live ObserveSettlement
// call sites (record/fresh.BillingSnapshotID). The HTTP/WS leg therefore
// drives the SHARED write path both callers invoke —
// observeCanonicalWalletSettlement — exactly as they call it; the Live legs
// drive the REAL Live machinery (the 3.7b fixture) through the window
// settlement (openai_live.go:1227) and the finalization remainder
// (openai_live.go:1602).
func TestObserveSettlementCarriesSnapshot(t *testing.T) {
	ctx := context.Background()
	db := startCanonicalWalletTestPostgres(t, ctx)
	outbox := &outboxStoreForTest{db: db}
	// The struct literal, not the constructor: this test verifies the
	// synchronous durable write only (the dispatcher would race the row's
	// status) — the same construction as
	// TestCanonicalWalletObserveSettlementIsDurableAndGetsDelivered.
	bridge := &CanonicalWalletBridge{
		cfg:      canonicalWalletTestConfig(config.CanonicalWalletModeEnforce),
		store:    &canonicalWalletStoreStub{},
		control:  &canonicalWalletControlStub{},
		outboxDB: db,
		outbox:   outbox,
		workerID: "test-worker-no-dispatcher",
	}
	user := &User{PlatformUserID: "shipany-user-76d", BillingCurrency: "CNY", Balance: 10}
	cost := &CostBreakdown{ActualCost: 0.05, BillingMode: string(BillingModeToken)}

	// The HTTP/WS leg: called with the snapshot id the callers hold
	// (input.BillingSnapshot.ID), the row carries it.
	require.True(t, observeCanonicalWalletSettlement(bridge, "req-76d-http", user, cost, false, true, nil, "tok-76d", "auth-76d", "wbs_76d_http"))
	var snapID sql.NullString
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT billing_snapshot_id FROM wallet_settlement_outbox WHERE gateway_request_id = $1`, "req-76d-http").Scan(&snapID))
	require.True(t, snapID.Valid, "the HTTP/WS caller's snapshot id lands on the outbox row")
	require.Equal(t, "wbs_76d_http", snapID.String)

	// The nil-snapshot caller (Count Tokens, off mode, pre-3.2 callers)
	// writes NULL — the callers pass "" and the column stays NULL.
	require.True(t, observeCanonicalWalletSettlement(bridge, "req-76d-plain", user, cost, false, true, nil, "", "", ""))
	var plainID sql.NullString
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT billing_snapshot_id FROM wallet_settlement_outbox WHERE gateway_request_id = $1`, "req-76d-plain").Scan(&plainID))
	require.False(t, plainID.Valid, "a caller holding no snapshot writes NULL, never a fabricated id")
}

// TestObserveSettlementCarriesSnapshotLive covers the two Live
// ObserveSettlement call sites through the real machinery: window 1's
// settlement (openai_live.go:1227 — the fresh record's snapshot id) and the
// last window's remainder at finalization (openai_live.go:1602 — the record
// argument's snapshot id).
func TestObserveSettlementCarriesSnapshotLive(t *testing.T) {
	f := newLiveWindowTestFixture(t, config.CanonicalWalletModeEnforce)
	callHash, _ := f.createSession(t)
	prov := f.provRecord(t, callHash)
	require.NotEmpty(t, prov.BillingSnapshotID, "the live provisional record carries its frozen snapshot id")

	// Window 1 settles (usage past its estimate, past the floor).
	E := prov.EstimatedUnits
	tokens := int(math.Ceil(2 * float64(E) / f.unitsPerOutputToken(t, callHash)))
	f.pumpUsage("resp-76d-1", 0, tokens)
	prov = f.pollWindowsLen(t, callHash, 2, 8*time.Second)

	id1 := CanonicalWalletSettlementEventID(callHash, f.user.PlatformUserID, "CNY")
	f.waitDeliveredThroughFake(t, id1, 15*time.Second)
	var snap1 sql.NullString
	require.NoError(t, f.db.QueryRowContext(f.ctx,
		`SELECT billing_snapshot_id FROM wallet_settlement_outbox WHERE event_id = $1`, id1).Scan(&snap1))
	require.True(t, snap1.Valid, "window 1's outbox row carries the record's snapshot id")
	require.Equal(t, prov.BillingSnapshotID, snap1.String)

	// The finalization remainder: usage after window 2's close, then
	// session.closed — the last window's settlement (:window:3) is built at
	// openai_live.go:1602 with record.BillingSnapshotID in scope.
	f.pumpUsage("resp-76d-2", 0, tokens)
	prov = f.pollWindowsLen(t, callHash, 3, 8*time.Second)
	tailTokens := 1000
	f.pumpUsage("resp-76d-tail", 0, tailTokens)
	f.conn.reads <- liveTestFrame{messageType: coderws.MessageText, payload: []byte(`{"type":"session.closed"}`)}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		prov = f.provRecord(t, callHash)
		if prov.Status == LiveProvisionalStatusFinalized {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.Equal(t, LiveProvisionalStatusFinalized, prov.Status, "the row finalizes after session.closed")

	id3 := CanonicalWalletSettlementEventID(liveWindowRequestID(callHash, 3), f.user.PlatformUserID, "CNY")
	f.waitDeliveredThroughFake(t, id3, 15*time.Second)
	var snap3 sql.NullString
	require.NoError(t, f.db.QueryRowContext(f.ctx,
		`SELECT billing_snapshot_id FROM wallet_settlement_outbox WHERE event_id = $1`, id3).Scan(&snap3))
	require.True(t, snap3.Valid, "the finalization remainder's outbox row carries the record's snapshot id")
	require.Equal(t, prov.BillingSnapshotID, snap3.String)
}
