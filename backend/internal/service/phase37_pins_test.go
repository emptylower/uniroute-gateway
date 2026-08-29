//go:build integration

// Phase 3.7a pins (redesign §13.3): the stoppable dispatcher's own test
// (§13.2.6) and test 48 (G7) live here; test 46 (the streaming differential
// invariant) is added below by Task 5, and test 47 lives beside its harness
// in openai_ws_v2_passthrough_authorization_test.go under that file's tag.

package service

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// countingOutbox wraps an outbox store and counts ClaimPendingOutboxEvents
// calls — the observable heartbeat of the dispatcher's tick loop.
type countingOutbox struct {
	CanonicalWalletOutboxStore
	claims atomic.Int64
}

func (c *countingOutbox) ClaimPendingOutboxEvents(ctx context.Context, workerID string, limit int) ([]CanonicalWalletOutboxEvent, error) {
	c.claims.Add(1)
	return c.CanonicalWalletOutboxStore.ClaimPendingOutboxEvents(ctx, workerID, limit)
}

// TestPhase37DispatcherStopsOnClose (redesign §13.2.6): Close stops BOTH tick
// loops — the dispatcher's claim counter and the reaper's reaper_ticks are
// unchanged across a ten-tick window after Close returns, and Close is
// idempotent and nil-safe. Close has no production caller; this test is the
// facility's contract for every t.Cleanup(b.Close) the suite registers.
func TestPhase37DispatcherStopsOnClose(t *testing.T) {
	ctx := context.Background()
	rdb := startCanonicalWalletTestRedis(t, ctx)
	db := startCanonicalWalletTestPostgres(t, ctx)
	store := &gatewayCacheAdapterForTest{rdb: rdb}
	counting := &countingOutbox{CanonicalWalletOutboxStore: &outboxStoreForTest{db: db}}
	now := time.Now().UTC()
	fake := newFakeEnsureControlPlane(t, func() time.Time { return now })
	user := "shipany-user-" + uuid.NewString()
	fake.fund(user, 10_000_000_000)

	cfg := canonicalWalletTestConfig(config.CanonicalWalletModeEnforce)
	cfg.ControlPlaneURL, cfg.Secret = fake.Server.URL, strings.Repeat("s", 32)
	cfg.ExpirySkewMarginMS, cfg.RequestTimeoutMS = 50, 20 // a 20 ms tick
	cfg.LeaseBudgetUnits = 500_000_000
	client := newCanonicalWalletHTTPClient(cfg, fake.Server.Client())
	client.now = func() time.Time { return now }
	b := newCanonicalWalletBridge(cfg, store, client, db, counting, 0, func() time.Time { return now })
	t.Cleanup(b.Close)

	// The loop is live: poll (the file's idiom — 50 ms steps, bounded
	// deadline) until the claim counter reaches three ticks.
	deadline := time.Now().Add(30 * time.Second)
	for counting.claims.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	require.GreaterOrEqual(t, counting.claims.Load(), int64(3), "the dispatcher loop is live")

	// Close blocks until both loops have exited — it must return well inside
	// the two-second bound even with a delivery in flight.
	closed := make(chan struct{})
	go func() {
		b.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return within 2 s")
	}

	// The ONE permitted wait in this plan (constraint 4): the absence of
	// further claims cannot be polled for — ten ticks of the 20 ms loop.
	claimsAtClose := counting.claims.Load()
	<-time.After(200 * time.Millisecond)
	require.Equal(t, claimsAtClose, counting.claims.Load(), "no further claim after Close")

	// Idempotent: a second Close returns immediately.
	b.Close()
	// Nil-safe: a bare nil bridge is a no-op, not a panic.
	var nilBridge *CanonicalWalletBridge
	nilBridge.Close()

	// Leg 2 — the reaper loop stops too: holds on (p34bHoldsConfig) and a
	// non-nil outboxDB start runHoldReaper alongside the dispatcher; the
	// global reaper_ticks counter (incremented once per tick before
	// reapOnce) is its heartbeat, asserted as a delta on the global.
	holdCfg := p34bHoldsConfig(config.CanonicalWalletModeEnforce)
	holdCfg.ControlPlaneURL, holdCfg.Secret = fake.Server.URL, strings.Repeat("s", 32)
	holdCfg.ExpirySkewMarginMS, holdCfg.RequestTimeoutMS = 50, 20
	holdCfg.LeaseBudgetUnits = 500_000_000
	holdCfg.OrphanSweepIntervalSeconds = 1
	holdClient := newCanonicalWalletHTTPClient(holdCfg, fake.Server.Client())
	holdClient.now = func() time.Time { return now }
	reaper := newCanonicalWalletBridge(holdCfg, store, holdClient, db, counting, 0, func() time.Time { return now })
	t.Cleanup(reaper.Close)

	ticksBase := canonicalWalletBridgeMetrics.reaperTicks.Load()
	deadline = time.Now().Add(30 * time.Second)
	for canonicalWalletBridgeMetrics.reaperTicks.Load() == ticksBase && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	require.Greater(t, canonicalWalletBridgeMetrics.reaperTicks.Load(), ticksBase, "the reaper loop is live")
	reaper.Close()
	ticksAtClose := canonicalWalletBridgeMetrics.reaperTicks.Load()
	<-time.After(200 * time.Millisecond)
	require.Equal(t, ticksAtClose, canonicalWalletBridgeMetrics.reaperTicks.Load(), "reapOnce is never entered again after Close")
}
