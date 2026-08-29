//go:build integration

package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Phase 3.7b (redesign §13.2.5): the claim script's observer branch on real
// Redis — SaveLiveCall seeds controller_heartbeat_ms = 0; an observer claim
// succeeds from pending only (unchanged semantics); TakeOverLiveObserver is
// refused while the heartbeat is fresh, succeeds past it, and rewrites the
// owner; the displaced owner's heartbeat is a no-op; a closed record refuses
// takeover.
func TestLiveControllerHeartbeatAndTakeover(t *testing.T) {
	ctx := context.Background()
	store := NewGatewayCache(integrationRedis).(service.LiveCallStore)

	newRecord := func() string {
		t.Helper()
		callHash := HashLiveCallID("call_" + uuid.NewString())
		record := &service.LiveCallRecord{
			CallID:     "call_" + uuid.NewString(),
			CallHash:   callHash,
			LeaseID:    "lease-1",
			CreatedAt:  time.Now().UTC(),
			ExpiresAt:  time.Now().UTC().Add(time.Hour),
			Controller: service.LiveControllerPending,
		}
		require.NoError(t, store.SaveLiveCall(ctx, record, time.Hour))
		return callHash
	}

	// SaveLiveCall seeds heartbeat 0 — absent counts as stale for takeover.
	hash1 := newRecord()
	state, err := store.GetLiveControllerState(ctx, hash1)
	require.NoError(t, err)
	require.Equal(t, service.LiveControllerPending, state.Controller)
	require.True(t, state.HeartbeatAt.IsZero(), "controller_heartbeat_ms must be 0 right after SaveLiveCall")

	// ClaimLiveController(observer) from pending succeeds — unchanged.
	claimed, err := store.ClaimLiveController(ctx, hash1, service.LiveControllerObserver, "owner-1")
	require.NoError(t, err)
	require.True(t, claimed)

	// ClaimLiveController(observer) while already observer is STILL refused
	// (ARGV stale_before_ms = 0 on the plain claim path).
	claimed, err = store.ClaimLiveController(ctx, hash1, service.LiveControllerObserver, "owner-1b")
	require.NoError(t, err)
	require.False(t, claimed, "a plain observer claim is granted only from pending")

	// A fresh heartbeat refuses the takeover.
	now := time.Now().UTC()
	require.NoError(t, store.HeartbeatLiveController(ctx, hash1, "owner-1", now))
	took, err := store.TakeOverLiveObserver(ctx, hash1, "owner-2", now)
	require.NoError(t, err)
	require.False(t, took, "a takeover at stale_before == the heartbeat instant must be refused (heartbeat not older)")

	// staleBefore later than the heartbeat succeeds and rewrites the owner.
	took, err = store.TakeOverLiveObserver(ctx, hash1, "owner-2", now.Add(2*time.Second))
	require.NoError(t, err)
	require.True(t, took)
	state, err = store.GetLiveControllerState(ctx, hash1)
	require.NoError(t, err)
	require.Equal(t, service.LiveControllerObserver, state.Controller)
	require.Equal(t, "owner-2", state.Owner)

	// The displaced owner's heartbeat is a no-op — no error, nothing written.
	require.NoError(t, store.HeartbeatLiveController(ctx, hash1, "owner-1", now.Add(3*time.Second)))
	state2, err := store.GetLiveControllerState(ctx, hash1)
	require.NoError(t, err)
	require.WithinDuration(t, state.HeartbeatAt, state2.HeartbeatAt, time.Millisecond, "owner-1's heartbeat after the takeover must not move")

	// A closed record refuses takeover.
	hash2 := newRecord()
	claimed, err = store.ClaimLiveController(ctx, hash2, service.LiveControllerObserver, "owner-3")
	require.NoError(t, err)
	require.True(t, claimed)
	require.NoError(t, store.HeartbeatLiveController(ctx, hash2, "owner-3", now))
	first, err := store.MarkLiveCallClosed(ctx, hash2, time.Hour)
	require.NoError(t, err)
	require.True(t, first)
	took, err = store.TakeOverLiveObserver(ctx, hash2, "owner-4", now.Add(time.Hour))
	require.NoError(t, err)
	require.False(t, took, "takeover on a closed record is refused")

	// A takeover with a heartbeat of 0 (never heartbeated observer) is stale.
	hash3 := newRecord()
	claimed, err = store.ClaimLiveController(ctx, hash3, service.LiveControllerObserver, "owner-5")
	require.NoError(t, err)
	require.True(t, claimed)
	took, err = store.TakeOverLiveObserver(ctx, hash3, "owner-6", now)
	require.NoError(t, err)
	require.True(t, took, "an observer that never heartbeated (ms = 0) counts as stale")
}
