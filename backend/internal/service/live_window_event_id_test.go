//go:build unit

package service

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestLiveWindowSettlementEventID pins the derivation redesign §15.3 leg 3
// specifies (3.8-G deviation 8 / §15 round-1 MINOR-1): the Live window
// settlement event id is DERIVED, never stored — liveWindowRequestID maps
// window 1 to the session's bare call hash and window N ≥ 2 to
// "<callHash>:window:N", then CanonicalWalletSettlementEventID hashes it.
// The reconciliation (4.1-S) recomputes exactly this; if the derivation
// drifts, every window's leg-2 match silently breaks, so this test is the
// contract both sides compile against.
func TestLiveWindowSettlementEventID(t *testing.T) {
	const (
		callHash = "live_callhash_abc123"
		user     = "shipany-user-42"
		cur      = "CNY"
	)

	// Window 1: the bare call hash — NOT "<callHash>:window:1" (a session
	// in flight across the 3.7b deploy cannot double-settle window 1).
	require.Equal(t,
		CanonicalWalletSettlementEventID(callHash, user, cur),
		LiveWindowSettlementEventID(callHash, 1, user, cur),
		"window 1's gateway request id is the bare call hash",
	)
	require.NotEqual(t,
		CanonicalWalletSettlementEventID(callHash+":window:1", user, cur),
		LiveWindowSettlementEventID(callHash, 1, user, cur),
		"window 1 must not hash the :window:1 form",
	)

	// Window 2: "<callHash>:window:2".
	require.Equal(t,
		CanonicalWalletSettlementEventID(callHash+":window:2", user, cur),
		LiveWindowSettlementEventID(callHash, 2, user, cur),
		"window 2's gateway request id is <callHash>:window:2",
	)

	// Every seq maps through the same single function, so one more spot
	// check at a deeper window is enough to pin the general shape.
	require.Equal(t,
		CanonicalWalletSettlementEventID(callHash+":window:8", user, cur),
		LiveWindowSettlementEventID(callHash, 8, user, cur),
	)
}
