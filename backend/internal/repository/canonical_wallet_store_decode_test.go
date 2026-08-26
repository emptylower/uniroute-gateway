package repository

import (
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

// The reservation Lua script's return shape is a contract, and every branch
// below is this decoder refusing to trust it. These cases are unreachable
// through a real Redis running the shipped script — which is exactly why the
// decoding lives in a pure function over the raw []any instead of inside the
// I/O call: it can be tested with plain Go values, with no driver to fake.
func TestDecodeCanonicalWalletReservationResultRejectsMalformedShapes(t *testing.T) {
	const user = "shipany-user-decode"

	_, err := decodeCanonicalWalletReservationResult(nil, user)
	require.EqualError(t, err, "canonical wallet reservation returned no result")

	_, err = decodeCanonicalWalletReservationResult([]any{struct{}{}}, user)
	require.ErrorContains(t, err, "unexpected Redis integer type")

	_, err = decodeCanonicalWalletReservationResult([]any{int64(0), "lease-1"}, user)
	require.EqualError(t, err, "canonical wallet reservation returned an invalid snapshot")

	_, err = decodeCanonicalWalletReservationResult([]any{int64(0), "lease-1", "CNY", struct{}{}, int64(0), int64(1)}, user)
	require.ErrorContains(t, err, "unexpected Redis integer type", "a malformed budget field must be reported, not silently zeroed")

	_, err = decodeCanonicalWalletReservationResult([]any{int64(0), "lease-1", "CNY", int64(10), struct{}{}, int64(1)}, user)
	require.ErrorContains(t, err, "unexpected Redis integer type", "a malformed consumed field must be reported, not silently zeroed")

	_, err = decodeCanonicalWalletReservationResult([]any{int64(0), "lease-1", "CNY", int64(10), int64(1), struct{}{}}, user)
	require.ErrorContains(t, err, "unexpected Redis integer type", "a malformed expiry field must be reported, not silently zeroed")

	_, err = decodeCanonicalWalletReservationResult([]any{int64(9)}, user)
	require.EqualError(t, err, "unknown canonical wallet reservation code 9")
}

func TestDecodeCanonicalWalletReservationResultMapsEveryScriptCode(t *testing.T) {
	const user = "shipany-user-decode"
	for code, want := range map[int64]error{
		1: service.ErrCanonicalWalletLeaseMissing,
		2: service.ErrCanonicalWalletLeaseCurrencyMismatch,
		3: service.ErrCanonicalWalletLeaseExpired,
		4: service.ErrCanonicalWalletLeaseExhausted,
		6: service.ErrCanonicalWalletReservationConflict,
	} {
		_, err := decodeCanonicalWalletReservationResult([]any{code}, user)
		require.ErrorIs(t, err, want, "script code %d", code)
	}
}

// Code 0 is a fresh reservation, code 5 the same event reserving twice
// against the same lease — both carry the full snapshot, and only Duplicate
// distinguishes them.
func TestDecodeCanonicalWalletReservationResultCarriesTheSnapshot(t *testing.T) {
	const user = "shipany-user-decode"
	snapshot := func(code int64) []any {
		return []any{code, "lease-7", "CNY", int64(1_000), int64(600), int64(1893456000000)}
	}

	fresh, err := decodeCanonicalWalletReservationResult(snapshot(0), user)
	require.NoError(t, err)
	require.False(t, fresh.Duplicate)
	require.Equal(t, "lease-7", fresh.Lease.LeaseID)
	require.Equal(t, user, fresh.Lease.PlatformUserID)
	require.Equal(t, "CNY", fresh.Lease.Currency)
	require.Equal(t, int64(1_000), fresh.Lease.BudgetUnits)
	require.Equal(t, int64(600), fresh.Lease.ConsumedUnits)
	require.Equal(t, int64(1893456000000), fresh.Lease.ExpiresAt.UnixMilli())

	duplicate, err := decodeCanonicalWalletReservationResult(snapshot(5), user)
	require.NoError(t, err)
	require.True(t, duplicate.Duplicate)
	require.Equal(t, int64(600), duplicate.Lease.ConsumedUnits)
}
