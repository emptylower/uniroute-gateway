package service

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddUnitsRejectsOverflow(t *testing.T) {
	_, err := AddUnits(math.MaxInt64-1, 2)
	require.ErrorIs(t, err, ErrCanonicalWalletUnitsOverflow)

	sum, err := AddUnits(100_00000000, 50_00000000) // 100 CNY + 50 CNY
	require.NoError(t, err)
	require.Equal(t, int64(150_00000000), sum)
}

func TestAddUnitsRejectsNegativeOperands(t *testing.T) {
	_, err := AddUnits(-1, 100)
	require.ErrorIs(t, err, ErrCanonicalWalletUnitsNegative)
	_, err = AddUnits(100, -1)
	require.ErrorIs(t, err, ErrCanonicalWalletUnitsNegative)
}

func TestSubUnitsRejectsNegativeResult(t *testing.T) {
	_, err := SubUnits(100, 101)
	require.ErrorIs(t, err, ErrCanonicalWalletUnitsNegative)

	diff, err := SubUnits(100_00000000, 40_00000000)
	require.NoError(t, err)
	require.Equal(t, int64(60_00000000), diff)
}

func TestSubUnitsRejectsNegativeOperands(t *testing.T) {
	_, err := SubUnits(-5, 1)
	require.ErrorIs(t, err, ErrCanonicalWalletUnitsNegative)
	_, err = SubUnits(5, -1)
	require.ErrorIs(t, err, ErrCanonicalWalletUnitsNegative)
}

func TestMulUnitsRejectsOverflow(t *testing.T) {
	_, err := MulUnits(math.MaxInt64/2+1, 2)
	require.ErrorIs(t, err, ErrCanonicalWalletUnitsOverflow)

	product, err := MulUnits(3, 100_00000000) // 3 requests at 100 CNY each
	require.NoError(t, err)
	require.Equal(t, int64(300_00000000), product)
}

func TestMulUnitsRejectsNegativeOperands(t *testing.T) {
	// The protocol domain has no legitimate use for a negative amount or a
	// negative factor (there is no such thing as "negative tokens" or
	// "negative CNY"), so both are rejected outright — MulUnits(math.MinInt64, -1)
	// would silently wrap instead of erroring without the guard.
	_, err := MulUnits(-1, 2)
	require.ErrorIs(t, err, ErrCanonicalWalletUnitsNegative)
	_, err = MulUnits(2, -1)
	require.ErrorIs(t, err, ErrCanonicalWalletUnitsNegative)
	_, err = MulUnits(math.MinInt64, -1)
	require.ErrorIs(t, err, ErrCanonicalWalletUnitsNegative)
}

func TestCreditsToUnitsMatchesFrozenConversion(t *testing.T) {
	// 1 ShipAny credit = 0.01 CNY = 1,000,000 units at cny-e8-v1 scale.
	units, err := CreditsToUnits(1)
	require.NoError(t, err)
	require.Equal(t, int64(1_000_000), units)

	units, err = CreditsToUnits(100)
	require.NoError(t, err)
	require.Equal(t, int64(100_000_000), units)
}

func TestCreditsToUnitsRejectsOverflowAndNegative(t *testing.T) {
	_, err := CreditsToUnits(-1)
	require.ErrorIs(t, err, ErrCanonicalWalletUnitsNegative)
	_, err = CreditsToUnits(math.MaxInt64)
	require.ErrorIs(t, err, ErrCanonicalWalletUnitsOverflow)
}
