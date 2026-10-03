package service

import (
	"errors"
	"math"
)

// usd-e8-v1: 1 USD = 100,000,000 integer units. See
// docs/superpowers/plans/2026-08-25-wallet-lease-phase-0-protocol.md.
const CanonicalWalletUnitVersion = "usd-e8-v1"
const canonicalWalletUnitsPerUSD = 100_000_000

var (
	ErrCanonicalWalletUnitsOverflow = errors.New("usd-e8-v1 amount overflows int64")
	ErrCanonicalWalletUnitsNegative = errors.New("usd-e8-v1 amount would go negative or accepts a negative operand — the protocol domain has no negative amounts")
)

// AddUnits adds two usd-e8-v1 amounts, rejecting int64 overflow and any
// negative operand — the protocol domain (Phase 0) never has a negative
// amount, so accepting one here would only hide a bug upstream.
func AddUnits(a, b int64) (int64, error) {
	if a < 0 || b < 0 {
		return 0, ErrCanonicalWalletUnitsNegative
	}
	if b > math.MaxInt64-a {
		return 0, ErrCanonicalWalletUnitsOverflow
	}
	return a + b, nil
}

// SubUnits subtracts b from a, rejecting a negative result or a negative
// operand — usd-e8-v1 amounts are never negative anywhere in the wallet
// lease protocol.
func SubUnits(a, b int64) (int64, error) {
	if a < 0 || b < 0 {
		return 0, ErrCanonicalWalletUnitsNegative
	}
	if b > a {
		return 0, ErrCanonicalWalletUnitsNegative
	}
	return a - b, nil
}

// MulUnits multiplies a usd-e8-v1 amount by a non-negative integer factor
// (e.g. a per-unit price by a token count), rejecting int64 overflow and
// any negative operand. `MulUnits(math.MinInt64, -1)` silently wraps without
// the negative guard — since this protocol has no legitimate use for a
// negative amount or a negative multiplier, both are rejected outright
// before any arithmetic is attempted.
func MulUnits(a int64, factor int64) (int64, error) {
	if a < 0 || factor < 0 {
		return 0, ErrCanonicalWalletUnitsNegative
	}
	if a == 0 || factor == 0 {
		return 0, nil
	}
	if a > math.MaxInt64/factor {
		return 0, ErrCanonicalWalletUnitsOverflow
	}
	return a * factor, nil
}
