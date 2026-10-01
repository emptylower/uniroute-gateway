package service

import "context"

// Optional extension of the shared Redis store. Ordinary callers and their
// test doubles keep the existing canonical wallet interface unchanged.
type MediaWalletStateStore interface {
	ProtectMediaHold(context.Context, CanonicalWalletLease, CanonicalWalletHold) error
	UnprotectMediaLease(context.Context, string, string, string, string, bool) error
	ReadMediaWalletState(context.Context, string, []string, []string) (MediaWalletRawState, error)
}
type MediaWalletRawLease struct {
	LeaseID       string `json:"lease_id"`
	BudgetUnits   int64  `json:"budget_units"`
	ConsumedUnits int64  `json:"consumed_units"`
	ReleasedUnits int64  `json:"released_units"`
	ExpiresAtMS   int64  `json:"expires_at_ms"`
	Sealed        bool   `json:"sealed"`
	Recovered     bool   `json:"recovered"`
	Present       bool   `json:"present"`
}
type MediaWalletRawState struct {
	Leases       []MediaWalletRawLease
	Holds        []CanonicalWalletHold
	Reservations map[string]string
	Fingerprint  string
}
