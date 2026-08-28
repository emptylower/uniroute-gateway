package service

import (
	"errors"
	"fmt"
	"net/http"
)

// ErrAuthorizationRefused is the sentinel every *AuthorizationRefusedError matches
// through errors.Is. It is TERMINAL: no forwarder loop retries it, no handler
// converts it into an UpstreamFailoverError, and the WS turn classifier reports
// it non-retryable (spec §2.0 "a refusal must be a distinct, terminal,
// client-visible error").
var ErrAuthorizationRefused = errors.New("wallet authorization refused")

type AuthorizationRefusalReason string

const (
	AuthorizationRefusalUnmarkedWrite        AuthorizationRefusalReason = "unmarked_write"   // neither a handle nor a non-billable mark (spec §2.0 default-billable)
	AuthorizationRefusalSnapshotMissing      AuthorizationRefusalReason = "snapshot_missing" // 3.2 froze nothing (off mode, or a record-mode freeze failure)
	AuthorizationRefusalEstimateFailed       AuthorizationRefusalReason = "estimate_failed"  // EstimateUpperBoundUnits errored (e.g. unbounded output with no model maximum — 3.7's top-up bounds it)
	AuthorizationRefusalIdentityMissing      AuthorizationRefusalReason = "identity_missing" // no platform user id — the same rule ObserveSettlement applies
	AuthorizationRefusalCurrency             AuthorizationRefusalReason = "currency_unsupported"
	AuthorizationRefusalBalanceShortfall     AuthorizationRefusalReason = "balance_shortfall" // ensureLease: the granted lease is below the amount (spec §2.0.1 step (3))
	AuthorizationRefusalLeaseUnavailable     AuthorizationRefusalReason = "lease_unavailable" // ensureLease: any other failure (control plane, store, expired grant)
	AuthorizationRefusalLiveStoreUnavailable AuthorizationRefusalReason = "live_provisional_store_unavailable"
)

// AuthorizationRefusedError is the one client-visible refusal shape.
type AuthorizationRefusedError struct {
	Reason          AuthorizationRefusalReason
	AuthorizationID string
	Token           string
	Detail          string
	Cause           error
}

func (e *AuthorizationRefusedError) Error() string {
	if e == nil {
		return ErrAuthorizationRefused.Error()
	}
	msg := fmt.Sprintf("%s: %s", ErrAuthorizationRefused.Error(), e.Reason)
	if e.AuthorizationID != "" {
		msg += " authorization=" + e.AuthorizationID
	}
	if e.Detail != "" {
		msg += " (" + e.Detail + ")"
	}
	if e.Cause != nil {
		msg += ": " + e.Cause.Error()
	}
	return msg
}

func (e *AuthorizationRefusedError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func (e *AuthorizationRefusedError) Is(target error) bool { return target == ErrAuthorizationRefused }

// AsAuthorizationRefused is the short-circuit every billable Do-error branch runs
// first (Task 8). It never matches an UpstreamFailoverError.
func AsAuthorizationRefused(err error) (*AuthorizationRefusedError, bool) {
	if err == nil {
		return nil, false
	}
	var refused *AuthorizationRefusedError
	if errors.As(err, &refused) && refused != nil {
		return refused, true
	}
	return nil, false
}

// The client-visible shape. HTTP: 402 with the error type below. WebSocket (3.3b):
// close status 4402 with the same reason string.
const (
	AuthorizationRefusedHTTPStatus    = http.StatusPaymentRequired
	AuthorizationRefusedErrorType     = "wallet_authorization_refused"
	AuthorizationRefusedMessage       = "Wallet authorization refused"
	AuthorizationRefusedWSCloseStatus = 4402
	AuthorizationRefusedWSCloseReason = "wallet_authorization_refused"
)
