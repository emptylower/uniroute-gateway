package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"time"
)

const WalletImmediateReleasePolicyVersion = "wallet-immediate-v5"
const WalletUnknownReleaseSoftLimitUnits int64 = 500000000

// WalletRiskRefusedError rejects new work without implying a balance shortfall.
type WalletRiskRefusedError struct {
	Status     int
	RetryAfter int // seconds, measured from primary-database time
	Cause      error
}

func (e *WalletRiskRefusedError) Error() string {
	if e.Status == http.StatusTooManyRequests {
		return "wallet unknown-release risk soft limit reached"
	}
	return "wallet unknown-release risk check unavailable"
}
func (e *WalletRiskRefusedError) Unwrap() error { return e.Cause }

func WalletRiskRefusalDetails(err error) (status, retryAfter int, ok bool) {
	var refused *WalletRiskRefusedError
	if !errors.As(err, &refused) || refused == nil {
		return 0, 0, false
	}
	return refused.Status, refused.RetryAfter, true
}

type WalletRiskAdmission struct {
	ParentAuthorizationID string
	PlatformUserID        string
	BillingSnapshotID     string
	Kind                  string
}

func (b *CanonicalWalletBridge) ImmediateWalletReleaseMode(kind string) string {
	if b == nil {
		return "off"
	}
	mode := "off"
	if kind == "llm" {
		mode = b.cfg.LLMImmediateReleaseMode
	}
	if kind == "media" {
		mode = b.cfg.MediaImmediateReleaseMode
	}
	if mode == "shadow" || mode == "enabled" {
		return mode
	}
	return "off"
}

// The first LLM cutover also gates new media before any hold, pin or POST.
// Existing admissions, polling and settlement recovery do not call the gate.
func (b *CanonicalWalletBridge) WalletRiskAdmissionRequired() bool {
	return b != nil && (b.cfg.LLMImmediateReleaseMode == "enabled" || b.cfg.MediaImmediateReleaseMode == "enabled")
}

type walletRiskQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type walletReleaseRiskEvent struct {
	ReleasedAt time.Time
	HeldUnits  int64
}

func unknownReleaseRetryAfter(now time.Time, events []walletReleaseRiskEvent) int {
	total := new(big.Int)
	for _, event := range events {
		total.Add(total, big.NewInt(event.HeldUnits))
	}
	limit := big.NewInt(WalletUnknownReleaseSoftLimitUnits)
	if total.Cmp(limit) < 0 {
		return 0
	}
	for _, event := range events {
		total.Sub(total, big.NewInt(event.HeldUnits))
		if total.Cmp(limit) < 0 {
			delta := event.ReleasedAt.Add(24 * time.Hour).Sub(now)
			seconds := int((delta + time.Second - 1) / time.Second)
			if seconds < 1 {
				seconds = 1
			}
			return seconds
		}
	}
	return 1
}

func checkUnknownReleaseSoftLimit(ctx context.Context, db walletRiskQueryer, user string) error {
	unavailable := func(err error) error {
		return &WalletRiskRefusedError{Status: http.StatusServiceUnavailable, RetryAfter: 1, Cause: err}
	}
	if db == nil || user == "" {
		return unavailable(errors.New("wallet risk primary database or identity missing"))
	}
	var now time.Time
	if err := db.QueryRowContext(ctx, `SELECT now()`).Scan(&now); err != nil {
		return unavailable(err)
	}
	rows, err := db.QueryContext(ctx, `SELECT released_at,held_units FROM wallet_unknown_release_counter WHERE platform_user_id=$1 AND released_at>$2::timestamptz-interval '24 hours' ORDER BY released_at,authorization_id`, user, now)
	if err != nil {
		return unavailable(err)
	}
	defer func() { _ = rows.Close() }()
	events := []walletReleaseRiskEvent{}
	for rows.Next() {
		var event walletReleaseRiskEvent
		if err = rows.Scan(&event.ReleasedAt, &event.HeldUnits); err != nil {
			return unavailable(err)
		}
		if event.HeldUnits <= 0 {
			return unavailable(errors.New("wallet risk counter amount invalid"))
		}
		events = append(events, event)
	}
	if err = rows.Err(); err != nil {
		return unavailable(err)
	}
	if retry := unknownReleaseRetryAfter(now, events); retry > 0 {
		return &WalletRiskRefusedError{Status: http.StatusTooManyRequests, RetryAfter: retry}
	}
	return nil
}

// CheckUnknownReleaseSoftLimit reads the primary PostgreSQL connection. The
// rolling total is an unlocked soft limit; concurrent admissions can exceed it.
func (b *CanonicalWalletBridge) CheckUnknownReleaseSoftLimit(ctx context.Context, user string) error {
	if !b.WalletRiskAdmissionRequired() {
		return nil
	}
	if b.outboxDB == nil {
		return &WalletRiskRefusedError{Status: http.StatusServiceUnavailable, RetryAfter: 1}
	}
	return checkUnknownReleaseSoftLimit(ctx, b.outboxDB, user)
}

// AdmitWalletRisk persists identity before the caller creates a hold. Replays
// read the original admission first, so a later risk cap does not reject an
// already admitted task. No estimate E is reserved or counted here.
func (b *CanonicalWalletBridge) AdmitWalletRisk(ctx context.Context, admission WalletRiskAdmission) error {
	if !b.WalletRiskAdmissionRequired() {
		return nil
	}
	if b.outboxDB == nil || admission.ParentAuthorizationID == "" || admission.PlatformUserID == "" || admission.BillingSnapshotID == "" || (admission.Kind != "llm" && admission.Kind != "media") {
		return &WalletRiskRefusedError{Status: http.StatusServiceUnavailable, RetryAfter: 1, Cause: errors.New("wallet risk admission identity unavailable")}
	}
	tx, err := b.outboxDB.BeginTx(ctx, nil)
	if err != nil {
		return &WalletRiskRefusedError{Status: http.StatusServiceUnavailable, RetryAfter: 1, Cause: err}
	}
	defer func() { _ = tx.Rollback() }()
	match := func() (bool, error) {
		var user, snapshot, kind, policy string
		err := tx.QueryRowContext(ctx, `SELECT platform_user_id,billing_snapshot_id,kind,policy_version FROM wallet_risk_admission WHERE parent_authorization_id=$1`, admission.ParentAuthorizationID).Scan(&user, &snapshot, &kind, &policy)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if user != admission.PlatformUserID || snapshot != admission.BillingSnapshotID || kind != admission.Kind || policy != WalletImmediateReleasePolicyVersion {
			return false, errors.New("wallet risk admission identity conflict")
		}
		return true, nil
	}
	admitted, err := match()
	if err != nil {
		return &WalletRiskRefusedError{Status: http.StatusServiceUnavailable, RetryAfter: 1, Cause: err}
	}
	if admitted {
		if err = tx.Commit(); err != nil {
			return &WalletRiskRefusedError{Status: http.StatusServiceUnavailable, RetryAfter: 1, Cause: err}
		}
		return nil
	}
	if err = checkUnknownReleaseSoftLimit(ctx, tx, admission.PlatformUserID); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO wallet_risk_admission(parent_authorization_id,platform_user_id,billing_snapshot_id,kind,policy_version) VALUES($1,$2,$3,$4,$5) ON CONFLICT(parent_authorization_id) DO NOTHING`, admission.ParentAuthorizationID, admission.PlatformUserID, admission.BillingSnapshotID, admission.Kind, WalletImmediateReleasePolicyVersion)
	if err != nil {
		return &WalletRiskRefusedError{Status: http.StatusServiceUnavailable, RetryAfter: 1, Cause: err}
	}
	if admitted, err = match(); err != nil || !admitted {
		return &WalletRiskRefusedError{Status: http.StatusServiceUnavailable, RetryAfter: 1, Cause: fmt.Errorf("wallet risk admission commit identity: %v", err)}
	}
	if err = tx.Commit(); err != nil {
		return &WalletRiskRefusedError{Status: http.StatusServiceUnavailable, RetryAfter: 1, Cause: err}
	}
	return nil
}
