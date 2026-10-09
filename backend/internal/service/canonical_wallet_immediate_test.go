//go:build unit

package service

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func immediateReceiptFixture(t *testing.T, status string) (WalletTaskPinReceipt, WalletTaskPinReceiptExpected) {
	t.Helper()
	r := WalletTaskPinReceipt{GatewayJobID: "auth-test", AuthorizationID: "auth-test", PlatformUserID: "user<&>\u2028", LeaseID: "lease-test", BillingSnapshotID: "snapshot-test", SettlementEventID: "event-test", Held: newCanonicalWalletAmountObject(1234), USDPolicyVersion: "usd-wallet-v1", AuthorizationKind: "llm", AuthorizationToken: "auth-test.1", Status: status}
	e := WalletTaskPinReceiptExpected{GatewayJobID: r.GatewayJobID, AuthorizationID: r.AuthorizationID, PlatformUserID: r.PlatformUserID, LeaseID: r.LeaseID, BillingSnapshotID: r.BillingSnapshotID, SettlementEventID: r.SettlementEventID, HeldUnits: 1234, AuthorizationKind: "llm", AuthorizationToken: r.AuthorizationToken, Status: status}
	if status == "expired_unknown" {
		r.ExpiryVersion, r.ExpiryPolicyVersion = 2, WalletImmediateReleasePolicyVersion
		r.ExpiryTerminalProof = strings.Repeat("a", 64)
		r.ExpiryDeadline, r.ExpiredAt = "2026-10-08T18:00:00.000Z", "2026-10-08T18:00:01.123Z"
		r.ExpiryReceiptID, r.Reason = r.AuthorizationID+":expiry:2", "financial_unknown"
		e.ExpiryDeadline, _ = time.Parse(time.RFC3339Nano, r.ExpiryDeadline)
		e.ExpiryTerminalProof = r.ExpiryTerminalProof
	} else {
		zero := newCanonicalWalletAmountObject(0)
		r.Actual, r.ReleasedAt = &zero, "2026-10-08T18:00:01.123Z"
		r.FinishReceiptID, r.Reason = r.AuthorizationID+":finish:1", "gateway_confirmed_zero"
	}
	signImmediateReceipt(t, &r)
	return r, e
}

func signImmediateReceipt(t *testing.T, receipt *WalletTaskPinReceipt) {
	t.Helper()
	payload, err := walletTaskPinReceiptSigningPayload(*receipt)
	require.NoError(t, err)
	mac := hmac.New(sha256.New, []byte("fixture-shared-secret"))
	_, _ = mac.Write(payload)
	receipt.ReceiptSignature = hex.EncodeToString(mac.Sum(nil))
}

func TestWalletImmediateReceiptWireAndTamperProtection(t *testing.T) {
	r, expected := immediateReceiptFixture(t, "expired_unknown")
	payload, err := walletTaskPinReceiptSigningPayload(r)
	require.NoError(t, err)
	require.Contains(t, string(payload), `"user<&>\u2028"`, "JSON escaping must agree with the Worker, including U+2028")
	require.NotContains(t, string(payload), `\u003c`)
	released, err := VerifyWalletTaskPinReceipt("fixture-shared-secret", expected, r)
	require.NoError(t, err)
	require.Equal(t, "2026-10-08T18:00:01.123Z", released.Format(time.RFC3339Nano))
	for name, mutate := range map[string]func(*WalletTaskPinReceipt){
		"token":         func(r *WalletTaskPinReceipt) { r.AuthorizationToken = "other" },
		"kind":          func(r *WalletTaskPinReceipt) { r.AuthorizationKind = "media" },
		"amount":        func(r *WalletTaskPinReceipt) { r.Held.AmountUnits = "1235" },
		"proof":         func(r *WalletTaskPinReceipt) { r.ExpiryTerminalProof = strings.Repeat("b", 64) },
		"policy":        func(r *WalletTaskPinReceipt) { r.ExpiryPolicyVersion = "other" },
		"version":       func(r *WalletTaskPinReceipt) { r.ExpiryVersion = 1 },
		"first-release": func(r *WalletTaskPinReceipt) { r.ExpiredAt = "2026-10-09T18:00:01.123Z" },
		"receipt":       func(r *WalletTaskPinReceipt) { r.ExpiryReceiptID = "other:expiry:2" },
		"reason":        func(r *WalletTaskPinReceipt) { r.Reason = "zero" },
		"currency":      func(r *WalletTaskPinReceipt) { r.Held.Currency = "CNY" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := r
			mutate(&changed)
			_, err := VerifyWalletTaskPinReceipt("fixture-shared-secret", expected, changed)
			require.Error(t, err)
		})
	}
	_, err = VerifyWalletTaskPinReceipt("wrong-secret", expected, r)
	require.Error(t, err)
}

func TestWalletImmediateZeroReceiptRequiresSignedActualAndFirstTime(t *testing.T) {
	r, expected := immediateReceiptFixture(t, "released")
	_, err := VerifyWalletTaskPinReceipt("fixture-shared-secret", expected, r)
	require.NoError(t, err)
	r.Actual = nil
	signImmediateReceipt(t, &r)
	_, err = VerifyWalletTaskPinReceipt("fixture-shared-secret", expected, r)
	require.Error(t, err, "missing actual cannot become an inferred zero")
	r, expected = immediateReceiptFixture(t, "released")
	r.ReleasedAt = "not-a-time"
	signImmediateReceipt(t, &r)
	_, err = VerifyWalletTaskPinReceipt("fixture-shared-secret", expected, r)
	require.Error(t, err)
}

func TestWalletImmediateRiskRetryAfterUsesFirstSufficientSlidingEvent(t *testing.T) {
	now := time.Date(2026, 10, 8, 18, 0, 0, 0, time.UTC)
	require.Zero(t, unknownReleaseRetryAfter(now, []walletReleaseRiskEvent{{now, 499999999}}))
	require.Equal(t, 3600, unknownReleaseRetryAfter(now, []walletReleaseRiskEvent{{now.Add(-23 * time.Hour), 200000000}, {now.Add(-2 * time.Hour), 300000000}, {now.Add(-time.Hour), 100000000}}))
	require.Equal(t, 7200, unknownReleaseRetryAfter(now, []walletReleaseRiskEvent{{now.Add(-23 * time.Hour), 1}, {now.Add(-22 * time.Hour), 499999999}, {now.Add(-time.Hour), 1}}))
	require.Equal(t, 86400, unknownReleaseRetryAfter(now, []walletReleaseRiskEvent{{now, math.MaxInt64}, {now, math.MaxInt64}}), "sum overflow must not admit new work")
}

func TestWalletImmediateRiskReadFailureIs503AndOffShadowHaveNoEffects(t *testing.T) {
	for _, mode := range []string{"off", "shadow"} {
		bridge := &CanonicalWalletBridge{cfg: config.CanonicalWalletConfig{LLMImmediateReleaseMode: mode, MediaImmediateReleaseMode: mode}}
		require.NoError(t, bridge.AdmitWalletRisk(context.Background(), WalletRiskAdmission{}))
		require.NoError(t, bridge.CheckUnknownReleaseSoftLimit(context.Background(), "user"))
	}
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	bridge := &CanonicalWalletBridge{outboxDB: db, cfg: config.CanonicalWalletConfig{LLMImmediateReleaseMode: "enabled"}}
	mock.ExpectQuery("SELECT now").WillReturnError(errors.New("primary unavailable"))
	err = bridge.CheckUnknownReleaseSoftLimit(context.Background(), "user")
	status, retry, ok := WalletRiskRefusalDetails(err)
	require.True(t, ok)
	require.Equal(t, http.StatusServiceUnavailable, status)
	require.Equal(t, 1, retry)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWalletImmediateAdmissionReplayDoesNotRegate(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	bridge := &CanonicalWalletBridge{outboxDB: db, cfg: config.CanonicalWalletConfig{LLMImmediateReleaseMode: "enabled"}}
	admission := WalletRiskAdmission{ParentAuthorizationID: "auth-test", PlatformUserID: "user-test", BillingSnapshotID: "snapshot-test", Kind: "media"}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT platform_user_id,billing_snapshot_id,kind,policy_version FROM wallet_risk_admission").WithArgs("auth-test").WillReturnRows(sqlmock.NewRows([]string{"platform_user_id", "billing_snapshot_id", "kind", "policy_version"}).AddRow("user-test", "snapshot-test", "media", WalletImmediateReleasePolicyVersion))
	mock.ExpectCommit()
	require.NoError(t, bridge.AdmitWalletRisk(context.Background(), admission))
	require.NoError(t, mock.ExpectationsWereMet(), "replay must not read the new 24h risk window")
}
