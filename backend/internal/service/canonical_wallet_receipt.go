package service

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"
)

// WalletTaskPinReceipt is the shared Go/Worker wire. Timestamp strings are kept
// verbatim for HMAC verification; comparisons use parsed UTC instants.
type WalletTaskPinReceipt struct {
	GatewayJobID          string                       `json:"gateway_job_id"`
	AuthorizationID       string                       `json:"authorization_id"`
	PlatformUserID        string                       `json:"platform_user_id"`
	LeaseID               string                       `json:"lease_id"`
	BillingSnapshotID     string                       `json:"billing_snapshot_id"`
	SettlementEventID     string                       `json:"settlement_event_id"`
	Held                  canonicalWalletAmountObject  `json:"held"`
	Actual                *canonicalWalletAmountObject `json:"actual,omitempty"`
	USDPolicyVersion      string                       `json:"usd_wallet_policy_version"`
	AuthorizationKind     string                       `json:"authorization_kind"`
	AuthorizationToken    string                       `json:"authorization_token"`
	Status                string                       `json:"status"`
	ExpiryVersion         int                          `json:"expiry_version"`
	ExpiryDeadline        string                       `json:"expiry_deadline,omitempty"`
	ExpiryPolicyVersion   string                       `json:"expiry_policy_version,omitempty"`
	ExpiryTerminalProof   string                       `json:"expiry_terminal_proof,omitempty"`
	ExpiredAt             string                       `json:"expired_at,omitempty"`
	ReleasedAt            string                       `json:"released_at,omitempty"`
	ExpiryReceiptID       string                       `json:"expiry_receipt_id,omitempty"`
	FinishReceiptID       string                       `json:"finish_receipt_id,omitempty"`
	LegacyCompletionProof string                       `json:"legacy_completion_proof,omitempty"`
	Reason                string                       `json:"reason"`
	ReceiptSignature      string                       `json:"receipt_signature"`
}

type WalletTaskPinReceiptExpected struct {
	GatewayJobID          string
	AuthorizationID       string
	PlatformUserID        string
	LeaseID               string
	BillingSnapshotID     string
	SettlementEventID     string
	HeldUnits             int64
	AuthorizationKind     string
	AuthorizationToken    string
	Status                string // expired_unknown or released
	ExpiryDeadline        time.Time
	ExpiryTerminalProof   string
	LegacyCompletionProof string
}

// The array and string formatting exactly match task-pin.ts. The final newline
// is not signed. HTML escaping is disabled; U+2028/U+2029 keep Go's escaped form
// and the Worker performs the same replacements after JSON.stringify.
func walletTaskPinReceiptSigningPayload(receipt WalletTaskPinReceipt) ([]byte, error) {
	firstRelease, receiptID := receipt.ExpiredAt, receipt.ExpiryReceiptID
	if receipt.Status == "released" {
		firstRelease, receiptID = receipt.ReleasedAt, receipt.FinishReceiptID
	}
	actual := "0"
	if receipt.Actual != nil {
		actual = receipt.Actual.AmountUnits
	}
	parts := []string{
		"wallet-task-pin-receipt-v2", receipt.Status, receipt.AuthorizationID,
		receipt.GatewayJobID, receipt.PlatformUserID, receipt.LeaseID,
		receipt.BillingSnapshotID, receipt.SettlementEventID, receipt.Held.AmountUnits,
		receipt.AuthorizationKind, receipt.AuthorizationToken, strconv.Itoa(receipt.ExpiryVersion),
		receipt.ExpiryPolicyVersion, receipt.ExpiryTerminalProof, receipt.ExpiryDeadline,
		firstRelease, receiptID, receipt.LegacyCompletionProof, actual,
	}
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(parts); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buffer.Bytes(), []byte("\n")), nil
}

// VerifyWalletTaskPinReceipt validates the receipt signature AND the persisted
// billing identity before any PG acknowledgement or Redis cleanup. A bare 404
// never reaches this function and is not an acknowledgement.
func VerifyWalletTaskPinReceipt(secret string, expected WalletTaskPinReceiptExpected, receipt WalletTaskPinReceipt) (time.Time, error) {
	mismatch := errors.New("wallet signed task-pin acknowledgement mismatch")
	if secret == "" || expected.AuthorizationToken == "" || (expected.AuthorizationKind != "llm" && expected.AuthorizationKind != "media") {
		return time.Time{}, mismatch
	}
	held, err := parseCanonicalWalletAmountObject("held", receipt.Held)
	if err != nil || held != expected.HeldUnits || held <= 0 || receipt.GatewayJobID != expected.GatewayJobID || receipt.AuthorizationID != expected.AuthorizationID || receipt.PlatformUserID != expected.PlatformUserID || receipt.LeaseID != expected.LeaseID || receipt.BillingSnapshotID != expected.BillingSnapshotID || receipt.SettlementEventID != expected.SettlementEventID || receipt.USDPolicyVersion != "usd-wallet-v1" || receipt.AuthorizationKind != expected.AuthorizationKind || receipt.AuthorizationToken != expected.AuthorizationToken || receipt.Status != expected.Status || receipt.LegacyCompletionProof != expected.LegacyCompletionProof {
		return time.Time{}, mismatch
	}
	var firstRelease string
	switch expected.Status {
	case "expired_unknown":
		deadline, parseErr := time.Parse(time.RFC3339Nano, receipt.ExpiryDeadline)
		if parseErr != nil || !deadline.Equal(expected.ExpiryDeadline) || receipt.ExpiryVersion != 2 || receipt.ExpiryPolicyVersion != WalletImmediateReleasePolicyVersion || receipt.ExpiryTerminalProof != expected.ExpiryTerminalProof || !walletProofHex(receipt.ExpiryTerminalProof) || receipt.ExpiryReceiptID != expected.AuthorizationID+":expiry:2" || receipt.FinishReceiptID != "" || receipt.ReleasedAt != "" || receipt.Reason != "financial_unknown" {
			return time.Time{}, mismatch
		}
		if receipt.Actual != nil {
			actual, parseErr := parseCanonicalWalletAmountObject("actual", *receipt.Actual)
			if parseErr != nil || actual != 0 {
				return time.Time{}, mismatch
			}
		}
		firstRelease = receipt.ExpiredAt
	case "released":
		if receipt.Actual == nil || receipt.ExpiryVersion != 0 || receipt.ExpiryDeadline != "" || receipt.ExpiryPolicyVersion != "" || receipt.ExpiryTerminalProof != "" || receipt.ExpiredAt != "" || receipt.ExpiryReceiptID != "" || receipt.FinishReceiptID != expected.AuthorizationID+":finish:1" || receipt.Reason != "gateway_confirmed_zero" {
			return time.Time{}, mismatch
		}
		actual, parseErr := parseCanonicalWalletAmountObject("actual", *receipt.Actual)
		if parseErr != nil || actual != 0 {
			return time.Time{}, mismatch
		}
		firstRelease = receipt.ReleasedAt
	default:
		return time.Time{}, mismatch
	}
	releasedAt, err := time.Parse(time.RFC3339Nano, firstRelease)
	if err != nil || releasedAt.IsZero() {
		return time.Time{}, mismatch
	}
	if !walletProofHex(receipt.ReceiptSignature) {
		return time.Time{}, mismatch
	}
	provided, err := hex.DecodeString(receipt.ReceiptSignature)
	if err != nil {
		return time.Time{}, mismatch
	}
	payload, err := walletTaskPinReceiptSigningPayload(receipt)
	if err != nil {
		return time.Time{}, err
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(payload)
	if !hmac.Equal(provided, mac.Sum(nil)) {
		return time.Time{}, mismatch
	}
	return releasedAt.UTC(), nil
}

func walletProofHex(proof string) bool {
	if len(proof) != 64 || strings.ToLower(proof) != proof {
		return false
	}
	_, err := hex.DecodeString(proof)
	return err == nil
}

func walletWireTimestamp(value time.Time) string {
	return value.UTC().Format("2006-01-02T15:04:05.000Z")
}
