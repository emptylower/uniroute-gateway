//go:build media_integration

package media_integration

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestExternalAvailabilityV5ReturnsOriginalSignedPGACKOnly(t *testing.T) {
	f, handle, _, base, secret := immediateV5Prepared(t)
	input := service.WalletAvailabilityInput{UnitVersion: "usd-e8-v1", USDPolicyVersion: "usd-wallet-v1"}
	for _, segment := range handle.Segments {
		input.FinancialReceiptAuthorizationIDs = append(input.FinancialReceiptAuthorizationIDs, segment.AuthorizationID)
		input.Leases = append(input.Leases, service.WalletAvailabilityLeaseInput{LeaseID: segment.LeaseID, BudgetUnits: strconv.FormatInt(segment.Basis.BudgetUnits, 10), CapturedUnits: "0", ReleasedUnits: "0", ReservedUnits: "0", Status: "active", ExpiresAt: segment.Basis.ExpiresAt})
	}
	input.FinancialReceiptAuthorizationIDs = append(input.FinancialReceiptAuthorizationIDs, input.FinancialReceiptAuthorizationIDs[0])
	bridge := immediateV5Bridge(t, f, base, secret, "enabled")
	deadline := time.Now().Add(-time.Second).UTC().Truncate(time.Millisecond)
	claimed, err := bridge.ClaimImmediateWalletExpiry(context.Background(), handle.ID, deadline, strings.Repeat("a", 64))
	require.NoError(t, err)
	require.True(t, claimed)
	before, err := f.svc.Availability(context.Background(), f.userID, input)
	require.NoError(t, err)
	require.Empty(t, before.CommittedFinancialReceipts, "intent cannot be presented as release authority")
	handled, err := bridge.RecoverImmediateWalletExpiry(context.Background(), handle.ID)
	require.True(t, handled)
	require.NoError(t, err)
	after, err := f.svc.Availability(context.Background(), f.userID, input)
	require.NoError(t, err)
	require.Len(t, after.CommittedFinancialReceipts, len(handle.Segments), "request duplicates do not duplicate receipts")
	for _, raw := range after.CommittedFinancialReceipts {
		var receipt service.WalletTaskPinReceipt
		require.NoError(t, json.Unmarshal(raw, &receipt))
		var original string
		require.NoError(t, f.db.QueryRow(`SELECT expiry_receipt::text FROM wallet_authorization_segment WHERE authorization_id=$1 AND expiry_ack_at IS NOT NULL`, receipt.AuthorizationID).Scan(&original))
		require.JSONEq(t, original, string(raw))
		require.Equal(t, "media-user", receipt.PlatformUserID)
		require.Equal(t, 2, receipt.ExpiryVersion)
		require.NotEmpty(t, receipt.ExpiredAt)
	}
	var foreign int64
	require.NoError(t, f.db.QueryRow(`INSERT INTO users(email,password_hash,platform_user_id,billing_currency,status,balance) VALUES('other-availability@example.test','test','other-availability-owner','USD','active',0) RETURNING id`).Scan(&foreign))
	scoped, err := f.svc.Availability(context.Background(), foreign, service.WalletAvailabilityInput{UnitVersion: "usd-e8-v1", USDPolicyVersion: "usd-wallet-v1", FinancialReceiptAuthorizationIDs: input.FinancialReceiptAuthorizationIDs})
	require.NoError(t, err)
	require.Empty(t, scoped.CommittedFinancialReceipts, "authenticated user scope excludes another user's acknowledged receipts")
}
