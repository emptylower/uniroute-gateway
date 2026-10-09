//go:build unit

package service

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestWalletAvailabilityV5ReceiptIdentityBounds(t *testing.T) {
	ids, err := availabilityReceiptIDs([]string{"auth-a", "auth-a", "auth-b"})
	require.NoError(t, err)
	require.Equal(t, []string{"auth-a", "auth-b"}, ids)
	for _, input := range [][]string{{""}, {" auth"}, {strings.Repeat("a", 257)}, make([]string, 129)} {
		_, err = availabilityReceiptIDs(input)
		require.Error(t, err)
	}
}

func TestWalletAvailabilityV5OnlyCommittedOriginalReceiptQuery(t *testing.T) {
	database, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer database.Close()
	mock.ExpectBegin()
	tx, err := database.Begin()
	require.NoError(t, err)
	raw := `{"authorization_id":"auth-a","platform_user_id":"owner-a","status":"released","expiry_version":0,"receipt_signature":"` + strings.Repeat("a", 64) + `"}`
	mock.ExpectQuery(`SELECT authorization_id,CASE WHEN zero_ack_at IS NOT NULL THEN zero_receipt ELSE expiry_receipt END FROM wallet_authorization_segment WHERE platform_user_id=\$1 AND authorization_id IN \(\$2\) AND \(\(zero_ack_at IS NOT NULL AND zero_receipt IS NOT NULL\) OR \(expiry_intent_version=2 AND expiry_ack_at IS NOT NULL AND expiry_receipt IS NOT NULL\)\)`).WithArgs("owner-a", "auth-a").WillReturnRows(sqlmock.NewRows([]string{"authorization_id", "receipt"}).AddRow("auth-a", raw))
	receipts, err := committedAvailabilityReceipts(context.Background(), tx, "owner-a", []string{"auth-a"})
	require.NoError(t, err)
	require.Equal(t, []json.RawMessage{json.RawMessage(raw)}, receipts, "forward the original receipt, never reconstruct ACK time or amount")
	mock.ExpectRollback()
	require.NoError(t, tx.Rollback())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWalletAvailabilityV5RejectsMalformedCommittedReceipt(t *testing.T) {
	for _, raw := range []string{
		`{"authorization_id":"auth-a","platform_user_id":"foreign","status":"released","receipt_signature":"` + strings.Repeat("a", 64) + `"}`,
		`{"authorization_id":"auth-a","platform_user_id":"owner-a","status":"expiry_pending","receipt_signature":"` + strings.Repeat("a", 64) + `"}`,
		`{"authorization_id":"auth-a","platform_user_id":"owner-a","status":"expired_unknown","expiry_version":1,"receipt_signature":"` + strings.Repeat("a", 64) + `"}`,
		`{"authorization_id":"auth-a","platform_user_id":"owner-a","status":"released"}`,
	} {
		database, mock, err := sqlmock.New()
		require.NoError(t, err)
		mock.ExpectBegin()
		tx, err := database.Begin()
		require.NoError(t, err)
		mock.ExpectQuery("SELECT authorization_id,CASE WHEN zero_ack_at IS NOT NULL").WithArgs("owner-a", "auth-a").WillReturnRows(sqlmock.NewRows([]string{"authorization_id", "receipt"}).AddRow("auth-a", raw))
		_, err = committedAvailabilityReceipts(context.Background(), tx, "owner-a", []string{"auth-a"})
		require.Error(t, err)
		mock.ExpectRollback()
		require.NoError(t, tx.Rollback())
		require.NoError(t, mock.ExpectationsWereMet())
		database.Close()
	}
}

func TestWalletAvailabilityV5RedisConflictIsScopedToRelatedLease(t *testing.T) {
	first := MediaWalletRawState{Leases: []MediaWalletRawLease{{LeaseID: "lease-a", BudgetUnits: 100, Present: true}, {LeaseID: "lease-b", BudgetUnits: 100, Present: true}}, Reservations: map[string]string{}}
	second := MediaWalletRawState{Leases: append([]MediaWalletRawLease(nil), first.Leases...), Holds: []CanonicalWalletHold{{AuthorizationID: "hold-a", LeaseID: "lease-a", HeldUnits: 10, State: "armed"}}, Reservations: map[string]string{"event-a": "lease-a"}}
	second.Leases[0].ConsumedUnits = 10
	require.NotEqual(t, availabilityLeaseFingerprint(first, "lease-a"), availabilityLeaseFingerprint(second, "lease-a"))
	require.Equal(t, availabilityLeaseFingerprint(first, "lease-b"), availabilityLeaseFingerprint(second, "lease-b"), "unrelated Redis lease observation retains its confirmation")
}
