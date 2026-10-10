//go:build unit

package service

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func barrierTestHandle() *AuthorizationHandle {
	return &AuthorizationHandle{ID: "auth-barrier", SnapshotID: "snapshot", AttemptKind: "llm", writes: []AuthorizationWrite{{Token: "auth-barrier.1"}}}
}

const (
	barrierDueQuery   = `SELECT bool_and\(expiry_deadline IS NOT NULL`
	barrierLockQuery  = `SELECT authorization_id FROM wallet_authorization_segment WHERE parent_authorization_id=\$1 ORDER BY ordinal FOR UPDATE`
	barrierGuardQuery = `SELECT bool_and\(a\.kind='llm'`
	barrierSealExec   = `UPDATE wallet_authorization_segment SET terminal_sealed_at`
)

// Not overdue: nothing is opened, locked or written.
func TestReleaseExpiredFeeBarrierDoesNothingBeforeTheDeadlinePlusGrace(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	b := &CanonicalWalletBridge{outboxDB: db, cfg: config.CanonicalWalletConfig{LLMImmediateReleaseMode: "enabled"}}
	mock.ExpectQuery(barrierDueQuery).WillReturnRows(sqlmock.NewRows([]string{"due"}).AddRow(false))
	mock.ExpectBegin() // must stay unmet: no transaction may be opened
	require.False(t, b.releaseExpiredFeeBarrier(context.Background(), barrierTestHandle(), "user", nil, WalletReaderEvidence{}))
	require.Error(t, mock.ExpectationsWereMet(), "the transaction expectation must be left unmet")
}

// Outside the enabled mode an overdue barrier is only reported: no transaction is opened.
func TestReleaseExpiredFeeBarrierNeverOpensATransactionOutsideEnabledMode(t *testing.T) {
	for _, mode := range []string{"off", "shadow"} {
		t.Run(mode, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			b := &CanonicalWalletBridge{outboxDB: db, cfg: config.CanonicalWalletConfig{LLMImmediateReleaseMode: mode}}
			mock.ExpectQuery(barrierDueQuery).WillReturnRows(sqlmock.NewRows([]string{"due"}).AddRow(true))
			mock.ExpectBegin()
			require.False(t, b.releaseExpiredFeeBarrier(context.Background(), barrierTestHandle(), "user", nil, WalletReaderEvidence{}))
			require.Error(t, mock.ExpectationsWereMet(), "the transaction expectation must be left unmet")
		})
	}
}

// One failing guard anywhere in the group (a staged fee, changed evidence, a segment
// still inside its grace...) rolls everything back: nothing is written or committed.
func TestReleaseExpiredFeeBarrierRollsBackWhenAnyGuardFails(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	b := &CanonicalWalletBridge{outboxDB: db, cfg: config.CanonicalWalletConfig{LLMImmediateReleaseMode: "enabled"}}
	mock.ExpectQuery(barrierDueQuery).WillReturnRows(sqlmock.NewRows([]string{"due"}).AddRow(true))
	mock.ExpectBegin()
	mock.ExpectQuery(barrierLockQuery).WithArgs("auth-barrier").WillReturnRows(sqlmock.NewRows([]string{"authorization_id"}).AddRow("a0").AddRow("a1"))
	mock.ExpectQuery(barrierGuardQuery).WillReturnRows(sqlmock.NewRows([]string{"guards", "barrier", "held"}).AddRow(false, true, int64(100)))
	mock.ExpectRollback()
	require.False(t, b.releaseExpiredFeeBarrier(context.Background(), barrierTestHandle(), "user", []byte(`{}`), WalletReaderEvidence{}))
	require.NoError(t, mock.ExpectationsWereMet(), "lock, guard, rollback: no UPDATE and no commit")
}

// If the seal touches a different number of segments than were locked, the whole
// transaction is rolled back rather than leaving a half-sealed group.
func TestReleaseExpiredFeeBarrierRollsBackWhenTheSealMissesASegment(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	b := &CanonicalWalletBridge{outboxDB: db, cfg: config.CanonicalWalletConfig{LLMImmediateReleaseMode: "enabled"}}
	mock.ExpectQuery(barrierDueQuery).WillReturnRows(sqlmock.NewRows([]string{"due"}).AddRow(true))
	mock.ExpectBegin()
	mock.ExpectQuery(barrierLockQuery).WithArgs("auth-barrier").WillReturnRows(sqlmock.NewRows([]string{"authorization_id"}).AddRow("a0").AddRow("a1"))
	mock.ExpectQuery(barrierGuardQuery).WillReturnRows(sqlmock.NewRows([]string{"guards", "barrier", "held"}).AddRow(true, true, int64(100)))
	mock.ExpectQuery(`SELECT COALESCE\(reader_evidence`).WillReturnRows(sqlmock.NewRows([]string{"e"}).AddRow([]byte(`{"complete":true}`)))
	mock.ExpectExec(barrierSealExec).WillReturnResult(sqlmock.NewResult(0, 1)) // only 1 of the 2 locked segments
	mock.ExpectRollback()
	require.False(t, b.releaseExpiredFeeBarrier(context.Background(), barrierTestHandle(), "user", []byte(`{"complete":true}`), WalletReaderEvidence{}))
	require.NoError(t, mock.ExpectationsWereMet(), "a partial seal is rolled back, never committed")
}

// The release claims the immediate expiry in the SAME transaction, so a sealed,
// barrier-free attempt can never be left without an owner.
func TestReleaseExpiredFeeBarrierSealsAndClaimsInOneCommit(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	b := &CanonicalWalletBridge{outboxDB: db, cfg: config.CanonicalWalletConfig{LLMImmediateReleaseMode: "enabled"}}
	sealed := sqlmock.NewRows([]string{"sealed", "proof"}).AddRow(time.Now(), "ab"+strings.Repeat("0", 62))
	mock.ExpectQuery(barrierDueQuery).WillReturnRows(sqlmock.NewRows([]string{"due"}).AddRow(true))
	mock.ExpectBegin()
	mock.ExpectQuery(barrierLockQuery).WithArgs("auth-barrier").WillReturnRows(sqlmock.NewRows([]string{"authorization_id"}).AddRow("a0"))
	mock.ExpectQuery(barrierGuardQuery).WillReturnRows(sqlmock.NewRows([]string{"guards", "barrier", "held"}).AddRow(true, true, int64(100)))
	mock.ExpectQuery(`SELECT COALESCE\(reader_evidence`).WillReturnRows(sqlmock.NewRows([]string{"e"}).AddRow([]byte(`{"complete":true}`)))
	mock.ExpectExec(barrierSealExec).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT terminal_sealed_at,expiry_terminal_proof`).WillReturnRows(sealed)
	mock.ExpectExec(`UPDATE wallet_authorization_segment SET state='expiry_pending',expiry_intent_version=2`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	// The best-effort intent-2 recovery that follows reads the segments; let it fail quietly.
	mock.ExpectQuery(`SELECT`).WillReturnError(context.Canceled)
	require.True(t, b.releaseExpiredFeeBarrier(context.Background(), barrierTestHandle(), "user", []byte(`{"complete":true}`), WalletReaderEvidence{Complete: true}))
	require.NoError(t, mock.ExpectationsWereMet())
}
