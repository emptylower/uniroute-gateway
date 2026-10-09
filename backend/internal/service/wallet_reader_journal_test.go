//go:build unit

package service

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestWalletReaderJournalRegistryRefusesNewVolumeBeforeOwnerSession(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	dir := t.TempDir()
	host, err := walletReaderJournalDirectory(dir)
	require.NoError(t, err)
	bridge := &CanonicalWalletBridge{outboxDB: db, cfg: config.CanonicalWalletConfig{ReaderJournalDirectory: dir}}
	mock.ExpectBegin()
	mock.ExpectExec("SELECT pg_advisory_xact_lock").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT EXISTS").WithArgs(host).WillReturnRows(sqlmock.NewRows([]string{"mismatch"}).AddRow(false))
	mock.ExpectExec("INSERT INTO wallet_reader_journal_volume").WithArgs(host).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT volume_id FROM wallet_reader_journal_volume").WillReturnRows(sqlmock.NewRows([]string{"volume_id"}).AddRow("original-mounted-volume"))
	mock.ExpectRollback()
	err = bridge.ensureWalletReaderJournal(context.Background())
	status, _, refused := WalletRiskRefusalDetails(err)
	require.True(t, refused)
	require.Equal(t, 503, status)
	require.Nil(t, bridge.readerJournalOwner, "wrong mount cannot claim a reader owner or create a hold")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWalletReaderJournalRegistryDoesNotAdoptHistoricalUnfinishedVolume(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	bridge := &CanonicalWalletBridge{outboxDB: db}
	mock.ExpectBegin()
	mock.ExpectExec("SELECT pg_advisory_xact_lock").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT EXISTS").WithArgs("replacement").WillReturnRows(sqlmock.NewRows([]string{"mismatch"}).AddRow(true))
	mock.ExpectRollback()
	require.Error(t, bridge.verifyWalletReaderJournalVolume(context.Background(), "replacement"))
	require.NoError(t, mock.ExpectationsWereMet(), "no registry INSERT is permitted before historical reader volume identity agrees")
}

func testWalletReaderJournal(t *testing.T) *walletReaderJournal {
	t.Helper()
	dir := t.TempDir()
	host, err := walletReaderJournalDirectory(dir)
	require.NoError(t, err)
	record := walletReaderJournalRecord{Version: 1, Host: host, OwnerID: "owner-original", AuthorizationID: "auth-original", Token: "auth-original.1", SnapshotID: "snapshot-original"}
	journal := &walletReaderJournal{dir: dir, path: walletReaderJournalPath(dir, record.AuthorizationID, record.Token), record: record}
	require.NoError(t, journal.acquire(false))
	require.NoError(t, journal.saveLocked())
	journal.release()
	return journal
}

func TestWalletReaderJournalVolumeIdentitySurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	first, err := walletReaderJournalDirectory(dir)
	require.NoError(t, err)
	second, err := walletReaderJournalDirectory(dir)
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".identity.json"), []byte(`{"version":1,"host":""}`), 0600))
	_, err = walletReaderJournalDirectory(dir)
	require.Error(t, err)
	bridge := &CanonicalWalletBridge{cfg: config.CanonicalWalletConfig{LLMImmediateReleaseMode: "enabled"}}
	err = bridge.ensureWalletReaderJournal(context.Background())
	status, retry, ok := WalletRiskRefusalDetails(err)
	require.True(t, ok)
	require.Equal(t, 503, status)
	require.Equal(t, 1, retry)
}

func TestWalletReaderJournalCheckpointSurvivesCrashBeforePositivePGTransfer(t *testing.T) {
	journal := testWalletReaderJournal(t)
	require.NoError(t, journal.beginRead())
	require.NoError(t, journal.checkpointLocked([]byte(`{"type":"response.completed","response":{"id":"r-original","usage":{"input_tokens":9007199254740993,"output_tokens":1,"source":"provider_chosen","raw_credits":0},"output":[{"text":"private output text"}]},"api_key":"private-credential"}`), "llm_ws_usage"))
	journal.release()
	reopened := &walletReaderJournal{dir: journal.dir, path: journal.path, record: journal.record}
	require.NoError(t, reopened.acquire(false))
	defer reopened.release()
	require.NoError(t, reopened.loadLocked())
	require.False(t, reopened.record.Evidence.ObservedPositive, "positive value is not adopted before the durable selected checkpoint")
	raw, err := os.ReadFile(journal.path)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "private output")
	require.NotContains(t, string(raw), "private-credential")
	require.NotContains(t, string(raw), "provider_chosen")
	require.NotContains(t, string(raw), "raw_credits")
	require.NoError(t, reopened.replayCheckpointLocked())
	require.True(t, reopened.record.Evidence.ObservedPositive)
	require.Equal(t, int64(9007199254740993), int64(reopened.record.Evidence.Tokens.InputTokens))
	require.Equal(t, "llm_ws_usage", reopened.record.Evidence.Source)
	require.Empty(t, reopened.record.PendingUsage)
}

func TestWalletReaderJournalMalformedCheckpointCannotBecomeZero(t *testing.T) {
	journal := testWalletReaderJournal(t)
	require.NoError(t, journal.beginRead())
	require.NoError(t, journal.checkpointLocked([]byte(`{"type":"response.completed","response":{"usage":{"input_tokens":3,"output_tokens":"bad"}}}`), "llm_ws_usage"))
	journal.release()
	require.NoError(t, journal.acquire(false))
	defer journal.release()
	require.NoError(t, journal.loadLocked())
	require.NoError(t, journal.replayCheckpointLocked())
	require.True(t, journal.record.Evidence.ObservedPositive)
	require.True(t, journal.record.Evidence.Malformed)
	require.False(t, journal.record.Evidence.Valid)
}

func TestWalletReaderJournalPGFailureRetriesIntactEvidence(t *testing.T) {
	journal := testWalletReaderJournal(t)
	require.NoError(t, journal.beginRead())
	e := WalletReaderEvidence{Present: true, Valid: true, ObservedPositive: true, Tokens: UsageTokens{InputTokens: 10, OutputTokens: 2}, Source: "llm_http_usage"}
	require.NoError(t, journal.observeLocked(e, true))
	journal.release()
	require.Error(t, journal.transfer(e, func(WalletReaderEvidence) error { return errors.New("primary unavailable") }))
	require.NoError(t, journal.acquire(false))
	require.NoError(t, journal.loadLocked())
	require.True(t, journal.record.Joined)
	require.True(t, journal.record.PersistenceFailed)
	journal.release()
	require.NoError(t, journal.transfer(WalletReaderEvidence{}, func(frozen WalletReaderEvidence) error {
		require.True(t, frozen.ObservedPositive)
		require.Equal(t, 10, frozen.Tokens.InputTokens)
		return nil
	}))
	require.NoError(t, journal.acquire(false))
	require.NoError(t, journal.loadLocked())
	require.False(t, journal.record.PersistenceFailed)
	journal.release()
	require.ErrorIs(t, journal.beginRead(), errWalletReaderFenced)
}

func TestWalletReaderJournalReadLockBlocksRecoveryAndFencePreventsNextRead(t *testing.T) {
	journal := testWalletReaderJournal(t)
	require.NoError(t, journal.beginRead())
	recovery := &walletReaderJournal{dir: journal.dir, path: journal.path, record: journal.record}
	acquired := make(chan error, 1)
	go func() { acquired <- recovery.acquire(true) }()
	require.Error(t, <-acquired, "OS lock must refuse recovery while raw Read owns this attempt")
	require.NoError(t, journal.observeLocked(WalletReaderEvidence{Source: "llm_http_usage"}, false))
	journal.release()
	require.NoError(t, recovery.acquire(true))
	require.NoError(t, recovery.loadLocked())
	recovery.record.Sealed = true
	recovery.record.Joined = true
	require.NoError(t, recovery.saveLocked())
	recovery.release()
	require.ErrorIs(t, journal.beginRead(), errWalletReaderFenced)
}

func TestWalletReaderJournalCorruptMissingAndWrongVolumeProtect(t *testing.T) {
	for _, kind := range []string{"corrupt", "missing", "wrong-volume", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			journal := testWalletReaderJournal(t)
			switch kind {
			case "corrupt":
				require.NoError(t, os.WriteFile(journal.path, []byte(`{`), 0600))
			case "missing":
				require.NoError(t, os.Remove(journal.path))
			case "wrong-volume":
				var record walletReaderJournalRecord
				raw, err := os.ReadFile(journal.path)
				require.NoError(t, err)
				require.NoError(t, json.Unmarshal(raw, &record))
				record.Host = "different-volume"
				require.NoError(t, writeWalletReaderJournal(journal.dir, journal.path, record))
			case "oversized":
				require.NoError(t, os.WriteFile(journal.path, []byte(strings.Repeat("x", walletReaderJournalBound+1)), 0600))
			}
			require.Error(t, journal.beginRead())
		})
	}
}

func TestWalletReaderJournalFsyncFailureDoesNotAdoptPositive(t *testing.T) {
	journal := testWalletReaderJournal(t)
	require.NoError(t, journal.beginRead())
	unavailable := filepath.Join(journal.dir, "missing-mount")
	original := journal.dir
	journal.dir = unavailable
	err := journal.checkpointLocked([]byte(`{"usage":{"input_tokens":5,"output_tokens":1}}`), "llm_http_usage")
	require.Error(t, err)
	require.False(t, journal.record.Evidence.ObservedPositive)
	require.NotEmpty(t, journal.record.PendingUsage)
	journal.dir = original
	journal.release()
	require.NoError(t, journal.transfer(WalletReaderEvidence{}, func(e WalletReaderEvidence) error {
		require.True(t, e.ObservedPositive, "restored mount must retry the selected checkpoint before a zero transfer")
		require.Equal(t, 5, e.Tokens.InputTokens)
		return nil
	}))
}

func TestWalletUsageIntegerExactAndCacheSubset(t *testing.T) {
	var got WalletReaderEvidence
	observeWalletUsage([]byte(`{"usage":{"input_tokens":9007199254740993,"output_tokens":0}}`), &got)
	require.Equal(t, int64(9007199254740993), int64(got.Tokens.InputTokens))
	require.True(t, got.Valid)
	for _, raw := range []string{`{"usage":{"input_tokens":0}}`, `{"usage":{"input_tokens":9223372036854775808,"output_tokens":0}}`, `{"usage":{"prompt_tokens":2,"completion_tokens":0,"prompt_tokens_details":{"cached_tokens":3}}}`} {
		var invalid WalletReaderEvidence
		observeWalletUsage([]byte(raw), &invalid)
		require.True(t, invalid.Malformed)
		require.False(t, invalid.Valid)
	}
}

func TestWalletReaderJournalFailedTransferKeepsFirstJoinedEvidence(t *testing.T) {
	journal := testWalletReaderJournal(t)
	require.NoError(t, journal.beginRead())
	require.NoError(t, journal.checkpointLocked([]byte(`{"usage":{"input_tokens":4,"output_tokens":2}}`), "llm_http_usage"))
	journal.release()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = journal.transfer(WalletReaderEvidence{}, func(e WalletReaderEvidence) error {
			require.True(t, e.ObservedPositive)
			return errors.New("PG failed")
		})
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("journal transfer stuck")
	}
	require.NoError(t, journal.acquire(false))
	defer journal.release()
	require.NoError(t, journal.loadLocked())
	require.True(t, journal.record.Evidence.ObservedPositive)
	require.True(t, journal.record.PersistenceFailed)
}
