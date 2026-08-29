//go:build integration

package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// testWalletOutboxWorkerID is this test file's claim token. Every
// ClaimPendingOutboxEvents/MarkOutboxEvent* call in these tests passes it, so
// the ownership guards added in Task 4 Step 4 (Codex round-5 review) are
// satisfied — a test that claimed with one token cannot resolve with another.
const testWalletOutboxWorkerID = "test-worker-a"

// resetWalletOutboxTable keeps each test hermetic against the shared
// integrationDB — several tests in this file legitimately leave rows behind
// in various states (a conflict-rejected event stays pending forever;
// claimed-but-never-resolved rows stay in_flight), and a leftover PENDING
// row would be claimed by the next test's ClaimPendingOutboxEvents call,
// breaking its require.Len(..., 1) assertions. Found by actually running
// the suite: the plan's tests each pass in isolation but interfere in
// sequence.
func resetWalletOutboxTable(t *testing.T) {
	t.Helper()
	_, err := integrationDB.ExecContext(context.Background(), `DELETE FROM wallet_settlement_outbox`)
	require.NoError(t, err)
}

// What these two tests prove for real: the outbox's own INSERT genuinely
// commits (or rolls back) as one atomic unit on the `*sql.Tx` it's given —
// real, just smaller than a "same transaction as the capture" name would
// imply (there is no open transaction available at ObserveSettlement's real
// call site to share; see Task 4 Step 6's Scope correction).
func TestWalletOutboxInsertCommitsWithinItsOwnTransaction(t *testing.T) {
	ctx := context.Background()
	resetWalletOutboxTable(t)
	store := NewWalletOutboxStore(integrationDB)
	event := service.CanonicalWalletSettlementEvent{
		EventID: "gwusg_" + uuid.NewString(), GatewayRequestID: "req-" + uuid.NewString(),
		PlatformUserID: "shipany-user-" + uuid.NewString(), LeaseID: "lease-" + uuid.NewString(),
		Currency: "CNY", AmountUnits: 45_000000, OccurredAt: time.Now().UTC(),
	}

	tx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, store.InsertOutboxEventTx(ctx, tx, event))
	require.NoError(t, tx.Commit())

	pending, err := store.ClaimPendingOutboxEvents(ctx, testWalletOutboxWorkerID, 10)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, event.EventID, pending[0].EventID)
	require.Equal(t, int64(45_000000), pending[0].AmountUnits)
}

func TestWalletOutboxRollbackDiscardsEvent(t *testing.T) {
	ctx := context.Background()
	resetWalletOutboxTable(t)
	store := NewWalletOutboxStore(integrationDB)
	event := service.CanonicalWalletSettlementEvent{
		EventID: "gwusg_" + uuid.NewString(), GatewayRequestID: "req-" + uuid.NewString(),
		PlatformUserID: "shipany-user-" + uuid.NewString(), LeaseID: "lease-" + uuid.NewString(),
		Currency: "CNY", AmountUnits: 1_000000, OccurredAt: time.Now().UTC(),
	}

	tx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, store.InsertOutboxEventTx(ctx, tx, event))
	require.NoError(t, tx.Rollback()) // simulates the local capture write failing in the same tx

	pending, err := store.ClaimPendingOutboxEvents(ctx, testWalletOutboxWorkerID, 10)
	require.NoError(t, err)
	for _, p := range pending {
		require.NotEqual(t, event.EventID, p.EventID, "a rolled-back transaction must not leave a claimable outbox row")
	}
}

func TestWalletOutboxRedeliversAfterFailureWithBackoff(t *testing.T) {
	ctx := context.Background()
	resetWalletOutboxTable(t)
	store := NewWalletOutboxStore(integrationDB)
	event := service.CanonicalWalletSettlementEvent{
		EventID: "gwusg_" + uuid.NewString(), GatewayRequestID: "req-" + uuid.NewString(),
		PlatformUserID: "shipany-user-" + uuid.NewString(), LeaseID: "lease-" + uuid.NewString(),
		Currency: "CNY", AmountUnits: 2_000000, OccurredAt: time.Now().UTC(),
	}
	tx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, store.InsertOutboxEventTx(ctx, tx, event))
	require.NoError(t, tx.Commit())

	pending, err := store.ClaimPendingOutboxEvents(ctx, testWalletOutboxWorkerID, 10)
	require.NoError(t, err)
	require.Len(t, pending, 1)

	require.NoError(t, store.MarkOutboxEventFailed(ctx, pending[0].ID, testWalletOutboxWorkerID, time.Now().UTC().Add(-time.Hour))) // 1hr comfortably beats max backoff (~4.3min at 8 attempts)
	stillPending, err := store.ClaimPendingOutboxEvents(ctx, testWalletOutboxWorkerID, 10)
	require.NoError(t, err)
	require.Len(t, stillPending, 1, "a failed delivery must remain pending for redelivery")
	require.Equal(t, 1, stillPending[0].AttemptCount)
}

func TestWalletOutboxDeadLettersAfterMaxAttempts(t *testing.T) {
	ctx := context.Background()
	resetWalletOutboxTable(t)
	store := NewWalletOutboxStore(integrationDB)
	event := service.CanonicalWalletSettlementEvent{
		EventID: "gwusg_" + uuid.NewString(), GatewayRequestID: "req-" + uuid.NewString(),
		PlatformUserID: "shipany-user-" + uuid.NewString(), LeaseID: "lease-" + uuid.NewString(),
		Currency: "CNY", AmountUnits: 3_000000, OccurredAt: time.Now().UTC(),
	}
	tx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, store.InsertOutboxEventTx(ctx, tx, event))
	require.NoError(t, tx.Commit())

	pending, err := store.ClaimPendingOutboxEvents(ctx, testWalletOutboxWorkerID, 10)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	rowID := pending[0].ID

	// MarkOutboxEventFailed only acts on a row still `in_flight` and returns
	// it to `pending` on an ordinary retry, matching what a real dispatcher
	// does: claim, attempt, fail, wait, re-claim, attempt again. Simulate
	// exactly that cycle here instead of calling MarkOutboxEventFailed
	// repeatedly against a row that already left `in_flight`.
	require.NoError(t, store.MarkOutboxEventFailed(ctx, rowID, testWalletOutboxWorkerID, time.Now().UTC().Add(-time.Hour)))
	for i := 1; i < walletOutboxMaxAttempts; i++ {
		reclaimed, err := store.ClaimPendingOutboxEvents(ctx, testWalletOutboxWorkerID, 10)
		require.NoError(t, err)
		require.Len(t, reclaimed, 1, "the retried row must be reclaimable again after returning to pending")
		require.Equal(t, rowID, reclaimed[0].ID)
		require.NoError(t, store.MarkOutboxEventFailed(ctx, rowID, testWalletOutboxWorkerID, time.Now().UTC().Add(-time.Hour)))
	}

	final, err := store.ClaimPendingOutboxEvents(ctx, testWalletOutboxWorkerID, 10)
	require.NoError(t, err)
	for _, p := range final {
		require.NotEqual(t, rowID, p.ID, "an event past max attempts must move to dead_letter, not stay claimable")
	}
	status, err := store.OutboxEventStatus(ctx, rowID)
	require.NoError(t, err)
	require.Equal(t, "dead_letter", status)
}

func TestWalletOutboxRejectsConflictingPayloadUnderSameEventID(t *testing.T) {
	ctx := context.Background()
	resetWalletOutboxTable(t)
	store := NewWalletOutboxStore(integrationDB)

	// Load the actual repricing_pair_a/repricing_pair_b amounts from the
	// shared fixture rather than hardcoding literals that happen to match
	// today — a future fixture change must desync this test loudly, not
	// silently drift from the frozen protocol document.
	fixture := loadWalletOutboxFixture(t)
	pairA := findWalletOutboxFixtureCase(t, fixture, "repricing_pair_a")
	pairB := findWalletOutboxFixtureCase(t, fixture, "repricing_pair_b")
	require.Equal(t, pairA.RequestID, pairB.RequestID, "the fixture's own pair must share one request_id — that shared identity is the whole point of this test")

	requestID := pairA.RequestID
	platformUserID := "shipany-user-" + uuid.NewString()
	leaseID := "lease-" + uuid.NewString()
	eventID := service.CanonicalWalletSettlementEventID(requestID, platformUserID, "CNY")

	first := service.CanonicalWalletSettlementEvent{
		EventID: eventID, GatewayRequestID: requestID, PlatformUserID: platformUserID,
		LeaseID: leaseID, Currency: "CNY", AmountUnits: pairA.AmountUnits, OccurredAt: time.Now().UTC(),
	}
	tx1, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, store.InsertOutboxEventTx(ctx, tx1, first))
	require.NoError(t, tx1.Commit())

	// Identical retry of the SAME event — must be a no-op, not an error.
	tx2, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, store.InsertOutboxEventTx(ctx, tx2, first))
	require.NoError(t, tx2.Commit())

	// A re-priced version of the same request under the same event_id must
	// be REJECTED, not silently accepted as a second settlement.
	reprice := first
	reprice.AmountUnits = pairB.AmountUnits
	tx3, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	err = store.InsertOutboxEventTx(ctx, tx3, reprice)
	require.ErrorIs(t, err, service.ErrCanonicalWalletOutboxPayloadConflict)
	_ = tx3.Rollback()
}

func TestWalletOutboxMarkFailedActuallyAppliesExponentialBackoff(t *testing.T) {
	ctx := context.Background()
	resetWalletOutboxTable(t)
	store := NewWalletOutboxStore(integrationDB)
	event := service.CanonicalWalletSettlementEvent{
		EventID: "gwusg_" + uuid.NewString(), GatewayRequestID: "req-" + uuid.NewString(),
		PlatformUserID: "shipany-user-" + uuid.NewString(), LeaseID: "lease-" + uuid.NewString(),
		Currency: "CNY", AmountUnits: 5_000000, OccurredAt: time.Now().UTC(),
	}
	tx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, store.InsertOutboxEventTx(ctx, tx, event))
	require.NoError(t, tx.Commit())

	pending, err := store.ClaimPendingOutboxEvents(ctx, testWalletOutboxWorkerID, 10)
	require.NoError(t, err)
	require.Len(t, pending, 1)

	realNow := time.Now().UTC()
	require.NoError(t, store.MarkOutboxEventFailed(ctx, pending[0].ID, testWalletOutboxWorkerID, realNow))

	// With a real "now" and one failed attempt (backoff = 2s), the row must
	// NOT be immediately claimable — this is exactly the property the
	// original buggy version violated (it always set next_attempt_at to
	// "now," making every failure instantly retriable regardless of
	// backoff).
	stillPending, err := store.ClaimPendingOutboxEvents(ctx, testWalletOutboxWorkerID, 10)
	require.NoError(t, err)
	for _, p := range stillPending {
		require.NotEqual(t, event.EventID, p.EventID, "a freshly-failed event must respect its backoff window, not be immediately reclaimable")
	}
}

func TestWalletOutboxReclaimsStaleInFlightEvents(t *testing.T) {
	ctx := context.Background()
	resetWalletOutboxTable(t)
	store := NewWalletOutboxStore(integrationDB)
	event := service.CanonicalWalletSettlementEvent{
		EventID: "gwusg_" + uuid.NewString(), GatewayRequestID: "req-" + uuid.NewString(),
		PlatformUserID: "shipany-user-" + uuid.NewString(), LeaseID: "lease-" + uuid.NewString(),
		Currency: "CNY", AmountUnits: 7_000000, OccurredAt: time.Now().UTC(),
	}
	tx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, store.InsertOutboxEventTx(ctx, tx, event))
	require.NoError(t, tx.Commit())

	claimed, err := store.ClaimPendingOutboxEvents(ctx, testWalletOutboxWorkerID, 10)
	require.NoError(t, err)
	require.Len(t, claimed, 1, "row must be in_flight now — simulating a dispatcher that claimed it and then crashed before calling MarkOutboxEventDelivered/Failed")

	// A short staleAfter (10ms) must NOT reclaim a row claimed moments ago —
	// otherwise a still-in-progress delivery would be reclaimed and
	// delivered twice by a second dispatcher.
	reclaimedTooSoon, err := store.ReclaimStaleInFlightEvents(ctx, time.Hour)
	require.NoError(t, err)
	require.Equal(t, int64(0), reclaimedTooSoon, "a row claimed moments ago is not stale under a 1-hour threshold")

	stillInFlight, err := store.ClaimPendingOutboxEvents(ctx, testWalletOutboxWorkerID, 10)
	require.NoError(t, err)
	require.Empty(t, stillInFlight, "the row must still be in_flight, not pending, since it was not reclaimed")

	// Backdate the claim by directly manipulating claimed_at, simulating
	// real elapsed time without a real sleep (same "simulated now" pattern
	// as MarkOutboxEventFailed's own backoff tests above).
	_, err = integrationDB.ExecContext(ctx, `UPDATE wallet_settlement_outbox SET claimed_at = $1 WHERE id = $2`, time.Now().UTC().Add(-time.Hour), claimed[0].ID)
	require.NoError(t, err)

	reclaimed, err := store.ReclaimStaleInFlightEvents(ctx, time.Minute)
	require.NoError(t, err)
	require.Equal(t, int64(1), reclaimed, "a claim backdated by an hour must be reclaimed under a 1-minute staleness threshold")

	// Reclaim it under a DIFFERENT worker id, simulating the second
	// dispatcher instance that picks up after the first one crashed.
	const secondWorkerID = "test-worker-b"
	pendingAgain, err := store.ClaimPendingOutboxEvents(ctx, secondWorkerID, 10)
	require.NoError(t, err)
	require.Len(t, pendingAgain, 1, "the reclaimed row must be pending (and therefore claimable) again")
	require.Equal(t, event.EventID, pendingAgain[0].EventID)

	// The ORIGINAL (now stale) claimant must not be able to resolve a row it
	// no longer owns — this is the property `WHERE status = 'in_flight'`
	// alone could not provide, and the reason claimed_by exists at all.
	// Both of the stale owner's resolve attempts must be silent no-ops,
	// leaving the row owned by the second worker.
	require.NoError(t, store.MarkOutboxEventDelivered(ctx, pendingAgain[0].ID, testWalletOutboxWorkerID))
	status, err := store.OutboxEventStatus(ctx, pendingAgain[0].ID)
	require.NoError(t, err)
	require.Equal(t, "in_flight", status, "a stale claimant must not be able to mark a reclaimed row delivered")

	require.NoError(t, store.MarkOutboxEventFailed(ctx, pendingAgain[0].ID, testWalletOutboxWorkerID, time.Now().UTC().Add(-time.Hour)))
	status, err = store.OutboxEventStatus(ctx, pendingAgain[0].ID)
	require.NoError(t, err)
	require.Equal(t, "in_flight", status, "a stale claimant must not be able to fail/reschedule a reclaimed row either")

	// The CURRENT owner can still resolve it normally.
	require.NoError(t, store.MarkOutboxEventDelivered(ctx, pendingAgain[0].ID, secondWorkerID))
	status, err = store.OutboxEventStatus(ctx, pendingAgain[0].ID)
	require.NoError(t, err)
	require.Equal(t, "delivered", status, "the current claim owner must still be able to resolve the row")
}

type walletOutboxFixtureCase struct {
	Name        string `json:"name"`
	RequestID   string `json:"request_id"`
	AmountUnits int64  `json:"amount_units,string"` // fixture stores this as a JSON string for bigint safety; the `,string` tag tells encoding/json to parse it as a number from a string
}

type walletOutboxFixture struct {
	Cases []walletOutboxFixtureCase `json:"cases"`
}

func loadWalletOutboxFixture(t *testing.T) walletOutboxFixture {
	t.Helper()
	raw, err := os.ReadFile("testdata/wallet-lease-cny-e8-v1.json")
	require.NoError(t, err, "copy the shared fixture into testdata/ per this task's Step 1 before running this test")
	var fixture walletOutboxFixture
	require.NoError(t, json.Unmarshal(raw, &fixture))
	return fixture
}

func findWalletOutboxFixtureCase(t *testing.T, fixture walletOutboxFixture, name string) walletOutboxFixtureCase {
	t.Helper()
	for _, c := range fixture.Cases {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("wallet lease fixture is missing case %q", name)
	return walletOutboxFixtureCase{}
}

// Phase 3.4a (redesign §9.3): every dead-letter carries a PERSISTED reason.
// MarkOutboxEventDeadLetter writes the caller's reason; MarkOutboxEventFailed's
// exhaustion branch writes 'attempts_exhausted'; a row that never dead-lettered
// reads back NULL (which also covers rows predating migration 212).
func TestWalletOutboxDeadLetterReason(t *testing.T) {
	ctx := context.Background()
	resetWalletOutboxTable(t)
	store := NewWalletOutboxStore(integrationDB)

	readReason := func(id int64) sql.NullString {
		var reason sql.NullString
		require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT dead_letter_reason FROM wallet_settlement_outbox WHERE id = $1`, id).Scan(&reason))
		return reason
	}
	insertAndClaim := func(amount int64) int64 {
		event := service.CanonicalWalletSettlementEvent{
			EventID: "gwusg_" + uuid.NewString(), GatewayRequestID: "req-" + uuid.NewString(),
			PlatformUserID: "shipany-user-" + uuid.NewString(), LeaseID: "lease-" + uuid.NewString(),
			Currency: "CNY", AmountUnits: amount, OccurredAt: time.Now().UTC(),
		}
		tx, err := integrationDB.BeginTx(ctx, nil)
		require.NoError(t, err)
		require.NoError(t, store.InsertOutboxEventTx(ctx, tx, event))
		require.NoError(t, tx.Commit())
		claimed, err := store.ClaimPendingOutboxEvents(ctx, testWalletOutboxWorkerID, 10)
		require.NoError(t, err)
		require.Len(t, claimed, 1)
		return claimed[0].ID
	}

	// A row dead-lettered through the named-reason writer.
	deadLetterID := insertAndClaim(21_000000)
	require.NoError(t, store.MarkOutboxEventDeadLetter(ctx, deadLetterID, testWalletOutboxWorkerID, "balance_shortfall"))
	reason := readReason(deadLetterID)
	require.True(t, reason.Valid)
	require.Equal(t, "balance_shortfall", reason.String)

	// A row driven to walletOutboxMaxAttempts through MarkOutboxEventFailed.
	exhaustedID := insertAndClaim(22_000000)
	require.NoError(t, store.MarkOutboxEventFailed(ctx, exhaustedID, testWalletOutboxWorkerID, time.Now().UTC().Add(-time.Hour)))
	for i := 1; i < walletOutboxMaxAttempts; i++ {
		reclaimed, err := store.ClaimPendingOutboxEvents(ctx, testWalletOutboxWorkerID, 10)
		require.NoError(t, err)
		require.Len(t, reclaimed, 1)
		require.Equal(t, exhaustedID, reclaimed[0].ID)
		require.NoError(t, store.MarkOutboxEventFailed(ctx, exhaustedID, testWalletOutboxWorkerID, time.Now().UTC().Add(-time.Hour)))
	}
	status, err := store.OutboxEventStatus(ctx, exhaustedID)
	require.NoError(t, err)
	require.Equal(t, "dead_letter", status, "the exhaustion branch must have dead-lettered the row")
	reason = readReason(exhaustedID)
	require.True(t, reason.Valid)
	require.Equal(t, "attempts_exhausted", reason.String)

	// A row still pending reads back NULL.
	pendingID := insertAndClaim(23_000000) // claimed → in_flight, never resolved
	reason = readReason(pendingID)
	require.False(t, reason.Valid, "a row that never dead-lettered carries no reason")
}

func TestProvideWalletOutboxStoreSatisfiesServiceInterface(t *testing.T) {
	var provided service.CanonicalWalletOutboxStore = ProvideWalletOutboxStore(integrationDB)
	require.NotNil(t, provided)
	// The Wire-facing provider returns the interface; the concrete type
	// keeps OutboxEventStatus for test observability.
	concrete := NewWalletOutboxStore(integrationDB)
	_, err := concrete.OutboxEventStatus(ctx0(), 1)
	require.Error(t, err, "a missing row surfaces sql.ErrNoRows rather than a fake status")
}

func ctx0() context.Context { return context.Background() }

func TestWalletOutboxInsertSurfacesRealTransactionErrors(t *testing.T) {
	ctx := context.Background()
	store := NewWalletOutboxStore(integrationDB)
	event := service.CanonicalWalletSettlementEvent{
		EventID: "gwusg_" + uuid.NewString(), GatewayRequestID: "req-" + uuid.NewString(),
		PlatformUserID: "shipany-user-" + uuid.NewString(), Currency: "CNY",
		AmountUnits: 1, OccurredAt: time.Now().UTC(),
	}
	tx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	// Abort the transaction with a real PostgreSQL error first — after that,
	// even SAVEPOINT execution fails, and InsertOutboxEventTx must surface
	// that error instead of pretending the insert succeeded or conflicted.
	_, abortErr := tx.ExecContext(ctx, `SELECT 1/0`)
	require.Error(t, abortErr, "division by zero aborts a real Postgres transaction")
	err = store.InsertOutboxEventTx(ctx, tx, event)
	require.Error(t, err)
	_ = tx.Rollback()

	// A dead database surfaces real driver errors from claim and reclaim
	// paths too — no silent empty batches over a broken connection.
	deadDB, err := sql.Open("postgres", "host=127.0.0.1 port=1 user=postgres dbname=postgres sslmode=disable connect_timeout=1")
	require.NoError(t, err)
	t.Cleanup(func() { _ = deadDB.Close() })
	dead := NewWalletOutboxStore(deadDB)
	_, err = dead.ClaimPendingOutboxEvents(ctx, testWalletOutboxWorkerID, 10)
	require.Error(t, err)
	_, err = dead.ReclaimStaleInFlightEvents(ctx, time.Minute)
	require.Error(t, err)
}

// InsertOutboxEventTx must propagate any insert failure that is NOT a unique
// violation, instead of falling through to the payload-hash comparison — that
// fallback SELECT is only meaningful when the row genuinely already exists.
// `SET LOCAL search_path` makes the table unresolvable for the rest of this
// transaction only, producing a real Postgres 42P01 with no mock and no
// cleanup: the setting dies with the transaction.
func TestWalletOutboxInsertPropagatesNonUniqueFailures(t *testing.T) {
	ctx := context.Background()
	resetWalletOutboxTable(t)
	store := NewWalletOutboxStore(integrationDB)
	event := service.CanonicalWalletSettlementEvent{
		EventID: "gwusg_" + uuid.NewString(), GatewayRequestID: "req-" + uuid.NewString(),
		PlatformUserID: "shipany-user-" + uuid.NewString(), LeaseID: "lease-" + uuid.NewString(),
		Currency: "CNY", AmountUnits: 12_000000, OccurredAt: time.Now().UTC(),
	}

	tx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(ctx, `SET LOCAL search_path TO pg_catalog`)
	require.NoError(t, err)

	err = store.InsertOutboxEventTx(ctx, tx, event)
	require.Error(t, err, "a non-unique insert failure must propagate")
	require.NotErrorIs(t, err, service.ErrCanonicalWalletOutboxPayloadConflict, "a genuine insert failure must not be reported as a repricing conflict")
	require.ErrorContains(t, err, "wallet_settlement_outbox")
}

func TestWalletOutboxBindEventLease(t *testing.T) {
	ctx := context.Background()
	resetWalletOutboxTable(t)
	store := NewWalletOutboxStore(integrationDB)
	event := service.CanonicalWalletSettlementEvent{
		EventID: "gwusg_" + uuid.NewString(), GatewayRequestID: "req-" + uuid.NewString(),
		PlatformUserID: "shipany-user-" + uuid.NewString(), LeaseID: "lease-orig",
		Currency: "CNY", AmountUnits: 10_000000, OccurredAt: time.Now().UTC(),
	}

	tx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, store.InsertOutboxEventTx(ctx, tx, event))
	require.NoError(t, tx.Commit())

	claimed, err := store.ClaimPendingOutboxEvents(ctx, testWalletOutboxWorkerID, 1)
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	// Another worker attempts to bind: claim lost
	err = store.BindOutboxEventLease(ctx, claimed[0].ID, "other-worker", "lease-new")
	require.ErrorIs(t, err, service.ErrCanonicalWalletOutboxClaimLost)

	// Current owner binds: succeeds
	require.NoError(t, store.BindOutboxEventLease(ctx, claimed[0].ID, testWalletOutboxWorkerID, "lease-new"))

	var boundLease string
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT lease_id FROM wallet_settlement_outbox WHERE id = $1`, claimed[0].ID).Scan(&boundLease))
	require.Equal(t, "lease-new", boundLease)

	// Dead DB surfaces Exec error
	deadDB, err := sql.Open("postgres", "host=127.0.0.1 port=1 user=postgres dbname=postgres sslmode=disable connect_timeout=1")
	require.NoError(t, err)
	t.Cleanup(func() { _ = deadDB.Close() })
	dead := NewWalletOutboxStore(deadDB)
	err = dead.BindOutboxEventLease(ctx, claimed[0].ID, testWalletOutboxWorkerID, "lease-new")
	require.Error(t, err)
}

// --- Phase 3.5 (redesign §11.9 + the durable parts of §11.3/§11.4) ---

// splitFixture inserts one claimed in_flight row and returns its id. The
// occurred_at is truncated to microsecond precision so the fixture hash
// computed in Go survives the Postgres TIMESTAMPTZ round trip byte-for-byte
// (RFC3339Nano trims trailing zeros; ns below µs would be dropped by the
// driver, changing the formatted string).
func splitFixture(t *testing.T, store *WalletOutboxStore, eventID string, amount int64, authID string, occurredAt time.Time) int64 {
	t.Helper()
	ctx := context.Background()
	event := service.CanonicalWalletSettlementEvent{
		EventID: eventID, GatewayRequestID: "req-" + eventID, PlatformUserID: "shipany-user-split",
		LeaseID: "lease-split", Currency: "CNY", AmountUnits: amount, OccurredAt: occurredAt,
		AuthorizationID: authID,
	}
	tx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, store.InsertOutboxEventTx(ctx, tx, event))
	require.NoError(t, tx.Commit())
	claimed, err := store.ClaimPendingOutboxEvents(ctx, testWalletOutboxWorkerID, 10)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.Equal(t, eventID, claimed[0].EventID)
	return claimed[0].ID
}

// splitRowState reads every split-relevant column of one row.
type splitRowState struct {
	Status         string
	AmountUnits    int64
	PendingRelease sql.NullInt64
	ClaimedBy      sql.NullString
	ClaimedAt      sql.NullTime
	NextAttemptAt  time.Time
	DeliveredAt    sql.NullTime
	PayloadHash    string
	ParentEventID  sql.NullString
	SplitDepth     int
	Authorization  sql.NullString
	AttemptCount   int
}

func readSplitRow(t *testing.T, byEventID string) splitRowState {
	t.Helper()
	var s splitRowState
	require.NoError(t, integrationDB.QueryRowContext(context.Background(), `
		SELECT status, amount_units, pending_release_units, claimed_by, claimed_at, next_attempt_at, delivered_at,
		       payload_hash, parent_event_id, split_depth, authorization_id, attempt_count
		FROM wallet_settlement_outbox WHERE event_id = $1`, byEventID,
	).Scan(&s.Status, &s.AmountUnits, &s.PendingRelease, &s.ClaimedBy, &s.ClaimedAt, &s.NextAttemptAt, &s.DeliveredAt,
		&s.PayloadHash, &s.ParentEventID, &s.SplitDepth, &s.Authorization, &s.AttemptCount))
	return s
}

// (a) authorization_id is persisted, and the payload hash does NOT cover it:
// the same event with and without an AuthorizationID hashes identically.
func TestWalletOutboxAuthorizationColumnAndHashIdentity(t *testing.T) {
	ctx := context.Background()
	resetWalletOutboxTable(t)
	store := NewWalletOutboxStore(integrationDB)
	occurredAt := time.Now().UTC().Truncate(time.Microsecond)

	withAuth := splitFixture(t, store, "gwusg_split_a", 10_000000, "auth-35", occurredAt)

	var authBack sql.NullString
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT authorization_id FROM wallet_settlement_outbox WHERE id = $1`, withAuth).Scan(&authBack))
	require.True(t, authBack.Valid)
	require.Equal(t, "auth-35", authBack.String)

	// A second event with an IDENTICAL payload (event_id differs — it is not
	// hashed; every other hashed field is the same) and NO authorization: the
	// stored payload_hash must be identical. splitFixture derives
	// GatewayRequestID from the event id, so this one is inserted by hand
	// with the SAME request id as the first fixture.
	event := service.CanonicalWalletSettlementEvent{
		EventID: "gwusg_split_b", GatewayRequestID: "req-gwusg_split_a", PlatformUserID: "shipany-user-split",
		LeaseID: "lease-split", Currency: "CNY", AmountUnits: 10_000000, OccurredAt: occurredAt,
	}
	tx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, store.InsertOutboxEventTx(ctx, tx, event))
	require.NoError(t, tx.Commit())

	hashWith := readSplitRow(t, "gwusg_split_a").PayloadHash
	hashWithout := readSplitRow(t, "gwusg_split_b").PayloadHash
	require.Equal(t, hashWith, hashWithout, "authorization_id must not be part of the payload hash (§11.9)")

	authB := readSplitRow(t, "gwusg_split_b").Authorization
	require.False(t, authB.Valid, "an event without an authorization stores NULL, not an empty string")
}

// (b) the split transaction: remainder insert, parent rewrite, no
// payload_hash touch, claim ownership, the no-op re-run, ClearPendingRelease.
func TestWalletOutboxSplitEventTransaction(t *testing.T) {
	ctx := context.Background()
	resetWalletOutboxTable(t)
	store := NewWalletOutboxStore(integrationDB)
	occurredAt := time.Now().UTC().Truncate(time.Microsecond)
	const A = int64(10_000000)
	const H = int64(4_000000)
	id := splitFixture(t, store, "gwusg_split_e", A, "auth-35", occurredAt)
	hashBefore := readSplitRow(t, "gwusg_split_e").PayloadHash

	// A worker that does not own the row: claim lost, nothing changes.
	_, err := store.SplitOutboxEvent(ctx, id, "other-worker", H, "gwusg_split_e:r1", true)
	require.ErrorIs(t, err, service.ErrCanonicalWalletOutboxClaimLost)
	untouched := readSplitRow(t, "gwusg_split_e")
	require.Equal(t, "in_flight", untouched.Status)
	require.Equal(t, A, untouched.AmountUnits)

	remainderUnits, err := store.SplitOutboxEvent(ctx, id, testWalletOutboxWorkerID, H, "gwusg_split_e:r1", true)
	require.NoError(t, err)
	require.Equal(t, A-H, remainderUnits, "the split returns the remainder's amount")

	parent := readSplitRow(t, "gwusg_split_e")
	require.Equal(t, "pending", parent.Status, "the parent returns to pending — the next tick delivers H (§11.3)")
	require.Equal(t, H, parent.AmountUnits)
	require.True(t, parent.PendingRelease.Valid)
	require.Equal(t, A-H, parent.PendingRelease.Int64, "pending_release_units records what is owed back to the lease")
	require.False(t, parent.ClaimedBy.Valid, "claimed_by is NULL again")
	require.False(t, parent.ClaimedAt.Valid, "claimed_at is NULL again")
	require.LessOrEqual(t, parent.NextAttemptAt, time.Now().UTC().Add(time.Second), "next_attempt_at <= now — deliberately no attempt increment and no backoff")
	require.Equal(t, 0, parent.AttemptCount, "the split consumes no attempt")
	require.Equal(t, hashBefore, parent.PayloadHash, "the split NEVER touches the parent's payload_hash")
	require.Empty(t, parent.ParentEventID.String)
	require.Equal(t, 0, parent.SplitDepth)
	require.Equal(t, "auth-35", parent.Authorization.String)

	// The remainder row: a distinct event whose hash is the hash of the
	// remainder AS AN EVENT (its own id, its own amount, unbound).
	remainder := readSplitRow(t, "gwusg_split_e:r1")
	require.Equal(t, "pending", remainder.Status)
	require.Equal(t, A-H, remainder.AmountUnits)
	require.True(t, remainder.ParentEventID.Valid)
	require.Equal(t, "gwusg_split_e", remainder.ParentEventID.String)
	require.Equal(t, 1, remainder.SplitDepth)
	require.False(t, remainder.ClaimedBy.Valid)
	require.Equal(t, 0, remainder.AttemptCount)
	require.Equal(t, "auth-35", remainder.Authorization.String, "authorization_id is copied to the remainder (§11.3)")
	expectedRemainderHash := walletOutboxPayloadHash(service.CanonicalWalletSettlementEvent{
		EventID: "gwusg_split_e:r1", GatewayRequestID: "req-gwusg_split_e", PlatformUserID: "shipany-user-split",
		LeaseID: "", Currency: "CNY", AmountUnits: A - H, OccurredAt: occurredAt,
	})
	require.Equal(t, expectedRemainderHash, remainder.PayloadHash)

	// The no-op re-run: re-claim the parent, split again with the SAME
	// remainder event id — the unique event_id makes it a no-op and the
	// parent is untouched (a second rewrite would subtract H twice).
	claimed, err := store.ClaimPendingOutboxEvents(ctx, testWalletOutboxWorkerID, 10)
	require.NoError(t, err)
	require.Len(t, claimed, 2, "the parent (pending again) and the remainder are both claimable")
	again, err := store.SplitOutboxEvent(ctx, id, testWalletOutboxWorkerID, H, "gwusg_split_e:r1", true)
	require.NoError(t, err)
	require.Equal(t, A-H, again)
	var remainderRows int
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT count(*) FROM wallet_settlement_outbox WHERE event_id = 'gwusg_split_e:r1'`).Scan(&remainderRows))
	require.Equal(t, 1, remainderRows, "no second remainder row")
	afterNoop := readSplitRow(t, "gwusg_split_e")
	require.Equal(t, H, afterNoop.AmountUnits, "the parent's amount is not rewritten a second time")
	require.True(t, afterNoop.PendingRelease.Valid)
	require.Equal(t, A-H, afterNoop.PendingRelease.Int64)
	require.Equal(t, hashBefore, afterNoop.PayloadHash)

	// (c) ClearPendingRelease nulls the column.
	require.NoError(t, store.ClearPendingRelease(ctx, id))
	cleared := readSplitRow(t, "gwusg_split_e")
	require.False(t, cleared.PendingRelease.Valid)
}

// drainPending resolves every currently-pending row so the next
// splitFixture's claim sees exactly its own row (the H=0 split leaves a
// claimable remainder behind).
func drainPending(t *testing.T, store *WalletOutboxStore) {
	t.Helper()
	ctx := context.Background()
	for {
		claimed, err := store.ClaimPendingOutboxEvents(ctx, testWalletOutboxWorkerID, 10)
		require.NoError(t, err)
		if len(claimed) == 0 {
			return
		}
		for _, e := range claimed {
			require.NoError(t, store.MarkOutboxEventDelivered(ctx, e.ID, testWalletOutboxWorkerID))
		}
	}
}

// The H = 0 leg (split_full: the parent is delivered in the transaction) and
// the proactive form (reserved = false: nothing reserved yet, so nothing is
// owed — pending_release_units stays NULL).
func TestWalletOutboxSplitZeroHeadroomAndProactiveForm(t *testing.T) {
	ctx := context.Background()
	resetWalletOutboxTable(t)
	store := NewWalletOutboxStore(integrationDB)
	occurredAt := time.Now().UTC().Truncate(time.Microsecond)
	const A = int64(10_000000)
	const H = int64(4_000000)

	// H = 0, reserved: the parent is delivered with pending_release_units = A.
	idZ := splitFixture(t, store, "gwusg_split_z", A, "auth-35", occurredAt)
	remainderUnits, err := store.SplitOutboxEvent(ctx, idZ, testWalletOutboxWorkerID, 0, "gwusg_split_z:r1", true)
	require.NoError(t, err)
	require.Equal(t, A, remainderUnits)
	parentZ := readSplitRow(t, "gwusg_split_z")
	require.Equal(t, "delivered", parentZ.Status, "H = 0 marks the parent delivered — split_full")
	require.True(t, parentZ.DeliveredAt.Valid, "delivered_at is set")
	require.Equal(t, int64(0), parentZ.AmountUnits)
	require.True(t, parentZ.PendingRelease.Valid)
	require.Equal(t, A, parentZ.PendingRelease.Int64)
	require.False(t, parentZ.ClaimedBy.Valid)
	require.Equal(t, A, readSplitRow(t, "gwusg_split_z:r1").AmountUnits, "the remainder carries the whole amount")
	drainPending(t, store)

	// H = 0, proactive: delivered with pending_release_units NULL.
	idZN := splitFixture(t, store, "gwusg_split_zn", A, "", occurredAt)
	_, err = store.SplitOutboxEvent(ctx, idZN, testWalletOutboxWorkerID, 0, "gwusg_split_zn:r1", false)
	require.NoError(t, err)
	parentZN := readSplitRow(t, "gwusg_split_zn")
	require.Equal(t, "delivered", parentZN.Status)
	require.False(t, parentZN.PendingRelease.Valid, "reserved = false: nothing was reserved, nothing is owed")
	drainPending(t, store)

	// H > 0, proactive: back to pending with pending_release_units NULL.
	idP := splitFixture(t, store, "gwusg_split_p", A, "", occurredAt)
	remainderUnits, err = store.SplitOutboxEvent(ctx, idP, testWalletOutboxWorkerID, H, "gwusg_split_p:r1", false)
	require.NoError(t, err)
	require.Equal(t, A-H, remainderUnits)
	parentP := readSplitRow(t, "gwusg_split_p")
	require.Equal(t, "pending", parentP.Status)
	require.Equal(t, H, parentP.AmountUnits)
	require.False(t, parentP.PendingRelease.Valid, "the proactive split records no pending release")
}

// (d) ClaimPendingOutboxEvents returns the four new columns.
func TestWalletOutboxClaimReturnsSplitColumns(t *testing.T) {
	ctx := context.Background()
	resetWalletOutboxTable(t)
	store := NewWalletOutboxStore(integrationDB)
	occurredAt := time.Now().UTC().Truncate(time.Microsecond)
	const A = int64(10_000000)
	const H = int64(4_000000)
	id := splitFixture(t, store, "gwusg_split_d", A, "auth-35", occurredAt)
	_, err := store.SplitOutboxEvent(ctx, id, testWalletOutboxWorkerID, H, "gwusg_split_d:r1", true)
	require.NoError(t, err)

	claimed, err := store.ClaimPendingOutboxEvents(ctx, testWalletOutboxWorkerID, 10)
	require.NoError(t, err)
	require.Len(t, claimed, 2)
	byEvent := map[string]service.CanonicalWalletOutboxEvent{}
	for _, e := range claimed {
		byEvent[e.EventID] = e
	}

	parent := byEvent["gwusg_split_d"]
	require.Empty(t, parent.ParentEventID)
	require.Equal(t, 0, parent.SplitDepth)
	require.Equal(t, "auth-35", parent.AuthorizationID)
	require.NotNil(t, parent.PendingReleaseUnits)
	require.Equal(t, A-H, *parent.PendingReleaseUnits)

	remainder := byEvent["gwusg_split_d:r1"]
	require.Equal(t, "gwusg_split_d", remainder.ParentEventID)
	require.Equal(t, 1, remainder.SplitDepth)
	require.Equal(t, "auth-35", remainder.AuthorizationID)
	require.Nil(t, remainder.PendingReleaseUnits)
}

// (e) SumDeadLetterUnits is the receivable figure (§11.4): the sum of
// amount_units over dead-letter rows carrying the named reason.
func TestWalletOutboxSumDeadLetterUnits(t *testing.T) {
	ctx := context.Background()
	resetWalletOutboxTable(t)
	store := NewWalletOutboxStore(integrationDB)
	occurredAt := time.Now().UTC().Truncate(time.Microsecond)

	idA := splitFixture(t, store, "gwusg_dl_a", 3_000000, "", occurredAt)
	idB := splitFixture(t, store, "gwusg_dl_b", 5_000000, "", occurredAt)
	idC := splitFixture(t, store, "gwusg_dl_c", 7_000000, "", occurredAt)
	require.NoError(t, store.MarkOutboxEventDeadLetter(ctx, idA, testWalletOutboxWorkerID, "balance_shortfall"))
	require.NoError(t, store.MarkOutboxEventDeadLetter(ctx, idB, testWalletOutboxWorkerID, "balance_shortfall"))
	require.NoError(t, store.MarkOutboxEventDeadLetter(ctx, idC, testWalletOutboxWorkerID, "attempts_exhausted"))

	sum, err := store.SumDeadLetterUnits(ctx, "balance_shortfall")
	require.NoError(t, err)
	require.Equal(t, int64(8_000000), sum, "only the named reason's rows are summed")

	sum, err = store.SumDeadLetterUnits(ctx, "contract_violation")
	require.NoError(t, err)
	require.Equal(t, int64(0), sum, "an unknown reason sums to zero, not an error")
}
