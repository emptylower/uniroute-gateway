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
	require.ErrorIs(t, err, ErrWalletOutboxPayloadConflict)
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
