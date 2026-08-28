//go:build unit

package service

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func newLiveFinalizationFixture(t *testing.T, mode string) (*liveAuthTestFixture, *LiveCallRecord) {
	t.Helper()
	f := newLiveAuthTestFixture(t, mode)

	// Create and save an active provisional record
	authID := "auth_finalization_test"
	callHash := hashLiveCallID("call_finalization_test")
	now := time.Now().UTC()

	provRec := &LiveProvisionalRecord{
		Token:             authID,
		AuthorizationID:   authID,
		UserID:            f.user.ID,
		PlatformUserID:    f.user.PlatformUserID,
		BillingCurrency:   "CNY",
		BillingSnapshotID: "bsnap_test_1",
		EstimatedUnits:    10_000,
		Status:            LiveProvisionalStatusActive,
		CallHash:          callHash,
		Windows: []LiveWindow{
			{WindowSeq: 1, Token: authID + ".1", PendingUnits: 10_000},
		},
		CreatedAt:   now,
		ActivatedAt: &now,
	}
	f.provStore.records[authID] = provRec

	liveRec := &LiveCallRecord{
		CallHash:               callHash,
		UserID:                 f.user.ID,
		APIKeyID:               f.apiKey.ID,
		AccountID:              f.account.ID,
		PlatformUserID:         f.user.PlatformUserID,
		AuthorizationToken:     authID + ".1",
		AuthorizationID:        authID,
		BillingSnapshotID:      "bsnap_test_1",
		BillingCurrency:        "CNY",
		Model:                  "claude-sonnet-4",
		RateMultiplier:         1.5,
		AccountRateMultiplier:  1.25,
		ExchangeRate:           7.0,
		ExchangeRateSource:     "test",
		ExchangeRateAsOf:       now,
		InputTokens:            100,
		OutputTokens:           50,
		CacheReadTokens:        10,
		InputPricePerToken:     3e-6,
		OutputPricePerToken:    15e-6,
		CacheReadPricePerToken: 1e-6,
		LeaseID:                "lease_1",
		CreatedAt:              now,
	}
	_ = f.liveStore.SaveLiveCall(context.Background(), liveRec, time.Hour)

	return f, liveRec
}

func newLiveFinalizationWithOutboxFixture(t *testing.T, mode string) (*liveAuthTestFixture, *LiveCallRecord, sqlmock.Sqlmock) {
	t.Helper()
	f, rec := newLiveFinalizationFixture(t, mode)
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	outbox := &outboxStoreStub{}
	bridge := newCanonicalWalletBridge(canonicalWalletTestConfig(mode), f.leaseStore, f.control, db, outbox)
	f.svc.canonicalWallet = bridge
	return f, rec, mock
}

func TestTryFinalizeLiveCallNilOutboxReleasesClaimAndReturnsActive(t *testing.T) {
	f, rec := newLiveFinalizationFixture(t, config.CanonicalWalletModeShadow)

	success := f.svc.tryFinalizeLiveCall(rec)
	require.True(t, success)

	// Because outbox is nil on this bridge, observeCanonicalWalletSettlement returns false,
	// so the claim is released back to active with empty settlement_event_id.
	provRec, err := f.provStore.Get(context.Background(), rec.AuthorizationID)
	require.NoError(t, err)
	require.Equal(t, LiveProvisionalStatusActive, provRec.Status)
	require.Empty(t, provRec.SettlementEventID)

	// Live call closed in Redis store
	controller, err := f.liveStore.GetLiveController(context.Background(), rec.CallHash)
	require.NoError(t, err)
	require.Equal(t, LiveControllerClosed, controller)

	// Metrics
	m := LiveProvisionalMetricsSnapshot()
	require.Equal(t, int64(0), m.Finalized)
	require.Equal(t, int64(1), m.SettlementNotEnqueued)
}

func TestTryFinalizeLiveCallTransitionsProvisionalToFinalizedWhenEnqueueSucceeds(t *testing.T) {
	f, rec, mock := newLiveFinalizationWithOutboxFixture(t, config.CanonicalWalletModeShadow)

	var observedEvent CanonicalWalletSettlementEvent
	var observedCount atomic.Int64
	f.svc.canonicalWallet.observedForTest = func(event CanonicalWalletSettlementEvent) {
		observedEvent = event
		observedCount.Add(1)
	}

	mock.ExpectBegin()
	mock.ExpectCommit()

	success := f.svc.tryFinalizeLiveCall(rec)
	require.True(t, success)
	require.NoError(t, mock.ExpectationsWereMet())

	// Task 3 observed event assertions (MINOR-1)
	require.Equal(t, int64(1), observedCount.Load())
	require.Equal(t, rec.PlatformUserID, observedEvent.PlatformUserID)
	require.Equal(t, rec.AuthorizationToken, observedEvent.AuthorizationToken)
	require.Equal(t, rec.AuthorizationID, observedEvent.AuthorizationID)
	require.Equal(t, rec.CallHash, observedEvent.GatewayRequestID)
	actualCost := ((100-10)*3e-6 + 50*15e-6 + 10*1e-6) * 7.0 * 1.5
	expectedUnits, err := canonicalWalletUnitsFromCNY(actualCost)
	require.NoError(t, err)
	require.Equal(t, expectedUnits, observedEvent.AmountUnits)
	require.Nil(t, observedEvent.LocalBalanceAfterUnits, "Live local balance after units must be nil")

	// Provisional store record transitioned to finalized
	provRec, err := f.provStore.Get(context.Background(), rec.AuthorizationID)
	require.NoError(t, err)
	require.Equal(t, LiveProvisionalStatusFinalized, provRec.Status)
	require.NotEmpty(t, provRec.SettlementEventID)
	require.NotNil(t, provRec.TerminalAt)
	require.Greater(t, provRec.Windows[0].SettledUnits, int64(0))

	// Live call closed in Redis store
	controller, err := f.liveStore.GetLiveController(context.Background(), rec.CallHash)
	require.NoError(t, err)
	require.Equal(t, LiveControllerClosed, controller)

	// Metrics
	m := LiveProvisionalMetricsSnapshot()
	require.Equal(t, int64(1), m.Finalized)
	require.Equal(t, int64(0), m.SettlementNotEnqueued)
}

// Plan named test (a): enqueue fails -> row back at active, empty settlement_event_id, a retry observes again
func TestTryFinalizeLiveCallEnqueueFailsReleasesClaimAndRetryObservesAgain(t *testing.T) {
	f, rec, mock := newLiveFinalizationWithOutboxFixture(t, config.CanonicalWalletModeShadow)

	var observedCount atomic.Int64
	f.svc.canonicalWallet.observedForTest = func(event CanonicalWalletSettlementEvent) {
		observedCount.Add(1)
	}

	// 1. First attempt: enqueue fails (BeginTx/Commit fails or rollback)
	mock.ExpectBegin()
	mock.ExpectRollback()
	failingOutbox := &outboxStoreStub{insertErr: errors.New("outbox insert failed")}
	f.svc.canonicalWallet.outbox = failingOutbox

	success1 := f.svc.tryFinalizeLiveCall(rec)
	require.True(t, success1)
	require.NoError(t, mock.ExpectationsWereMet())
	require.Equal(t, int64(1), observedCount.Load())

	// Row is released back to active, empty settlement_event_id
	provRec1, err := f.provStore.Get(context.Background(), rec.AuthorizationID)
	require.NoError(t, err)
	require.Equal(t, LiveProvisionalStatusActive, provRec1.Status)
	require.Empty(t, provRec1.SettlementEventID)
	require.Equal(t, int64(1), LiveProvisionalMetricsSnapshot().SettlementNotEnqueued)

	// 2. Retry: enqueue succeeds -> row transitions to finalized, retry observed again
	_ = f.liveStore.SaveLiveCall(context.Background(), rec, time.Hour)
	f.svc.canonicalWallet.outbox = &outboxStoreStub{}
	mock.ExpectBegin()
	mock.ExpectCommit()

	success2 := f.svc.tryFinalizeLiveCall(rec)
	require.True(t, success2)
	require.NoError(t, mock.ExpectationsWereMet())
	require.Equal(t, int64(2), observedCount.Load(), "retry must observe again when row was active")

	provRec2, err := f.provStore.Get(context.Background(), rec.AuthorizationID)
	require.NoError(t, err)
	require.Equal(t, LiveProvisionalStatusFinalized, provRec2.Status)
	require.NotEmpty(t, provRec2.SettlementEventID)
	require.Equal(t, int64(1), LiveProvisionalMetricsSnapshot().Finalized)
}

// Plan named test (b): CompleteFinalization fails after a committed enqueue -> row stays finalizing, the retry does NOT observe again
func TestTryFinalizeLiveCallCompleteFailsKeepsFinalizingAndRetryRefusesClaim(t *testing.T) {
	f, rec, mock := newLiveFinalizationWithOutboxFixture(t, config.CanonicalWalletModeShadow)

	var observedCount atomic.Int64
	f.svc.canonicalWallet.observedForTest = func(event CanonicalWalletSettlementEvent) {
		observedCount.Add(1)
	}

	// 1. First attempt: enqueue commits, but CompleteFinalization in store fails
	mock.ExpectBegin()
	mock.ExpectCommit()
	f.provStore.completeErr = errors.New("complete finalization db timeout")

	success1 := f.svc.tryFinalizeLiveCall(rec)
	require.False(t, success1, "CompleteFinalization failure must return false to schedule retry")
	require.NoError(t, mock.ExpectationsWereMet())
	require.Equal(t, int64(1), observedCount.Load())

	// Row stays in finalizing status
	provRec1, err := f.provStore.Get(context.Background(), rec.AuthorizationID)
	require.NoError(t, err)
	require.Equal(t, LiveProvisionalStatusFinalizing, provRec1.Status, "row must remain in finalizing state")

	// 2. Retry: row is finalizing -> claimLiveProvisionalFinalization returns claimed=false,
	// observer is NOT called again, skips claim block and completes remaining close
	f.provStore.completeErr = nil
	success2 := f.svc.tryFinalizeLiveCall(rec)
	require.True(t, success2)
	require.Equal(t, int64(1), observedCount.Load(), "retry must NOT observe again when claim is not acquired")
}

func TestTryFinalizeLiveCallClaimProtectsConcurrentFinalize(t *testing.T) {
	f, rec, mock := newLiveFinalizationWithOutboxFixture(t, config.CanonicalWalletModeShadow)
	mock.ExpectBegin()
	mock.ExpectCommit()

	var wg sync.WaitGroup
	results := make([]bool, 2)

	for i := 0; i < 2; i++ {
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[idx] = f.svc.tryFinalizeLiveCall(rec)
		}()
	}
	wg.Wait()

	require.True(t, results[0])
	require.True(t, results[1])

	provRec, err := f.provStore.Get(context.Background(), rec.AuthorizationID)
	require.NoError(t, err)
	require.Equal(t, LiveProvisionalStatusFinalized, provRec.Status)
	require.Equal(t, int64(1), LiveProvisionalMetricsSnapshot().Finalized)
}

func TestTryFinalizeLiveCallClaimFailureReturnsFalse(t *testing.T) {
	f, rec := newLiveFinalizationFixture(t, config.CanonicalWalletModeShadow)

	// Cause provisional store claim to fail with error
	f.provStore.claimErr = errors.New("db connection lost")

	success := f.svc.tryFinalizeLiveCall(rec)
	require.False(t, success, "claim store error must return false to schedule retry")
}

type failingUsageBillingRepo struct {
	UsageBillingRepository
}

func (r *failingUsageBillingRepo) Apply(_ context.Context, _ *UsageBillingCommand) (*UsageBillingApplyResult, error) {
	return nil, errors.New("billing repo write error")
}

func TestTryFinalizeLiveCallBillingFailureDoesNotClaim(t *testing.T) {
	f, rec := newLiveFinalizationFixture(t, config.CanonicalWalletModeShadow)

	// Inject billing failure
	f.svc.usageBillingRepo = &failingUsageBillingRepo{}

	success := f.svc.tryFinalizeLiveCall(rec)
	require.False(t, success)

	// Claim block is not reached; row remains active
	provRec, err := f.provStore.Get(context.Background(), rec.AuthorizationID)
	require.NoError(t, err)
	require.Equal(t, LiveProvisionalStatusActive, provRec.Status)
}

func TestTryFinalizeLiveCallLegacyRecordWithoutAuthorizationTokenSucceeds(t *testing.T) {
	f, rec := newLiveFinalizationFixture(t, config.CanonicalWalletModeShadow)
	rec.AuthorizationID = ""
	rec.AuthorizationToken = ""
	_ = f.liveStore.SaveLiveCall(context.Background(), rec, time.Hour)

	success := f.svc.tryFinalizeLiveCall(rec)
	require.True(t, success)

	controller, err := f.liveStore.GetLiveController(context.Background(), rec.CallHash)
	require.NoError(t, err)
	require.Equal(t, LiveControllerClosed, controller)

	// Provisional store was not touched for legacy record
	provRec, err := f.provStore.Get(context.Background(), "auth_finalization_test")
	require.NoError(t, err)
	require.Equal(t, LiveProvisionalStatusActive, provRec.Status)
}

func TestLiveProvisionalStoreAndServiceHelpersCoverage(t *testing.T) {
	// 1. newLiveProvisionalStore
	require.Nil(t, newLiveProvisionalStore(nil))
	require.NotNil(t, newLiveProvisionalStore(&sql.DB{}))

	// 2. saveLiveProvisional edge cases
	svc := &OpenAIGatewayService{}
	rec := &LiveProvisionalRecord{Token: "test_tok", AuthorizationID: "test_auth"}
	handle := &AuthorizationHandle{ID: "test_auth"}

	// mode disabled -> false, nil
	written, err := svc.saveLiveProvisional(context.Background(), rec, handle)
	require.NoError(t, err)
	require.False(t, written)

	// mode via cfg when authorizer is nil
	svc.authorizer = nil
	svc.cfg = &config.Config{CanonicalWallet: config.CanonicalWalletConfig{Mode: config.CanonicalWalletModeShadow, RequestTimeoutMS: 500}}
	written, err = svc.saveLiveProvisional(context.Background(), rec, handle)
	require.NoError(t, err)
	require.False(t, written)

	// mode shadow with nil store -> false, nil, metric incremented
	svc.authorizer = &CanonicalWalletAuthorizer{cfg: &config.Config{CanonicalWallet: config.CanonicalWalletConfig{Mode: config.CanonicalWalletModeShadow}}}
	written, err = svc.saveLiveProvisional(context.Background(), rec, handle)
	require.NoError(t, err)
	require.False(t, written)

	// mode enforce with nil store -> false, refusal error
	svc.authorizer = &CanonicalWalletAuthorizer{cfg: &config.Config{CanonicalWallet: config.CanonicalWalletConfig{Mode: config.CanonicalWalletModeEnforce}}}
	written, err = svc.saveLiveProvisional(context.Background(), rec, handle)
	require.Error(t, err)
	require.False(t, written)

	// store save error in shadow mode -> false, nil
	provStore := newInMemoryLiveProvisionalStore()
	provStore.saveErr = errors.New("save err")
	svc.liveProvisional = provStore
	svc.authorizer = &CanonicalWalletAuthorizer{cfg: &config.Config{CanonicalWallet: config.CanonicalWalletConfig{Mode: config.CanonicalWalletModeShadow}}}
	written, err = svc.saveLiveProvisional(context.Background(), rec, handle)
	require.NoError(t, err)
	require.False(t, written)

	// store save error in enforce mode -> false, refusal error
	svc.authorizer = &CanonicalWalletAuthorizer{cfg: &config.Config{CanonicalWallet: config.CanonicalWalletConfig{Mode: config.CanonicalWalletModeEnforce}}}
	written, err = svc.saveLiveProvisional(context.Background(), rec, handle)
	require.Error(t, err)
	require.False(t, written)

	// store save success with authorizer == nil and cfg timeout
	provStore.saveErr = nil
	svc.authorizer = nil
	svc.cfg = &config.Config{CanonicalWallet: config.CanonicalWalletConfig{Mode: config.CanonicalWalletModeShadow, RequestTimeoutMS: 500}}
	written, err = svc.saveLiveProvisional(context.Background(), rec, handle)
	require.NoError(t, err)
	require.True(t, written)

	// 3. abortLiveProvisional edge cases
	svc.liveProvisional = nil
	svc.abortLiveProvisional("", nil) // nil store, empty ID -> no-op
	svc.abortLiveProvisional("tok", nil)

	svc.liveProvisional = provStore
	svc.abortLiveProvisional("", nil) // empty ID -> no-op
	provStore.abortErr = errors.New("abort failed")
	svc.abortLiveProvisional("tok", errors.New("cause")) // abort error branch logged

	// 4. activateLiveProvisional edge cases
	svc.liveProvisional = nil
	svc.activateLiveProvisional("", "") // nil store, empty ID -> no-op
	svc.activateLiveProvisional("tok", "hash")

	svc.liveProvisional = provStore
	svc.activateLiveProvisional("", "") // empty ID -> no-op
	provStore.activateErr = errors.New("activate failed")
	svc.activateLiveProvisional("tok", "hash") // activate error branch logged

	// 5. claimLiveProvisionalFinalization edge cases
	svc.liveProvisional = nil
	claimed, err := svc.claimLiveProvisionalFinalization("")
	require.NoError(t, err)
	require.False(t, claimed)
	claimed, err = svc.claimLiveProvisionalFinalization("tok")
	require.NoError(t, err)
	require.False(t, claimed)

	svc.liveProvisional = provStore
	claimed, err = svc.claimLiveProvisionalFinalization("")
	require.NoError(t, err)
	require.False(t, claimed)

	// 6. completeLiveProvisionalFinalization edge cases
	svc.liveProvisional = nil
	require.NoError(t, svc.completeLiveProvisionalFinalization("", "", 0))
	require.NoError(t, svc.completeLiveProvisionalFinalization("tok", "event", 100))

	svc.liveProvisional = provStore
	require.NoError(t, svc.completeLiveProvisionalFinalization("", "", 0))
	provStore.completeErr = errors.New("complete error")
	require.Error(t, svc.completeLiveProvisionalFinalization("tok", "event", 100))

	// 7. releaseLiveProvisionalFinalizationClaim edge cases
	svc.liveProvisional = nil
	svc.releaseLiveProvisionalFinalizationClaim("")
	svc.releaseLiveProvisionalFinalizationClaim("tok")

	svc.liveProvisional = provStore
	svc.releaseLiveProvisionalFinalizationClaim("")
	provStore.releaseErr = errors.New("release error")
	svc.releaseLiveProvisionalFinalizationClaim("tok") // release error branch logged
}
