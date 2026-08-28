//go:build unit

package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

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

func TestTryFinalizeLiveCallTransitionsProvisionalToFinalized(t *testing.T) {
	f, rec := newLiveFinalizationFixture(t, config.CanonicalWalletModeShadow)

	success := f.svc.tryFinalizeLiveCall(rec)
	require.True(t, success)

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
	require.Equal(t, int64(0), m.StoreUnavailable)
}

func TestTryFinalizeLiveCallClaimProtectsConcurrentFinalize(t *testing.T) {
	f, rec := newLiveFinalizationFixture(t, config.CanonicalWalletModeShadow)

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

func TestTryFinalizeLiveCallStoreOutageDoesNotBlockBilling(t *testing.T) {
	f, rec := newLiveFinalizationFixture(t, config.CanonicalWalletModeShadow)

	// Cause provisional store claim to fail
	f.provStore.saveErr = errors.New("db connection lost")

	success := f.svc.tryFinalizeLiveCall(rec)
	require.True(t, success, "store outage must not block billing finalization")

	controller, err := f.liveStore.GetLiveController(context.Background(), rec.CallHash)
	require.NoError(t, err)
	require.Equal(t, LiveControllerClosed, controller)

	m := LiveProvisionalMetricsSnapshot()
	require.GreaterOrEqual(t, m.StoreUnavailable, int64(1))
}

type failingUsageBillingRepo struct {
	UsageBillingRepository
}

func (r *failingUsageBillingRepo) Apply(_ context.Context, _ *UsageBillingCommand) (*UsageBillingApplyResult, error) {
	return nil, errors.New("billing repo write error")
}

func TestTryFinalizeLiveCallBillingFailureReleasesClaim(t *testing.T) {
	f, rec := newLiveFinalizationFixture(t, config.CanonicalWalletModeShadow)

	// Inject billing failure
	f.svc.usageBillingRepo = &failingUsageBillingRepo{}

	success := f.svc.tryFinalizeLiveCall(rec)
	require.False(t, success)

	// Claim must be released back to active
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
