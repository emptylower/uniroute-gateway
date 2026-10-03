//go:build integration

package service_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func startLiveRestartPostgres(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()
	// Lockstep rule: any migration that adds a column to
	// wallet_settlement_outbox, wallet_live_provisional or wallet_hold_outcome
	// must be added HERE and to startCanonicalWalletTestPostgres's inline DDL.
	db := service.SharedTestPostgresDBForTest(t)

	outboxSQL, err := os.ReadFile(filepath.Join("..", "..", "migrations", "208_wallet_settlement_outbox.sql"))
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, string(outboxSQL))
	require.NoError(t, err)

	provSQL, err := os.ReadFile(filepath.Join("..", "..", "migrations", "211_wallet_live_provisional.sql"))
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, string(provSQL))
	require.NoError(t, err)

	// Phase 3.5 (migrations 212 + 214): the outbox insert writes
	// authorization_id and 214's comments name 212's dead_letter_reason.
	deadLetterSQL, err := os.ReadFile(filepath.Join("..", "..", "migrations", "212_wallet_outbox_dead_letter_reason.sql"))
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, string(deadLetterSQL))
	require.NoError(t, err)
	splitSQL, err := os.ReadFile(filepath.Join("..", "..", "migrations", "214_wallet_outbox_split_and_authorization.sql"))
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, string(splitSQL))
	require.NoError(t, err)

	// Phase 4.2-G (migration 216): outbox billing_snapshot_id and redrive_count.
	snapshotSQL, err := os.ReadFile(filepath.Join("..", "..", "migrations", "216_wallet_outbox_billing_snapshot.sql"))
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, string(snapshotSQL))
	require.NoError(t, err)

	// Migrations 209 + 218 + 219: ObserveSettlement reads wallet_authorization_segment before
	// it settles (209 is the billing-snapshot table 218 references).
	for _, migration := range []string{"209_wallet_billing_snapshot.sql", "218_wallet_authorization_segments.sql", "219_wallet_attempt_protection.sql"} {
		content, readErr := os.ReadFile(filepath.Join("..", "..", "migrations", migration))
		require.NoError(t, readErr)
		_, err = db.ExecContext(ctx, string(content))
		require.NoError(t, err)
	}

	return db
}

func startLiveRestartRedis(t *testing.T, ctx context.Context) *redis.Client {
	t.Helper()
	// Phase 3.7c: the ONE shared Redis container per run (empty keyspace
	// per call); the inline container start, its address reads and its
	// teardown cleanup are gone.
	return service.SharedTestRedisClientForTest(t)
}

func TestOpenAILiveRestartSurvivalAndRedisLossIntegration(t *testing.T) {
	service.EnableOpenAIAdvancedSchedulerForTest()
	ctx := context.Background()
	db := startLiveRestartPostgres(t, ctx)
	rdb := startLiveRestartRedis(t, ctx)

	snapService, apiKey, user, account, billing, resolver, fx := service.NewSnapshotTestFixtureForTest(t)
	user.PlatformUserID = "user_restart_test"
	apiKey.User = user

	cfg := &config.Config{}
	cfg.Default.RateMultiplier = 1
	cfg.CanonicalWallet = service.CanonicalWalletTestConfigForTest(config.CanonicalWalletModeShadow)
	cfg.CanonicalWallet.BillingSnapshotMode = "record"
	cfg.JWT.Secret = "test-jwt-secret-32-bytes-long!!!"

	realOutbox := repository.NewWalletOutboxStore(db)
	leaseStore := &service.LiveRestartLeaseStoreStub{
		Lease: &service.CanonicalWalletLease{
			LeaseID:        "lease_restart_cw",
			PlatformUserID: user.PlatformUserID,
			Currency:       "USD",
			BudgetUnits:    100_000_000,
			ExpiresAt:      time.Now().Add(5 * time.Minute),
		},
	}
	control := &service.LiveRestartControlStub{}
	bridge := service.NewCanonicalWalletBridgeForTest(t, cfg.CanonicalWallet, leaseStore, control, db, realOutbox)
	authorizer := service.NewCanonicalWalletAuthorizer(cfg, bridge, snapService)

	// --- Instance A: creates the session via CreateLiveCall over the real repository.GatewayCache ---
	cacheA := repository.NewGatewayCache(rdb)
	concurrencyCacheA := repository.NewConcurrencyCache(rdb, 15, 60)
	provStoreA := service.NewLiveProvisionalStoreForTest(db)

	callID1 := "call_restart_1"
	callHash1 := service.HashLiveCallIDForTest(callID1)

	httpUpstreamA := &service.LiveRestartHTTPUpstreamStub{
		DoFn: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Location": {"/backend-api/codex/calls/" + callID1}},
				Body:       io.NopCloser(strings.NewReader("v=0\r\n")),
			}, nil
		},
	}

	svcA := service.NewLiveRestartTestService(service.LiveRestartServiceOptions{
		Cfg:                cfg,
		Cache:              cacheA,
		LiveProvisional:    provStoreA,
		CanonicalWallet:    bridge,
		Authorizer:         authorizer,
		HTTPUpstream:       httpUpstreamA,
		AccountRepo:        &service.LiveRestartAccountRepoStub{Account: account},
		UsageBillingRepo:   &service.LiveRestartUsageBillingRepoStub{},
		UsageLogRepo:       &service.LiveRestartUsageLogRepoStub{},
		BillingService:     billing,
		Resolver:           resolver,
		ExchangeRates:      fx,
		Snapshots:          snapService,
		Attestation:        service.LiveRestartAttestationStub{HeaderValue: `{"v":1,"s":0,"t":"v1.test"}`},
		Scheduler:          &service.LiveRestartSchedulerStub{Account: account},
		ConcurrencyService: service.NewConcurrencyService(concurrencyCacheA),
	})

	req1 := &service.LiveCallRequest{
		SDP:     "v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n",
		Session: json.RawMessage(`{"model":"claude-sonnet-4","max_output_tokens":4096}`),
	}
	identity1 := service.LiveCallIdentity{
		User:                user,
		APIKey:              apiKey,
		UserID:              user.ID,
		APIKeyID:            apiKey.ID,
		GroupID:             apiKey.GroupID,
		BillingCurrency:     "CNY",
		RateMultiplier:      1.0,
		GroupRateMultiplier: 1.5,
		BillingModel:        "claude-sonnet-4",
	}

	created1, err := svcA.CreateLiveCall(ctx, req1, identity1, 5)
	require.NoError(t, err)
	require.NotNil(t, created1)
	require.Equal(t, callID1, created1.CallID)

	// --- Instance B: fresh store and cache objects over the same DB and Redis ---
	cacheB := repository.NewGatewayCache(rdb)
	concurrencyCacheB := repository.NewConcurrencyCache(rdb, 15, 60)
	provStoreB := service.NewLiveProvisionalStoreForTest(db)
	liveStoreB := cacheB.(service.LiveCallStore)

	// B recovers the LiveCallRecord from the real repository GatewayCache in Redis
	// (exercising the real hash serialization and hash fields)
	liveRec, err := liveStoreB.GetLiveCall(ctx, callHash1)
	require.NoError(t, err)
	require.Equal(t, callID1, liveRec.CallID)
	require.Equal(t, callHash1, liveRec.CallHash)
	require.NotEmpty(t, liveRec.AuthorizationToken)
	require.NotEmpty(t, liveRec.AuthorizationID)
	require.Equal(t, user.PlatformUserID, liveRec.PlatformUserID)

	// B recovers the provisional row from Postgres via GetByCallHash
	loadedProv, err := provStoreB.GetByCallHash(ctx, callHash1)
	require.NoError(t, err)
	require.Equal(t, liveRec.AuthorizationID, loadedProv.Token)
	require.Equal(t, service.LiveProvisionalStatusActive, loadedProv.Status)
	require.Len(t, loadedProv.Windows, 1)
	require.Equal(t, 1, loadedProv.Windows[0].WindowSeq)
	require.Equal(t, liveRec.AuthorizationToken, loadedProv.Windows[0].Token+".1")
	require.Equal(t, int64(0), loadedProv.Windows[0].PendingUnits)

	// B constructs fresh OpenAIGatewayService and finalizes
	svcB := service.NewLiveRestartTestService(service.LiveRestartServiceOptions{
		Cfg:                cfg,
		Cache:              cacheB,
		LiveProvisional:    provStoreB,
		CanonicalWallet:    bridge,
		Authorizer:         authorizer,
		AccountRepo:        &service.LiveRestartAccountRepoStub{Account: account},
		UsageBillingRepo:   &service.LiveRestartUsageBillingRepoStub{},
		UsageLogRepo:       &service.LiveRestartUsageLogRepoStub{},
		BillingService:     billing,
		Resolver:           resolver,
		ExchangeRates:      fx,
		Snapshots:          snapService,
		Attestation:        service.LiveRestartAttestationStub{HeaderValue: `{"v":1,"s":0,"t":"v1.test"}`},
		Scheduler:          &service.LiveRestartSchedulerStub{Account: account},
		ConcurrencyService: service.NewConcurrencyService(concurrencyCacheB),
	})

	// Simulate accumulated usage during the live call session
	liveRec.InputTokens = 1000
	liveRec.OutputTokens = 500
	require.NoError(t, liveStoreB.SaveLiveCall(ctx, liveRec, time.Hour))

	success := svcB.TryFinalizeLiveCallForTest(liveRec)
	require.True(t, success)

	// In Postgres: provisional row is now finalized with settlement event ID
	finalizedProv, err := provStoreB.Get(ctx, liveRec.AuthorizationID)
	require.NoError(t, err)
	require.Equal(t, service.LiveProvisionalStatusFinalized, finalizedProv.Status)
	require.NotEmpty(t, finalizedProv.SettlementEventID)
	require.NotNil(t, finalizedProv.TerminalAt)
	require.Greater(t, finalizedProv.Windows[0].SettledUnits, int64(0))

	// In Postgres outbox: settlement event was enqueued
	var outboxCount int
	var outboxPlatformUser string
	var outboxGatewayReqID string
	var outboxAmountUnits int64
	var outboxLocalBalance *int64
	err = db.QueryRowContext(ctx, "SELECT platform_user_id, gateway_request_id, amount_units, local_balance_after_units FROM wallet_settlement_outbox WHERE event_id = $1", finalizedProv.SettlementEventID).Scan(
		&outboxPlatformUser, &outboxGatewayReqID, &outboxAmountUnits, &outboxLocalBalance,
	)
	require.NoError(t, err)
	require.Equal(t, user.PlatformUserID, outboxPlatformUser)
	require.Equal(t, callHash1, outboxGatewayReqID)
	require.Equal(t, finalizedProv.Windows[0].SettledUnits, outboxAmountUnits)
	require.Nil(t, outboxLocalBalance, "Live settlement outbox event local_balance_after_units must be NULL")

	err = db.QueryRowContext(ctx, "SELECT count(*) FROM wallet_settlement_outbox WHERE event_id = $1", finalizedProv.SettlementEventID).Scan(&outboxCount)
	require.NoError(t, err)
	require.Equal(t, 1, outboxCount)

	// In Redis: live call is closed
	controller, err := liveStoreB.GetLiveController(ctx, callHash1)
	require.NoError(t, err)
	require.Equal(t, service.LiveControllerClosed, controller)

	// --- Redis Loss Variant ---
	callID2 := "call_restart_loss_2"
	callHash2 := service.HashLiveCallIDForTest(callID2)

	httpUpstreamA.DoFn = func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Location": {"/backend-api/codex/calls/" + callID2}},
			Body:       io.NopCloser(strings.NewReader("v=0\r\n")),
		}, nil
	}

	created2, err := svcA.CreateLiveCall(ctx, req1, identity1, 5)
	require.NoError(t, err)
	require.NotNil(t, created2)
	require.Equal(t, callID2, created2.CallID)

	lossProvBefore, err := provStoreB.GetByCallHash(ctx, callHash2)
	require.NoError(t, err)
	require.Equal(t, service.LiveProvisionalStatusActive, lossProvBefore.Status)

	// Delete from Redis to simulate Redis data loss
	redisKey := "live:call:" + callHash2
	deleted, err := rdb.Del(ctx, redisKey).Result()
	require.NoError(t, err)
	require.GreaterOrEqual(t, deleted, int64(1))

	lossLiveCall := &service.LiveCallRecord{CallHash: callHash2}
	lossFinalized := svcB.TryFinalizeLiveCallForTest(lossLiveCall)
	require.True(t, lossFinalized, "ErrLiveCallNotFound path returns true")

	// Provisional row stays active in Postgres (reconciled by Phase 4)
	lossProvAfter, err := provStoreB.Get(ctx, lossProvBefore.Token)
	require.NoError(t, err)
	require.Equal(t, service.LiveProvisionalStatusActive, lossProvAfter.Status)
}
