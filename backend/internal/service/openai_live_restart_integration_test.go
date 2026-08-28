//go:build integration

package service

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

func startLiveRestartPostgres(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()
	container, err := tcpostgres.Run(ctx, "postgres:18.1-alpine3.23",
		tcpostgres.WithDatabase("live_restart_test"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		tcpostgres.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.PingContext(ctx))

	_, err = db.ExecContext(ctx, `
		CREATE TABLE wallet_live_provisional (
			token TEXT PRIMARY KEY,
			authorization_id TEXT NOT NULL,
			call_hash TEXT NOT NULL DEFAULT '',
			platform_user_id TEXT NOT NULL,
			user_id BIGINT NOT NULL,
			api_key_id BIGINT NOT NULL,
			account_id BIGINT NOT NULL,
			billing_currency TEXT NOT NULL,
			billing_snapshot_id TEXT NOT NULL,
			estimated_units BIGINT NOT NULL,
			status TEXT NOT NULL,
			windows JSONB NOT NULL DEFAULT '[]'::jsonb,
			settlement_event_id TEXT NOT NULL DEFAULT '',
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			activated_at TIMESTAMPTZ,
			terminal_at TIMESTAMPTZ
		);
		CREATE UNIQUE INDEX idx_wallet_live_provisional_call_hash
			ON wallet_live_provisional (call_hash)
			WHERE call_hash <> '';

		CREATE TABLE wallet_settlement_outbox (
			id BIGSERIAL PRIMARY KEY,
			event_id TEXT NOT NULL UNIQUE,
			platform_user_id TEXT NOT NULL,
			lease_id TEXT,
			gateway_request_id TEXT NOT NULL,
			currency TEXT NOT NULL,
			amount_units BIGINT NOT NULL,
			local_balance_after_units BIGINT,
			payload_hash TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'pending',
			attempt_count INT NOT NULL DEFAULT 0,
			next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			claimed_at TIMESTAMPTZ,
			claimed_by TEXT,
			occurred_at TIMESTAMPTZ NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			delivered_at TIMESTAMPTZ
		);
	`)
	require.NoError(t, err)
	return db
}

func startLiveRestartRedis(t *testing.T, ctx context.Context) *redis.Client {
	t.Helper()
	container, err := tcredis.Run(ctx, "redis:8.4-alpine")
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "6379/tcp")
	require.NoError(t, err)

	client := redis.NewClient(&redis.Options{Addr: fmt.Sprintf("%s:%d", host, port.Int())})
	t.Cleanup(func() { _ = client.Close() })
	require.NoError(t, client.Ping(ctx).Err())
	return client
}

type redisLiveStoreForTest struct {
	GatewayCache
	rdb *redis.Client
}

func (s *redisLiveStoreForTest) liveCallKey(callHash string) string {
	return "openai:live:call:" + callHash
}

func (s *redisLiveStoreForTest) SaveLiveCall(ctx context.Context, record *LiveCallRecord, ttl time.Duration) error {
	if record == nil || record.CallHash == "" || record.CallID == "" {
		return fmt.Errorf("invalid live call record")
	}
	values := map[string]any{
		"call_id":                    record.CallID,
		"account_id":                 record.AccountID,
		"api_key_id":                 record.APIKeyID,
		"user_id":                    record.UserID,
		"group_id":                   record.GroupID,
		"subscription_id":            record.SubscriptionID,
		"lease_id":                   record.LeaseID,
		"model":                      record.Model,
		"input_tokens":               record.InputTokens,
		"output_tokens":              record.OutputTokens,
		"cache_read_tokens":          record.CacheReadTokens,
		"billing_currency":           record.BillingCurrency,
		"rate_multiplier":            record.RateMultiplier,
		"group_rate_multiplier":      record.GroupRateMultiplier,
		"account_rate_multiplier":    record.AccountRateMultiplier,
		"exchange_rate":              record.ExchangeRate,
		"exchange_rate_source":       record.ExchangeRateSource,
		"exchange_rate_as_of":        record.ExchangeRateAsOf.UnixMilli(),
		"api_key_quota":              record.APIKeyQuota,
		"rate_limit_5h":              record.RateLimit5h,
		"rate_limit_1d":              record.RateLimit1d,
		"rate_limit_7d":              record.RateLimit7d,
		"subscription_billing":       record.SubscriptionBilling,
		"input_price_per_token":      record.InputPricePerToken,
		"output_price_per_token":     record.OutputPricePerToken,
		"cache_read_price_per_token": record.CacheReadPricePerToken,
		"billing_snapshot_id":        record.BillingSnapshotID,
		"authorization_token":        record.AuthorizationToken,
		"authorization_id":           record.AuthorizationID,
		"platform_user_id":           record.PlatformUserID,
		"created_at":                 record.CreatedAt.UnixMilli(),
		"expires_at":                 record.ExpiresAt.UnixMilli(),
		"controller":                 record.Controller,
		"controller_owner":           record.ControllerOwner,
		"user_agent":                 record.UserAgent,
		"ip_address":                 record.IPAddress,
		"inbound_endpoint":           record.InboundEndpoint,
		"attestation":                record.AttestationCiphertext,
	}
	key := s.liveCallKey(record.CallHash)
	pipe := s.rdb.TxPipeline()
	pipe.HSet(ctx, key, values)
	pipe.Expire(ctx, key, ttl)
	_, err := pipe.Exec(ctx)
	return err
}

func (s *redisLiveStoreForTest) GetLiveCall(ctx context.Context, callHash string) (*LiveCallRecord, error) {
	values, err := s.rdb.HGetAll(ctx, s.liveCallKey(callHash)).Result()
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, ErrLiveCallNotFound
	}
	parseInt := func(field string) int64 {
		value, _ := strconv.ParseInt(values[field], 10, 64)
		return value
	}
	parseFloat := func(field string) float64 {
		value, _ := strconv.ParseFloat(values[field], 64)
		return value
	}
	return &LiveCallRecord{
		CallID:                 values["call_id"],
		CallHash:               callHash,
		AccountID:              parseInt("account_id"),
		APIKeyID:               parseInt("api_key_id"),
		UserID:                 parseInt("user_id"),
		GroupID:                parseInt("group_id"),
		SubscriptionID:         parseInt("subscription_id"),
		LeaseID:                values["lease_id"],
		Model:                  values["model"],
		InputTokens:            int(parseInt("input_tokens")),
		OutputTokens:           int(parseInt("output_tokens")),
		CacheReadTokens:        int(parseInt("cache_read_tokens")),
		BillingCurrency:        values["billing_currency"],
		RateMultiplier:         parseFloat("rate_multiplier"),
		GroupRateMultiplier:    parseFloat("group_rate_multiplier"),
		AccountRateMultiplier:  parseFloat("account_rate_multiplier"),
		ExchangeRate:           parseFloat("exchange_rate"),
		ExchangeRateSource:     values["exchange_rate_source"],
		ExchangeRateAsOf:       time.UnixMilli(parseInt("exchange_rate_as_of")),
		APIKeyQuota:            parseFloat("api_key_quota"),
		RateLimit5h:            parseFloat("rate_limit_5h"),
		RateLimit1d:            parseFloat("rate_limit_1d"),
		RateLimit7d:            parseFloat("rate_limit_7d"),
		SubscriptionBilling:    values["subscription_billing"] == "1" || values["subscription_billing"] == "true",
		InputPricePerToken:     parseFloat("input_price_per_token"),
		OutputPricePerToken:    parseFloat("output_price_per_token"),
		CacheReadPricePerToken: parseFloat("cache_read_price_per_token"),
		BillingSnapshotID:      values["billing_snapshot_id"],
		AuthorizationToken:     values["authorization_token"],
		AuthorizationID:        values["authorization_id"],
		PlatformUserID:         values["platform_user_id"],
		CreatedAt:              time.UnixMilli(parseInt("created_at")),
		ExpiresAt:              time.UnixMilli(parseInt("expires_at")),
		Controller:             values["controller"],
		ControllerOwner:        values["controller_owner"],
	}, nil
}

func (s *redisLiveStoreForTest) ClaimLiveController(ctx context.Context, callHash, controller, owner string) (bool, error) {
	key := s.liveCallKey(callHash)
	res, err := s.rdb.HSet(ctx, key, "controller", controller, "controller_owner", owner).Result()
	return res >= 0, err
}

func (s *redisLiveStoreForTest) ReleaseLiveController(ctx context.Context, callHash, owner string) (bool, error) {
	key := s.liveCallKey(callHash)
	res, err := s.rdb.HSet(ctx, key, "controller", LiveControllerPending, "controller_owner", "").Result()
	return res >= 0, err
}

func (s *redisLiveStoreForTest) GetLiveController(ctx context.Context, callHash string) (string, error) {
	val, err := s.rdb.HGet(ctx, s.liveCallKey(callHash), "controller").Result()
	if err == redis.Nil {
		return "", ErrLiveCallNotFound
	}
	return val, err
}

func (s *redisLiveStoreForTest) MarkLiveCallClosed(ctx context.Context, callHash string, ttl time.Duration) (bool, error) {
	key := s.liveCallKey(callHash)
	pipe := s.rdb.TxPipeline()
	pipe.HSet(ctx, key, "controller", LiveControllerClosed, "controller_owner", "")
	pipe.Expire(ctx, key, ttl)
	_, err := pipe.Exec(ctx)
	return err == nil, err
}

func (s *redisLiveStoreForTest) GetSessionAccountID(context.Context, int64, string) (int64, error) {
	return 0, nil
}
func (s *redisLiveStoreForTest) SetSessionAccountID(context.Context, int64, string, int64, time.Duration) error {
	return nil
}
func (s *redisLiveStoreForTest) DeleteSessionAccountID(context.Context, int64, string) error {
	return nil
}
func (s *redisLiveStoreForTest) RefreshSessionAccountTTL(context.Context, int64, string, time.Duration) error {
	return nil
}
func (s *redisLiveStoreForTest) GetAccountLoad(context.Context, int64) (*AccountLoadInfo, error) {
	return &AccountLoadInfo{}, nil
}

func (s *redisLiveStoreForTest) AccumulateLiveUsage(context.Context, string, string, int, int, int) (bool, error) {
	return true, nil
}

func newLiveRestartSnapshotFixture(t testing.TB) (*BillingSnapshotService, *APIKey, *User, *Account) {
	t.Helper()
	cfg := &config.Config{}
	cfg.Default.RateMultiplier = 1
	cfg.CanonicalWallet.BillingSnapshotMode = "record"
	cfg.Billing.ExchangeRate.BootstrapUSDToCNY = 7.0
	billing := NewBillingService(&config.Config{}, nil)
	resolver := NewModelPricingResolver(nil, billing)
	fx := NewExchangeRateService(cfg)
	svc := NewBillingSnapshotService(cfg, resolver, billing, fx, nil)
	gid := int64(7)
	group := &Group{ID: 7, RateMultiplier: 1.5}
	user := &User{ID: 42, BillingCurrency: "CNY", PlatformUserID: "user_restart_test"}
	apiKey := &APIKey{ID: 11, GroupID: &gid, Group: group, User: user}
	rate := 1.25
	account := &Account{ID: 99, RateMultiplier: &rate, Platform: PlatformOpenAI}
	return svc, apiKey, user, account
}

func TestOpenAILiveRestartSurvivalAndRedisLossIntegration(t *testing.T) {
	ctx := context.Background()
	db := startLiveRestartPostgres(t, ctx)
	rdb := startLiveRestartRedis(t, ctx)

	snapService, apiKey, user, account := newLiveRestartSnapshotFixture(t)
	user.PlatformUserID = "user_restart_test"
	apiKey.User = user

	cfg := &config.Config{}
	cfg.Default.RateMultiplier = 1
	cfg.CanonicalWallet = canonicalWalletTestConfig(config.CanonicalWalletModeShadow)
	cfg.CanonicalWallet.BillingSnapshotMode = "record"
	cfg.JWT.Secret = "test-jwt-secret-32-bytes-long!!!"

	outbox := &outboxStoreForTest{db: db}
	bridge := newCanonicalWalletBridge(cfg.CanonicalWallet, &canonicalWalletStoreStub{}, &canonicalWalletControlStub{}, db, outbox)
	authorizer := NewCanonicalWalletAuthorizer(cfg, bridge, snapService)

	account.Platform = PlatformOpenAI
	account.Type = AccountTypeOAuth
	account.Concurrency = 2
	account.Credentials = map[string]any{"access_token": "test-token"}

	// --- Instance A: creates the session ---
	provStoreA := newLiveProvisionalStore(db)
	cacheA := &redisLiveStoreForTest{rdb: rdb}

	authID := "auth_restart_1"
	callHash := hashLiveCallID("call_restart_1")
	now := time.Now().UTC().Truncate(time.Millisecond)

	rec := &LiveProvisionalRecord{
		Token:             authID,
		AuthorizationID:   authID,
		CallHash:          callHash,
		PlatformUserID:    user.PlatformUserID,
		UserID:            user.ID,
		APIKeyID:          apiKey.ID,
		AccountID:         account.ID,
		BillingCurrency:   "CNY",
		BillingSnapshotID: "bsnap_test_restart",
		EstimatedUnits:    10_000,
		Status:            LiveProvisionalStatusProvisional,
		Windows: []LiveWindow{
			{WindowSeq: 1, Token: authID + ".1", PendingUnits: 10_000},
		},
		CreatedAt:   now,
		ActivatedAt: &now,
	}
	require.NoError(t, provStoreA.Save(ctx, rec))
	require.NoError(t, provStoreA.Activate(ctx, authID, callHash, now))

	liveRec := &LiveCallRecord{
		CallID:                 "call_restart_1",
		CallHash:               callHash,
		AccountID:              account.ID,
		APIKeyID:               apiKey.ID,
		UserID:                 user.ID,
		PlatformUserID:         user.PlatformUserID,
		AuthorizationToken:     authID + ".1",
		AuthorizationID:        authID,
		BillingSnapshotID:      "bsnap_test_restart",
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
		LeaseID:                "lease_restart_1",
		Controller:             LiveControllerPending,
		CreatedAt:              now,
		ExpiresAt:              now.Add(time.Hour),
	}
	require.NoError(t, cacheA.SaveLiveCall(ctx, liveRec, time.Hour))

	// --- Instance B: fresh store and cache objects over the same DB and Redis ---
	provStoreB := newLiveProvisionalStore(db)
	cacheB := &redisLiveStoreForTest{rdb: rdb}

	// B recovers the row via GetByCallHash
	loadedProv, err := provStoreB.GetByCallHash(ctx, callHash)
	require.NoError(t, err)
	require.Equal(t, authID, loadedProv.Token)
	require.Equal(t, LiveProvisionalStatusActive, loadedProv.Status)
	require.Len(t, loadedProv.Windows, 1)
	require.Equal(t, 1, loadedProv.Windows[0].WindowSeq)
	require.Equal(t, authID+".1", loadedProv.Windows[0].Token)
	require.Equal(t, int64(10_000), loadedProv.Windows[0].PendingUnits)

	// B constructs OpenAIGatewayService and finalizes
	svcB := &OpenAIGatewayService{
		cfg:                    cfg,
		cache:                  cacheB,
		liveProvisional:        provStoreB,
		canonicalWallet:        bridge,
		authorizer:             authorizer,
		deferredService:        NewDeferredService(nil, nil, time.Second),
		resolver:               snapService.resolver,
		billingService:         snapService.billing,
		exchangeRates:          snapService.exchangeRates,
		billingSnapshotSettler: billingSnapshotSettler{snapshots: snapService, billing: snapService.billing},
		usageBillingRepo:       &openAIRecordUsageBillingRepoStub{},
		usageLogRepo:           &liveTestUsageRepo{},
	}

	success := svcB.tryFinalizeLiveCall(liveRec)
	require.True(t, success)

	// In Postgres: provisional row is now finalized with settlement event ID
	finalizedProv, err := provStoreB.Get(ctx, authID)
	require.NoError(t, err)
	require.Equal(t, LiveProvisionalStatusFinalized, finalizedProv.Status)
	require.NotEmpty(t, finalizedProv.SettlementEventID)
	require.NotNil(t, finalizedProv.TerminalAt)

	// In Postgres outbox: settlement event was enqueued
	var outboxCount int
	err = db.QueryRowContext(ctx, "SELECT count(*) FROM wallet_settlement_outbox WHERE event_id = $1", finalizedProv.SettlementEventID).Scan(&outboxCount)
	require.NoError(t, err)
	require.Equal(t, 1, outboxCount)

	// --- Redis Loss Variant ---
	lossAuthID := "auth_restart_loss"
	lossCallHash := hashLiveCallID("call_restart_loss")
	lossRec := &LiveProvisionalRecord{
		Token:             lossAuthID,
		AuthorizationID:   lossAuthID,
		CallHash:          lossCallHash,
		PlatformUserID:    user.PlatformUserID,
		UserID:            user.ID,
		APIKeyID:          apiKey.ID,
		AccountID:         account.ID,
		BillingCurrency:   "CNY",
		BillingSnapshotID: "bsnap_loss",
		EstimatedUnits:    5_000,
		Status:            LiveProvisionalStatusProvisional,
		Windows: []LiveWindow{
			{WindowSeq: 1, Token: lossAuthID + ".1", PendingUnits: 5_000},
		},
		CreatedAt:   now,
		ActivatedAt: &now,
	}
	require.NoError(t, provStoreB.Save(ctx, lossRec))
	require.NoError(t, provStoreB.Activate(ctx, lossAuthID, lossCallHash, now))

	// Do NOT save to Redis (simulating Redis data loss)
	lossLiveCall := &LiveCallRecord{CallHash: lossCallHash}
	lossFinalized := svcB.tryFinalizeLiveCall(lossLiveCall)
	require.True(t, lossFinalized, "ErrLiveCallNotFound path returns true")

	// Provisional row stays active in Postgres (reconciled by Phase 4)
	lossProvAfter, err := provStoreB.Get(ctx, lossAuthID)
	require.NoError(t, err)
	require.Equal(t, LiveProvisionalStatusActive, lossProvAfter.Status)
}
