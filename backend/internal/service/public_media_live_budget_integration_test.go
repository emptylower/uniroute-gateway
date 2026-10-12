//go:build integration && budget_integration

package service_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func liveBudgetWirePost(t *testing.T, path string, body any) []byte {
	t.Helper()
	base := os.Getenv("UNIROUTE_WALLET_WIRE_FIXTURE_URL")
	u, err := url.Parse(base)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", u.Hostname())
	secret := os.Getenv("UNIROUTE_WALLET_WIRE_FIXTURE_SECRET")
	require.GreaterOrEqual(t, len(secret), 32)
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, base+path, bytes.NewReader(raw))
	require.NoError(t, err)
	req.Header.Set("x-fixture-secret", secret)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	raw, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Less(t, resp.StatusCode, 300, string(raw))
	return raw
}
func TestPublicMediaBudgetEnforceLiveIdleFinalizationAndCrashRecovery(t *testing.T) {
	ctx := context.Background()
	dsn := os.Getenv("UNIROUTE_MEDIA_TEST_POSTGRES_DSN")
	require.NotEmpty(t, dsn)
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", u.Hostname())
	require.Equal(t, "55432", u.Port())
	require.True(t, strings.HasPrefix(u.Path, "/uniroute_media_test_"))
	template := strings.TrimPrefix(u.Path, "/")
	dbName := "uniroute_media_test_live_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	adminURL := *u
	adminURL.Path = "/postgres"
	admin, err := sql.Open("postgres", adminURL.String())
	require.NoError(t, err)
	defer admin.Close()
	_, err = admin.Exec(`CREATE DATABASE ` + dbName + ` TEMPLATE ` + template)
	require.NoError(t, err)
	u.Path = "/" + dbName
	db, err := sql.Open("postgres", u.String())
	require.NoError(t, err)
	defer func() { db.Close(); admin.Exec(`DROP DATABASE ` + dbName + ` WITH (FORCE)`) }()
	addr := os.Getenv("UNIROUTE_MEDIA_TEST_REDIS_ADDR")
	require.Equal(t, "127.0.0.1:56379", addr)
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: 15})
	defer rdb.Close()
	require.NoError(t, rdb.FlushDB(ctx).Err())
	platformUser := "live-budget-" + uuid.NewString()
	lease := "lease-" + uuid.NewString()
	liveBudgetWirePost(t, "/__fixture/seed", map[string]any{"platform_user_id": platformUser, "grant_units": "600000000", "lease_id": lease, "budget_units": "300000000"})
	cfg := &config.Config{}
	cfg.CanonicalWallet = config.CanonicalWalletConfig{Mode: "enforce", Holds: "on", EnforceReady: true, OrphanGraceSeconds: 2, OrphanSweepIntervalSeconds: 1, OrphanSweepBatch: 100, ReceivableRedriveIntervalSeconds: 3600, USDWalletEnabled: true, USDPolicyVersion: "usd-wallet-v1", BillingSnapshotMode: "settle", ControlPlaneURL: os.Getenv("UNIROUTE_WALLET_WIRE_FIXTURE_URL"), Secret: os.Getenv("UNIROUTE_WALLET_WIRE_FIXTURE_SECRET"), Issuer: "sub2api", Audience: "shipany-wallet", Version: "v1", RequestTimeoutMS: 3000, LeaseTTLSeconds: 300, LeaseBudgetUnits: 500000000, PoolExpiryMode: "shadow", ReaderJournalDirectory: t.TempDir()}
	cache := repository.NewGatewayCache(rdb)
	wallet := cache.(service.CanonicalWalletLeaseStore)
	require.NoError(t, wallet.InstallCanonicalWalletLease(ctx, service.CanonicalWalletLease{LeaseID: lease, PlatformUserID: platformUser, Currency: "USD", FundingScope: "legacy", BudgetUnits: 300000000, ExpiresAt: time.Now().Add(time.Hour)}))
	var userID, keyID, accountID int64
	require.NoError(t, db.QueryRow(`INSERT INTO users(email,password_hash,platform_user_id,billing_currency,status,balance)VALUES($1,'test',$2,'USD','active',0)RETURNING id`, platformUser+"@example.test", platformUser).Scan(&userID))
	require.NoError(t, db.QueryRow(`INSERT INTO api_keys(user_id,key,name,status,quota,rate_limit_5h)VALUES($1,$2,'Live Test','active',0.8,0.8)RETURNING id`, userID, uuid.NewString()).Scan(&keyID))
	require.NoError(t, db.QueryRow(`SELECT account_id FROM gateway_media_provider LIMIT 1`).Scan(&accountID))
	user := &service.User{ID: userID, PlatformUserID: platformUser, BillingCurrency: "USD"}
	snap := &service.BillingSnapshot{ID: "snap-" + uuid.NewString(), Version: service.BillingSnapshotVersion, FrozenAt: time.Now().UTC(), Family: service.BillingFamilyLive, UserID: userID, APIKeyID: keyID, AccountID: accountID, RequestedModel: "live-test", BillingModel: "live-test", ProviderPlatform: "openai", Pricing: service.BillingSnapshotPricing{Mode: service.BillingModeToken}, Flags: service.BillingSnapshotFlags{USDWalletPolicyVersion: "usd-wallet-v1"}, FX: service.ExchangeRateSnapshot{}}
	// Freeze the same valid canonical USD basis used in runtime snapshots.
	snap.FX = service.ExchangeRateSnapshot{BaseCurrency: "USD", QuoteCurrency: "USD", Rate: 1, Source: "usd-wallet-v1", AsOf: time.Now().UTC()}
	snapshots := service.NewBillingSnapshotService(cfg, nil, nil, nil, repository.ProvideBillingSnapshotStore(db))
	require.NoError(t, snapshots.Persist(ctx, snap))
	bridge := service.NewCanonicalWalletBridge(cfg, wallet, db, repository.ProvideWalletOutboxStore(db))
	defer bridge.Close()
	authorizer := service.NewCanonicalWalletAuthorizer(cfg, bridge, snapshots)
	first, err := authorizer.Authorize(ctx, service.AuthorizeInput{Snapshot: snap, User: user, FixedEstimateUnits: 30000000})
	require.NoError(t, err)
	callHash := "call-" + uuid.NewString()
	prov := service.ProvideLiveProvisionalStore(cfg, db)
	require.NoError(t, prov.Save(ctx, &service.LiveProvisionalRecord{Token: first.ID, AuthorizationID: first.ID, CallHash: callHash, PlatformUserID: platformUser, UserID: userID, APIKeyID: keyID, AccountID: accountID, BillingCurrency: "USD", BillingSnapshotID: snap.ID, EstimatedUnits: 30000000, Status: service.LiveProvisionalStatusActive, Windows: []service.LiveWindow{{WindowSeq: 1, Token: first.ID, LeaseID: lease}}}))
	require.True(t, bridge.ObserveSettlement(service.CanonicalWalletSettlementEvent{GatewayRequestID: callHash, PlatformUserID: platformUser, Currency: "USD", AmountUnits: 20000000, OccurredAt: time.Now().UTC(), AuthorizationID: first.ID, BillingSnapshotID: snap.ID}))
	idle, err := authorizer.Authorize(ctx, service.AuthorizeInput{Snapshot: snap, User: user, FixedEstimateUnits: 30000000})
	require.NoError(t, err)
	require.NoError(t, prov.AdvanceLiveWindow(ctx, first.ID, 1, 20000000, service.LiveWindow{WindowSeq: 2, Token: idle.ID, LeaseID: lease, OpenedAtMS: time.Now().UnixMilli()}))
	record := &service.LiveCallRecord{CallID: "call-" + uuid.NewString(), CallHash: callHash, UserID: userID, APIKeyID: keyID, AccountID: accountID, PlatformUserID: platformUser, AuthorizationID: first.ID, AuthorizationToken: first.ID, LeaseID: lease, BillingSnapshotID: snap.ID, BillingCurrency: "USD", Model: "live-test", RateMultiplier: 1, AccountRateMultiplier: 1, ExchangeRateAsOf: time.Now().UTC(), InputTokens: 20, InputPricePerToken: 0.01, APIKeyQuota: 0.8, RateLimit5h: 0.8, CreatedAt: time.Now().UTC(), Controller: service.LiveControllerClosed}
	require.NoError(t, cache.(service.LiveCallStore).SaveLiveCall(ctx, record, time.Hour))
	svc := service.NewLiveRestartTestService(service.LiveRestartServiceOptions{Cfg: cfg, Cache: cache, LiveProvisional: prov, CanonicalWallet: bridge, Authorizer: authorizer, UsageBillingRepo: repository.NewUsageBillingRepository(nil, db), UsageLogRepo: repository.NewUsageLogRepository(nil, db), Snapshots: snapshots})
	require.True(t, svc.TryFinalizeLiveCallForTest(record))
	require.True(t, svc.TryFinalizeLiveCallForTest(record))
	var spent float64
	var pending int
	require.NoError(t, db.QueryRow(`SELECT quota_used FROM api_keys WHERE id=$1`, keyID).Scan(&spent))
	require.InDelta(t, 0.2, spent, 1e-8)
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM api_key_budget_authorization WHERE api_key_id=$1 AND state NOT IN('captured','released')`, keyID).Scan(&pending))
	require.Zero(t, pending)
	hold, err := wallet.GetCanonicalWalletHold(ctx, platformUser, idle.ID)
	require.NoError(t, err)
	require.Equal(t, "released", hold.State)
	var terminal int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND state='finished' AND pin_state='finished'`, idle.ID).Scan(&terminal))
	require.Equal(t, len(idle.Segments), terminal)
	body := liveBudgetWirePost(t, "/__fixture/snapshot", map[string]any{"platform_user_id": platformUser})
	var result struct {
		Pins []struct {
			AuthorizationID string `json:"authorizationId"`
			State           string `json:"state"`
		}
	}
	require.NoError(t, json.Unmarshal(body, &result))
	found := false
	for _, pin := range result.Pins {
		if pin.AuthorizationID == idle.ID {
			found = true
			require.Equal(t, "released", pin.State)
		}
	}
	require.True(t, found)

	// Persisted completion failures must recover after a bridge restart. The
	// second phase has a positive final remainder and a revoked, deleted Key.
	for _, positive := range []bool{false, true} {
		firstNext, e := authorizer.Authorize(ctx, service.AuthorizeInput{Snapshot: snap, User: user, FixedEstimateUnits: 30000000})
		require.NoError(t, e)
		nextHash := "call-" + uuid.NewString()
		nextProv := &service.LiveProvisionalRecord{Token: firstNext.ID, AuthorizationID: firstNext.ID, CallHash: nextHash, PlatformUserID: platformUser, UserID: userID, APIKeyID: keyID, AccountID: accountID, BillingCurrency: "USD", BillingSnapshotID: snap.ID, EstimatedUnits: 30000000, Status: service.LiveProvisionalStatusActive, Windows: []service.LiveWindow{{WindowSeq: 1, Token: firstNext.ID, LeaseID: firstNext.LeaseID}}}
		require.NoError(t, prov.Save(ctx, nextProv))
		require.True(t, bridge.ObserveSettlement(service.CanonicalWalletSettlementEvent{GatewayRequestID: nextHash, PlatformUserID: platformUser, Currency: "USD", AmountUnits: 10000000, OccurredAt: time.Now().UTC(), AuthorizationID: firstNext.ID, BillingSnapshotID: snap.ID}))
		last, e := authorizer.Authorize(ctx, service.AuthorizeInput{Snapshot: snap, User: user, FixedEstimateUnits: 30000000})
		require.NoError(t, e)
		require.NoError(t, prov.AdvanceLiveWindow(ctx, firstNext.ID, 1, 10000000, service.LiveWindow{WindowSeq: 2, Token: last.ID, LeaseID: last.LeaseID, OpenedAtMS: time.Now().UnixMilli()}))
		nextRecord := *record
		nextRecord.CallID = "call-" + uuid.NewString()
		nextRecord.CallHash = nextHash
		nextRecord.AuthorizationID = firstNext.ID
		nextRecord.AuthorizationToken = firstNext.ID
		nextRecord.InputTokens = 10
		if positive {
			nextRecord.InputTokens = 15
		}
		require.NoError(t, cache.(service.LiveCallStore).SaveLiveCall(ctx, &nextRecord, time.Hour))
		if positive {
			_, e = db.Exec(`UPDATE api_keys SET status='disabled',deleted_at=now() WHERE id=$1`, keyID)
			require.NoError(t, e)
		}
		_, e = db.Exec(`CREATE FUNCTION test_live_completion_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.token='` + firstNext.ID + `' AND NEW.status='finalized' THEN RAISE EXCEPTION 'durable Live completion fault'; END IF; RETURN NEW; END $$; CREATE TRIGGER test_live_completion_fault BEFORE UPDATE ON wallet_live_provisional FOR EACH ROW EXECUTE FUNCTION test_live_completion_fault()`)
		require.NoError(t, e)
		require.False(t, svc.TryFinalizeLiveCallForTest(&nextRecord))
		bridge.Close()
		_, e = db.Exec(`DROP TRIGGER test_live_completion_fault ON wallet_live_provisional; DROP FUNCTION test_live_completion_fault()`)
		require.NoError(t, e)
		bridge = service.NewCanonicalWalletBridge(cfg, wallet, db, repository.ProvideWalletOutboxStore(db))
		defer bridge.Close()
		authorizer = service.NewCanonicalWalletAuthorizer(cfg, bridge, snapshots)
		svc = service.NewLiveRestartTestService(service.LiveRestartServiceOptions{Cfg: cfg, Cache: cache, LiveProvisional: prov, CanonicalWallet: bridge, Authorizer: authorizer, UsageBillingRepo: repository.NewUsageBillingRepository(nil, db), UsageLogRepo: repository.NewUsageLogRepository(nil, db), Snapshots: snapshots})
		require.Eventually(t, func() bool {
			p, e := prov.Get(ctx, firstNext.ID)
			return e == nil && p.Status == service.LiveProvisionalStatusFinalized
		}, 15*time.Second, 50*time.Millisecond)
		require.True(t, svc.TryFinalizeLiveCallForTest(&nextRecord))
		require.True(t, svc.TryFinalizeLiveCallForTest(&nextRecord))
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM api_key_budget_authorization WHERE api_key_id=$1 AND state NOT IN('captured','released')`, keyID).Scan(&pending))
		require.Zero(t, pending)
		require.NoError(t, db.QueryRow(`SELECT quota_used FROM api_keys WHERE id=$1`, keyID).Scan(&spent))
		if positive {
			require.InDelta(t, 0.45, spent, 1e-8)
			var status string
			require.NoError(t, db.QueryRow(`SELECT status FROM api_keys WHERE id=$1`, keyID).Scan(&status))
			require.Equal(t, "disabled", status)
		} else {
			require.InDelta(t, 0.3, spent, 1e-8)
		}
		if !positive {
			h, e := wallet.GetCanonicalWalletHold(ctx, platformUser, last.ID)
			require.NoError(t, e)
			require.Equal(t, "released", h.State)
		}
	}
}
