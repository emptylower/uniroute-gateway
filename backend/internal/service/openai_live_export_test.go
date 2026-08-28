//go:build integration

package service

import (
	"context"
	"database/sql"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/Wei-Shaw/sub2api/internal/platform/liveattestation"
)

func NewLiveProvisionalStoreForTest(db *sql.DB) LiveProvisionalStore {
	return newLiveProvisionalStore(db)
}

func NewCanonicalWalletBridgeForTest(cfg config.CanonicalWalletConfig, store CanonicalWalletLeaseStore, control canonicalWalletControlPlane, outboxDB *sql.DB, outbox CanonicalWalletOutboxStore) *CanonicalWalletBridge {
	return newCanonicalWalletBridge(cfg, store, control, outboxDB, outbox, 0)
}

func HashLiveCallIDForTest(callID string) string {
	return hashLiveCallID(callID)
}

func CanonicalWalletTestConfigForTest(mode string) config.CanonicalWalletConfig {
	return canonicalWalletTestConfig(mode)
}

func (s *OpenAIGatewayService) TryFinalizeLiveCallForTest(record *LiveCallRecord) bool {
	return s.tryFinalizeLiveCall(record)
}

type LiveRestartServiceOptions struct {
	Cfg                *config.Config
	Cache              GatewayCache
	LiveProvisional    LiveProvisionalStore
	CanonicalWallet    *CanonicalWalletBridge
	Authorizer         *CanonicalWalletAuthorizer
	HTTPUpstream       HTTPUpstream
	AccountRepo        AccountRepository
	UsageBillingRepo   UsageBillingRepository
	UsageLogRepo       UsageLogRepository
	BillingService     *BillingService
	Resolver           *ModelPricingResolver
	ExchangeRates      *ExchangeRateService
	Snapshots          *BillingSnapshotService
	Attestation        liveattestation.Provider
	AttestationCipher  SecretEncryptor
	Scheduler          OpenAIAccountScheduler
	ConcurrencyService *ConcurrencyService
}

func NewLiveRestartTestService(opts LiveRestartServiceOptions) *OpenAIGatewayService {
	cipher := opts.AttestationCipher
	if cipher == nil && opts.Cfg != nil {
		cipher = newLiveAttestationCipher(opts.Cfg)
	}
	upstream := opts.HTTPUpstream
	if upstream != nil {
		upstream = NewAuthorizingHTTPUpstream(upstream, opts.Cfg)
	}
	return &OpenAIGatewayService{
		cfg:                    opts.Cfg,
		cache:                  opts.Cache,
		liveProvisional:        opts.LiveProvisional,
		canonicalWallet:        opts.CanonicalWallet,
		authorizer:             opts.Authorizer,
		httpUpstream:           upstream,
		accountRepo:            opts.AccountRepo,
		usageBillingRepo:       opts.UsageBillingRepo,
		usageLogRepo:           opts.UsageLogRepo,
		billingService:         opts.BillingService,
		resolver:               opts.Resolver,
		exchangeRates:          opts.ExchangeRates,
		liveAttestation:        opts.Attestation,
		liveAttestationCipher:  cipher,
		openaiScheduler:        opts.Scheduler,
		concurrencyService:     opts.ConcurrencyService,
		deferredService:        NewDeferredService(nil, nil, time.Second),
		billingSnapshotSettler: billingSnapshotSettler{snapshots: opts.Snapshots, billing: opts.BillingService},
	}
}

type LiveRestartAccountRepoStub struct {
	AccountRepository
	Account *Account
}

func (r *LiveRestartAccountRepoStub) GetByID(ctx context.Context, id int64) (*Account, error) {
	if r.Account != nil && r.Account.ID == id {
		return r.Account, nil
	}
	return nil, ErrAccountNotFound
}

func (r *LiveRestartAccountRepoStub) ListSchedulableByGroupIDAndPlatform(_ context.Context, _ int64, _ string) ([]Account, error) {
	if r.Account != nil {
		return []Account{*r.Account}, nil
	}
	return nil, nil
}

func (r *LiveRestartAccountRepoStub) ListSchedulableAccounts(_ context.Context) ([]Account, error) {
	if r.Account != nil {
		return []Account{*r.Account}, nil
	}
	return nil, nil
}

func EnableOpenAIAdvancedSchedulerForTest() {
	openAIAdvancedSchedulerSettingCache.Store(&cachedOpenAIAdvancedSchedulerSetting{
		enabled:   true,
		expiresAt: time.Now().Add(time.Hour).UnixNano(),
	})
}

type LiveRestartUsageBillingRepoStub struct {
	UsageBillingRepository
}

func (r *LiveRestartUsageBillingRepoStub) Apply(ctx context.Context, cmd *UsageBillingCommand) (*UsageBillingApplyResult, error) {
	return &UsageBillingApplyResult{
		Applied: true,
	}, nil
}

type LiveRestartUsageLogRepoStub struct {
	UsageLogRepository
}

func (r *LiveRestartUsageLogRepoStub) Create(ctx context.Context, log *UsageLog) (bool, error) {
	return true, nil
}

type LiveRestartSchedulerStub struct {
	Account *Account
}

func (s *LiveRestartSchedulerStub) Select(ctx context.Context, req OpenAIAccountScheduleRequest) (*AccountSelectionResult, OpenAIAccountScheduleDecision, error) {
	return &AccountSelectionResult{
		Account:     s.Account,
		Acquired:    true,
		ReleaseFunc: func() {},
	}, OpenAIAccountScheduleDecision{}, nil
}

func (s *LiveRestartSchedulerStub) ReportResult(int64, bool, *int) {}
func (s *LiveRestartSchedulerStub) ReportSwitch()                  {}
func (s *LiveRestartSchedulerStub) SnapshotMetrics() OpenAIAccountSchedulerMetricsSnapshot {
	return OpenAIAccountSchedulerMetricsSnapshot{}
}

type LiveRestartAttestationStub struct {
	HeaderValue string
}

func (s LiveRestartAttestationStub) Check(context.Context) error {
	return nil
}

func (s LiveRestartAttestationStub) Generate(context.Context) (string, error) {
	return s.HeaderValue, nil
}

type LiveRestartHTTPUpstreamStub struct {
	DoFn func(req *http.Request) (*http.Response, error)
}

func (s *LiveRestartHTTPUpstreamStub) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	if s.DoFn != nil {
		return s.DoFn(req)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Location": {"/backend-api/codex/calls/call_restart_1"}},
		Body:       io.NopCloser(strings.NewReader("v=0\r\n")),
	}, nil
}

func (s *LiveRestartHTTPUpstreamStub) DoWithTLS(req *http.Request, proxyURL string, accountID int64, accountConcurrency int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return s.Do(req, proxyURL, accountID, accountConcurrency)
}

type LiveRestartLeaseStoreStub struct {
	Lease *CanonicalWalletLease
}

func (s *LiveRestartLeaseStoreStub) InstallCanonicalWalletLease(_ context.Context, lease CanonicalWalletLease) error {
	s.Lease = &lease
	return nil
}
func (s *LiveRestartLeaseStoreStub) GetCanonicalWalletLease(context.Context, string) (*CanonicalWalletLease, error) {
	return s.Lease, nil
}
func (s *LiveRestartLeaseStoreStub) GetCanonicalWalletLeaseByID(_ context.Context, _, _ string) (*CanonicalWalletLease, error) {
	return s.Lease, nil
}
func (s *LiveRestartLeaseStoreStub) ReserveCanonicalWalletLease(_ context.Context, _, _, _, _ string, _ int64, _ time.Time) (*CanonicalWalletReservation, error) {
	if s.Lease == nil {
		return nil, ErrCanonicalWalletLeaseMissing
	}
	return &CanonicalWalletReservation{Lease: *s.Lease}, nil
}

type LiveRestartControlStub struct {
	Lease *CanonicalWalletLease
}

func (s *LiveRestartControlStub) AcquireLease(ctx context.Context, request canonicalWalletLeaseRequest) (*CanonicalWalletLease, error) {
	if s.Lease != nil {
		return s.Lease, nil
	}
	return &CanonicalWalletLease{
		LeaseID:        "lease_restart_cw",
		PlatformUserID: request.PlatformUserID,
		Currency:       request.Currency,
		BudgetUnits:    100_000_000,
		ExpiresAt:      time.Now().Add(5 * time.Minute),
	}, nil
}

func (s *LiveRestartControlStub) SubmitSettlement(ctx context.Context, event CanonicalWalletSettlementEvent) (*CanonicalWalletSettlementResult, error) {
	return &CanonicalWalletSettlementResult{Accepted: true}, nil
}

type LiveRestartChannelRepoStub struct {
	ChannelRepository
}

func (r *LiveRestartChannelRepoStub) ListAll(context.Context) ([]Channel, error) {
	return nil, nil
}
func (r *LiveRestartChannelRepoStub) GetGroupPlatforms(_ context.Context, _ []int64) (map[int64]string, error) {
	return map[int64]string{7: "openai"}, nil
}

func NewSnapshotTestFixtureForTest(t testing.TB) (*BillingSnapshotService, *APIKey, *User, *Account, *BillingService, *ModelPricingResolver, *ExchangeRateService) {
	cfg := &config.Config{}
	cfg.Default.RateMultiplier = 1
	cfg.CanonicalWallet.BillingSnapshotMode = "record"
	cfg.Billing.ExchangeRate.BootstrapUSDToCNY = 7.0
	billing := NewBillingService(&config.Config{}, nil)
	cs := NewChannelService(&LiveRestartChannelRepoStub{}, nil, nil, nil)
	resolver := NewModelPricingResolver(cs, billing)
	fx := NewExchangeRateService(cfg)
	svc := NewBillingSnapshotService(cfg, resolver, billing, fx, nil)
	svc.now = func() time.Time { return time.Date(2026, 8, 27, 3, 0, 0, 0, time.UTC) }
	group := &Group{ID: 7, RateMultiplier: 1.5, ImageRateIndependent: true, ImageRateMultiplier: 2.5}
	gid := int64(7)
	user := &User{ID: 42, BillingCurrency: "CNY"}
	apiKey := &APIKey{ID: 11, GroupID: &gid, Group: group, User: user}
	rate := 1.25
	account := &Account{
		ID:             99,
		RateMultiplier: &rate,
		Platform:       PlatformOpenAI,
		Type:           AccountTypeOAuth,
		Concurrency:    2,
		Credentials:    map[string]any{"access_token": "test-token"},
	}
	return svc, apiKey, user, account, billing, resolver, fx
}
