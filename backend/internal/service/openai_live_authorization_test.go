//go:build unit

package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/stretchr/testify/require"
)

type stubOpenAIScheduler struct {
	selectFn func(ctx context.Context, req OpenAIAccountScheduleRequest) (*AccountSelectionResult, OpenAIAccountScheduleDecision, error)
}

func (s *stubOpenAIScheduler) Select(ctx context.Context, req OpenAIAccountScheduleRequest) (*AccountSelectionResult, OpenAIAccountScheduleDecision, error) {
	if s.selectFn != nil {
		return s.selectFn(ctx, req)
	}
	return nil, OpenAIAccountScheduleDecision{}, nil
}

func (s *stubOpenAIScheduler) ReportResult(int64, bool, *int) {}
func (s *stubOpenAIScheduler) ReportSwitch()                  {}
func (s *stubOpenAIScheduler) SnapshotMetrics() OpenAIAccountSchedulerMetricsSnapshot {
	return OpenAIAccountSchedulerMetricsSnapshot{}
}

type inMemoryLiveProvisionalStore struct {
	mu      sync.Mutex
	records map[string]*LiveProvisionalRecord
	saveErr error
	onSave  func(rec *LiveProvisionalRecord)
}

func newInMemoryLiveProvisionalStore() *inMemoryLiveProvisionalStore {
	return &inMemoryLiveProvisionalStore{records: make(map[string]*LiveProvisionalRecord)}
}

func (s *inMemoryLiveProvisionalStore) Save(ctx context.Context, rec *LiveProvisionalRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saveErr != nil {
		return s.saveErr
	}
	if s.onSave != nil {
		s.onSave(rec)
	}
	if _, exists := s.records[rec.Token]; exists {
		return nil
	}
	copy := *rec
	s.records[rec.Token] = &copy
	return nil
}

func (s *inMemoryLiveProvisionalStore) Activate(ctx context.Context, token, callHash string, activatedAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[token]
	if !ok || rec.Status != LiveProvisionalStatusProvisional {
		return ErrLiveProvisionalNotFound
	}
	rec.CallHash = callHash
	rec.ActivatedAt = &activatedAt
	rec.Status = LiveProvisionalStatusActive
	return nil
}

func (s *inMemoryLiveProvisionalStore) Abort(ctx context.Context, token string, terminalAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[token]
	if !ok || rec.Status != LiveProvisionalStatusProvisional {
		return ErrLiveProvisionalNotFound
	}
	rec.TerminalAt = &terminalAt
	rec.Status = LiveProvisionalStatusAborted
	return nil
}

func (s *inMemoryLiveProvisionalStore) ClaimFinalization(ctx context.Context, token string, claimedAt time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saveErr != nil {
		return false, s.saveErr
	}
	rec, ok := s.records[token]
	if !ok || rec.Status != LiveProvisionalStatusActive {
		return false, nil
	}
	rec.Status = LiveProvisionalStatusFinalizing
	return true, nil
}

func (s *inMemoryLiveProvisionalStore) CompleteFinalization(ctx context.Context, token, settlementEventID string, settledUnits int64, terminalAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saveErr != nil {
		return s.saveErr
	}
	rec, ok := s.records[token]
	if !ok || rec.Status != LiveProvisionalStatusFinalizing {
		return ErrLiveProvisionalNotFound
	}
	rec.SettlementEventID = settlementEventID
	rec.TerminalAt = &terminalAt
	rec.Status = LiveProvisionalStatusFinalized
	if len(rec.Windows) > 0 {
		rec.Windows[0].SettledUnits = settledUnits
	}
	return nil
}

func (s *inMemoryLiveProvisionalStore) ReleaseFinalizationClaim(ctx context.Context, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[token]
	if !ok || rec.Status != LiveProvisionalStatusFinalizing {
		return ErrLiveProvisionalNotFound
	}
	rec.Status = LiveProvisionalStatusActive
	return nil
}

func (s *inMemoryLiveProvisionalStore) Get(ctx context.Context, token string) (*LiveProvisionalRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[token]
	if !ok {
		return nil, ErrLiveProvisionalNotFound
	}
	copy := *rec
	return &copy, nil
}

func (s *inMemoryLiveProvisionalStore) GetByCallHash(ctx context.Context, callHash string) (*LiveProvisionalRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rec := range s.records {
		if rec.CallHash == callHash {
			copy := *rec
			return &copy, nil
		}
	}
	return nil, ErrLiveProvisionalNotFound
}

func (s *liveTestStore) GetSessionAccountID(context.Context, int64, string) (int64, error) {
	return 0, nil
}
func (s *liveTestStore) SetSessionAccountID(context.Context, int64, string, int64, time.Duration) error {
	return nil
}
func (s *liveTestStore) DeleteSessionAccountID(context.Context, int64, string) error {
	return nil
}
func (s *liveTestStore) RefreshSessionAccountTTL(context.Context, int64, string, time.Duration) error {
	return nil
}
func (s *liveTestStore) GetAccountLoad(context.Context, int64) (*AccountLoadInfo, error) {
	return &AccountLoadInfo{}, nil
}

type liveAuthTestFixture struct {
	svc          *OpenAIGatewayService
	liveStore    *liveTestStore
	provStore    *inMemoryLiveProvisionalStore
	httpUpstream *liveHTTPUpstreamStub
	account      *Account
	apiKey       *APIKey
	user         *User
	authorizer   *CanonicalWalletAuthorizer
	bridge       *CanonicalWalletBridge
	leaseStore   *canonicalWalletStoreStub
	control      *canonicalWalletControlStub
}

func newLiveAuthTestFixture(t *testing.T, mode string) *liveAuthTestFixture {
	t.Helper()
	ResetAuthorizationMetricsForTest()
	openAIAdvancedSchedulerSettingCache.Store(&cachedOpenAIAdvancedSchedulerSetting{
		enabled:   true,
		expiresAt: time.Now().Add(time.Hour).UnixNano(),
	})
	snapService, apiKey, user, account := newSnapshotTestFixture(t)
	user.PlatformUserID = "shipany-user-" + t.Name()
	apiKey.User = user

	cfg := &config.Config{}
	cfg.Default.RateMultiplier = 1
	cfg.CanonicalWallet = canonicalWalletTestConfig(mode)
	cfg.CanonicalWallet.BillingSnapshotMode = "record"
	cfg.JWT.Secret = "test-jwt-secret-32-bytes-long!!!"

	leaseStore := &canonicalWalletStoreStub{}
	control := &canonicalWalletControlStub{
		lease: CanonicalWalletLease{
			LeaseID:        "lease-test-1",
			PlatformUserID: user.PlatformUserID,
			Currency:       "CNY",
			BudgetUnits:    100_000_000,
			ExpiresAt:      time.Now().Add(5 * time.Minute),
		},
	}
	bridge := newCanonicalWalletBridge(cfg.CanonicalWallet, leaseStore, control, nil, nil)
	authorizer := NewCanonicalWalletAuthorizer(cfg, bridge, snapService)

	liveStore := &liveTestStore{}
	concurrencyCache := &liveTestConcurrencyCache{}
	provStore := newInMemoryLiveProvisionalStore()
	httpUpstream := &liveHTTPUpstreamStub{}
	attestationCipher := newLiveAttestationCipher(cfg)

	account.Platform = PlatformOpenAI
	account.Type = AccountTypeOAuth
	account.Concurrency = 2
	account.Credentials = map[string]any{"access_token": "test-token"}

	scheduler := &stubOpenAIScheduler{
		selectFn: func(ctx context.Context, req OpenAIAccountScheduleRequest) (*AccountSelectionResult, OpenAIAccountScheduleDecision, error) {
			return &AccountSelectionResult{
				Account:     account,
				Acquired:    true,
				ReleaseFunc: func() {},
			}, OpenAIAccountScheduleDecision{}, nil
		},
	}

	svc := &OpenAIGatewayService{
		cfg:                    cfg,
		cache:                  liveStore,
		concurrencyService:     NewConcurrencyService(concurrencyCache),
		liveProvisional:        provStore,
		httpUpstream:           httpUpstream,
		openaiScheduler:        scheduler,
		accountRepo:            &liveTestAccountRepo{account: account},
		liveAttestation:        liveAttestationStub{header: `{"v":1,"s":0,"t":"v1.test"}`},
		liveAttestationCipher:  attestationCipher,
		resolver:               snapService.resolver,
		billingService:         snapService.billing,
		exchangeRates:          snapService.exchangeRates,
		billingSnapshotSettler: billingSnapshotSettler{snapshots: snapService, billing: snapService.billing},
		authorizer:             authorizer,
		canonicalWallet:        bridge,
		deferredService:        NewDeferredService(nil, nil, time.Second),
		usageBillingRepo:       &openAIRecordUsageBillingRepoStub{},
		usageLogRepo:           &liveTestUsageRepo{},
	}

	return &liveAuthTestFixture{
		svc:          svc,
		liveStore:    liveStore,
		provStore:    provStore,
		httpUpstream: httpUpstream,
		account:      account,
		apiKey:       apiKey,
		user:         user,
		authorizer:   authorizer,
		bridge:       bridge,
		leaseStore:   leaseStore,
		control:      control,
	}
}

func defaultLiveCallRequest() *LiveCallRequest {
	return &LiveCallRequest{
		SDP:     "v=0\r\n",
		Session: json.RawMessage(`{"model":"claude-sonnet-4","max_output_tokens":4096,"instructions":"test session"}`),
	}
}

func defaultLiveCallIdentity(f *liveAuthTestFixture) LiveCallIdentity {
	return LiveCallIdentity{
		APIKeyID:        f.apiKey.ID,
		UserID:          f.user.ID,
		APIKey:          f.apiKey,
		User:            f.user,
		BillingCurrency: "CNY",
		RateMultiplier:  1.0,
		BillingModel:    "claude-sonnet-4",
	}
}

func TestCreateLiveCallWritesTheProvisionalRowBeforeThePost(t *testing.T) {
	f := newLiveAuthTestFixture(t, config.CanonicalWalletModeShadow)

	postHappened := false
	var handleInPost *AuthorizationHandle
	origDo := f.httpUpstream.Do
	_ = origDo

	// Verify happens-before: when Do is called, provisional record MUST already exist
	postChecker := &liveHTTPUpstreamStub{}
	f.svc.httpUpstream = &hookedHTTPUpstream{
		doFn: func(req *http.Request) (*http.Response, error) {
			postHappened = true
			handleInPost = AuthorizationHandleFromContext(req.Context())
			require.NotNil(t, handleInPost)
			require.NotEmpty(t, handleInPost.ID)

			// Provisional record must exist and have status 'provisional' at the instant POST is made
			rec, err := f.provStore.Get(context.Background(), handleInPost.ID)
			require.NoError(t, err)
			require.Equal(t, LiveProvisionalStatusProvisional, rec.Status)
			require.Equal(t, "", rec.CallHash)
			require.Equal(t, f.user.PlatformUserID, rec.PlatformUserID)

			return postChecker.Do(req, "", 1, 1)
		},
	}

	created, err := f.svc.CreateLiveCall(context.Background(), defaultLiveCallRequest(), defaultLiveCallIdentity(f), 5)
	require.NoError(t, err)
	require.True(t, postHappened)
	require.NotNil(t, created)
	require.Equal(t, "call_test", created.CallID)

	// After success: row is active with the call hash
	rec, err := f.provStore.Get(context.Background(), handleInPost.ID)
	require.NoError(t, err)
	require.Equal(t, LiveProvisionalStatusActive, rec.Status)
	require.Equal(t, hashLiveCallID("call_test"), rec.CallHash)
	require.NotNil(t, rec.ActivatedAt)

	// Redis record carries token, authorization_id, and platform_user_id
	liveRec, err := f.liveStore.GetLiveCall(context.Background(), hashLiveCallID("call_test"))
	require.NoError(t, err)
	require.Equal(t, handleInPost.SettledToken(), liveRec.AuthorizationToken)
	require.Equal(t, handleInPost.ID, liveRec.AuthorizationID)
	require.Equal(t, f.user.PlatformUserID, liveRec.PlatformUserID)

	// Metrics
	m := LiveProvisionalMetricsSnapshot()
	require.Equal(t, int64(1), m.Written)
	require.Equal(t, int64(1), m.Activated)
	require.Equal(t, int64(0), m.Aborted)
}

type hookedHTTPUpstream struct {
	doFn func(req *http.Request) (*http.Response, error)
}

func (h *hookedHTTPUpstream) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	return h.doFn(req)
}

func (h *hookedHTTPUpstream) DoWithTLS(req *http.Request, proxyURL string, accountID int64, accountConcurrency int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return h.Do(req, proxyURL, accountID, accountConcurrency)
}

func TestCreateLiveCallEachAttemptHasItsOwnAuthorization(t *testing.T) {
	f := newLiveAuthTestFixture(t, config.CanonicalWalletModeShadow)

	account1 := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Concurrency: 2, Credentials: map[string]any{"access_token": "token-1"}}
	account2 := &Account{ID: 2, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Concurrency: 2, Credentials: map[string]any{"access_token": "token-2"}}

	f.svc.openaiScheduler = &stubOpenAIScheduler{
		selectFn: func(ctx context.Context, req OpenAIAccountScheduleRequest) (*AccountSelectionResult, OpenAIAccountScheduleDecision, error) {
			if req.ExcludedIDs != nil {
				if _, excluded := req.ExcludedIDs[1]; excluded {
					return &AccountSelectionResult{Account: account2, Acquired: true, ReleaseFunc: func() {}}, OpenAIAccountScheduleDecision{}, nil
				}
			}
			return &AccountSelectionResult{Account: account1, Acquired: true, ReleaseFunc: func() {}}, OpenAIAccountScheduleDecision{}, nil
		},
	}

	attemptCount := 0
	f.svc.httpUpstream = &hookedHTTPUpstream{
		doFn: func(req *http.Request) (*http.Response, error) {
			attemptCount++
			if attemptCount == 1 {
				// Fail attempt 1 with a failover-able error
				return nil, &UpstreamFailoverError{StatusCode: http.StatusBadGateway, ResponseBody: []byte("bad gateway")}
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Location": {"/backend-api/codex/call_second"}},
				Body:       io.NopCloser(strings.NewReader("v=0\r\n")),
			}, nil
		},
	}

	created, err := f.svc.CreateLiveCall(context.Background(), defaultLiveCallRequest(), defaultLiveCallIdentity(f), 5)
	require.NoError(t, err)
	require.Equal(t, "call_second", created.CallID)
	require.Equal(t, 2, attemptCount)

	// Two records in provStore: the first aborted, the second active
	f.provStore.mu.Lock()
	require.Len(t, f.provStore.records, 2)
	var abortedRec, activeRec *LiveProvisionalRecord
	for _, rec := range f.provStore.records {
		if rec.Status == LiveProvisionalStatusAborted {
			abortedRec = rec
		} else if rec.Status == LiveProvisionalStatusActive {
			activeRec = rec
		}
	}
	f.provStore.mu.Unlock()

	require.NotNil(t, abortedRec, "first attempt provisional row must be aborted")
	require.NotNil(t, activeRec, "second attempt provisional row must be active")
	require.NotEqual(t, abortedRec.Token, activeRec.Token, "two distinct authorization ids")
	require.Equal(t, hashLiveCallID("call_second"), activeRec.CallHash)

	m := LiveProvisionalMetricsSnapshot()
	require.Equal(t, int64(2), m.Written)
	require.Equal(t, int64(1), m.Activated)
	require.Equal(t, int64(1), m.Aborted)
}

func TestCreateLiveCallRefusesInEnforceWhenAuthorizationIsRefused(t *testing.T) {
	f := newLiveAuthTestFixture(t, config.CanonicalWalletModeEnforce)

	// Force balance shortfall refusal
	f.control.leaseErr = ErrCanonicalWalletBalanceShortfall

	postCalled := false
	f.svc.httpUpstream = &hookedHTTPUpstream{
		doFn: func(req *http.Request) (*http.Response, error) {
			postCalled = true
			return nil, nil
		},
	}

	_, err := f.svc.CreateLiveCall(context.Background(), defaultLiveCallRequest(), defaultLiveCallIdentity(f), 5)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrAuthorizationRefused))
	require.False(t, postCalled, "POST must never be made when authorization is refused")

	f.provStore.mu.Lock()
	require.Empty(t, f.provStore.records, "no provisional record when authorization is refused before save")
	f.provStore.mu.Unlock()
}

func TestCreateLiveCallStoreOutageIsCountedInShadowAndRefusedInEnforce(t *testing.T) {
	// 1. Shadow mode: save fails -> POST proceeds, counter + 1, no row
	fShadow := newLiveAuthTestFixture(t, config.CanonicalWalletModeShadow)
	fShadow.provStore.saveErr = errors.New("database connection timeout")

	postInShadow := false
	fShadow.svc.httpUpstream = &hookedHTTPUpstream{
		doFn: func(req *http.Request) (*http.Response, error) {
			postInShadow = true
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Location": {"/backend-api/codex/call_shadow"}},
				Body:       io.NopCloser(strings.NewReader("v=0\r\n")),
			}, nil
		},
	}

	created, err := fShadow.svc.CreateLiveCall(context.Background(), defaultLiveCallRequest(), defaultLiveCallIdentity(fShadow), 5)
	require.NoError(t, err)
	require.NotNil(t, created)
	require.True(t, postInShadow)
	require.Equal(t, int64(1), LiveProvisionalMetricsSnapshot().StoreUnavailable)

	// 2. Enforce mode: save fails -> refused with live_provisional_store_unavailable, no POST
	fEnforce := newLiveAuthTestFixture(t, config.CanonicalWalletModeEnforce)
	fEnforce.provStore.saveErr = errors.New("database connection timeout")

	postInEnforce := false
	fEnforce.svc.httpUpstream = &hookedHTTPUpstream{
		doFn: func(req *http.Request) (*http.Response, error) {
			postInEnforce = true
			return nil, nil
		},
	}

	_, err = fEnforce.svc.CreateLiveCall(context.Background(), defaultLiveCallRequest(), defaultLiveCallIdentity(fEnforce), 5)
	require.Error(t, err)
	var refusedErr *AuthorizationRefusedError
	require.True(t, errors.As(err, &refusedErr))
	require.Equal(t, AuthorizationRefusalLiveStoreUnavailable, refusedErr.Reason)
	require.False(t, postInEnforce)
}

func TestCreateLiveCallDisabledModeIsUnchanged(t *testing.T) {
	f := newLiveAuthTestFixture(t, config.CanonicalWalletModeDisabled)

	var recordedHeaders http.Header
	f.svc.httpUpstream = &hookedHTTPUpstream{
		doFn: func(req *http.Request) (*http.Response, error) {
			recordedHeaders = req.Header.Clone()
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Location": {"/backend-api/codex/call_disabled"}},
				Body:       io.NopCloser(strings.NewReader("v=0\r\n")),
			}, nil
		},
	}

	created, err := f.svc.CreateLiveCall(context.Background(), defaultLiveCallRequest(), defaultLiveCallIdentity(f), 5)
	require.NoError(t, err)
	require.NotNil(t, created)

	f.provStore.mu.Lock()
	require.Empty(t, f.provStore.records, "disabled mode must not write any provisional record")
	f.provStore.mu.Unlock()

	require.Equal(t, int64(0), LiveProvisionalMetricsSnapshot().Written)
	require.Equal(t, "Bearer test-token", recordedHeaders.Get("Authorization"))
	require.Equal(t, "quicksilver=v2", recordedHeaders.Get("OpenAI-Alpha"))
}

func TestCreateLiveCallTwoConcurrentSessionsBothWriteRows(t *testing.T) {
	f := newLiveAuthTestFixture(t, config.CanonicalWalletModeShadow)

	f.svc.httpUpstream = &hookedHTTPUpstream{
		doFn: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Location": {"/backend-api/codex/call_concurrent"}},
				Body:       io.NopCloser(strings.NewReader("v=0\r\n")),
			}, nil
		},
	}

	var wg sync.WaitGroup
	errs := make([]error, 2)
	createds := make([]*LiveCallCreated, 2)

	for i := 0; i < 2; i++ {
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			createds[idx], errs[idx] = f.svc.CreateLiveCall(context.Background(), defaultLiveCallRequest(), defaultLiveCallIdentity(f), 5)
		}()
	}
	wg.Wait()

	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	require.NotNil(t, createds[0])
	require.NotNil(t, createds[1])

	f.provStore.mu.Lock()
	require.Len(t, f.provStore.records, 2)
	tokens := make([]string, 0, 2)
	for token := range f.provStore.records {
		tokens = append(tokens, token)
	}
	f.provStore.mu.Unlock()
	require.NotEqual(t, tokens[0], tokens[1])
}

func TestLiveSDPPostRefusalIsTerminal(t *testing.T) {
	f := newLiveAuthTestFixture(t, config.CanonicalWalletModeEnforce)

	// Decorate upstream HTTP client with CanonicalWalletHTTPClientDecorator
	innerDoCalls := 0
	rawUpstream := &hookedHTTPUpstream{
		doFn: func(req *http.Request) (*http.Response, error) {
			innerDoCalls++
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Location": {"/backend-api/codex/call_decorated"}},
				Body:       io.NopCloser(strings.NewReader("v=0\r\n")),
			}, nil
		},
	}
	dec := NewAuthorizingHTTPUpstream(rawUpstream, f.svc.cfg)
	f.svc.httpUpstream = dec

	// Inject refusal into AuthorizeBillableAttempt hook for test
	f.svc.SetAuthorizeBillableAttemptHookForTest(func(snap *BillingSnapshot, apiKey *APIKey, estimate EstimateInput) {
		// When called, we let it run, but simulate handle carrying a refusal in enforce
	})

	// To test the 3.3a decorator refusal at openai_live.go:405 directly:
	// We construct an attemptCtx carrying a handle whose Refusal is set.
	refusal := &AuthorizationRefusedError{
		Reason:          AuthorizationRefusalBalanceShortfall,
		AuthorizationID: "auth_injected_refusal",
	}
	handleWithRefusal := &AuthorizationHandle{
		ID:      "auth_injected_refusal",
		Refusal: refusal,
	}

	attemptCtx := WithAuthorizationHandle(context.Background(), handleWithRefusal)
	created, createErr := f.svc.createUpstreamLiveCall(attemptCtx, f.account, defaultLiveCallRequest(), `{"v":1,"s":0,"t":"v1.test"}`, nil)

	require.Error(t, createErr)
	require.Nil(t, created)
	var refusedErr *AuthorizationRefusedError
	require.True(t, errors.As(createErr, &refusedErr))
	require.Equal(t, AuthorizationRefusalBalanceShortfall, refusedErr.Reason)
	require.Equal(t, 0, innerDoCalls, "inner HTTP call must never be made when decorator refuses")
}
