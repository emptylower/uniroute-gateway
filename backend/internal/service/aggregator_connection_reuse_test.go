package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/Wei-Shaw/sub2api/internal/pkg/pagination"
)

type fakeReuseAccountRepo struct {
	accounts map[int64]*Account
	nextID int64
}

func newFakeReuseAccountRepo() *fakeReuseAccountRepo {
	return &fakeReuseAccountRepo{accounts: map[int64]*Account{}, nextID: 100}
}
func (f *fakeReuseAccountRepo) Create(ctx context.Context, acc *Account) error {
	acc.ID = f.nextID
	f.nextID++
	cp := *acc
	f.accounts[acc.ID] = &cp
	return nil
}
func (f *fakeReuseAccountRepo) GetByID(ctx context.Context, id int64) (*Account, error) {
	if a, ok := f.accounts[id]; ok {
		return a, nil
	}
	return nil, ErrAccountNotFound
}
func (f *fakeReuseAccountRepo) GetByIDs(ctx context.Context, ids []int64) ([]*Account, error) { return nil, nil }
func (f *fakeReuseAccountRepo) ExistsByID(ctx context.Context, id int64) (bool, error) { _, ok := f.accounts[id]; return ok, nil }
func (f *fakeReuseAccountRepo) GetByCRSAccountID(ctx context.Context, crsAccountID string) (*Account, error) { return nil, nil }
func (f *fakeReuseAccountRepo) FindByExtraField(ctx context.Context, key string, value any) ([]Account, error) { return nil, nil }
func (f *fakeReuseAccountRepo) ListCRSAccountIDs(ctx context.Context) (map[string]int64, error) { return nil, nil }
func (f *fakeReuseAccountRepo) Update(ctx context.Context, acc *Account) error {
	if _, ok := f.accounts[acc.ID]; !ok {
		return ErrAccountNotFound
	}
	cp := *acc
	f.accounts[acc.ID] = &cp
	return nil
}
func (f *fakeReuseAccountRepo) Delete(ctx context.Context, id int64) error { delete(f.accounts, id); return nil }
func (f *fakeReuseAccountRepo) List(ctx context.Context, params pagination.PaginationParams) ([]Account, *pagination.PaginationResult, error) { return nil, nil, nil }
func (f *fakeReuseAccountRepo) ListWithFilters(ctx context.Context, params pagination.PaginationParams, platform, accountType, status, search string, groupID int64, privacyMode string) ([]Account, *pagination.PaginationResult, error) { return nil, nil, nil }
func (f *fakeReuseAccountRepo) ListAllWithFilters(ctx context.Context, platform, accountType, status, search string, groupID int64, privacyMode string) ([]Account, error) { return nil, nil }
func (f *fakeReuseAccountRepo) ListByGroup(ctx context.Context, groupID int64) ([]Account, error) { return nil, nil }
func (f *fakeReuseAccountRepo) ListActive(ctx context.Context) ([]Account, error) { return nil, nil }
func (f *fakeReuseAccountRepo) ListByPlatform(ctx context.Context, platform string) ([]Account, error) { return nil, nil }
func (f *fakeReuseAccountRepo) UpdateLastUsed(ctx context.Context, id int64) error { return nil }
func (f *fakeReuseAccountRepo) BatchUpdateLastUsed(ctx context.Context, updates map[int64]time.Time) error { return nil }
func (f *fakeReuseAccountRepo) SetError(ctx context.Context, id int64, errorMsg string) error { return nil }
func (f *fakeReuseAccountRepo) ClearError(ctx context.Context, id int64) error { return nil }
func (f *fakeReuseAccountRepo) SetSchedulable(ctx context.Context, id int64, schedulable bool) error { return nil }
func (f *fakeReuseAccountRepo) AutoPauseExpiredAccounts(ctx context.Context, now time.Time) (int64, error) { return 0, nil }
func (f *fakeReuseAccountRepo) BindGroups(ctx context.Context, accountID int64, groupIDs []int64) error { return nil }
func (f *fakeReuseAccountRepo) ListSchedulable(ctx context.Context) ([]Account, error) { return nil, nil }
func (f *fakeReuseAccountRepo) ListSchedulableByGroupID(ctx context.Context, groupID int64) ([]Account, error) { return nil, nil }
func (f *fakeReuseAccountRepo) ListSchedulableByPlatform(ctx context.Context, platform string) ([]Account, error) { return nil, nil }
func (f *fakeReuseAccountRepo) ListSchedulableByGroupIDAndPlatform(ctx context.Context, groupID int64, platform string) ([]Account, error) { return nil, nil }
func (f *fakeReuseAccountRepo) ListSchedulableByPlatforms(ctx context.Context, platforms []string) ([]Account, error) { return nil, nil }
func (f *fakeReuseAccountRepo) ListSchedulableByGroupIDAndPlatforms(ctx context.Context, groupID int64, platforms []string) ([]Account, error) { return nil, nil }
func (f *fakeReuseAccountRepo) ListSchedulableUngroupedByPlatform(ctx context.Context, platform string) ([]Account, error) { return nil, nil }
func (f *fakeReuseAccountRepo) ListSchedulableUngroupedByPlatforms(ctx context.Context, platforms []string) ([]Account, error) { return nil, nil }
func (f *fakeReuseAccountRepo) ListModelAvailabilityCandidates(ctx context.Context, groupID *int64, platforms []string, includeGrouped bool) ([]Account, error) { return nil, nil }
func (f *fakeReuseAccountRepo) SetRateLimited(ctx context.Context, id int64, resetAt time.Time) error { return nil }
func (f *fakeReuseAccountRepo) SetModelRateLimit(ctx context.Context, id int64, scope string, resetAt time.Time, reason ...string) error { return nil }
func (f *fakeReuseAccountRepo) SetOverloaded(ctx context.Context, id int64, until time.Time) error { return nil }
func (f *fakeReuseAccountRepo) SetTempUnschedulable(ctx context.Context, id int64, until time.Time, reason string) error { return nil }
func (f *fakeReuseAccountRepo) ClearTempUnschedulable(ctx context.Context, id int64) error { return nil }
func (f *fakeReuseAccountRepo) ClearRateLimit(ctx context.Context, id int64) error { return nil }
func (f *fakeReuseAccountRepo) ClearAntigravityQuotaScopes(ctx context.Context, id int64) error { return nil }
func (f *fakeReuseAccountRepo) ClearModelRateLimits(ctx context.Context, id int64) error { return nil }
func (f *fakeReuseAccountRepo) UpdateSessionWindow(ctx context.Context, id int64, start, end *time.Time, status string) error { return nil }
func (f *fakeReuseAccountRepo) UpdateSessionWindowEnd(ctx context.Context, id int64, end time.Time) error { return nil }
func (f *fakeReuseAccountRepo) UpdateExtra(ctx context.Context, id int64, updates map[string]any) error { return nil }
func (f *fakeReuseAccountRepo) BulkUpdate(ctx context.Context, ids []int64, updates AccountBulkUpdate) (int64, error) { return 0, nil }
func (f *fakeReuseAccountRepo) IncrementQuotaUsed(ctx context.Context, id int64, amount float64) error { return nil }
func (f *fakeReuseAccountRepo) ResetQuotaUsed(ctx context.Context, id int64) error { return nil }
func (f *fakeReuseAccountRepo) RevertProxyFallback(ctx context.Context, accountID int64) error { return nil }
func (f *fakeReuseAccountRepo) ListShadowsByParent(ctx context.Context, parentID int64) ([]*Account, error) { return nil, nil }

type fakeReuseRepo struct {
	scopes map[string]int64
}

func newFakeReuseRepo() *fakeReuseRepo { return &fakeReuseRepo{scopes: map[string]int64{}} }
func (f *fakeReuseRepo) FindByScope(ctx context.Context, cid int64, p GovernanceProvider, pr AccountProtocol, ep, clientID string) (int64, bool, error) {
	key := string(rune(cid)) + string(p) + string(pr) + ep + clientID
	if id, ok := f.scopes[key]; ok {
		return id, true, nil
	}
	return 0, false, nil
}
func (f *fakeReuseRepo) Create(ctx context.Context, cid int64, p GovernanceProvider, pr AccountProtocol, ep, clientID string, accountID int64) error {
	key := string(rune(cid)) + string(p) + string(pr) + ep + clientID
	f.scopes[key] = accountID
	return nil
}

func TestAggregatorConnectionReuseInactiveThenProbeThenActivation(t *testing.T) {
	connRepo := newFakeUpstreamConnRepo()
	anthropic := GovernanceProviderAnthropic
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500); w.Write([]byte(`error`)) }))
	defer srv.Close()
	agg := &UpstreamConnection{Kind: "aggregator", BaseURL: srv.URL, CredentialVersion: 1, Status: "active"}
	agg.Provider = nil
	_ = connRepo.Create(context.Background(), agg, "enc:agg-cred")
	aggID := agg.ID
	fpConn := &UpstreamConnection{Kind: "first_party", Provider: &anthropic, BaseURL: "https://api.anthropic.com", CredentialVersion: 1, Status: "active"}
	_ = connRepo.Create(context.Background(), fpConn, "enc:fp")
	accountRepo := newFakeReuseAccountRepo()
	reuseRepo := newFakeReuseRepo()
	probeRepo := &fakeProbeRepo{}
	probeSvc := NewAccountEndpointProbeService(probeRepo, srv.Client())
	svc := NewAggregatorConnectionReuseServiceWithRepo(connRepo, accountRepo, reuseRepo, probeSvc)
	_, err := svc.Reuse(context.Background(), ReuseAggregatorConnectionInput{ConnectionID: fpConn.ID, Provider: GovernanceProviderOpenAI, Protocol: AccountProtocolOpenAI, NormalizedEndpoint: "/v1/chat/completions", ClientRequestID: "req-1", CredentialVersion: 1})
	require.Error(t, err)
	require.Contains(t, err.Error(), "first-party")
	id, err := svc.Reuse(context.Background(), ReuseAggregatorConnectionInput{ConnectionID: aggID, Provider: GovernanceProviderOpenAI, Protocol: AccountProtocolOpenAI, NormalizedEndpoint: "/v1/chat/completions", ClientRequestID: "req-2", CredentialVersion: 1})
	require.NoError(t, err)
	require.NotZero(t, id)
	acc, _ := accountRepo.GetByID(context.Background(), id)
	require.NotNil(t, acc)
	require.Equal(t, "disabled", acc.Status)
}

func TestAggregatorConnectionReuseIdempotency(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500); w.Write([]byte(`error`)) }))
	defer srv.Close()
	connRepo := newFakeUpstreamConnRepo()
	agg := &UpstreamConnection{Kind: "aggregator", BaseURL: srv.URL, CredentialVersion: 5, Status: "active"}
	_ = connRepo.Create(context.Background(), agg, "enc:agg")
	accountRepo := newFakeReuseAccountRepo()
	reuseRepo := newFakeReuseRepo()
	probeSvc := NewAccountEndpointProbeService(&fakeProbeRepo{}, srv.Client())
	svc := NewAggregatorConnectionReuseServiceWithRepo(connRepo, accountRepo, reuseRepo, probeSvc)
	input := ReuseAggregatorConnectionInput{ConnectionID: agg.ID, Provider: GovernanceProviderGemini, Protocol: AccountProtocolGemini, NormalizedEndpoint: "/v1beta/models", ClientRequestID: "idem-1", CredentialVersion: 5}
	id1, err := svc.Reuse(context.Background(), input)
	require.NoError(t, err)
	id2, err := svc.Reuse(context.Background(), input)
	require.NoError(t, err)
	require.Equal(t, id1, id2)
}

func TestAggregatorConnectionReuseNoCapacityReservation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500); w.Write([]byte(`error`)) }))
	defer srv.Close()
	connRepo := newFakeUpstreamConnRepo()
	agg := &UpstreamConnection{Kind: "aggregator", BaseURL: srv.URL, CredentialVersion: 1, Status: "active"}
	_ = connRepo.Create(context.Background(), agg, "enc:agg")
	accountRepo := newFakeReuseAccountRepo()
	reuseRepo := newFakeReuseRepo()
	svc := NewAggregatorConnectionReuseServiceWithRepo(connRepo, accountRepo, reuseRepo, NewAccountEndpointProbeService(&fakeProbeRepo{}, srv.Client()))
	_, _ = svc.Reuse(context.Background(), ReuseAggregatorConnectionInput{ConnectionID: agg.ID, Provider: GovernanceProviderOpenAI, Protocol: AccountProtocolOpenAI, NormalizedEndpoint: "/v1/chat/completions", ClientRequestID: "c1", CredentialVersion: 1})
	_, _ = svc.Reuse(context.Background(), ReuseAggregatorConnectionInput{ConnectionID: agg.ID, Provider: GovernanceProviderGemini, Protocol: AccountProtocolGemini, NormalizedEndpoint: "/v1beta/models", ClientRequestID: "c2", CredentialVersion: 1})
	require.Len(t, accountRepo.accounts, 2)
	for _, acc := range accountRepo.accounts {
		require.NotNil(t, acc.ConnectionID)
		require.Equal(t, agg.ID, *acc.ConnectionID)
	}
}
