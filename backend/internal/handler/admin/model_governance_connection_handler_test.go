package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type stubConnLinkRepo struct {
	conns  map[int64]*service.UpstreamConnection
	nextID int64
}

func newStubConnLinkRepo() *stubConnLinkRepo {
	return &stubConnLinkRepo{conns: map[int64]*service.UpstreamConnection{}, nextID: 100}
}

func (r *stubConnLinkRepo) Create(_ context.Context, conn *service.UpstreamConnection, _ string) error {
	r.nextID++
	conn.ID = r.nextID
	r.conns[conn.ID] = conn
	return nil
}

func (r *stubConnLinkRepo) GetByID(_ context.Context, id int64) (*service.UpstreamConnection, string, error) {
	conn, ok := r.conns[id]
	if !ok {
		return nil, "", nil
	}
	return conn, "enc", nil
}

func (r *stubConnLinkRepo) UpdateCredential(_ context.Context, _ int64, _ int64, _ string) (int64, error) {
	return 0, nil
}

func (r *stubConnLinkRepo) BatchGetByIDs(_ context.Context, ids []int64) (map[int64]*service.UpstreamConnection, error) {
	out := map[int64]*service.UpstreamConnection{}
	for _, id := range ids {
		if conn, ok := r.conns[id]; ok {
			out[id] = conn
		}
	}
	return out, nil
}

func (r *stubConnLinkRepo) ListAll(_ context.Context) ([]*service.UpstreamConnection, error) {
	out := make([]*service.UpstreamConnection, 0, len(r.conns))
	for _, conn := range r.conns {
		out = append(out, conn)
	}
	return out, nil
}

func (r *stubConnLinkRepo) UpdateStatus(_ context.Context, _ int64, _ string) error { return nil }

type stubLinkEncryptor struct{}

func (stubLinkEncryptor) Encrypt(plaintext string) (string, error) { return "enc:" + plaintext, nil }
func (stubLinkEncryptor) Decrypt(ciphertext string) (string, error) {
	return ciphertext, nil
}

type linkAccountTestRepo struct {
	service.AccountRepository
	accounts map[int64]*service.Account
	updated  map[int64]int64
}

func (r *linkAccountTestRepo) GetByID(_ context.Context, id int64) (*service.Account, error) {
	account, ok := r.accounts[id]
	if !ok {
		return nil, service.ErrAccountNotFound
	}
	return account, nil
}

func (r *linkAccountTestRepo) Update(_ context.Context, account *service.Account) error {
	r.updated[account.ID] = *account.ConnectionID
	return nil
}

func newConnectionLinkHandler(repo *stubConnLinkRepo, accounts *linkAccountTestRepo) *ModelGovernanceConnectionHandler {
	svc := service.NewUpstreamConnectionService(repo, stubLinkEncryptor{})
	return NewModelGovernanceConnectionHandler(svc, nil, nil, accounts)
}

func setupConnectionLinkRouter(handler *ModelGovernanceConnectionHandler) (*gin.Engine, *httptest.ResponseRecorder) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST("/connections", handler.Create)
	router.PUT("/accounts/:id/connection", handler.LinkAccount)
	recorder := httptest.NewRecorder()
	return router, recorder
}

func doConnectionLinkJSON(
	router *gin.Engine,
	recorder *httptest.ResponseRecorder,
	method,
	path string,
	body any,
	headers map[string]string,
) {
	payload, _ := json.Marshal(body)
	req := httptest.NewRequest(method, path, bytes.NewReader(payload))
	req.Header.Set("content-type", "application/json")
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	router.ServeHTTP(recorder, req)
}

func TestCreateConnectionMintsActiveAggregator(t *testing.T) {
	repo := newStubConnLinkRepo()
	handler := newConnectionLinkHandler(repo, &linkAccountTestRepo{updated: map[int64]int64{}})
	router, recorder := setupConnectionLinkRouter(handler)

	doConnectionLinkJSON(router, recorder, http.MethodPost, "/connections", map[string]any{
		"kind": "aggregator", "base_url": "https://agg.example.com/", "credential": "sk-test",
	}, nil)

	require.Equal(t, http.StatusCreated, recorder.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
	require.Equal(t, "aggregator", body["kind"])
	require.Nil(t, body["provider"])
	require.Equal(t, "https://agg.example.com", body["base_url"], "trailing slash normalized")
	require.NotContains(t, recorder.Body.String(), "sk-test", "credential never echoed")
	require.Len(t, repo.conns, 1)
}

func TestCreateConnectionReplayReturnsExistingWithoutDuplicate(t *testing.T) {
	repo := newStubConnLinkRepo()
	handler := newConnectionLinkHandler(repo, &linkAccountTestRepo{updated: map[int64]int64{}})
	router, recorder := setupConnectionLinkRouter(handler)

	body := map[string]any{"kind": "aggregator", "base_url": "https://agg.example.com", "credential": "sk-1"}
	doConnectionLinkJSON(router, recorder, http.MethodPost, "/connections", body, nil)
	require.Equal(t, http.StatusCreated, recorder.Code)

	recorder2 := httptest.NewRecorder()
	doConnectionLinkJSON(router, recorder2, http.MethodPost, "/connections", body, nil)
	require.Equal(t, http.StatusOK, recorder2.Code)
	require.Len(t, repo.conns, 1, "no duplicate row on natural-identity replay")
}

func TestLinkAccountRequiresIdempotencyKey(t *testing.T) {
	repo := newStubConnLinkRepo()
	accounts := &linkAccountTestRepo{
		accounts: map[int64]*service.Account{4: {ID: 4, Platform: "anthropic"}},
		updated:  map[int64]int64{},
	}
	handler := newConnectionLinkHandler(repo, accounts)
	router, recorder := setupConnectionLinkRouter(handler)

	doConnectionLinkJSON(router, recorder, http.MethodPut, "/accounts/4/connection",
		map[string]any{"connection_id": 1}, nil)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Empty(t, accounts.updated)
}

func TestLinkAccountAggregatorAcceptsAnyGovernedPlatform(t *testing.T) {
	repo := newStubConnLinkRepo()
	repo.conns[9] = &service.UpstreamConnection{ID: 9, Kind: "aggregator", BaseURL: "https://agg.example.com", Status: "active"}
	accounts := &linkAccountTestRepo{
		accounts: map[int64]*service.Account{4: {ID: 4, Platform: "anthropic"}},
		updated:  map[int64]int64{},
	}
	handler := newConnectionLinkHandler(repo, accounts)
	router, recorder := setupConnectionLinkRouter(handler)

	doConnectionLinkJSON(router, recorder, http.MethodPut, "/accounts/4/connection",
		map[string]any{"connection_id": 9}, map[string]string{"Idempotency-Key": "link-1"})

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, int64(9), accounts.updated[4])
}

func TestLinkAccountFirstPartyMismatchRejected(t *testing.T) {
	openai := service.GovernanceProviderOpenAI
	repo := newStubConnLinkRepo()
	repo.conns[5] = &service.UpstreamConnection{ID: 5, Kind: "first_party", Provider: &openai, Status: "active"}
	accounts := &linkAccountTestRepo{
		accounts: map[int64]*service.Account{4: {ID: 4, Platform: "anthropic"}},
		updated:  map[int64]int64{},
	}
	handler := newConnectionLinkHandler(repo, accounts)
	router, recorder := setupConnectionLinkRouter(handler)

	doConnectionLinkJSON(router, recorder, http.MethodPut, "/accounts/4/connection",
		map[string]any{"connection_id": 5}, map[string]string{"Idempotency-Key": "link-2"})

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Empty(t, accounts.updated)
}

func TestLinkAccountInactiveConnectionRejected(t *testing.T) {
	repo := newStubConnLinkRepo()
	repo.conns[6] = &service.UpstreamConnection{ID: 6, Kind: "aggregator", Status: "suspended"}
	accounts := &linkAccountTestRepo{
		accounts: map[int64]*service.Account{4: {ID: 4, Platform: "anthropic"}},
		updated:  map[int64]int64{},
	}
	handler := newConnectionLinkHandler(repo, accounts)
	router, recorder := setupConnectionLinkRouter(handler)

	doConnectionLinkJSON(router, recorder, http.MethodPut, "/accounts/4/connection",
		map[string]any{"connection_id": 6}, map[string]string{"Idempotency-Key": "link-3"})

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Empty(t, accounts.updated)
}

func TestLinkAccountNotFoundPaths(t *testing.T) {
	repo := newStubConnLinkRepo()
	accounts := &linkAccountTestRepo{
		accounts: map[int64]*service.Account{4: {ID: 4, Platform: "anthropic"}},
		updated:  map[int64]int64{},
	}
	handler := newConnectionLinkHandler(repo, accounts)
	router, recorder := setupConnectionLinkRouter(handler)

	doConnectionLinkJSON(router, recorder, http.MethodPut, "/accounts/4/connection",
		map[string]any{"connection_id": 404}, map[string]string{"Idempotency-Key": "k"})
	require.Equal(t, http.StatusNotFound, recorder.Code)

	recorder2 := httptest.NewRecorder()
	doConnectionLinkJSON(router, recorder2, http.MethodPut, "/accounts/999/connection",
		map[string]any{"connection_id": 1}, map[string]string{"Idempotency-Key": "k"})
	require.Equal(t, http.StatusNotFound, recorder2.Code)
}
