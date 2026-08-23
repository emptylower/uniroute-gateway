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
	var envelope struct {
		Code int            `json:"code"`
		Data map[string]any `json:"data"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &envelope))
	require.Equal(t, 0, envelope.Code)
	body := envelope.Data
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

func TestDeriveFromAccountCreatesAggregatorAndLinks(t *testing.T) {
	repo := newStubConnLinkRepo()
	accounts := &linkAccountTestRepo{
		accounts: map[int64]*service.Account{
			4: {ID: 4, Platform: "anthropic", Credentials: map[string]any{
				"base_url": "https://agg.example.com/v1",
				"api_key":  "sk-aggregator",
			}},
		},
		updated: map[int64]int64{},
	}
	handler := newConnectionLinkHandler(repo, accounts)
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST("/accounts/:id/derive-connection", handler.DeriveFromAccount)
	recorder := httptest.NewRecorder()

	req := httptest.NewRequest(http.MethodPost, "/accounts/4/derive-connection", nil)
	req.Header.Set("Idempotency-Key", "derive-1")
	router.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusCreated, recorder.Code)
	var envelope struct {
		Code int            `json:"code"`
		Data map[string]any `json:"data"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &envelope))
	require.Equal(t, 0, envelope.Code)
	body := envelope.Data
	require.Equal(t, "aggregator", body["kind"])
	require.Nil(t, body["provider"], "aggregator must stay provider-less")
	require.Equal(t, true, body["linked"])
	require.NotContains(t, recorder.Body.String(), "sk-aggregator", "credential never echoed")
	connID := int64(body["connection_id"].(float64))
	require.Equal(t, connID, accounts.updated[4], "account linked to derived connection")
}

func TestDeriveFromAccountReplayIsIdempotent(t *testing.T) {
	repo := newStubConnLinkRepo()
	accounts := &linkAccountTestRepo{
		accounts: map[int64]*service.Account{
			4: {ID: 4, Platform: "anthropic", Credentials: map[string]any{
				"base_url": "https://agg.example.com",
				"api_key":  "sk-1",
			}},
		},
		updated: map[int64]int64{},
	}
	handler := newConnectionLinkHandler(repo, accounts)
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST("/accounts/:id/derive-connection", handler.DeriveFromAccount)

	rec1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodPost, "/accounts/4/derive-connection", nil)
	req1.Header.Set("Idempotency-Key", "k")
	router.ServeHTTP(rec1, req1)
	require.Equal(t, http.StatusCreated, rec1.Code)

	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/accounts/4/derive-connection", nil)
	req2.Header.Set("Idempotency-Key", "k")
	router.ServeHTTP(rec2, req2)
	require.Equal(t, http.StatusOK, rec2.Code)
	require.Len(t, repo.conns, 1, "replay must not duplicate the connection")
}

func TestDeriveFromAccountRejectsMissingParts(t *testing.T) {
	base := func(creds map[string]any) *linkAccountTestRepo {
		return &linkAccountTestRepo{
			accounts: map[int64]*service.Account{4: {ID: 4, Platform: "anthropic", Credentials: creds}},
			updated:  map[int64]int64{},
		}
	}
	cases := []struct {
		name    string
		creds   map[string]any
		wantMsg string
	}{
		{"no base_url", map[string]any{"api_key": "sk"}, "no custom base_url"},
		{"no api_key", map[string]any{"base_url": "https://x"}, "no api_key credential"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo := newStubConnLinkRepo()
			handler := newConnectionLinkHandler(repo, base(tc.creds))
			gin.SetMode(gin.TestMode)
			router := gin.New()
			router.POST("/accounts/:id/derive-connection", handler.DeriveFromAccount)
			recorder := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/accounts/4/derive-connection", nil)
			req.Header.Set("Idempotency-Key", "k")
			router.ServeHTTP(recorder, req)
			require.Equal(t, http.StatusBadRequest, recorder.Code)
			require.Contains(t, recorder.Body.String(), tc.wantMsg)
			require.Empty(t, repo.conns)
		})
	}
}

func TestDeriveFromAccountRejectsUngovernedPlatform(t *testing.T) {
	repo := newStubConnLinkRepo()
	accounts := &linkAccountTestRepo{
		accounts: map[int64]*service.Account{
			7: {ID: 7, Platform: "vertex", Credentials: map[string]any{
				"base_url": "https://x", "api_key": "sk",
			}},
		},
		updated: map[int64]int64{},
	}
	handler := newConnectionLinkHandler(repo, accounts)
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST("/accounts/:id/derive-connection", handler.DeriveFromAccount)
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/accounts/7/derive-connection", nil)
	req.Header.Set("Idempotency-Key", "k")
	router.ServeHTTP(recorder, req)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Contains(t, recorder.Body.String(), "governed platform")
}
