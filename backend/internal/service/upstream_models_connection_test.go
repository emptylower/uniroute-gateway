package service

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeConnRepoForTest struct {
	conn *UpstreamConnection
	enc  string
	err  error
}

func (f *fakeConnRepoForTest) GetByID(ctx context.Context, id int64) (*UpstreamConnection, string, error) {
	if f.err != nil {
		return nil, "", f.err
	}
	return f.conn, f.enc, nil
}
func (f *fakeConnRepoForTest) Create(ctx context.Context, conn *UpstreamConnection, encryptedCredential string) error {
	return nil
}
func (f *fakeConnRepoForTest) UpdateCredential(ctx context.Context, id int64, expectedVersion int64, encryptedCredential string) (int64, error) {
	return 0, nil
}
func (f *fakeConnRepoForTest) UpdateStatus(ctx context.Context, id int64, status string) error { return nil }
func (f *fakeConnRepoForTest) BatchGetByIDs(ctx context.Context, ids []int64) (map[int64]*UpstreamConnection, error) {
	return nil, nil
}
func (f *fakeConnRepoForTest) ListAll(ctx context.Context) ([]*UpstreamConnection, error) { return nil, nil }

func TestResolveUpstreamRequestMaterial_ConnectionBaseURLOverridesAccount(t *testing.T) {
	account := &Account{
		Platform: PlatformAnthropic,
		Credentials: map[string]interface{}{
			"base_url": "https://api.anthropic.com",
			"api_key":  "test-key",
		},
	}
	connID := int64(42)
	account.ConnectionID = &connID
	account.Protocol = connTestStrPtr("anthropic")
	// Mock connection repo returning override URL
	provider := GovernanceProviderAnthropic
	fakeRepo := &fakeConnRepoForTest{
		conn: &UpstreamConnection{
			ID:       42,
			Kind:     "first_party",
			Provider: &provider,
			BaseURL:  "https://override.example.com",
		},
		enc: "enc:fake",
	}
	svc := &AccountTestService{
		upstreamConnRepo: fakeRepo,
	}
	material, err := svc.resolveUpstreamRequestMaterial(context.Background(), account)
	require.NoError(t, err)
	require.NotNil(t, material.Connection)
	require.Equal(t, "https://override.example.com", material.BaseURL)
	// Provider must not be overridden by connection
	require.NotNil(t, material.Provider)
	require.Equal(t, GovernanceProviderAnthropic, *material.Provider)
}

func connTestStrPtr(s string) *string { return &s }

func TestBuildUpstreamModelsRequest_EndpointRootGetsModelsSuffix(t *testing.T) {
	// Regression: governance EndpointPath is the API ROOT ("/v1"); the model
	// list must be fetched at {base}{root}/models, not at the root itself.
	connID := int64(7)
	endpoint := "/v1"
	account := &Account{
		Platform:     PlatformOpenAI,
		Type:         AccountTypeAPIKey,
		Credentials:  map[string]any{"api_key": "sk-test"},
		ConnectionID: &connID,
		EndpointPath: &endpoint,
	}
	svc := &AccountTestService{
		upstreamConnRepo: &fakeConnRepoForTest{
			conn: &UpstreamConnection{ID: connID, Kind: "aggregator", BaseURL: "https://api.aicodewith.ai"},
		},
	}
	req, err := svc.buildUpstreamModelsRequest(context.Background(), account)
	require.NoError(t, err)
	require.Equal(t, "https://api.aicodewith.ai/v1/models", req.URL.String())
	require.Equal(t, "Bearer sk-test", req.Header.Get("Authorization"))
}

func TestBuildUpstreamModelsRequest_LegacyFullModelsPathPreserved(t *testing.T) {
	connID := int64(7)
	endpoint := "/v1/models"
	account := &Account{
		Platform:     PlatformOpenAI,
		Type:         AccountTypeAPIKey,
		Credentials:  map[string]any{"api_key": "sk-test"},
		ConnectionID: &connID,
		EndpointPath: &endpoint,
	}
	svc := &AccountTestService{
		upstreamConnRepo: &fakeConnRepoForTest{
			conn: &UpstreamConnection{ID: connID, Kind: "aggregator", BaseURL: "https://agg.example.com"},
		},
	}
	req, err := svc.buildUpstreamModelsRequest(context.Background(), account)
	require.NoError(t, err)
	require.Equal(t, "https://agg.example.com/v1/models", req.URL.String())
}

func TestFetchUpstreamModelDiscoveryGeminiOpenAISurfaceFallback(t *testing.T) {
	// Real aggregator shape: gemini surface 404s the model listing; the unified
	// catalog on the sibling openai surface (/v1/models) carries gemini-* ids.
	connID := int64(11)
	endpoint := "/v1beta"
	account := &Account{
		Platform:     PlatformGemini,
		Type:         AccountTypeAPIKey,
		Credentials:  map[string]any{"api_key": "sk-test"},
		ConnectionID: &connID,
		EndpointPath: &endpoint,
	}
	var urls []string
	upstream := &queuedHTTPUpstreamStub{
		responses: []*http.Response{
			{
				StatusCode: http.StatusNotFound,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(`{"error":"不支持的请求路径，请检查 API 地址是否正确 (request_id: req_x)"}`)),
			},
			{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body: io.NopCloser(strings.NewReader(
					`{"object":"list","data":[{"id":"gemini-3-pro-preview"},{"id":"claude-opus-5"},{"id":"gemini-3.5-flash"}]}`)),
			},
		},
		onCall: func(req *http.Request, _ *queuedHTTPUpstreamStub) { urls = append(urls, req.URL.String()) },
	}
	svc := &AccountTestService{
		httpUpstream: upstream,
		upstreamConnRepo: &fakeConnRepoForTest{
			conn: &UpstreamConnection{ID: connID, Kind: "aggregator", BaseURL: "https://agg.example.com/gemini_cli"},
		},
		cfg: upstreamModelSyncTestConfig(),
	}
	discovery, err := svc.FetchUpstreamModelDiscovery(context.Background(), account)
	require.NoError(t, err)
	require.Equal(t, []string{"gemini-3-pro-preview", "gemini-3.5-flash"}, discovery.Models, "fallback must keep only gemini ids")
	require.Len(t, urls, 2)
	require.Equal(t, "https://agg.example.com/v1/models", urls[1], "fallback hits the openai surface at the site origin")
}

func TestFetchUpstreamModelDiscoveryGeminiFallbackAlso404StillFails(t *testing.T) {
	connID := int64(11)
	endpoint := "/v1beta"
	account := &Account{
		Platform:     PlatformGemini,
		Type:         AccountTypeAPIKey,
		Credentials:  map[string]any{"api_key": "sk-test"},
		ConnectionID: &connID,
		EndpointPath: &endpoint,
	}
	upstream := &queuedHTTPUpstreamStub{
		responses: []*http.Response{
			{StatusCode: http.StatusNotFound, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"error":"不支持的请求路径"}`))},
			{StatusCode: http.StatusNotFound, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"error":"不支持的请求路径"}`))},
		},
	}
	svc := &AccountTestService{
		httpUpstream: upstream,
		upstreamConnRepo: &fakeConnRepoForTest{
			conn: &UpstreamConnection{ID: connID, Kind: "aggregator", BaseURL: "https://agg.example.com/gemini_cli"},
		},
		cfg: upstreamModelSyncTestConfig(),
	}
	_, err := svc.FetchUpstreamModelDiscovery(context.Background(), account)
	require.Error(t, err)
}
