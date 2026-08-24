package service

import (
	"context"
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
