package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeProbeRepo struct {
	latest *AccountEndpointProbe
	inserted []*AccountEndpointProbe
}

func (f *fakeProbeRepo) FindLatestValid(ctx context.Context, accountID int64, now time.Time) (*AccountEndpointProbe, error) {
	if f.latest != nil && f.latest.AccountID == accountID && f.latest.ExpiresAt.After(now) && f.latest.Status == "success" {
		return f.latest, nil
	}
	return nil, nil
}
func (f *fakeProbeRepo) FindByKey(ctx context.Context, accountID int64, connectionID int64, provider GovernanceProvider, protocol AccountProtocol, endpoint string, credentialVersion, configVersion int64) (*AccountEndpointProbe, error) {
	return nil, nil
}
func (f *fakeProbeRepo) Insert(ctx context.Context, probe *AccountEndpointProbe) error {
	f.inserted = append(f.inserted, probe)
	probe.ID = int64(len(f.inserted))
	return nil
}
func (f *fakeProbeRepo) InvalidateOnConfigChange(ctx context.Context, accountID int64) error { return nil }

func TestAccountEndpointProbeMinimalProtocolRequests(t *testing.T) {
	for _, tc := range []struct{
		protocol AccountProtocol
		provider GovernanceProvider
		endpoint string
	}{
		{AccountProtocolAnthropic, GovernanceProviderAnthropic, "/v1/messages"},
		{AccountProtocolOpenAI, GovernanceProviderOpenAI, "/v1/chat/completions"},
		{AccountProtocolGemini, GovernanceProviderGemini, "/v1beta/models"},
	} {
		// Mock server that returns success shape per protocol
		var handler http.HandlerFunc
		switch tc.protocol {
		case AccountProtocolAnthropic:
			handler = func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{"id":"test","content":[{"type":"text","text":"hi"}]}`)) }
		case AccountProtocolOpenAI:
			handler = func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{"id":"chatcmpl-123","choices":[{"message":{"content":"hi"}}]}`)) }
		case AccountProtocolGemini:
			handler = func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{"candidates":[{"content":{"parts":[{"text":"hi"}]}}]}`)) }
		}
		srv := httptest.NewServer(handler)
		repo := &fakeProbeRepo{}
		svc := NewAccountEndpointProbeService(repo, srv.Client())
		conn := &UpstreamConnection{ID: 1, Kind: "first_party", Provider: &tc.provider, BaseURL: srv.URL, CredentialVersion: 1, Status: "active"}
		probe, err := svc.Probe(context.Background(), 1, conn, tc.provider, tc.protocol, tc.endpoint, "test-cred")
		srv.Close()
		require.NoError(t, err)
		require.NotNil(t, probe)
		require.Equal(t, "success", probe.Status)
		require.True(t, probe.ExpiresAt.After(probe.ProbedAt))
		require.Equal(t, 24*time.Hour, probe.ExpiresAt.Sub(probe.ProbedAt))
	}
}

func TestAccountEndpointProbeTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.Write([]byte(`{"id":"test"}`))
	}))
	defer srv.Close()
	repo := &fakeProbeRepo{}
	// Use client with short timeout via service's 10s bound; we simulate timeout by using context with 50ms
	svc := NewAccountEndpointProbeService(repo, &http.Client{Timeout: 50 * time.Millisecond})
	conn := &UpstreamConnection{ID: 1, Kind: "first_party", Provider: func() *GovernanceProvider { p := GovernanceProviderOpenAI; return &p }(), BaseURL: srv.URL, CredentialVersion: 1, Status: "active"}
	probe, err := svc.Probe(context.Background(), 1, conn, GovernanceProviderOpenAI, AccountProtocolOpenAI, "/v1/chat/completions", "cred")
	require.NoError(t, err)
	require.Equal(t, "failed", probe.Status)
}

func TestAccountEndpointProbeMalformedSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`not-json`))
	}))
	defer srv.Close()
	repo := &fakeProbeRepo{}
	svc := NewAccountEndpointProbeService(repo, srv.Client())
	conn := &UpstreamConnection{ID: 1, Kind: "first_party", Provider: func() *GovernanceProvider { p := GovernanceProviderOpenAI; return &p }(), BaseURL: srv.URL, CredentialVersion: 1, Status: "active"}
	probe, _ := svc.Probe(context.Background(), 1, conn, GovernanceProviderOpenAI, AccountProtocolOpenAI, "/v1/chat/completions", "cred")
	require.Equal(t, "failed", probe.Status)
	require.True(t, probe.ResponseSummary["malformed"] == true)
}

func TestAccountEndpointProbe401Scope(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(401)
		w.Write([]byte(`{"error":"revoked"}`))
	}))
	defer srv.Close()
	repo := &fakeProbeRepo{}
	svc := NewAccountEndpointProbeService(repo, srv.Client())
	conn := &UpstreamConnection{ID: 1, Kind: "first_party", Provider: func() *GovernanceProvider { p := GovernanceProviderOpenAI; return &p }(), BaseURL: srv.URL, CredentialVersion: 1, Status: "active"}
	probe, _ := svc.Probe(context.Background(), 1, conn, GovernanceProviderOpenAI, AccountProtocolOpenAI, "/v1/chat/completions", "cred")
	require.Equal(t, "failed", probe.Status)
	require.NotEmpty(t, probe.ResponseSummary["failure_scope"])
}

func TestAccountEndpointProbeModel404Scope(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
		w.Write([]byte(`model gpt-4o-mini not found`))
	}))
	defer srv.Close()
	repo := &fakeProbeRepo{}
	svc := NewAccountEndpointProbeService(repo, srv.Client())
	conn := &UpstreamConnection{ID: 1, Kind: "first_party", Provider: func() *GovernanceProvider { p := GovernanceProviderOpenAI; return &p }(), BaseURL: srv.URL, CredentialVersion: 1, Status: "active"}
	probe, _ := svc.Probe(context.Background(), 1, conn, GovernanceProviderOpenAI, AccountProtocolOpenAI, "/v1/chat/completions", "cred")
	require.Equal(t, "failed", probe.Status)
	require.Equal(t, "model", probe.ResponseSummary["failure_scope"])
}

func TestAccountEndpointProbe24hValidityAndInvalidation(t *testing.T) {
	repo := &fakeProbeRepo{}
	svc := NewAccountEndpointProbeService(repo, nil)
	now := time.Now()
	probe := &AccountEndpointProbe{AccountID: 42, ConnectionID: 1, Provider: GovernanceProviderAnthropic, Protocol: AccountProtocolAnthropic, NormalizedEndpoint: "/v1/messages", CredentialVersion: 1, ConfigVersion: 1, Status: "success", ProbedAt: now, ExpiresAt: now.Add(24 * time.Hour)}
	repo.latest = probe
	valid, _ := svc.IsValid(context.Background(), 42)
	require.True(t, valid)
	// Simulate credential version bump invalidates: clear latest and check invalid
	repo.latest = nil
	valid, _ = svc.IsValid(context.Background(), 42)
	require.False(t, valid)
	// Expired probe
	expired := &AccountEndpointProbe{AccountID: 42, ConnectionID: 1, Provider: GovernanceProviderAnthropic, Protocol: AccountProtocolAnthropic, NormalizedEndpoint: "/v1/messages", CredentialVersion: 1, ConfigVersion: 1, Status: "success", ProbedAt: now.Add(-25 * time.Hour), ExpiresAt: now.Add(-1 * time.Hour)}
	repo.latest = expired
	valid, _ = svc.IsValid(context.Background(), 42)
	require.False(t, valid)
}

func TestAccountEndpointProbeEvidenceRedaction(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Echo body containing fake token
		body := `{"id":"test","content":[{"text":"hi"}]}` + " sk-test123"
		w.Write([]byte(body))
	}))
	defer srv.Close()
	repo := &fakeProbeRepo{}
	svc := NewAccountEndpointProbeService(repo, srv.Client())
	conn := &UpstreamConnection{ID: 1, Kind: "first_party", Provider: func() *GovernanceProvider { p := GovernanceProviderAnthropic; return &p }(), BaseURL: srv.URL, CredentialVersion: 1, Status: "active"}
	probe, _ := svc.Probe(context.Background(), 1, conn, GovernanceProviderAnthropic, AccountProtocolAnthropic, "/v1/messages", "cred")
	require.Contains(t, probe.ResponseSummary["body_truncated"], "***")
	require.NotContains(t, probe.ResponseSummary["body_truncated"], "sk-test123")
	_ = strings.Contains
}
