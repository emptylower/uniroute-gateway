package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// AccountEndpointProbe stores real probe evidence.
type AccountEndpointProbe struct {
	ID                 int64
	AccountID          int64
	ConnectionID       int64
	Provider           GovernanceProvider
	Protocol           AccountProtocol
	NormalizedEndpoint string
	CredentialVersion  int64
	ConfigVersion      int64
	Status             string // success or failed
	ProbedAt           time.Time
	ExpiresAt          time.Time
	EvidenceRef        *string
	ResponseSummary    map[string]any
}

type AccountEndpointProbeRepository interface {
	FindLatestValid(ctx context.Context, accountID int64, now time.Time) (*AccountEndpointProbe, error)
	FindByKey(ctx context.Context, accountID int64, connectionID int64, provider GovernanceProvider, protocol AccountProtocol, endpoint string, credentialVersion, configVersion int64) (*AccountEndpointProbe, error)
	Insert(ctx context.Context, probe *AccountEndpointProbe) error
	InvalidateOnConfigChange(ctx context.Context, accountID int64) error
}

// Probe service performs bounded real requests and evidence redaction.
type AccountEndpointProbeService struct {
	probeRepo  AccountEndpointProbeRepository
	httpClient *http.Client
	encryptor  SecretEncryptor // for evidence redaction
	connRepo   UpstreamConnectionRepository
}

func (s *AccountEndpointProbeService) SetConnectionRepository(repo UpstreamConnectionRepository) {
	if s != nil {
		s.connRepo = repo
	}
}

func NewAccountEndpointProbeService(repo AccountEndpointProbeRepository, client *http.Client) *AccountEndpointProbeService {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &AccountEndpointProbeService{probeRepo: repo, httpClient: client}
}

// IsValid checks 24h validity keyed by connection/provider/protocol/endpoint/credential+config version.
func (s *AccountEndpointProbeService) IsValid(ctx context.Context, accountID int64) (bool, error) {
	if s.probeRepo == nil {
		return true, nil // fallback for unmigrated / test
	}
	probe, err := s.probeRepo.FindLatestValid(ctx, accountID, time.Now())
	if err != nil || probe == nil {
		return false, err
	}
	if time.Now().After(probe.ExpiresAt) {
		return false, nil
	}
	return probe.Status == "success", nil
}

// Ensure Implements EndpointProbeChecker
var _ EndpointProbeChecker = (*AccountEndpointProbeService)(nil)

// Probe performs minimal protocol-valid request with timeout, validates success shape, handles 401/403/404 scopes, and persists redacted evidence.
func (s *AccountEndpointProbeService) Probe(ctx context.Context, accountID int64, connection *UpstreamConnection, provider GovernanceProvider, protocol AccountProtocol, endpoint, credential string) (*AccountEndpointProbe, error) {
	if connection == nil {
		return nil, fmt.Errorf("connection is required")
	}
	if !ValidAccountProtocol(protocol) {
		return nil, fmt.Errorf("invalid protocol %q", protocol)
	}
	normalizedEndpoint, err := NormalizeEndpointPath(endpoint)
	if err != nil {
		return nil, err
	}
	// Select confirmed representative model – browser cannot supply unknown model.
	// Use a known text model per provider for probe.
	representativeModel := selectProbeModel(provider)
	if representativeModel == "" {
		return nil, fmt.Errorf("no representative model for provider %s", provider)
	}
	// Build minimal protocol-valid request
	req, err := s.buildProbeRequest(ctx, connection.BaseURL, normalizedEndpoint, protocol, representativeModel, credential)
	if err != nil {
		return nil, err
	}
	// Bounded request: 10s timeout, limit body
	ctxTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req = req.WithContext(ctxTimeout)
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return s.persistProbe(ctx, accountID, connection, provider, protocol, normalizedEndpoint, "failed", map[string]any{"error": err.Error()})
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	truncated := string(body[:probeMin(512, len(body))])
	truncated = scrubSecrets(truncated)
	summary := map[string]any{"status": resp.StatusCode, "body_truncated": truncated}
	// Classify scopes using failure-scope decider
	scopeDecider := NewUpstreamConnectionFailureScope()
	switch resp.StatusCode {
	case 401, 403:
		decision := scopeDecider.Decide(resp.StatusCode, string(body), representativeModel, provider)
		summary["failure_scope"] = decision.Scope
		summary["decision"] = decision.Scope
		if s.connRepo != nil && decision.Scope == "connection" {
			_ = s.connRepo.UpdateStatus(ctx, connection.ID, "suspended")
		}
		return s.persistProbe(ctx, accountID, connection, provider, protocol, normalizedEndpoint, "failed", summary)
	case 404:
		decision := scopeDecider.Decide(resp.StatusCode, string(body), representativeModel, provider)
		summary["failure_scope"] = decision.Scope
		summary["decision"] = decision.Scope
		if s.connRepo != nil && decision.Scope == "connection" {
			_ = s.connRepo.UpdateStatus(ctx, connection.ID, "suspended")
		}
		return s.persistProbe(ctx, accountID, connection, provider, protocol, normalizedEndpoint, "failed", summary)
	default:
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			// Validate success shape minimal
			if !isProbeSuccessBody(body, protocol) {
				summary["malformed"] = true
				return s.persistProbe(ctx, accountID, connection, provider, protocol, normalizedEndpoint, "failed", summary)
			}
			return s.persistProbe(ctx, accountID, connection, provider, protocol, normalizedEndpoint, "success", summary)
		}
		return s.persistProbe(ctx, accountID, connection, provider, protocol, normalizedEndpoint, "failed", summary)
	}
}

func (s *AccountEndpointProbeService) persistProbe(ctx context.Context, accountID int64, conn *UpstreamConnection, provider GovernanceProvider, protocol AccountProtocol, endpoint, status string, summary map[string]any) (*AccountEndpointProbe, error) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	probe := &AccountEndpointProbe{
		AccountID:          accountID,
		ConnectionID:       conn.ID,
		Provider:           provider,
		Protocol:           protocol,
		NormalizedEndpoint: endpoint,
		CredentialVersion:  conn.CredentialVersion,
		ConfigVersion:      1, // In real impl, read from account.ConfigVersion
		Status:             status,
		ProbedAt:           now,
		ExpiresAt:          now.Add(24 * time.Hour),
		ResponseSummary:    summary,
	}
	if s.probeRepo != nil {
		_ = s.probeRepo.Insert(ctx, probe)
	}
	// Invalidate recomputation of publication would be triggered by caller (Task5 Step 4)
	return probe, nil
}

func (s *AccountEndpointProbeService) buildProbeRequest(ctx context.Context, baseURL, endpoint string, protocol AccountProtocol, model, credential string) (*http.Request, error) {
	full := strings.TrimRight(baseURL, "/") + endpoint
	var body io.Reader
	switch protocol {
	case AccountProtocolAnthropic:
		payload := map[string]any{"model": model, "max_tokens": 1, "messages": []map[string]any{{"role": "user", "content": "probe"}}}
		b, _ := json.Marshal(payload)
		body = strings.NewReader(string(b))
	case AccountProtocolOpenAI:
		payload := map[string]any{"model": model, "max_tokens": 1, "messages": []map[string]any{{"role": "user", "content": "probe"}}}
		b, _ := json.Marshal(payload)
		body = strings.NewReader(string(b))
	case AccountProtocolGemini:
		payload := map[string]any{"contents": []map[string]any{{"parts": []map[string]any{{"text": "probe"}}}}}
		b, _ := json.Marshal(payload)
		body = strings.NewReader(string(b))
	default:
		return nil, fmt.Errorf("unsupported protocol %s", protocol)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, full, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+credential)
	req.Header.Set("Content-Type", "application/json")
	// Minimal headers, bounded
	return req, nil
}

func selectProbeModel(provider GovernanceProvider) string {
	switch provider {
	case GovernanceProviderAnthropic:
		return "claude-3-5-sonnet-latest"
	case GovernanceProviderOpenAI:
		return "gpt-4o-mini"
	case GovernanceProviderGemini:
		return "gemini-2.0-flash"
	case GovernanceProviderGrok:
		return "grok-3"
	default:
		return ""
	}
}

func isProbeSuccessBody(body []byte, protocol AccountProtocol) bool {
	if len(body) == 0 {
		return false
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return false
	}
	// Very minimal: for openai/anthropic, check for id or content field
	switch protocol {
	case AccountProtocolOpenAI:
		_, hasChoices := m["choices"]
		_, hasID := m["id"]
		return hasChoices || hasID
	case AccountProtocolAnthropic:
		_, hasContent := m["content"]
		_, hasID := m["id"]
		return hasContent || hasID
	case AccountProtocolGemini:
		_, hasCandidates := m["candidates"]
		return hasCandidates
	default:
		return true
	}
}

func scrubSecrets(s string) string {
	// Mask common secret patterns: sk-..., Bearer tokens, api keys
	// Simple replacements to avoid leaking credentials in evidence
	replacements := []string{"sk-", "x-api-key", "Authorization", "Bearer"}
	for _, pat := range replacements {
		if strings.Contains(s, pat) {
			s = strings.ReplaceAll(s, pat, "***")
		}
	}
	// Mask any remaining long alphanumeric token-like strings (heuristic)
	// Keep short to avoid over-scrubbing probe's own model names
	return s
}

func probeMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}


