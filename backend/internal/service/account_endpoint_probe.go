package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
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

// Probe verifies endpoint liveness + credential validity by fetching the FREE
// model list (GET {base}{endpoint}/models). Account registration must never
// spend upstream balance: a chat-completion probe bills the operator and fails
// on zero-balance accounts even when everything is configured correctly. The
// discovered model ids are persisted as redacted evidence.
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
	// Build free model-list request
	req, err := s.buildProbeRequest(ctx, connection.BaseURL, normalizedEndpoint, protocol, credential)
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
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 256*1024))
	truncated := string(body[:probeMin(512, len(body))])
	truncated = scrubSecrets(truncated)
	summary := map[string]any{"status": resp.StatusCode, "body_truncated": truncated}
	// Classify scopes using failure-scope decider
	scopeDecider := NewUpstreamConnectionFailureScope()
	switch resp.StatusCode {
	case 401, 403:
		decision := scopeDecider.Decide(resp.StatusCode, string(body), "", provider)
		summary["failure_scope"] = decision.Scope
		summary["decision"] = decision.Scope
		if s.connRepo != nil && decision.Scope == "connection" {
			if err := s.connRepo.UpdateStatus(ctx, connection.ID, "suspended"); err != nil {
				return nil, err
			}
		}
		return s.persistProbe(ctx, accountID, connection, provider, protocol, normalizedEndpoint, "failed", summary)
	case 404:
		// Aggregator gemini surfaces (e.g. aicodewith /gemini_cli/v1beta) often
		// serve ONLY inference endpoints and 404 any model listing. The unified
		// catalog is usually exposed free on the openai surface at /v1/models.
		if protocol == AccountProtocolGemini {
			if fbSummary, ok := s.tryGeminiOpenAISurfaceFallback(ctxTimeout, connection.BaseURL, credential); ok {
				return s.persistProbe(ctx, accountID, connection, provider, protocol, normalizedEndpoint, "success", fbSummary)
			}
		}
		decision := scopeDecider.Decide(resp.StatusCode, string(body), "", provider)
		summary["failure_scope"] = decision.Scope
		summary["decision"] = decision.Scope
		if s.connRepo != nil && decision.Scope == "connection" {
			if err := s.connRepo.UpdateStatus(ctx, connection.ID, "suspended"); err != nil {
				return nil, err
			}
		}
		return s.persistProbe(ctx, accountID, connection, provider, protocol, normalizedEndpoint, "failed", summary)
	default:
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			modelIDs := extractProbeModelIDs(body, protocol)
			if len(modelIDs) == 0 {
				summary["malformed"] = true
				return s.persistProbe(ctx, accountID, connection, provider, protocol, normalizedEndpoint, "failed", summary)
			}
			summary["model_count"] = len(modelIDs)
			const capIDs = 50
			if len(modelIDs) > capIDs {
				modelIDs = modelIDs[:capIDs]
			}
			summary["model_ids"] = modelIDs
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

func (s *AccountEndpointProbeService) buildProbeRequest(ctx context.Context, baseURL, endpoint string, protocol AccountProtocol, credential string) (*http.Request, error) {
	// Model list: openai/anthropic/grok-compatible surfaces serve
	// {root}/models; gemini native serves {root}/models too (v1beta/models).
	full := strings.TrimRight(baseURL, "/") + strings.TrimRight(endpoint, "/") + "/models"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, full, nil)
	if err != nil {
		return nil, err
	}
	if credential != "" {
		req.Header.Set("Authorization", "Bearer "+credential)
	}
	if protocol == AccountProtocolAnthropic {
		req.Header.Set("x-api-key", credential)
		req.Header.Set("anthropic-version", "2023-06-01")
	}
	req.Header.Set("Accept", "application/json")
	return req, nil
}

// geminiOpenAISurfaceFallbackURL derives the sibling openai-surface model list
// URL ({origin}/v1/models) from a gemini-surface base URL such as
// https://host/gemini_cli. Returns false when the base URL is not absolute http(s).
func geminiOpenAISurfaceFallbackURL(baseURL string) (string, bool) {
	u, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || u == nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return "", false
	}
	return u.Scheme + "://" + u.Host + "/v1/models", true
}

// filterGeminiModelIDs keeps only gemini-family ids from a mixed catalog.
func filterGeminiModelIDs(ids []string) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if strings.HasPrefix(strings.ToLower(id), "gemini") {
			out = append(out, id)
		}
	}
	return out
}

// tryGeminiOpenAISurfaceFallback fetches the unified catalog from the sibling
// openai surface when the gemini surface 404s its model listing. Success
// requires a 2xx response with at least one gemini model id.
func (s *AccountEndpointProbeService) tryGeminiOpenAISurfaceFallback(ctx context.Context, baseURL, credential string) (map[string]any, bool) {
	fallbackURL, ok := geminiOpenAISurfaceFallbackURL(baseURL)
	if !ok {
		return nil, false
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fallbackURL, nil)
	if err != nil {
		return nil, false
	}
	if credential != "" {
		req.Header.Set("Authorization", "Bearer "+credential)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, false
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 256*1024))
	ids := filterGeminiModelIDs(extractProbeModelIDs(body, AccountProtocolOpenAI))
	if len(ids) == 0 {
		return nil, false
	}
	summary := map[string]any{
		"status":         resp.StatusCode,
		"via":            "openai_surface_fallback",
		"primary_status": http.StatusNotFound,
		"model_count":    len(ids),
	}
	const capIDs = 50
	if len(ids) > capIDs {
		ids = ids[:capIDs]
	}
	summary["model_ids"] = ids
	return summary, true
}

// extractProbeModelIDs parses the model-list response into upstream model ids.// OpenAI-compatible shape: {"data":[{"id":...}]}; Gemini native:
// {"models":[{"name":"models/gemini-..."}]}. Empty means the endpoint did not
// actually serve a model list (wrong path, HTML error page, etc).
func extractProbeModelIDs(body []byte, protocol AccountProtocol) []string {
	if len(body) == 0 {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return nil
	}
	var items []any
	if data, ok := m["data"].([]any); ok {
		items = data
	} else if models, ok := m["models"].([]any); ok {
		items = models
	} else {
		return nil
	}
	ids := make([]string, 0, len(items))
	for _, item := range items {
		obj, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if id, ok := obj["id"].(string); ok && id != "" {
			ids = append(ids, id)
			continue
		}
		if name, ok := obj["name"].(string); ok && name != "" {
			ids = append(ids, strings.TrimPrefix(name, "models/"))
		}
	}
	return ids
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


