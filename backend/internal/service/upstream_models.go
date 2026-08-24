package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/antigravity"
	"github.com/Wei-Shaw/sub2api/internal/pkg/claude"
	"github.com/Wei-Shaw/sub2api/internal/pkg/geminicli"
)

const upstreamModelsBodyLimit int64 = 8 << 20

type codexModelsManifestFetcher interface {
	FetchCodexModelsManifest(context.Context, *Account, string, string) (*CodexModelsManifest, error)
}

// AccountModelDiscoveryStore atomically changes only model discovery fields in
// account credentials so token refreshes and other credential updates are not
// overwritten by a slow upstream manifest request.
type AccountModelDiscoveryStore interface {
	UpdateModelDiscovery(ctx context.Context, accountID int64, mapping map[string]any, discovery map[string]any) error
}

// UpstreamModelSyncErrorKind classifies model sync failures for safe HTTP mapping.
type UpstreamModelSyncErrorKind string

const (
	// UpstreamModelSyncErrorConfiguration means the account or server configuration cannot perform the sync.
	UpstreamModelSyncErrorConfiguration UpstreamModelSyncErrorKind = "configuration"
	// UpstreamModelSyncErrorUnsupported means the account format is intentionally unsupported for live model sync.
	UpstreamModelSyncErrorUnsupported UpstreamModelSyncErrorKind = "unsupported"
	// UpstreamModelSyncErrorUpstream means the configured upstream failed or returned an unusable response.
	UpstreamModelSyncErrorUpstream UpstreamModelSyncErrorKind = "upstream"
)

// UpstreamModelSyncError keeps internal failure details wrapped while exposing a safe client message.
type UpstreamModelSyncError struct {
	Kind    UpstreamModelSyncErrorKind
	Message string
	Err     error
}

func (e *UpstreamModelSyncError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err == nil {
		return e.Message
	}
	return e.Message + ": " + e.Err.Error()
}

func (e *UpstreamModelSyncError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// SafeMessage returns the sanitized message that can be sent to API clients.
func (e *UpstreamModelSyncError) SafeMessage() string {
	if e == nil || strings.TrimSpace(e.Message) == "" {
		return "Failed to sync upstream models"
	}
	return e.Message
}

func newUpstreamModelSyncConfigError(message string, err error) error {
	return &UpstreamModelSyncError{Kind: UpstreamModelSyncErrorConfiguration, Message: message, Err: err}
}

func newUpstreamModelSyncUnsupportedError(message string, err error) error {
	return &UpstreamModelSyncError{Kind: UpstreamModelSyncErrorUnsupported, Message: message, Err: err}
}

func newUpstreamModelSyncUpstreamError(message string, err error) error {
	return &UpstreamModelSyncError{Kind: UpstreamModelSyncErrorUpstream, Message: message, Err: err}
}

// FetchUpstreamSupportedModels returns the legacy normalized model list.
func (s *AccountTestService) FetchUpstreamSupportedModels(ctx context.Context, account *Account) ([]string, error) {
	discovery, err := s.FetchUpstreamModelDiscovery(ctx, account)
	if err != nil {
		return nil, err
	}
	if len(discovery.Models) == 0 {
		return nil, newUpstreamModelSyncUpstreamError("Upstream returned no supported models", nil)
	}
	return discovery.Models, nil
}

// FetchUpstreamModelDiscovery fetches both the legacy normalized catalog and
// the original accepted payload used only as governance evidence.
func (s *AccountTestService) FetchUpstreamModelDiscovery(ctx context.Context, account *Account) (UpstreamModelDiscovery, error) {
	if s == nil {
		return UpstreamModelDiscovery{}, newUpstreamModelSyncConfigError("Account test service is not configured", nil)
	}
	if account == nil {
		return UpstreamModelDiscovery{}, newUpstreamModelSyncConfigError("Account is required", nil)
	}
	if account.IsCredentialShadow() {
		return UpstreamModelDiscovery{}, newUpstreamModelSyncUnsupportedError(
			"Model discovery must be run on the parent account", nil,
		)
	}
	if account.IsOpenAIOAuth() {
		return s.fetchOpenAIOAuthUpstreamModelDiscovery(ctx, account)
	}

	if account.Platform == PlatformAntigravity && account.Type != AccountTypeAPIKey {
		return s.fetchAntigravityOAuthUpstreamModelDiscovery(ctx, account)
	}

	if s.httpUpstream == nil {
		return UpstreamModelDiscovery{}, newUpstreamModelSyncConfigError("Upstream HTTP client is not configured", nil)
	}

	req, err := s.buildUpstreamModelsRequest(ctx, account)
	if err != nil {
		return UpstreamModelDiscovery{}, err
	}

	proxyURL := upstreamModelsProxyURL(account)
	resp, err := s.doUpstreamModelsRequest(req, proxyURL, account)
	if err != nil {
		return UpstreamModelDiscovery{}, newUpstreamModelSyncUpstreamError("Failed to request upstream model list", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, upstreamModelsBodyLimit+1))
	if err != nil {
		return UpstreamModelDiscovery{}, newUpstreamModelSyncUpstreamError("Failed to read upstream model list", err)
	}
	if int64(len(body)) > upstreamModelsBodyLimit {
		return UpstreamModelDiscovery{}, newUpstreamModelSyncUpstreamError("Upstream model list response is too large", fmt.Errorf("response exceeds %d bytes", upstreamModelsBodyLimit))
	}

	usedOpenAISurfaceFallback := false

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		// Aggregator gemini surfaces (e.g. aicodewith /gemini_cli/v1beta) serve
		// ONLY inference endpoints and 404 any model listing; the unified
		// catalog is exposed free on the sibling openai surface at /v1/models.
		if resp.StatusCode == http.StatusNotFound && account.IsGemini() {
			if fbResp, fbBody, ok := s.retryGeminiModelsOnOpenAISurface(ctx, req, proxyURL, account); ok {
				_ = resp.Body.Close()
				resp = fbResp
				body = fbBody
				usedOpenAISurfaceFallback = true
			}
		}
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return UpstreamModelDiscovery{}, newUpstreamModelSyncUpstreamError(
			fmt.Sprintf("Upstream model list request failed with HTTP %d", resp.StatusCode),
			fmt.Errorf("upstream model list returned HTTP %d", resp.StatusCode),
		)
	}

	extractModels := extractUpstreamModelIDViews
	if account.IsGrok() {
		extractModels = extractGrokUpstreamModelIDViews
	}
	models, evidenceModelIDs, err := extractModels(body)
	if err != nil {
		return UpstreamModelDiscovery{}, newUpstreamModelSyncUpstreamError("Upstream model list response was not valid JSON", err)
	}
	if usedOpenAISurfaceFallback {
		// Unified catalog is cross-family; a gemini connection only serves gemini ids.
		models = filterGeminiModelIDs(models)
	}
	rawSnapshot, err := marshalUpstreamRawSnapshot(body, map[string]any{
		"source":       "http",
		"status_code":  resp.StatusCode,
		"content_type": resp.Header.Get("Content-Type"),
		"etag":         resp.Header.Get("ETag"),
	})
	if err != nil {
		return UpstreamModelDiscovery{}, newUpstreamModelSyncUpstreamError("Upstream model list response was not valid JSON", err)
	}
	return UpstreamModelDiscovery{Models: models, EvidenceModelIDs: evidenceModelIDs, RawSnapshot: rawSnapshot}, nil
}

func (s *AccountTestService) fetchOpenAIOAuthUpstreamModels(ctx context.Context, account *Account) ([]string, error) {
	discovery, err := s.fetchOpenAIOAuthUpstreamModelDiscovery(ctx, account)
	if err != nil {
		return nil, err
	}
	if len(discovery.Models) == 0 {
		return nil, newUpstreamModelSyncUpstreamError("Upstream returned no supported models", nil)
	}
	return discovery.Models, nil
}

func (s *AccountTestService) fetchOpenAIOAuthUpstreamModelDiscovery(ctx context.Context, account *Account) (UpstreamModelDiscovery, error) {
	if s.codexModelsFetcher == nil {
		return UpstreamModelDiscovery{}, newUpstreamModelSyncConfigError("OpenAI Codex model discovery is not configured", nil)
	}

	manifest, err := s.codexModelsFetcher.FetchCodexModelsManifest(ctx, account, "", "")
	if err != nil {
		return UpstreamModelDiscovery{}, newUpstreamModelSyncUpstreamError("Failed to request upstream model list", err)
	}
	if manifest == nil || manifest.NotModified || len(manifest.Body) == 0 {
		return UpstreamModelDiscovery{}, newUpstreamModelSyncUpstreamError("Upstream returned no supported models", nil)
	}

	models, evidenceModelIDs, err := extractUpstreamModelIDViews(manifest.Body)
	if err != nil {
		return UpstreamModelDiscovery{}, newUpstreamModelSyncUpstreamError("Upstream model list response was not valid JSON", err)
	}
	rawSnapshot, err := marshalUpstreamRawSnapshot(manifest.Body, map[string]any{
		"source": "manifest",
		"etag":   manifest.ETag,
	})
	if err != nil {
		return UpstreamModelDiscovery{}, newUpstreamModelSyncUpstreamError("Upstream model list response was not valid JSON", err)
	}
	return UpstreamModelDiscovery{Models: models, EvidenceModelIDs: evidenceModelIDs, RawSnapshot: rawSnapshot}, nil
}

// PersistDiscoveredModels preserves the legacy direct-call behavior while
// routing production syncs through PersistUpstreamModelDiscovery.
func (s *AccountTestService) PersistDiscoveredModels(ctx context.Context, account *Account, models []string, syncedAt time.Time) error {
	return s.PersistUpstreamModelDiscovery(ctx, account, newSynthesizedUpstreamModelDiscovery(models, "legacy_call"), syncedAt)
}

// PersistUpstreamModelDiscovery records non-authoritative evidence before
// applying the pre-existing runtime mapping update.
func (s *AccountTestService) PersistUpstreamModelDiscovery(ctx context.Context, account *Account, discoveryInput UpstreamModelDiscovery, syncedAt time.Time) error {
	if s == nil || s.modelDiscoveryStore == nil {
		return newUpstreamModelSyncConfigError("Account model discovery store is not configured", nil)
	}
	if account == nil {
		return newUpstreamModelSyncConfigError("Account is required", nil)
	}
	if account.IsCredentialShadow() {
		return newUpstreamModelSyncUnsupportedError(
			"Model discovery must be run on the parent account", nil,
		)
	}
	models := dedupeAndSortModelIDs(discoveryInput.Models)
	// Family filter at rest: an account only publishes the model families its
	// platform can actually serve (the anthropic surface provably 400s foreign
	// families). Raw fetched IDs remain in the observation evidence above; the
	// stored mapping is the schedulable truth.
	schedulable := SchedulableModelsForPlatform(account.Platform, models)
	if s.modelObservationRepository == nil {
		return newUpstreamModelSyncConfigError("Model observation repository is not configured", nil)
	}

	provider := governanceProviderForPlatform(account.Platform)
	rawSnapshot := discoveryInput.RawSnapshot
	if len(rawSnapshot) == 0 {
		return newUpstreamModelSyncUpstreamError("Upstream model evidence snapshot is empty", nil)
	}
	syncedAt = CanonicalGovernanceTime(syncedAt)
	idempotencySeed := fmt.Sprintf("%d\n%s\n%s", account.ID, syncedAt.Format(time.RFC3339Nano), rawSnapshot)
	idempotencyKey := fmt.Sprintf("model-discovery:%x", sha256.Sum256([]byte(idempotencySeed)))
	if _, err := s.modelObservationRepository.RecordDiscovery(ctx, DiscoveryBatchInput{
		IdempotencyKey:  idempotencyKey,
		AccountID:       account.ID,
		AccountProvider: provider,
		RoutingPlatform: account.Platform,
		ModelIDs:        dedupeExactModelIDs(discoveryInput.EvidenceModelIDs),
		RawSnapshot:     rawSnapshot,
		ObservedAt:      syncedAt,
	}); err != nil {
		return fmt.Errorf("record upstream model observation: %w", err)
	}
	if len(schedulable) == 0 {
		return newUpstreamModelSyncUpstreamError("Upstream returned no supported models", nil)
	}

	mapping := make(map[string]any, len(schedulable))
	for requestedModel, upstreamModel := range account.GetModelMapping() {
		if requestedModel != upstreamModel || strings.Contains(requestedModel, "*") {
			mapping[requestedModel] = upstreamModel
		}
	}
	for _, model := range schedulable {
		if _, customized := mapping[model]; !customized {
			mapping[model] = model
		}
	}

	discovery := map[string]any{
		"source":    "upstream",
		"models":    models,
		"schedulable_models": schedulable,
		"synced_at": syncedAt.Format(time.RFC3339),
	}
	return s.modelDiscoveryStore.UpdateModelDiscovery(ctx, account.ID, mapping, discovery)
}

func governanceProviderForPlatform(platform string) *GovernanceProvider {
	switch platform {
	case PlatformAnthropic, PlatformOpenAI, PlatformGemini, PlatformGrok:
		provider := GovernanceProvider(platform)
		return &provider
	case "claude":
		provider := GovernanceProviderAnthropic
		return &provider
	default:
		return nil
	}
}

// ResolveAccountProtocol is the exported Task 4 resolver for admin and test flows.
func ResolveAccountProtocol(account *Account) AccountProtocol {
	return accountProtocolForRequest(account)
}

// Phase 4: protocol and provider are independent. Protocol determines wire format (anthropic|openai|gemini);
// provider determines governance ownership. Aggregator connections supply baseURL/credential/proxy via connection.
func accountProtocolForRequest(account *Account) AccountProtocol {
	if account == nil {
		return ""
	}
	if account.Protocol != nil && ValidAccountProtocol(AccountProtocol(*account.Protocol)) {
		return AccountProtocol(*account.Protocol)
	}
	// Default mapping preserves legacy behavior for unmigrated accounts
	switch account.Platform {
	case PlatformAnthropic, "claude":
		return AccountProtocolAnthropic
	case PlatformOpenAI:
		return AccountProtocolOpenAI
	case PlatformGemini, PlatformAntigravity:
		return AccountProtocolGemini
	case PlatformGrok:
		return AccountProtocolOpenAI // Grok provider over OpenAI protocol
	default:
		return AccountProtocolOpenAI
	}
}

// UpstreamRequestMaterial resolves connection base URL/credential/proxy and account provider/protocol/endpoint independently.
type UpstreamRequestMaterial struct {
	Connection *UpstreamConnection
	Provider   *GovernanceProvider
	Protocol   AccountProtocol
	Endpoint   string
	BaseURL    string
}

func (s *AccountTestService) resolveUpstreamRequestMaterial(ctx context.Context, account *Account) (*UpstreamRequestMaterial, error) {
	if account == nil {
		return nil, fmt.Errorf("account is required")
	}
	material := &UpstreamRequestMaterial{
		Provider: governanceProviderForPlatform(account.Platform),
		Protocol: accountProtocolForRequest(account),
	}
	if account.EndpointPath != nil {
		ep, err := NormalizeEndpointPath(*account.EndpointPath)
		if err == nil {
			material.Endpoint = ep
		} else {
			material.Endpoint = *account.EndpointPath
		}
	}
	// Resolve connection if migrated; otherwise fallback to legacy account-derived base URL.
	// Explicit aggregator reuse supplies provider=null connection with OpenAI protocol; still resolve via connection.
	if account.ConnectionID != nil && s.upstreamConnRepo != nil {
		if conn, _, err := s.upstreamConnRepo.GetByID(ctx, *account.ConnectionID); err == nil && conn != nil {
			material.Connection = conn
			if conn.BaseURL != "" {
				material.BaseURL = conn.BaseURL
			}
			// Connection provider never overrides account governance provider.
		}
	} else if account.ConnectionID != nil {
		// No repo wired: keep BaseURL empty to trigger legacy fallback (preserves tests with stub).
		material.BaseURL = ""
	}
	if material.BaseURL == "" {
		// Legacy fallback: derive from account credentials (preserves unmigrated rows)
		switch material.Protocol {
		case AccountProtocolAnthropic:
			material.BaseURL = account.GetBaseURL()
		case AccountProtocolOpenAI:
			material.BaseURL = account.GetOpenAIBaseURL()
		case AccountProtocolGemini:
			material.BaseURL = account.GetGeminiBaseURL(geminicli.AIStudioBaseURL)
		default:
			material.BaseURL = account.GetBaseURL()
		}
	}
	return material, nil
}

func (s *AccountTestService) buildUpstreamModelsRequest(ctx context.Context, account *Account) (*http.Request, error) {
	// Phase 4: resolve connection base URL/credential/proxy independently from provider/protocol/endpoint
	// This keeps discovery non-authoritative and preserves wildcard observations.
	material, err := s.resolveUpstreamRequestMaterial(ctx, account)
	if err != nil {
		return nil, err
	}
	// If connection provides base URL, use it (overrides account-derived URL) – R3 ②.
	if material.BaseURL != "" && material.Connection != nil {
		return s.buildRequestFromMaterial(ctx, material, account)
	}
	switch {
	case account.Platform == PlatformAntigravity:
		return s.buildAntigravityAPIKeyModelsRequest(ctx, account)
	case account.IsGrok():
		return s.buildGrokUpstreamModelsRequest(ctx, account)
	case account.IsOpenAI():
		return s.buildOpenAIUpstreamModelsRequest(ctx, account)
	case account.IsGemini():
		return s.buildGeminiUpstreamModelsRequest(ctx, account)
	case account.IsAnthropic():
		return s.buildAnthropicUpstreamModelsRequest(ctx, account)
	default:
		return nil, newUpstreamModelSyncUnsupportedError(
			fmt.Sprintf("Unsupported platform for upstream model sync: %s", account.Platform), nil,
		)
	}
}

func (s *AccountTestService) buildRequestFromMaterial(ctx context.Context, material *UpstreamRequestMaterial, account *Account) (*http.Request, error) {
	// Connection-sourced material takes precedence; legacy per-platform switches are fallback only.
	baseURL := strings.TrimRight(material.BaseURL, "/")
	// EndpointPath is the API root ("/v1", "/v1beta"); the model list lives at
	// {root}/models. Tolerate legacy rows that stored the full path already.
	endpoint := strings.TrimRight(material.Endpoint, "/")
	if endpoint == "" {
		endpoint = "/v1"
	}
	fullURL := baseURL + endpoint
	if !strings.HasSuffix(fullURL, "/models") {
		fullURL += "/models"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return nil, err
	}
	// Credential from connection is encrypted at rest; for probe we use account credential as fallback
	// (plaintext copy still exists in reuse-created accounts until Phase 6 removes it – see plan deferral).
	if cred, ok := account.Credentials["api_key"].(string); ok && cred != "" {
		req.Header.Set("Authorization", "Bearer "+cred)
		req.Header.Set("x-api-key", cred)
	}
	return req, nil
}

// retryGeminiModelsOnOpenAISurface re-asks the site origin's openai surface
// ({origin}/v1/models) for the model list when the gemini surface 404s it.
// Free endpoint (same contract as the connection probe). Returns the new
// response and its (already bounded) body.
func (s *AccountTestService) retryGeminiModelsOnOpenAISurface(ctx context.Context, origReq *http.Request, proxyURL string, account *Account) (*http.Response, []byte, bool) {
	if origReq == nil || origReq.URL == nil {
		return nil, nil, false
	}
	fallbackURL, ok := geminiOpenAISurfaceFallbackURL(origReq.URL.String())
	if !ok {
		return nil, nil, false
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fallbackURL, nil)
	if err != nil {
		return nil, nil, false
	}
	if cred, ok := account.Credentials["api_key"].(string); ok && cred != "" {
		req.Header.Set("Authorization", "Bearer "+cred)
		req.Header.Set("x-api-key", cred)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := s.doUpstreamModelsRequest(req, proxyURL, account)
	if err != nil {
		return nil, nil, false
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, upstreamModelsBodyLimit+1))
	if err != nil || int64(len(body)) > upstreamModelsBodyLimit {
		_ = resp.Body.Close()
		return nil, nil, false
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		_ = resp.Body.Close()
		return nil, nil, false
	}
	return resp, body, true
}

func (s *AccountTestService) buildGrokUpstreamModelsRequest(ctx context.Context, account *Account) (*http.Request, error) {
	if account == nil {
		return nil, newUpstreamModelSyncConfigError("Account is required", nil)
	}

	var (
		authToken         string
		normalizedBaseURL string
		isOAuth           = account.IsGrokOAuth()
	)
	switch account.Type {
	case AccountTypeAPIKey:
		authToken = strings.TrimSpace(account.GetCredential("api_key"))
		if authToken == "" {
			return nil, newUpstreamModelSyncConfigError("No Grok API key is available", nil)
		}

		baseURL := strings.TrimSpace(account.GetCredential("base_url"))
		if baseURL == "" {
			baseURL = "https://api.x.ai"
		}
		validatedBaseURL, err := s.validateUpstreamBaseURL(baseURL)
		if err != nil {
			return nil, newUpstreamModelSyncConfigError("Invalid Grok base URL", err)
		}
		normalizedBaseURL = validatedBaseURL
	case AccountTypeOAuth:
		if s.grokTokenProvider == nil {
			return nil, newUpstreamModelSyncConfigError("Grok token provider is not configured", nil)
		}
		accessToken, err := s.grokTokenProvider.GetAccessTokenForManualTest(ctx, account)
		if err != nil {
			return nil, newUpstreamModelSyncUpstreamError("Failed to get Grok access token", err)
		}
		authToken = strings.TrimSpace(accessToken)
		if authToken == "" {
			return nil, newUpstreamModelSyncConfigError("No Grok access token is available", nil)
		}

		validator, err := grokBaseURLValidator(account, s.cfg)
		if err != nil {
			return nil, newUpstreamModelSyncConfigError("Invalid Grok base URL", err)
		}
		validatedBaseURL, err := validator(account.GetGrokBaseURL())
		if err != nil {
			return nil, newUpstreamModelSyncConfigError("Invalid Grok base URL", err)
		}
		normalizedBaseURL = validatedBaseURL
	default:
		return nil, newUpstreamModelSyncUnsupportedError(
			fmt.Sprintf("Unsupported Grok account type for upstream model sync: %s", account.Type), nil,
		)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, buildOpenAIModelsURL(normalizedBaseURL), nil)
	if err != nil {
		return nil, newUpstreamModelSyncConfigError("Invalid Grok model list URL", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+authToken)
	if isOAuth {
		// The shared HTTP transport adds the official CLI marker/version for the
		// exact proxy host. Keep the request builder aligned with the other Grok
		// probes and only forward account identity headers to that trusted host.
		applyGrokCLIHeaders(req.Header)
		if isGrokCLIProxyTarget(req.URL.String()) {
			if userID := strings.TrimSpace(account.GetCredential("sub")); userID != "" {
				req.Header.Set("X-UserID", userID)
			}
			if email := strings.TrimSpace(account.GetCredential("email")); email != "" {
				req.Header.Set("X-Email", email)
			}
		}
	}
	account.ApplyHeaderOverrides(req.Header)
	return req, nil
}

func (s *AccountTestService) buildAnthropicUpstreamModelsRequest(ctx context.Context, account *Account) (*http.Request, error) {
	if account.IsBedrock() || account.Type == AccountTypeServiceAccount {
		return nil, newUpstreamModelSyncUnsupportedError(
			fmt.Sprintf("Unsupported Anthropic account type for upstream model sync: %s", account.Type), nil,
		)
	}

	baseURL := "https://api.anthropic.com"
	authHeaderName := ""
	authHeaderValue := ""
	apiKeyAuthToken := ""
	betaHeader := ""

	if account.IsOAuth() {
		accessToken := strings.TrimSpace(account.GetCredential("access_token"))
		if accessToken == "" && s.claudeTokenProvider != nil {
			token, tokenErr := s.claudeTokenProvider.GetAccessToken(ctx, account)
			if tokenErr != nil {
				return nil, newUpstreamModelSyncUpstreamError("Failed to get Anthropic access token", tokenErr)
			}
			accessToken = strings.TrimSpace(token)
		}
		if accessToken == "" {
			return nil, newUpstreamModelSyncConfigError("No Anthropic access token is available", nil)
		}
		authHeaderName = "Authorization"
		authHeaderValue = "Bearer " + accessToken
		betaHeader = claude.DefaultBetaHeader
	} else if account.Type == AccountTypeAPIKey {
		apiKey := strings.TrimSpace(account.GetCredential("api_key"))
		if apiKey == "" {
			return nil, newUpstreamModelSyncConfigError("No Anthropic API key is available", nil)
		}
		baseURL = account.GetBaseURL()
		if strings.TrimSpace(baseURL) == "" {
			baseURL = "https://api.anthropic.com"
		}
		apiKeyAuthToken = apiKey
		betaHeader = claude.APIKeyBetaHeader
	} else {
		return nil, newUpstreamModelSyncUnsupportedError(
			fmt.Sprintf("Unsupported Anthropic account type for upstream model sync: %s", account.Type), nil,
		)
	}

	normalizedBaseURL, err := s.validateUpstreamBaseURL(baseURL)
	if err != nil {
		return nil, newUpstreamModelSyncConfigError("Invalid Anthropic base URL", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, buildV1ModelsURL(normalizedBaseURL), nil)
	if err != nil {
		return nil, newUpstreamModelSyncConfigError("Invalid Anthropic model list URL", err)
	}
	for key, value := range claude.DefaultHeaders {
		req.Header.Set(key, value)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("anthropic-version", "2023-06-01")
	req.Header.Set("anthropic-beta", betaHeader)
	if authHeaderName != "" {
		req.Header.Set(authHeaderName, authHeaderValue)
	} else {
		setAnthropicAPIKeyAuthHeader(req.Header, account, apiKeyAuthToken)
	}
	// 账号级请求头覆写：模型列表探测与真实转发保持一致的最终头
	account.ApplyHeaderOverrides(req.Header)
	return req, nil
}

func (s *AccountTestService) buildAntigravityAPIKeyModelsRequest(ctx context.Context, account *Account) (*http.Request, error) {
	if account.Type != AccountTypeAPIKey {
		return nil, newUpstreamModelSyncUnsupportedError(
			fmt.Sprintf("Unsupported Antigravity account type for upstream model sync: %s", account.Type), nil,
		)
	}
	apiKey := strings.TrimSpace(account.GetCredential("api_key"))
	if apiKey == "" {
		return nil, newUpstreamModelSyncConfigError("No Antigravity API key is available", nil)
	}

	baseURL := strings.TrimRight(strings.TrimSpace(account.GetCredential("base_url")), "/")
	if baseURL == "" {
		return nil, newUpstreamModelSyncConfigError("Antigravity API-key base URL is required for upstream model sync", nil)
	}
	if !strings.HasSuffix(strings.ToLower(baseURL), "/antigravity") {
		return nil, newUpstreamModelSyncUnsupportedError(
			"Antigravity API-key upstream model sync requires a compatible gateway base URL ending in /antigravity; use Antigravity OAuth for official Cloud Code upstreams",
			nil,
		)
	}
	normalizedBaseURL, err := s.validateUpstreamBaseURL(baseURL)
	if err != nil {
		return nil, newUpstreamModelSyncConfigError("Invalid Antigravity base URL", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, buildV1ModelsURL(normalizedBaseURL), nil)
	if err != nil {
		return nil, newUpstreamModelSyncConfigError("Invalid Antigravity model list URL", err)
	}
	for key, value := range claude.DefaultHeaders {
		req.Header.Set(key, value)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("anthropic-version", "2023-06-01")
	req.Header.Set("anthropic-beta", claude.APIKeyBetaHeader)
	req.Header.Set("x-api-key", apiKey)
	return req, nil
}

func (s *AccountTestService) buildOpenAIUpstreamModelsRequest(ctx context.Context, account *Account) (*http.Request, error) {
	if account.Type != AccountTypeAPIKey {
		return nil, newUpstreamModelSyncUnsupportedError(
			fmt.Sprintf("Unsupported OpenAI account type for upstream model sync: %s", account.Type), nil,
		)
	}
	apiKey := strings.TrimSpace(account.GetOpenAIApiKey())
	if apiKey == "" {
		return nil, newUpstreamModelSyncConfigError("No OpenAI API key is available", nil)
	}

	baseURL := account.GetOpenAIBaseURL()
	if strings.TrimSpace(baseURL) == "" {
		baseURL = "https://api.openai.com"
	}
	normalizedBaseURL, err := s.validateUpstreamBaseURL(baseURL)
	if err != nil {
		return nil, newUpstreamModelSyncConfigError("Invalid OpenAI base URL", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, buildOpenAIModelsURL(normalizedBaseURL), nil)
	if err != nil {
		return nil, newUpstreamModelSyncConfigError("Invalid OpenAI model list URL", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	// 账号级请求头覆写：模型列表探测与真实转发保持一致的最终头
	account.ApplyHeaderOverrides(req.Header)
	return req, nil
}

func (s *AccountTestService) buildGeminiUpstreamModelsRequest(ctx context.Context, account *Account) (*http.Request, error) {
	baseURL := account.GetGeminiBaseURL(geminicli.AIStudioBaseURL)
	if strings.TrimSpace(baseURL) == "" {
		baseURL = geminicli.AIStudioBaseURL
	}
	normalizedBaseURL, err := s.validateUpstreamBaseURL(baseURL)
	if err != nil {
		return nil, newUpstreamModelSyncConfigError("Invalid Gemini base URL", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, buildGeminiModelsURL(normalizedBaseURL), nil)
	if err != nil {
		return nil, newUpstreamModelSyncConfigError("Invalid Gemini model list URL", err)
	}
	req.Header.Set("Accept", "application/json")

	switch account.Type {
	case AccountTypeAPIKey:
		apiKey := strings.TrimSpace(account.GetCredential("api_key"))
		if apiKey == "" {
			return nil, newUpstreamModelSyncConfigError("No Gemini API key is available", nil)
		}
		req.Header.Set("x-goog-api-key", apiKey)
	case AccountTypeOAuth:
		if strings.TrimSpace(account.GetCredential("project_id")) != "" {
			return nil, newUpstreamModelSyncUnsupportedError("Gemini Code Assist model listing is not supported by this sync button", nil)
		}
		if s.geminiTokenProvider == nil {
			return nil, newUpstreamModelSyncConfigError("Gemini token provider is not configured", nil)
		}
		accessToken, tokenErr := s.geminiTokenProvider.GetAccessToken(ctx, account)
		if tokenErr != nil {
			return nil, newUpstreamModelSyncUpstreamError("Failed to get Gemini access token", tokenErr)
		}
		accessToken = strings.TrimSpace(accessToken)
		if accessToken == "" {
			return nil, newUpstreamModelSyncConfigError("No Gemini access token is available", nil)
		}
		req.Header.Set("Authorization", "Bearer "+accessToken)
	default:
		return nil, newUpstreamModelSyncUnsupportedError(
			fmt.Sprintf("Unsupported Gemini account type for upstream model sync: %s", account.Type), nil,
		)
	}

	return req, nil
}

func (s *AccountTestService) fetchAntigravityOAuthUpstreamModels(ctx context.Context, account *Account) ([]string, error) {
	discovery, err := s.fetchAntigravityOAuthUpstreamModelDiscovery(ctx, account)
	if err != nil {
		return nil, err
	}
	if len(discovery.Models) == 0 {
		return nil, newUpstreamModelSyncUpstreamError("Upstream returned no supported models", nil)
	}
	return discovery.Models, nil
}

func (s *AccountTestService) fetchAntigravityOAuthUpstreamModelDiscovery(ctx context.Context, account *Account) (UpstreamModelDiscovery, error) {
	if s.antigravityGatewayService == nil || s.antigravityGatewayService.GetTokenProvider() == nil {
		return UpstreamModelDiscovery{}, newUpstreamModelSyncConfigError("Antigravity token provider is not configured", nil)
	}

	accessToken, err := s.antigravityGatewayService.GetTokenProvider().GetAccessToken(ctx, account)
	if err != nil {
		return UpstreamModelDiscovery{}, newUpstreamModelSyncUpstreamError("Failed to get Antigravity access token", err)
	}
	accessToken = strings.TrimSpace(accessToken)
	if accessToken == "" {
		return UpstreamModelDiscovery{}, newUpstreamModelSyncConfigError("No Antigravity access token is available", nil)
	}

	client, err := antigravity.NewClient(upstreamModelsProxyURL(account))
	if err != nil {
		return UpstreamModelDiscovery{}, newUpstreamModelSyncConfigError("Failed to configure Antigravity client", err)
	}
	modelsResp, _, acceptedResponse, err := client.FetchAvailableModelsWithEvidence(ctx, accessToken, strings.TrimSpace(account.GetCredential("project_id")))
	if err != nil {
		return UpstreamModelDiscovery{}, newUpstreamModelSyncUpstreamError("Failed to fetch Antigravity available models", err)
	}
	if modelsResp == nil {
		return UpstreamModelDiscovery{}, newUpstreamModelSyncUpstreamError("Upstream returned no supported models", nil)
	}

	evidenceModelIDs, err := extractJSONObjectKeysInOrder(acceptedResponse.Body, "models")
	if err != nil {
		return UpstreamModelDiscovery{}, newUpstreamModelSyncUpstreamError("Upstream model list response was not valid JSON", err)
	}
	rawSnapshot, err := marshalUpstreamRawSnapshot(acceptedResponse.Body, map[string]any{
		"source":       "antigravity_oauth",
		"endpoint":     acceptedResponse.Endpoint,
		"status_code":  acceptedResponse.StatusCode,
		"content_type": acceptedResponse.ContentType,
		"etag":         acceptedResponse.ETag,
		"request_id":   acceptedResponse.RequestID,
	})
	if err != nil {
		return UpstreamModelDiscovery{}, newUpstreamModelSyncUpstreamError("Upstream model list response was not valid JSON", err)
	}
	return UpstreamModelDiscovery{
		Models:           dedupeAndSortModelIDs(evidenceModelIDs),
		EvidenceModelIDs: dedupeExactModelIDs(evidenceModelIDs),
		RawSnapshot:      rawSnapshot,
	}, nil
}

func (s *AccountTestService) doUpstreamModelsRequest(req *http.Request, proxyURL string, account *Account) (*http.Response, error) {
	if s.tlsFPProfileService == nil {
		return s.httpUpstream.DoWithTLS(req, proxyURL, account.ID, account.Concurrency, nil)
	}
	return s.httpUpstream.DoWithTLS(req, proxyURL, account.ID, account.Concurrency, s.tlsFPProfileService.ResolveTLSProfile(account))
}

func upstreamModelsProxyURL(account *Account) string {
	if account != nil && account.ProxyID != nil && account.Proxy != nil {
		return account.Proxy.URL()
	}
	return ""
}

func buildV1ModelsURL(base string) string {
	normalized := strings.TrimRight(strings.TrimSpace(base), "/")
	if strings.HasSuffix(normalized, "/v1/models") {
		return normalized
	}
	if strings.HasSuffix(normalized, "/v1") {
		return normalized + "/models"
	}
	return normalized + "/v1/models"
}

func buildOpenAIModelsURL(base string) string {
	return buildOpenAIEndpointURL(base, "/v1/models")
}

func buildGeminiModelsURL(base string) string {
	normalized := strings.TrimRight(strings.TrimSpace(base), "/")
	if strings.HasSuffix(normalized, "/v1beta/models") {
		return normalized
	}
	if strings.HasSuffix(normalized, "/v1beta") {
		return normalized + "/models"
	}
	return normalized + "/v1beta/models"
}

type upstreamModelEntry struct {
	ID           string          `json:"id"`
	Slug         string          `json:"slug"`
	Model        string          `json:"model"`
	ModelID      string          `json:"modelId"`
	ModelIDSnake string          `json:"model_id"`
	Name         string          `json:"name"`
	Meta         json.RawMessage `json:"_meta"`
}

type upstreamModelEntryMetadata struct {
	ID           string `json:"id"`
	Model        string `json:"model"`
	ModelID      string `json:"modelId"`
	ModelIDSnake string `json:"model_id"`
	Name         string `json:"name"`
}

func extractUpstreamModelIDs(body []byte) ([]string, error) {
	return extractUpstreamModelIDsWithSelector(body, upstreamModelEntryID)
}

func extractUpstreamModelIDViews(body []byte) ([]string, []string, error) {
	legacy, err := extractUpstreamModelIDsWithSelector(body, upstreamModelEntryID)
	if err != nil {
		return nil, nil, err
	}
	exact, err := extractUpstreamModelIDsInOrder(body, upstreamModelEntryExactID)
	return legacy, exact, err
}

func extractGrokUpstreamModelIDs(body []byte) ([]string, error) {
	return extractUpstreamModelIDsWithSelector(body, grokUpstreamModelEntryID)
}

func extractGrokUpstreamModelIDViews(body []byte) ([]string, []string, error) {
	legacy, err := extractUpstreamModelIDsWithSelector(body, grokUpstreamModelEntryID)
	if err != nil {
		return nil, nil, err
	}
	exact, err := extractUpstreamModelIDsInOrder(body, grokUpstreamModelEntryExactID)
	return legacy, exact, err
}

func extractUpstreamModelIDsWithSelector(body []byte, selectID func(upstreamModelEntry) string) ([]string, error) {
	models, err := extractUpstreamModelIDsInOrder(body, selectID)
	if err != nil {
		return nil, err
	}
	return dedupeAndSortModelIDs(models), nil
}

func extractUpstreamModelIDsInOrder(body []byte, selectID func(upstreamModelEntry) string) ([]string, error) {
	var response struct {
		Data   []upstreamModelEntry `json:"data"`
		Models []upstreamModelEntry `json:"models"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		var arrayResponse []upstreamModelEntry
		if arrayErr := json.Unmarshal(body, &arrayResponse); arrayErr != nil {
			return nil, fmt.Errorf("parse upstream model list: %w", err)
		}

		models := make([]string, 0, len(arrayResponse))
		for _, entry := range arrayResponse {
			models = append(models, selectID(entry))
		}
		return models, nil
	}

	models := make([]string, 0, len(response.Data)+len(response.Models))
	for _, entry := range response.Data {
		models = append(models, selectID(entry))
	}
	for _, entry := range response.Models {
		models = append(models, selectID(entry))
	}

	if len(models) == 0 {
		var arrayResponse []upstreamModelEntry
		if err := json.Unmarshal(body, &arrayResponse); err == nil {
			for _, entry := range arrayResponse {
				models = append(models, selectID(entry))
			}
		}
	}

	return models, nil
}

func upstreamModelEntryID(entry upstreamModelEntry) string {
	modelID := strings.TrimSpace(entry.ID)
	if modelID == "" {
		modelID = strings.TrimSpace(entry.Slug)
	}
	if modelID == "" {
		modelID = strings.TrimSpace(entry.Name)
	}
	return strings.TrimPrefix(modelID, "models/")
}

func upstreamModelEntryExactID(entry upstreamModelEntry) string {
	for _, candidate := range []string{entry.ID, entry.Slug, entry.Name} {
		if strings.TrimSpace(candidate) != "" {
			return candidate
		}
	}
	return ""
}

func grokUpstreamModelEntryID(entry upstreamModelEntry) string {
	return strings.TrimPrefix(strings.TrimSpace(grokUpstreamModelEntryExactID(entry)), "models/")
}

func grokUpstreamModelEntryExactID(entry upstreamModelEntry) string {
	candidates := []string{
		entry.Model,
		entry.ModelID,
		entry.ModelIDSnake,
		entry.ID,
	}
	if len(entry.Meta) > 0 {
		var meta upstreamModelEntryMetadata
		if err := json.Unmarshal(entry.Meta, &meta); err == nil {
			candidates = append(candidates,
				meta.Model,
				meta.ModelID,
				meta.ModelIDSnake,
				meta.ID,
				meta.Name,
			)
		}
	}
	// `name` is a display label in the Grok catalog, so keep it as the final
	// compatibility fallback rather than preferring it over protocol model IDs.
	candidates = append(candidates, entry.Name)
	for _, candidate := range candidates {
		if strings.TrimSpace(candidate) != "" {
			return candidate
		}
	}
	return ""
}

func dedupeAndSortModelIDs(models []string) []string {
	seen := make(map[string]struct{}, len(models))
	result := make([]string, 0, len(models))
	for _, model := range models {
		model = strings.TrimSpace(model)
		if model == "" {
			continue
		}
		if _, exists := seen[model]; exists {
			continue
		}
		seen[model] = struct{}{}
		result = append(result, model)
	}
	sort.Strings(result)
	return result
}

func dedupeExactModelIDs(models []string) []string {
	seen := make(map[string]struct{}, len(models))
	result := make([]string, 0, len(models))
	for _, model := range models {
		if strings.TrimSpace(model) == "" {
			continue
		}
		if _, exists := seen[model]; exists {
			continue
		}
		seen[model] = struct{}{}
		result = append(result, model)
	}
	return result
}

func marshalUpstreamRawSnapshot(payload []byte, responseMetadata map[string]any) ([]byte, error) {
	if !json.Valid(payload) {
		return nil, errors.New("payload is not valid JSON")
	}
	return json.Marshal(map[string]any{
		// JSONB may canonicalize parsed JSON; base64 preserves the accepted bytes exactly.
		"payload_base64": base64.StdEncoding.EncodeToString(payload),
		"response":       responseMetadata,
	})
}

func extractJSONObjectKeysInOrder(payload []byte, field string) ([]string, error) {
	var document map[string]json.RawMessage
	if err := json.Unmarshal(payload, &document); err != nil {
		return nil, err
	}
	object, ok := document[field]
	if !ok {
		return nil, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(object))
	start, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if delimiter, ok := start.(json.Delim); !ok || delimiter != '{' {
		return nil, fmt.Errorf("%s must be a JSON object", field)
	}
	keys := make([]string, 0)
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		keys = append(keys, key.(string))
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
	}
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	return dedupeExactModelIDs(keys), nil
}

func newSynthesizedUpstreamModelDiscovery(models []string, source string) UpstreamModelDiscovery {
	legacyModels := dedupeAndSortModelIDs(models)
	rawSnapshot, _ := json.Marshal(map[string]any{
		"payload":  map[string]any{"models": models},
		"response": map[string]any{"source": source},
	})
	return UpstreamModelDiscovery{
		Models:           legacyModels,
		EvidenceModelIDs: append([]string(nil), models...),
		RawSnapshot:      rawSnapshot,
	}
}
