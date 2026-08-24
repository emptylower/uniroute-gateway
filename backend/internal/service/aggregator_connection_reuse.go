package service

import (
	"context"
	"fmt"
	"strings"
)

type AggregatorReuseRepository interface {
	FindByScope(ctx context.Context, connectionID int64, provider GovernanceProvider, protocol AccountProtocol, endpoint, clientRequestID string) (int64, bool, error)
	Create(ctx context.Context, connectionID int64, provider GovernanceProvider, protocol AccountProtocol, endpoint, clientRequestID string, accountID int64) error
}

type AggregatorConnectionReuseService struct {
	connRepo    UpstreamConnectionRepository
	accountRepo AccountRepository
	reuseRepo   AggregatorReuseRepository
	probeSvc    *AccountEndpointProbeService
	encryptor   SecretEncryptor
}

func NewAggregatorConnectionReuseService(connRepo UpstreamConnectionRepository, accountRepo AccountRepository, probeSvc *AccountEndpointProbeService) *AggregatorConnectionReuseService {
	return &AggregatorConnectionReuseService{connRepo: connRepo, accountRepo: accountRepo, probeSvc: probeSvc}
}

func NewAggregatorConnectionReuseServiceWithRepo(connRepo UpstreamConnectionRepository, accountRepo AccountRepository, reuseRepo AggregatorReuseRepository, probeSvc *AccountEndpointProbeService) *AggregatorConnectionReuseService {
	return &AggregatorConnectionReuseService{connRepo: connRepo, accountRepo: accountRepo, reuseRepo: reuseRepo, probeSvc: probeSvc}
}

func NewAggregatorConnectionReuseServiceWithEncryptor(connRepo UpstreamConnectionRepository, accountRepo AccountRepository, reuseRepo AggregatorReuseRepository, probeSvc *AccountEndpointProbeService, encryptor SecretEncryptor) *AggregatorConnectionReuseService {
	return &AggregatorConnectionReuseService{connRepo: connRepo, accountRepo: accountRepo, reuseRepo: reuseRepo, probeSvc: probeSvc, encryptor: encryptor}
}

func (s *AggregatorConnectionReuseService) SetEncryptor(enc SecretEncryptor) {
	s.encryptor = enc
}

type ReuseAggregatorConnectionInput struct {
	ConnectionID       int64
	Provider           GovernanceProvider
	Protocol           AccountProtocol
	NormalizedEndpoint string
	ClientRequestID    string
	CredentialVersion  int64 // If-Match
	ActorID            string
}

// Reuse creates an account inactive transactionally, runs probe after commit, then version-checked activation.
// Only after activation may current-gate recomputation publish eligible models; failed probe leaves inactive and preserves evidence.
func (s *AggregatorConnectionReuseService) Reuse(ctx context.Context, input ReuseAggregatorConnectionInput) (int64, error) {
	if input.ConnectionID == 0 {
		return 0, fmt.Errorf("connection_id is required")
	}
	if !ValidGovernanceProvider(input.Provider) {
		return 0, fmt.Errorf("invalid provider")
	}
	if !ValidAccountProtocol(input.Protocol) {
		return 0, fmt.Errorf("invalid protocol")
	}
	if strings.TrimSpace(input.NormalizedEndpoint) == "" {
		return 0, fmt.Errorf("endpoint is required")
	}
	if strings.TrimSpace(input.ClientRequestID) == "" {
		return 0, fmt.Errorf("client_request_id is required")
	}
	// First-party rejection
	var conn *UpstreamConnection
	var encCred string
	if s.connRepo != nil {
		c, cred, err := s.connRepo.GetByID(ctx, input.ConnectionID)
		if err != nil {
			return 0, err
		}
		conn = c
		encCred = cred
		if conn.Kind != "aggregator" {
			return 0, fmt.Errorf("first-party connections cannot be reused as aggregator")
		}
		if conn.Provider != nil {
			return 0, fmt.Errorf("aggregator connection must have null provider")
		}
		if conn.CredentialVersion != input.CredentialVersion {
			return 0, fmt.Errorf("credential version conflict")
		}
	}
	// Idempotency: check existing reuse request
	if s.reuseRepo != nil {
		if id, found, err := s.reuseRepo.FindByScope(ctx, input.ConnectionID, input.Provider, input.Protocol, input.NormalizedEndpoint, input.ClientRequestID); err == nil && found {
			return id, nil
		}
	}
	// Confirmed-model-only selection
	model := selectProbeModel(input.Provider)
	if model == "" {
		return 0, fmt.Errorf("no representative model for provider %s", input.Provider)
	}
	// Decrypt connection credential for reuse (R2)
	var plainCred string
	if encCred != "" {
		if s.encryptor != nil {
			dec, err := s.encryptor.Decrypt(encCred)
			if err != nil {
				// Decrypt failure – create inactive account with evidence and return, do not fall back to temp
				platform := providerToPlatform(input.Provider)
				failAccount := &Account{
					Name: fmt.Sprintf("reuse-%s-%d", input.Provider, input.ConnectionID), Platform: platform, Type: AccountTypeAPIKey,
					Credentials: map[string]any{}, Extra: map[string]any{}, Status: "disabled", Schedulable: false, ConnectionID: &input.ConnectionID, ConfigVersion: 1,
				}
				protoStrFail := string(input.Protocol)
				epFail := input.NormalizedEndpoint
				failAccount.Protocol = &protoStrFail
				failAccount.EndpointPath = &epFail
				_ = s.accountRepo.Create(ctx, failAccount)
				if s.reuseRepo != nil {
					_ = s.reuseRepo.Create(ctx, input.ConnectionID, input.Provider, input.Protocol, input.NormalizedEndpoint, input.ClientRequestID, failAccount.ID)
				}
				return failAccount.ID, nil
			}
			plainCred = dec
		} else {
			// Test path without encryptor: encCred may be plaintext or prefixed with enc:
			if strings.HasPrefix(encCred, "enc:") {
				plainCred = strings.TrimPrefix(encCred, "enc:")
			} else {
				plainCred = encCred
			}
		}
	}
	// Transactionally create inactive account + reuse request with real credential
	platform := providerToPlatform(input.Provider)
	creds := map[string]any{}
	if plainCred != "" {
		creds["api_key"] = plainCred
	}
	// The runtime (scheduling, model sync, usage) only understands
	// credentials.base_url — governance EndpointPath is metadata. Derive the
	// runtime base by joining the connection base with the endpoint's path
	// prefix ("/chatgpt/v1" → base+"/chatgpt", "/v1" → base).
	if conn != nil {
		creds["base_url"] = deriveRuntimeBaseURL(conn.BaseURL, input.NormalizedEndpoint)
	}
	newAccount := &Account{
		Name: fmt.Sprintf("reuse-%s-%d", input.Provider, input.ConnectionID), Platform: platform, Type: AccountTypeAPIKey,
		Credentials: creds, Extra: map[string]any{}, Status: "disabled", Schedulable: false, ConnectionID: &input.ConnectionID, ConfigVersion: 1,
	}
	protoStr := string(input.Protocol)
	ep := input.NormalizedEndpoint
	newAccount.Protocol = &protoStr
	newAccount.EndpointPath = &ep

	if s.accountRepo == nil {
		return 0, fmt.Errorf("account repository not configured")
	}
	// Create inactive
	if err := s.accountRepo.Create(ctx, newAccount); err != nil {
		if isServiceUniqueViolation(err) && s.reuseRepo != nil {
			if id, found, _ := s.reuseRepo.FindByScope(ctx, input.ConnectionID, input.Provider, input.Protocol, input.NormalizedEndpoint, input.ClientRequestID); found {
				return id, nil
			}
		}
		return 0, fmt.Errorf("create inactive account: %w", err)
	}
	// Record reuse request
	if s.reuseRepo != nil {
		_ = s.reuseRepo.Create(ctx, input.ConnectionID, input.Provider, input.Protocol, input.NormalizedEndpoint, input.ClientRequestID, newAccount.ID)
	}
	// Post-commit: probe candidate models in order until one succeeds. Every
	// attempt persists its own evidence row; first success activates.
	if s.probeSvc != nil && conn != nil {
		credential := plainCred
		for _, model := range selectProbeModels(input.Provider) {
			probe, err := s.probeSvc.ProbeWithModel(ctx, newAccount.ID, conn, input.Provider, input.Protocol, input.NormalizedEndpoint, credential, model)
			if err != nil || probe == nil || probe.Status != "success" {
				continue
			}
			// Probe success: version-checked activation transition
			newAccount.Status = "active"
			newAccount.Schedulable = true
			// Use optimistic version check: ensure config_version still 1 before activation
			if err := s.accountRepo.Update(ctx, newAccount); err != nil {
				return 0, fmt.Errorf("activate account: %w", err)
			}
			// After activation, publication recomputation would publish eligible models (Phase 5)
			break
		}
	}
	return newAccount.ID, nil
}

// deriveRuntimeBaseURL derives the runtime API root from the connection base
// plus the probed endpoint path: everything BEFORE the first version segment
// ("/v1", "/v1beta", "/v1alpha") is a path prefix the runtime base must carry;
// from the version segment on, the runtime client appends paths itself.
// Examples: ("https://h", "/v1/chat/completions") → "https://h";
// ("https://h", "/chatgpt/v1") → "https://h/chatgpt";
// ("https://h", "/api/v1beta/models") → "https://h/api".
func deriveRuntimeBaseURL(baseURL, normalizedEndpoint string) string {
	base := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	ep := strings.TrimSpace(normalizedEndpoint)
	for _, seg := range []string{"/v1beta", "/v1alpha", "/v1"} {
		if i := strings.Index(ep, seg+"/"); i >= 0 {
			ep = ep[:i]
			break
		}
		if strings.HasSuffix(ep, seg) {
			ep = strings.TrimSuffix(ep, seg)
			break
		}
	}
	ep = strings.Trim(ep, "/")
	if ep == "" {
		return base
	}
	return base + "/" + ep
}

func providerToPlatform(p GovernanceProvider) string {
	switch p {
	case GovernanceProviderAnthropic:
		return "claude"
	case GovernanceProviderOpenAI:
		return "openai"
	case GovernanceProviderGemini:
		return "gemini"
	case GovernanceProviderGrok:
		return "grok"
	default:
		return string(p)
	}
}
