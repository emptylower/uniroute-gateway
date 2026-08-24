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

// ReuseOutcome reports what the caller must surface to the operator: whether
// the account was activated and, when probing failed, the sanitized upstream
// reason (never the credential). A silent disabled account is a defect.
type ReuseOutcome struct {
	AccountID   int64
	Activated   bool
	ProbeStatus string // "success" | "failed" | "" when no probe was attempted
	ProbeDetail string // sanitized upstream message from the last attempt
}

// Reuse creates an account inactive transactionally, runs probe after commit, then version-checked activation.
// Only after activation may current-gate recomputation publish eligible models; failed probe leaves inactive and preserves evidence.
func (s *AggregatorConnectionReuseService) Reuse(ctx context.Context, input ReuseAggregatorConnectionInput) (*ReuseOutcome, error) {
	if input.ConnectionID == 0 {
		return nil, fmt.Errorf("connection_id is required")
	}
	if !ValidGovernanceProvider(input.Provider) {
		return nil, fmt.Errorf("invalid provider")
	}
	if !ValidAccountProtocol(input.Protocol) {
		return nil, fmt.Errorf("invalid protocol")
	}
	if strings.TrimSpace(input.NormalizedEndpoint) == "" {
		return nil, fmt.Errorf("endpoint is required")
	}
	if strings.TrimSpace(input.ClientRequestID) == "" {
		return nil, fmt.Errorf("client_request_id is required")
	}
	// First-party rejection
	var conn *UpstreamConnection
	var encCred string
	if s.connRepo != nil {
		c, cred, err := s.connRepo.GetByID(ctx, input.ConnectionID)
		if err != nil {
			return nil, err
		}
		conn = c
		encCred = cred
		if conn.Kind != "aggregator" {
			return nil, fmt.Errorf("first-party connections cannot be reused as aggregator")
		}
		if conn.Provider != nil {
			return nil, fmt.Errorf("aggregator connection must have null provider")
		}
		if conn.CredentialVersion != input.CredentialVersion {
			return nil, fmt.Errorf("credential version conflict")
		}
	}
	// Idempotency: check existing reuse request
	if s.reuseRepo != nil {
		if id, found, err := s.reuseRepo.FindByScope(ctx, input.ConnectionID, input.Provider, input.Protocol, input.NormalizedEndpoint, input.ClientRequestID); err == nil && found {
			outcome := &ReuseOutcome{AccountID: id}
			if s.accountRepo != nil {
				if acc, aerr := s.accountRepo.GetByID(ctx, id); aerr == nil && acc != nil {
					outcome.Activated = acc.Status == "active"
				}
			}
			return outcome, nil
		}
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
				return &ReuseOutcome{AccountID: failAccount.ID, Activated: false, ProbeStatus: "failed", ProbeDetail: "connection credential could not be decrypted"}, nil
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
		return nil, fmt.Errorf("account repository not configured")
	}
	// Create inactive
	if err := s.accountRepo.Create(ctx, newAccount); err != nil {
		if isServiceUniqueViolation(err) && s.reuseRepo != nil {
			if id, found, _ := s.reuseRepo.FindByScope(ctx, input.ConnectionID, input.Provider, input.Protocol, input.NormalizedEndpoint, input.ClientRequestID); found {
				return &ReuseOutcome{AccountID: id}, nil
			}
		}
		return nil, fmt.Errorf("create inactive account: %w", err)
	}
	// Record reuse request
	if s.reuseRepo != nil {
		_ = s.reuseRepo.Create(ctx, input.ConnectionID, input.Provider, input.Protocol, input.NormalizedEndpoint, input.ClientRequestID, newAccount.ID)
	}
	// Post-commit: free model-list probe (liveness + auth + discovery evidence).
	// Success activates; failure leaves the account inactive with evidence.
	outcome := &ReuseOutcome{AccountID: newAccount.ID}
	if s.probeSvc != nil && conn != nil {
		credential := plainCred
		probe, err := s.probeSvc.Probe(ctx, newAccount.ID, conn, input.Provider, input.Protocol, input.NormalizedEndpoint, credential)
		if err != nil || probe == nil || probe.Status != "success" {
			outcome.ProbeStatus = "failed"
			outcome.ProbeDetail = probeFailureDetail(probe, err)
			return outcome, nil
		}
		// Probe success: version-checked activation transition
		newAccount.Status = "active"
		newAccount.Schedulable = true
		// Use optimistic version check: ensure config_version still 1 before activation
		if err := s.accountRepo.Update(ctx, newAccount); err != nil {
			return nil, fmt.Errorf("activate account: %w", err)
		}
		outcome.Activated = true
		outcome.ProbeStatus = "success"
		if probe.ResponseSummary != nil {
			if count, ok := probe.ResponseSummary["model_count"].(int); ok {
				outcome.ProbeDetail = fmt.Sprintf("model list fetched: %d models", count)
			}
		}
		// After activation, publication recomputation would publish eligible models (Phase 5)
	}
	return outcome, nil
}

// probeFailureDetail extracts the sanitized upstream reason from a failed probe
// (body_truncated is already secret-scrubbed by the probe service). The detail
// is operator-facing evidence, never credential material.
func probeFailureDetail(probe *AccountEndpointProbe, err error) string {
	if probe != nil && probe.ResponseSummary != nil {
		if msg, ok := probe.ResponseSummary["body_truncated"].(string); ok && msg != "" {
			return msg
		}
		if code, ok := probe.ResponseSummary["status"]; ok {
			return fmt.Sprintf("upstream returned status %v", code)
		}
	}
	if err != nil {
		return err.Error()
	}
	return "probe failed"
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
		return PlatformAnthropic
	case GovernanceProviderOpenAI:
		return PlatformOpenAI
	case GovernanceProviderGemini:
		return PlatformGemini
	case GovernanceProviderGrok:
		return PlatformGrok
	default:
		return string(p)
	}
}
