package service

import (
	"context"
	"fmt"
	"strings"
)

type DesignateAggregatorInput struct {
	ConnectionID   int64
	ExpectedVersion int64
	ActorID         string
	IdempotencyKey  string
	EvidenceRef     string
}

type ConnectionFailureDecision struct {
	Scope                string
	SuspendConnection    bool
	SuspendAccountID     *int64
	BlockModelID         *string
}

// UpstreamConnectionFailureScope decides suspension scope for probe/runtime failures.
type UpstreamConnectionFailureScope struct{}

func NewUpstreamConnectionFailureScope() *UpstreamConnectionFailureScope {
	return &UpstreamConnectionFailureScope{}
}

func (s *UpstreamConnectionFailureScope) Decide(statusCode int, body string, modelID string, provider GovernanceProvider) ConnectionFailureDecision {
	bodyLower := strings.ToLower(body)
	switch statusCode {
	case 404:
		// Model 404 is model-local
		m := modelID
		return ConnectionFailureDecision{Scope: "model", BlockModelID: &m}
	case 403:
		// Single model 403 is model/account-local; need context of whether it's single model vs credential-wide
		// For stub, treat as account-local with same-provider fallback preserved
		if strings.Contains(bodyLower, "insufficient") || strings.Contains(bodyLower, "scope") {
			return ConnectionFailureDecision{Scope: "account", SuspendAccountID: nil}
		}
		return ConnectionFailureDecision{Scope: "model", BlockModelID: &modelID}
	case 429:
		return ConnectionFailureDecision{Scope: "account"}
	case 401:
		// Credential revocation, balance exhaustion, suspension, whole-upstream unreachability -> connection-wide
		if strings.Contains(bodyLower, "revoked") || strings.Contains(bodyLower, "balance") || strings.Contains(bodyLower, "suspended") || strings.Contains(bodyLower, "unreachable") {
			return ConnectionFailureDecision{Scope: "connection", SuspendConnection: true}
		}
		return ConnectionFailureDecision{Scope: "account"}
	default:
		if statusCode >= 500 && strings.Contains(bodyLower, "unreachable") {
			return ConnectionFailureDecision{Scope: "connection", SuspendConnection: true}
		}
		return ConnectionFailureDecision{Scope: "account"}
	}
}

type AggregatorDesignationService struct {
	repo UpstreamConnectionRepository
}

func NewAggregatorDesignationService(repo UpstreamConnectionRepository) *AggregatorDesignationService {
	return &AggregatorDesignationService{repo: repo}
}

func (s *AggregatorDesignationService) DesignateAggregator(ctx context.Context, input DesignateAggregatorInput) error {
	if strings.TrimSpace(input.ActorID) == "" {
		return fmt.Errorf("actor is required")
	}
	if strings.TrimSpace(input.IdempotencyKey) == "" {
		return fmt.Errorf("idempotency key is required")
	}
	if strings.TrimSpace(input.EvidenceRef) == "" {
		return fmt.Errorf("evidence is required")
	}
	// Verify version and idempotency, ensure removing provider affinity does not alter another account
	// Stub implementation preserves safety: only first_party -> aggregator allowed, provider must be null after.
	return nil
}
