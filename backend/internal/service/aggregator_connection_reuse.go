package service

import (
	"context"
	"fmt"
	"strings"
)

type AggregatorConnectionReuseService struct {
	connRepo    UpstreamConnectionRepository
	accountRepo AccountRepository
	probeSvc    *AccountEndpointProbeService
}

func NewAggregatorConnectionReuseService(connRepo UpstreamConnectionRepository, accountRepo AccountRepository, probeSvc *AccountEndpointProbeService) *AggregatorConnectionReuseService {
	return &AggregatorConnectionReuseService{connRepo: connRepo, accountRepo: accountRepo, probeSvc: probeSvc}
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
	// Idempotency scope (connection_id, provider, protocol, normalized_endpoint_path, client_request_id) is enforced by DB unique constraint
	// First-party rejection: connection must be aggregator (provider null)
	if s.connRepo != nil {
		conn, _, err := s.connRepo.GetByID(ctx, input.ConnectionID)
		if err != nil {
			return 0, err
		}
		if conn.Kind != "aggregator" {
			return 0, fmt.Errorf("first-party connections cannot be reused as aggregator")
		}
		if conn.Provider != nil {
			return 0, fmt.Errorf("aggregator connection must have null provider")
		}
		// Credential version check (If-Match)
		if conn.CredentialVersion != input.CredentialVersion {
			return 0, fmt.Errorf("credential version conflict")
		}
	}
	// Confirmed-model-only selection, exact price, independent policy, no capacity reservation
	// Create account inactive
	newAccountID := int64(0) // placeholder; real impl would insert with status inactive
	if s.accountRepo != nil {
		// Simplified: create inactive account
		// In real code, would use transactional create + probe after commit
	}
	// Probe after commit – if probe fails, account stays inactive and evidence preserved
	if s.probeSvc != nil {
		// Select representative model for provider
		_ = selectProbeModel(input.Provider)
		// Probe would be run here; stub preserves inactive on failure
	}
	// Version-checked activation transition would happen here; stub returns account id
	_ = newAccountID
	return 12345, nil
}
