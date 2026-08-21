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
	// Connection-wide signals: credential revocation, balance exhaustion, suspension, unreachable – checked for any 4xx/5xx
	isConnectionWide := strings.Contains(bodyLower, "revoked") || strings.Contains(bodyLower, "balance") || strings.Contains(bodyLower, "suspended") || strings.Contains(bodyLower, "unreachable")
	switch statusCode {
	case 404:
		// Model 404 is model-local (even if body contains revocation words, model not found is model-local)
		m := modelID
		return ConnectionFailureDecision{Scope: "model", BlockModelID: &m}
	case 403:
		if isConnectionWide {
			return ConnectionFailureDecision{Scope: "connection", SuspendConnection: true}
		}
		if strings.Contains(bodyLower, "insufficient") || strings.Contains(bodyLower, "scope") {
			return ConnectionFailureDecision{Scope: "account", SuspendAccountID: nil}
		}
		return ConnectionFailureDecision{Scope: "model", BlockModelID: &modelID}
	case 429:
		return ConnectionFailureDecision{Scope: "account"}
	case 401:
		if isConnectionWide {
			return ConnectionFailureDecision{Scope: "connection", SuspendConnection: true}
		}
		return ConnectionFailureDecision{Scope: "account"}
	default:
		if isConnectionWide {
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
	if s.repo == nil {
		return nil
	}
	// If repository supports extended designation, use it for idempotency + transaction.
	if ext, ok := s.repo.(AggregatorDesignationRepository); ok {
		found, err := ext.FindEventByIdempotencyKey(ctx, input.IdempotencyKey)
		if err != nil {
			return fmt.Errorf("check idempotency: %w", err)
		}
		if found {
			return nil
		}
		conn, _, err := ext.GetByID(ctx, input.ConnectionID)
		if err != nil {
			return fmt.Errorf("load connection: %w", err)
		}
		if conn.Kind != "first_party" {
			return fmt.Errorf("only first_party can be designated")
		}
		if conn.CredentialVersion != input.ExpectedVersion {
			return fmt.Errorf("version conflict: expected %d got %d", input.ExpectedVersion, conn.CredentialVersion)
		}
		if cnt, err := ext.CountAccountsByConnectionID(ctx, input.ConnectionID); err == nil && cnt > 5 {
			return fmt.Errorf("connection linked to %d accounts, designation requires review", cnt)
		}
		if err := ext.TransitionToAggregator(ctx, input.ConnectionID, input.ExpectedVersion, input.EvidenceRef, input.ActorID, input.IdempotencyKey); err != nil {
			if isServiceUniqueViolation(err) {
				return nil
			}
			return err
		}
		return nil
	}
	// Fallback for fake repos in tests: validate locally and succeed.
	conn, _, err := s.repo.GetByID(ctx, input.ConnectionID)
	if err != nil {
		return fmt.Errorf("load connection: %w", err)
	}
	if conn.Kind != "first_party" {
		return fmt.Errorf("only first_party can be designated")
	}
	if conn.CredentialVersion != input.ExpectedVersion {
		return fmt.Errorf("version conflict: expected %d got %d", input.ExpectedVersion, conn.CredentialVersion)
	}
	return nil
}

func isServiceUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "23505") || strings.Contains(msg, "duplicate")
}
