package service

import (
	"context"
	"fmt"
	"strings"
)

// RegistryDecisionInput captures a reviewed registry decision.
type RegistryDecisionInput struct {
	IdempotencyKey string
	ExpectedVersion int64
	CanonicalID    string
	Provider       GovernanceProvider
	Modality       string
	Status         string
	Aliases        []string
	ActorID        string
	EvidenceRef    *string
}

// ModelRegistryRepository abstracts transactional registry writes and snapshot reads.
type ModelRegistryRepository interface {
	CreateDecision(ctx context.Context, input RegistryDecisionInput) (*ModelRegistryEntry, int64, error)
	GetSnapshot(ctx context.Context) (*ModelRegistrySnapshot, error)
	ListEntries(ctx context.Context) ([]ModelRegistryEntry, error)
	GetEntry(ctx context.Context, canonicalID string) (*ModelRegistryEntry, error)
	RebuildProjection(ctx context.Context) error
}

// ModelRegistryService exposes reviewed registry writes.
type ModelRegistryService interface {
	GetSnapshot(ctx context.Context) (*ModelRegistrySnapshot, error)
	List(ctx context.Context) ([]ModelRegistryEntry, error)
	Get(ctx context.Context, canonicalID string) (*ModelRegistryEntry, error)
	CreateDecision(ctx context.Context, input RegistryDecisionInput) (*ModelRegistryEntry, error)
	Rebuild(ctx context.Context) error
}

type modelRegistryService struct {
	repo ModelRegistryRepository
}

func NewModelRegistryService(repo ModelRegistryRepository) ModelRegistryService {
	return &modelRegistryService{repo: repo}
}

func (s *modelRegistryService) GetSnapshot(ctx context.Context) (*ModelRegistrySnapshot, error) {
	if s.repo == nil {
		return &ModelRegistrySnapshot{Version: 0, Entries: map[string]ModelRegistryEntry{}, Aliases: map[string]string{}}, nil
	}
	return s.repo.GetSnapshot(ctx)
}

func (s *modelRegistryService) List(ctx context.Context) ([]ModelRegistryEntry, error) {
	if s.repo == nil {
		return nil, nil
	}
	return s.repo.ListEntries(ctx)
}

func (s *modelRegistryService) Get(ctx context.Context, canonicalID string) (*ModelRegistryEntry, error) {
	if s.repo == nil {
		return nil, fmt.Errorf("model registry repository is not configured")
	}
	canonicalID = strings.TrimSpace(canonicalID)
	if canonicalID == "" {
		return nil, fmt.Errorf("canonical ID is required")
	}
	return s.repo.GetEntry(ctx, canonicalID)
}

func (s *modelRegistryService) CreateDecision(ctx context.Context, input RegistryDecisionInput) (*ModelRegistryEntry, error) {
	if err := validateRegistryDecisionInput(input); err != nil {
		return nil, err
	}
	if s.repo == nil {
		return nil, fmt.Errorf("model registry repository is not configured")
	}
	// Normalize canonical ID: case-sensitive after trimming.
	input.CanonicalID = strings.TrimSpace(input.CanonicalID)
	normalizedAliases := make([]string, 0, len(input.Aliases))
	for _, alias := range input.Aliases {
		alias = strings.TrimSpace(alias)
		if alias == "" {
			return nil, fmt.Errorf("alias must not be empty")
		}
		if strings.Contains(alias, "*") {
			return nil, fmt.Errorf("alias must not contain wildcard")
		}
		normalizedAliases = append(normalizedAliases, alias)
	}
	input.Aliases = normalizedAliases

	entry, _, err := s.repo.CreateDecision(ctx, input)
	return entry, err
}

func (s *modelRegistryService) Rebuild(ctx context.Context) error {
	if s.repo == nil {
		return fmt.Errorf("model registry repository is not configured")
	}
	return s.repo.RebuildProjection(ctx)
}

func validateRegistryDecisionInput(input RegistryDecisionInput) error {
	if strings.TrimSpace(input.IdempotencyKey) == "" {
		return fmt.Errorf("idempotency key is required")
	}
	if input.ExpectedVersion < 0 {
		return fmt.Errorf("expected version must be non-negative")
	}
	if strings.TrimSpace(input.CanonicalID) == "" {
		return fmt.Errorf("canonical ID is required")
	}
	if strings.Contains(strings.TrimSpace(input.CanonicalID), "*") {
		return fmt.Errorf("canonical ID must not contain wildcard")
	}
	if !ValidGovernanceProvider(input.Provider) {
		return fmt.Errorf("invalid provider %q", input.Provider)
	}
	if !ValidModelModality(input.Modality) {
		return fmt.Errorf("invalid modality %q", input.Modality)
	}
	if !ValidModelLifecycle(input.Status) {
		return fmt.Errorf("invalid lifecycle %q", input.Status)
	}
	if strings.TrimSpace(input.ActorID) == "" {
		return fmt.Errorf("actor ID is required")
	}
	seen := make(map[string]struct{}, len(input.Aliases))
	for _, alias := range input.Aliases {
		trimmed := strings.TrimSpace(alias)
		if trimmed == "" {
			return fmt.Errorf("alias must not be empty")
		}
		if trimmed == strings.TrimSpace(input.CanonicalID) {
			return fmt.Errorf("alias must not equal canonical ID")
		}
		if _, exists := seen[trimmed]; exists {
			return fmt.Errorf("duplicate alias %q", trimmed)
		}
		seen[trimmed] = struct{}{}
	}
	return nil
}

var _ ModelRegistryService = (*modelRegistryService)(nil)
