package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
)

// ModelAuthorizationKey is the cache/lookup key for eligibility.
type ModelAuthorizationKey struct {
	AccountID        int64
	ChannelID        int64
	CanonicalModelID string
}

// ModelAuthorizationDecision is the cached projection row.
type ModelAuthorizationDecision struct {
	Eligibility     string
	Reason          string
	RegistryVersion int64
	ChannelVersion  int64
}

// RecomputeInput is the transactional batch input.
type RecomputeInput struct {
	BatchID          string
	IdempotencyKey   string
	RegistryVersion  int64
	ChannelVersions  map[int64]int64
	ActorID          string
	Items            []RecomputeItem
}

// RecomputeItem is a single eligibility delta.
type RecomputeItem struct {
	AccountID         int64
	ChannelID         int64
	CanonicalModelID  string
	Eligibility       string
	Reason            string
	RegistryVersion   int64
	ChannelVersion    int64
	QuarantineBatchID *string
}

// ModelAuthorizationStore is the durable store for the eligibility projection.
type ModelAuthorizationStore interface {
	Decision(ctx context.Context, key ModelAuthorizationKey) (ModelAuthorizationDecision, error)
	RecomputeBatch(ctx context.Context, input RecomputeInput) error
	IsChannelModelEligible(ctx context.Context, channelID int64, canonicalModelID string) (bool, error)
	IsCanonicalEligible(ctx context.Context, canonicalModelID string) (bool, error)
}

// modelPublicationService is the cache-aware service wrapper.
// It holds an L1 sync.Map cache; Redis invalidation is best-effort and only published after commit.
type modelPublicationService struct {
	store ModelAuthorizationStore
	cache sync.Map // map[ModelAuthorizationKey]ModelAuthorizationDecision
}

// NewModelPublicationService creates the service. Store may be nil in tests with fake.
func NewModelPublicationService(store ModelAuthorizationStore) *modelPublicationService {
	return &modelPublicationService{store: store}
}

func (s *modelPublicationService) Decision(ctx context.Context, key ModelAuthorizationKey) (ModelAuthorizationDecision, error) {
	if v, ok := s.cache.Load(key); ok {
		if d, ok2 := v.(ModelAuthorizationDecision); ok2 {
			return d, nil
		}
	}
	if s.store == nil {
		return ModelAuthorizationDecision{}, fmt.Errorf("publication store is not configured")
	}
	dec, err := s.store.Decision(ctx, key)
	if err != nil {
		return ModelAuthorizationDecision{}, err
	}
	s.cache.Store(key, dec)
	return dec, nil
}

func (s *modelPublicationService) RecomputeBatch(ctx context.Context, input RecomputeInput) error {
	if err := validateRecomputeInput(input); err != nil {
		return err
	}
	if s.store == nil {
		return fmt.Errorf("publication store is not configured")
	}
	// Call durable store transactionally.
	if err := s.store.RecomputeBatch(ctx, input); err != nil {
		return err
	}
	// Publish cache invalidation only after commit (store guarantees commit before return).
	for _, it := range input.Items {
		key := ModelAuthorizationKey{AccountID: it.AccountID, ChannelID: it.ChannelID, CanonicalModelID: it.CanonicalModelID}
		s.cache.Delete(key)
	}
	return nil
}

func (s *modelPublicationService) InvalidateKey(key ModelAuthorizationKey) {
	s.cache.Delete(key)
}

func (s *modelPublicationService) IsChannelModelEligible(ctx context.Context, channelID int64, canonicalModelID string) (bool, error) {
	if s.store == nil {
		return false, fmt.Errorf("publication store is not configured")
	}
	return s.store.IsChannelModelEligible(ctx, channelID, canonicalModelID)
}

func (s *modelPublicationService) IsCanonicalEligible(ctx context.Context, canonicalModelID string) (bool, error) {
	if s.store == nil {
		return false, fmt.Errorf("publication store is not configured")
	}
	return s.store.IsCanonicalEligible(ctx, canonicalModelID)
}

func validateRecomputeInput(input RecomputeInput) error {
	if strings.TrimSpace(input.BatchID) == "" {
		return fmt.Errorf("batch id is required")
	}
	if strings.TrimSpace(input.IdempotencyKey) == "" {
		return fmt.Errorf("idempotency key is required")
	}
	if input.RegistryVersion <= 0 {
		return fmt.Errorf("registry version must be > 0")
	}
	if len(input.Items) == 0 {
		return fmt.Errorf("recompute items are required")
	}
	if strings.TrimSpace(input.ActorID) == "" {
		return fmt.Errorf("actor is required")
	}
	for _, it := range input.Items {
		if it.AccountID <= 0 || it.ChannelID <= 0 {
			return fmt.Errorf("item account and channel must be > 0")
		}
		if strings.TrimSpace(it.CanonicalModelID) == "" {
			return fmt.Errorf("canonical model id is required")
		}
		if !ValidEligibility(it.Eligibility) {
			return fmt.Errorf("invalid eligibility %q", it.Eligibility)
		}
		if strings.TrimSpace(it.Reason) == "" {
			return fmt.Errorf("reason is required")
		}
		if it.RegistryVersion <= 0 || it.ChannelVersion <= 0 {
			return fmt.Errorf("item versions must be > 0")
		}
		if it.RegistryVersion != input.RegistryVersion {
			return fmt.Errorf("item registry version must match batch registry version")
		}
		if exp, ok := input.ChannelVersions[it.ChannelID]; ok && exp != it.ChannelVersion {
			return fmt.Errorf("item channel version must match batch channel versions")
		}
	}
	return nil
}

// requestHash computes deterministic hash for idempotency deduplication.
func publicationRequestHash(input RecomputeInput) string {
	// Canonical JSON of sorted items.
	type itemHash struct {
		AccountID        int64  `json:"account_id"`
		ChannelID        int64  `json:"channel_id"`
		CanonicalModelID string `json:"canonical_model_id"`
		Eligibility      string `json:"eligibility"`
		Reason           string `json:"reason"`
		RegistryVersion  int64  `json:"registry_version"`
		ChannelVersion   int64  `json:"channel_version"`
	}
	// Deterministic by AccountID/ChannelID/CanonicalID order; caller must ensure sorted for stable hash.
	// For implementation, sort here.
	hashItems := make([]itemHash, len(input.Items))
	for i, it := range input.Items {
		hashItems[i] = itemHash{
			AccountID: it.AccountID, ChannelID: it.ChannelID, CanonicalModelID: it.CanonicalModelID,
			Eligibility: it.Eligibility, Reason: it.Reason, RegistryVersion: it.RegistryVersion, ChannelVersion: it.ChannelVersion,
		}
	}
	payload, _ := json.Marshal(map[string]any{
		"batch_id":         input.BatchID,
		"registry_version": input.RegistryVersion,
		"channel_versions": input.ChannelVersions,
		"items":            hashItems,
	})
	sum := sha256.Sum256(payload)
	return fmt.Sprintf("sha256:%x", sum)
}

var _ ModelAuthorizationStore = (*modelPublicationService)(nil)
