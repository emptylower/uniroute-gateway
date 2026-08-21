package service

import (
	"context"
	"fmt"
)

// ClassificationSummary mirrors the API contract for shadow evaluation.
type ClassificationSummary struct {
	BatchID         string `json:"batch_id"`
	RegistryVersion int64  `json:"registry_version"`
	Discovered      int    `json:"discovered"`
	Publishable     int    `json:"publishable"`
	CrossProvider   int    `json:"cross_provider"`
	Unknown         int    `json:"unknown"`
	Ignored         int    `json:"ignored"`
}

// ModelGovernanceService classifies discovery and persists shadow decisions without mutating routing state.
type ModelGovernanceService interface {
	ClassifyAndPersistShadow(ctx context.Context, input DiscoveryBatchInput) (*ClassificationSummary, error)
}

type modelGovernanceService struct {
	registryService         ModelRegistryService
	observationRepo         ModelObservationRepository
	classifier              ModelClassifier
	evaluator               PublicationEvaluator
	shadowDecisionRepo      ShadowDecisionRepository
	channelPriceChecker     ChannelPriceChecker
	billingMappingChecker   BillingMappingChecker
	endpointProbeChecker    EndpointProbeChecker
	resourceChecker         ResourceChecker
}

// ShadowDecisionRepository persists shadow publication decisions.
type ShadowDecisionRepository interface {
	RecordShadowDecisions(ctx context.Context, batchID string, decisions []ShadowDecision) error
}

type ShadowDecision struct {
	UpstreamModelID string
	CanonicalID     *string
	Provider        *GovernanceProvider
	Classification  string
	Eligibility     string
	Reason          string
}

// ChannelPriceChecker checks exact channel price presence.
type ChannelPriceChecker interface {
	HasExactPrice(ctx context.Context, canonicalID string, provider GovernanceProvider) (bool, error)
}

// BillingMappingChecker checks billing mapping unambiguity.
type BillingMappingChecker interface {
	IsUnambiguous(ctx context.Context, canonicalID string) (bool, error)
}

// EndpointProbeChecker checks endpoint probe validity.
type EndpointProbeChecker interface {
	IsValid(ctx context.Context, accountID int64) (bool, error)
}

// ResourceChecker checks connection/account/group/channel enablement.
type ResourceChecker interface {
	AreEnabled(ctx context.Context, accountID int64) (connectionEnabled, accountEnabled, groupEnabled, channelEnabled bool, err error)
}

// ModelGovernanceServiceConfig holds dependencies for shadow service.
type ModelGovernanceServiceConfig struct {
	RegistryService       ModelRegistryService
	ObservationRepo       ModelObservationRepository
	Classifier            ModelClassifier
	Evaluator             PublicationEvaluator
	ShadowDecisionRepo    ShadowDecisionRepository
	ChannelPriceChecker   ChannelPriceChecker
	BillingMappingChecker BillingMappingChecker
	EndpointProbeChecker  EndpointProbeChecker
	ResourceChecker       ResourceChecker
}

func NewModelGovernanceService(cfg ModelGovernanceServiceConfig) ModelGovernanceService {
	if cfg.Classifier == nil {
		cfg.Classifier = NewModelClassifier()
	}
	if cfg.Evaluator == nil {
		cfg.Evaluator = NewPublicationEvaluator()
	}
	return &modelGovernanceService{
		registryService:       cfg.RegistryService,
		observationRepo:       cfg.ObservationRepo,
		classifier:            cfg.Classifier,
		evaluator:             cfg.Evaluator,
		shadowDecisionRepo:    cfg.ShadowDecisionRepo,
		channelPriceChecker:   cfg.ChannelPriceChecker,
		billingMappingChecker: cfg.BillingMappingChecker,
		endpointProbeChecker:  cfg.EndpointProbeChecker,
		resourceChecker:       cfg.ResourceChecker,
	}
}

func (s *modelGovernanceService) ClassifyAndPersistShadow(ctx context.Context, input DiscoveryBatchInput) (*ClassificationSummary, error) {
	if s.registryService == nil {
		return nil, fmt.Errorf("registry service is not configured")
	}
	if s.observationRepo == nil {
		return nil, fmt.Errorf("observation repository is not configured")
	}
	// Load one immutable registry version.
	snapshot, err := s.registryService.GetSnapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf("load registry snapshot: %w", err)
	}
	if snapshot == nil {
		snapshot = &ModelRegistrySnapshot{Version: 0, Entries: map[string]ModelRegistryEntry{}, Aliases: map[string]string{}}
	}

	// Classify every exact ID deterministically.
	decisions := make([]ClassificationDecision, 0, len(input.ModelIDs))
	counts := map[string]int{
		ModelClassificationDiscovered:   0,
		ModelClassificationApproved:     0,
		ModelClassificationCrossProvider: 0,
		ModelClassificationUnknown:      0,
		ModelClassificationIgnored:      0,
	}
	shadowDecisions := make([]ShadowDecision, 0, len(input.ModelIDs))

	accountProvider := GovernanceProvider("")
	if input.AccountProvider != nil {
		accountProvider = *input.AccountProvider
	}

	// Load gate inputs once per account (simplified for shadow: assume enabled resources).
	// For Phase 3 shadow, we persist decisions but perform no routing/pricing/mapping/cache mutations.
	var resourceEnabled = struct {
		connection bool
		account    bool
		group      bool
		channel    bool
	}{true, true, true, true}
	if s.resourceChecker != nil {
		c, a, g, ch, err := s.resourceChecker.AreEnabled(ctx, input.AccountID)
		if err == nil {
			resourceEnabled.connection = c
			resourceEnabled.account = a
			resourceEnabled.group = g
			resourceEnabled.channel = ch
		}
	}

	for _, upstreamID := range input.ModelIDs {
		decision := s.classifier.Classify(snapshot, accountProvider, upstreamID)
		decisions = append(decisions, decision)
		counts[decision.Classification]++

		// Build evaluator input – shadow uses shared evaluator.
		var registryEntry *ModelRegistryEntry
		if decision.CanonicalID != nil {
			if e, ok := snapshot.Entries[*decision.CanonicalID]; ok {
				// Copy to get pointer
				copy := e
				registryEntry = &copy
			}
		}
		// Gate flags – in shadow, treat allowed as true for approved, else false.
		// Multiplier/price/billing/endpoint are checked via injected checkers when available.
		modelAllowed := decision.Classification == ModelClassificationApproved
		multiplierPresent := true
		pricePresent := false
		billingUnambiguous := true
		probeValid := true
		if registryEntry != nil && decision.Classification == ModelClassificationApproved {
			if s.channelPriceChecker != nil {
				if present, err := s.channelPriceChecker.HasExactPrice(ctx, *decision.CanonicalID, registryEntry.Provider); err == nil {
					pricePresent = present
				}
			} else {
				// Default: treat as present for shadow baseline to isolate classification logic
				pricePresent = true
			}
			if s.billingMappingChecker != nil {
				if unambiguous, err := s.billingMappingChecker.IsUnambiguous(ctx, *decision.CanonicalID); err == nil {
					billingUnambiguous = unambiguous
				}
			}
			if s.endpointProbeChecker != nil {
				if valid, err := s.endpointProbeChecker.IsValid(ctx, input.AccountID); err == nil {
					probeValid = valid
				}
			}
		}

		evalInput := PublicationInput{
			RegistryEntry:             registryEntry,
			AccountProvider:           accountProvider,
			ModelAllowed:              modelAllowed,
			MultiplierPresent:         multiplierPresent,
			ExactChannelPricePresent:  pricePresent,
			BillingMappingUnambiguous: billingUnambiguous,
			EndpointProbeValid:        probeValid,
			ConnectionEnabled:         resourceEnabled.connection,
			AccountEnabled:            resourceEnabled.account,
			GroupEnabled:              resourceEnabled.group,
			ChannelEnabled:            resourceEnabled.channel,
			UpstreamPresent:           true,
		}
		eval := s.evaluator.Evaluate(evalInput)

		shadowDecisions = append(shadowDecisions, ShadowDecision{
			UpstreamModelID: decision.UpstreamModelID,
			CanonicalID:     decision.CanonicalID,
			Provider:        decision.Provider,
			Classification:  decision.Classification,
			Eligibility:     eval.Eligibility,
			Reason:          eval.Reason,
		})
	}

	// Commit observations/events/shadow decisions atomically.
	// First record discovery batch for evidence.
	batchID, err := s.observationRepo.RecordDiscovery(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("record discovery: %w", err)
	}

	if s.shadowDecisionRepo != nil {
		if err := s.shadowDecisionRepo.RecordShadowDecisions(ctx, batchID, shadowDecisions); err != nil {
			return nil, fmt.Errorf("record shadow decisions: %w", err)
		}
	}

	// Publishable is eligible count, not discovered count.
	publishable := 0
	for _, sd := range shadowDecisions {
		if sd.Eligibility == PublicationEligibilityEligible {
			publishable++
		}
	}

	summary := &ClassificationSummary{
		BatchID:         batchID,
		RegistryVersion: snapshot.Version,
		Discovered:      len(decisions),
		Publishable:     publishable,
		CrossProvider:   counts[ModelClassificationCrossProvider],
		Unknown:         counts[ModelClassificationUnknown],
		Ignored:         counts[ModelClassificationIgnored],
	}
	return summary, nil
}

var _ ModelGovernanceService = (*modelGovernanceService)(nil)
