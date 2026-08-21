package service

import "strings"

// ModelClassifier resolves exact canonical IDs and reviewed aliases.
type ModelClassifier interface {
	Classify(snapshot *ModelRegistrySnapshot, accountProvider GovernanceProvider, upstreamID string) ClassificationDecision
}

type modelClassifier struct{}

// NewModelClassifier creates a pure classifier.
func NewModelClassifier() ModelClassifier {
	return &modelClassifier{}
}

func (c *modelClassifier) Classify(snapshot *ModelRegistrySnapshot, accountProvider GovernanceProvider, upstreamID string) ClassificationDecision {
	normalized := strings.TrimSpace(upstreamID)
	decision := ClassificationDecision{
		UpstreamModelID: upstreamID,
	}

	if normalized == "" {
		decision.Classification = ModelClassificationUnknown
		decision.Reason = "unknown_identity"
		return decision
	}

	if strings.Contains(normalized, "*") {
		decision.Classification = ModelClassificationIgnored
		decision.Reason = "wildcard"
		return decision
	}

	// Resolve canonical map first, alias map second. Exact, case-sensitive after trim.
	var entry *ModelRegistryEntry
	var canonical string
	if snapshot != nil {
		if e, ok := snapshot.Entries[normalized]; ok {
			// Copy to avoid aliasing loop variable
			copy := e
			entry = &copy
			canonical = e.CanonicalID
			if canonical == "" {
				canonical = normalized
			}
		} else if aliasTarget, ok := snapshot.Aliases[normalized]; ok {
			if e, ok := snapshot.Entries[aliasTarget]; ok {
				copy := e
				entry = &copy
				canonical = e.CanonicalID
				if canonical == "" {
					canonical = aliasTarget
				}
			}
		}
	}

	if entry == nil {
		decision.Classification = ModelClassificationUnknown
		decision.Reason = "unknown_identity"
		return decision
	}

	// Found entry – populate canonical/provider for observability even when non-publishable.
	canonicalCopy := canonical
	providerCopy := entry.Provider
	decision.CanonicalID = &canonicalCopy
	decision.Provider = &providerCopy

	// Modality is registry-owned, not substring-derived.
	if entry.Modality != ModelModalityText {
		decision.Classification = ModelClassificationIgnored
		decision.Reason = "unsupported_modality"
		return decision
	}

	// Lifecycle handling: retired is terminal, deprecated is not active.
	// Classifier still resolves identity; evaluator enforces terminal/blocked semantics.
	// We classify retired as ignored with explicit reason, deprecated as blocked-equivalent.
	if entry.Status == ModelLifecycleRetired {
		decision.Classification = ModelClassificationIgnored
		decision.Reason = "registry_retired"
		return decision
	}
	if entry.Status == ModelLifecycleDeprecated {
		decision.Classification = ModelClassificationIgnored
		decision.Reason = "registry_not_active"
		return decision
	}
	if entry.Status != ModelLifecycleActive {
		decision.Classification = ModelClassificationIgnored
		decision.Reason = "registry_not_active"
		return decision
	}

	// Active text entry: provider match determines approved vs cross_provider.
	if entry.Provider == accountProvider {
		decision.Classification = ModelClassificationApproved
		decision.Reason = "registry_approved"
		return decision
	}
	decision.Classification = ModelClassificationCrossProvider
	decision.Reason = "provider_mismatch"
	return decision
}

// Ensure compile-time interface compliance.
var _ ModelClassifier = (*modelClassifier)(nil)
