package service

// PublicationEvaluation is the derived eligibility projection.
type PublicationEvaluation struct {
	Eligibility string
	Reason      string
}

// PublicationEvaluator computes the eight-condition gate in shadow and enforce modes.
type PublicationEvaluator interface {
	Evaluate(input PublicationInput) PublicationEvaluation
}

type publicationEvaluator struct{}

// NewPublicationEvaluator creates the single pure publication evaluator.
func NewPublicationEvaluator() PublicationEvaluator {
	return &publicationEvaluator{}
}

// Evaluate applies a stable ordered gate. Shadow mode persists its decision but performs no mutation;
// Phase 4 reuses the same evaluator.
func (e *publicationEvaluator) Evaluate(input PublicationInput) PublicationEvaluation {
	// Unknown identity => quarantined
	if input.RegistryEntry == nil {
		return PublicationEvaluation{Eligibility: PublicationEligibilityQuarantined, Reason: "unknown_identity"}
	}

	// Provider mismatch => quarantined
	if input.RegistryEntry.Provider != input.AccountProvider {
		return PublicationEvaluation{Eligibility: PublicationEligibilityQuarantined, Reason: "provider_mismatch"}
	}

	// Retired is terminal
	if input.RegistryEntry.Status == ModelLifecycleRetired {
		return PublicationEvaluation{Eligibility: PublicationEligibilityRetired, Reason: "registry_retired"}
	}

	// Registry must be active
	if input.RegistryEntry.Status != ModelLifecycleActive {
		return PublicationEvaluation{Eligibility: PublicationEligibilityBlocked, Reason: "registry_not_active"}
	}

	// Only text modality is publishable; registry is source of truth, not substring
	if input.RegistryEntry.Modality != ModelModalityText {
		return PublicationEvaluation{Eligibility: PublicationEligibilityBlocked, Reason: "unsupported_modality"}
	}

	// Resource enablement – fail closed
	if !input.ConnectionEnabled {
		return PublicationEvaluation{Eligibility: PublicationEligibilityBlocked, Reason: "connection_disabled"}
	}
	if !input.AccountEnabled {
		return PublicationEvaluation{Eligibility: PublicationEligibilityBlocked, Reason: "account_disabled"}
	}
	if !input.GroupEnabled {
		return PublicationEvaluation{Eligibility: PublicationEligibilityBlocked, Reason: "group_disabled"}
	}
	if !input.ChannelEnabled {
		return PublicationEvaluation{Eligibility: PublicationEligibilityBlocked, Reason: "channel_disabled"}
	}

	// Upstream presence – separate from commercial gates
	if !input.UpstreamPresent {
		return PublicationEvaluation{Eligibility: PublicationEligibilityBlocked, Reason: "upstream_missing"}
	}

	// Account model allowlist
	if !input.ModelAllowed {
		return PublicationEvaluation{Eligibility: PublicationEligibilityBlocked, Reason: "model_not_allowed"}
	}

	// Commercial gates
	if !input.MultiplierPresent {
		return PublicationEvaluation{Eligibility: PublicationEligibilityBlocked, Reason: "multiplier_missing"}
	}
	if !input.ExactChannelPricePresent {
		// A price for another provider/channel/model/billing mode is represented as absent.
		return PublicationEvaluation{Eligibility: PublicationEligibilityBlocked, Reason: "price_missing"}
	}
	if !input.BillingMappingUnambiguous {
		return PublicationEvaluation{Eligibility: PublicationEligibilityBlocked, Reason: "billing_mapping_ambiguous"}
	}
	if !input.EndpointProbeValid {
		return PublicationEvaluation{Eligibility: PublicationEligibilityBlocked, Reason: "endpoint_probe_missing"}
	}

	return PublicationEvaluation{Eligibility: PublicationEligibilityEligible, Reason: "eligible"}
}

var _ PublicationEvaluator = (*publicationEvaluator)(nil)
