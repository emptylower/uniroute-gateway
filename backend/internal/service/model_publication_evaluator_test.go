package service

import "testing"

func publicationBaselineInput() PublicationInput {
	provider := GovernanceProviderAnthropic
	entry := &ModelRegistryEntry{
		CanonicalID: "claude-3-5-sonnet",
		Provider:    provider,
		Modality:    ModelModalityText,
		Status:      ModelLifecycleActive,
	}
	return PublicationInput{
		RegistryEntry:             entry,
		AccountProvider:           provider,
		ModelAllowed:              true,
		MultiplierPresent:         true,
		ExactChannelPricePresent:  true,
		BillingMappingUnambiguous: true,
		EndpointProbeValid:        true,
		ConnectionEnabled:         true,
		AccountEnabled:            true,
		GroupEnabled:              true,
		ChannelEnabled:            true,
		UpstreamPresent:           true,
	}
}

func TestPublicationEvaluator_OneValidBaselineTruthTable(t *testing.T) {
	evaluator := NewPublicationEvaluator()

	t.Run("all gates eligible", func(t *testing.T) {
		input := publicationBaselineInput()
		got := evaluator.Evaluate(input)
		if got.Eligibility != PublicationEligibilityEligible {
			t.Errorf("eligibility = %q want %q", got.Eligibility, PublicationEligibilityEligible)
		}
		if got.Reason != "eligible" {
			t.Errorf("reason = %q want %q", got.Reason, "eligible")
		}
	})

	t.Run("unknown identity quarantined", func(t *testing.T) {
		input := publicationBaselineInput()
		input.RegistryEntry = nil
		got := evaluator.Evaluate(input)
		if got.Eligibility != PublicationEligibilityQuarantined {
			t.Errorf("eligibility = %q want %q", got.Eligibility, PublicationEligibilityQuarantined)
		}
		if got.Reason != "unknown_identity" {
			t.Errorf("reason = %q want %q", got.Reason, "unknown_identity")
		}
	})

	t.Run("provider mismatch quarantined", func(t *testing.T) {
		input := publicationBaselineInput()
		input.AccountProvider = GovernanceProviderOpenAI
		got := evaluator.Evaluate(input)
		if got.Eligibility != PublicationEligibilityQuarantined {
			t.Errorf("eligibility = %q want %q", got.Eligibility, PublicationEligibilityQuarantined)
		}
		if got.Reason != "provider_mismatch" {
			t.Errorf("reason = %q want %q", got.Reason, "provider_mismatch")
		}
	})

	t.Run("retired terminal", func(t *testing.T) {
		input := publicationBaselineInput()
		input.RegistryEntry.Status = ModelLifecycleRetired
		got := evaluator.Evaluate(input)
		if got.Eligibility != PublicationEligibilityRetired {
			t.Errorf("eligibility = %q want %q", got.Eligibility, PublicationEligibilityRetired)
		}
		if got.Reason != "registry_retired" {
			t.Errorf("reason = %q want %q", got.Reason, "registry_retired")
		}
	})

	ordinaryBlocked := []struct {
		name       string
		mutate     func(*PublicationInput)
		wantReason string
	}{
		{name: "registry not active deprecated", mutate: func(in *PublicationInput) { in.RegistryEntry.Status = ModelLifecycleDeprecated }, wantReason: "registry_not_active"},
		{name: "unsupported modality image", mutate: func(in *PublicationInput) { in.RegistryEntry.Modality = ModelModalityImage }, wantReason: "unsupported_modality"},
		{name: "unsupported modality audio", mutate: func(in *PublicationInput) { in.RegistryEntry.Modality = ModelModalityAudio }, wantReason: "unsupported_modality"},
		{name: "model not allowed", mutate: func(in *PublicationInput) { in.ModelAllowed = false }, wantReason: "model_not_allowed"},
		{name: "multiplier missing", mutate: func(in *PublicationInput) { in.MultiplierPresent = false }, wantReason: "multiplier_missing"},
		{name: "price missing", mutate: func(in *PublicationInput) { in.ExactChannelPricePresent = false }, wantReason: "price_missing"},
		{name: "billing mapping ambiguous", mutate: func(in *PublicationInput) { in.BillingMappingUnambiguous = false }, wantReason: "billing_mapping_ambiguous"},
		{name: "endpoint probe missing", mutate: func(in *PublicationInput) { in.EndpointProbeValid = false }, wantReason: "endpoint_probe_missing"},
		{name: "connection disabled", mutate: func(in *PublicationInput) { in.ConnectionEnabled = false }, wantReason: "connection_disabled"},
		{name: "account disabled", mutate: func(in *PublicationInput) { in.AccountEnabled = false }, wantReason: "account_disabled"},
		{name: "group disabled", mutate: func(in *PublicationInput) { in.GroupEnabled = false }, wantReason: "group_disabled"},
		{name: "channel disabled", mutate: func(in *PublicationInput) { in.ChannelEnabled = false }, wantReason: "channel_disabled"},
	}

	for _, tc := range ordinaryBlocked {
		t.Run(tc.name, func(t *testing.T) {
			input := publicationBaselineInput()
			tc.mutate(&input)
			got := evaluator.Evaluate(input)
			if got.Eligibility != PublicationEligibilityBlocked {
				t.Errorf("eligibility = %q want %q for %s", got.Eligibility, PublicationEligibilityBlocked, tc.name)
			}
			if got.Reason != tc.wantReason {
				t.Errorf("reason = %q want %q for %s", got.Reason, tc.wantReason, tc.name)
			}
		})
	}

	t.Run("upstream missing blocked", func(t *testing.T) {
		input := publicationBaselineInput()
		input.UpstreamPresent = false
		got := evaluator.Evaluate(input)
		if got.Eligibility != PublicationEligibilityBlocked {
			t.Errorf("eligibility = %q want %q", got.Eligibility, PublicationEligibilityBlocked)
		}
		if got.Reason != "upstream_missing" {
			t.Errorf("reason = %q want %q", got.Reason, "upstream_missing")
		}
	})

	t.Run("price for another provider represented as absent", func(t *testing.T) {
		input := publicationBaselineInput()
		// Price exists but for another provider/model/channel/billing_mode => ExactChannelPricePresent false
		input.ExactChannelPricePresent = false
		got := evaluator.Evaluate(input)
		if got.Eligibility != PublicationEligibilityBlocked || got.Reason != "price_missing" {
			t.Errorf("price for another dimension should be blocked/price_missing, got %v/%v", got.Eligibility, got.Reason)
		}
	})
}

func TestPublicationEvaluator_Ordering(t *testing.T) {
	evaluator := NewPublicationEvaluator()

	// Unknown identity takes precedence over other failures
	t.Run("unknown beats price missing", func(t *testing.T) {
		input := publicationBaselineInput()
		input.RegistryEntry = nil
		input.ExactChannelPricePresent = false
		input.MultiplierPresent = false
		got := evaluator.Evaluate(input)
		if got.Reason != "unknown_identity" {
			t.Errorf("unknown should take precedence, got %q", got.Reason)
		}
	})

	// Provider mismatch takes precedence over retired
	t.Run("provider mismatch beats retired", func(t *testing.T) {
		input := publicationBaselineInput()
		input.AccountProvider = GovernanceProviderOpenAI
		input.RegistryEntry.Status = ModelLifecycleRetired
		got := evaluator.Evaluate(input)
		if got.Reason != "provider_mismatch" {
			t.Errorf("provider mismatch should beat retired, got %q", got.Reason)
		}
	})

	// Retired beats blocked
	t.Run("retired beats registry_not_active", func(t *testing.T) {
		input := publicationBaselineInput()
		// retired is terminal, even if other gates missing
		input.RegistryEntry.Status = ModelLifecycleRetired
		input.ModelAllowed = false
		got := evaluator.Evaluate(input)
		if got.Eligibility != PublicationEligibilityRetired {
			t.Errorf("retired should beat blocked, got %q", got.Eligibility)
		}
	})
}
