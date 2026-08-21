package service

import "testing"

func TestModelClassifier_TruthTable(t *testing.T) {
	anthropic := GovernanceProviderAnthropic
	openai := GovernanceProviderOpenAI

	// Snapshot with mixed entries.
	providerAnthropic := GovernanceProviderAnthropic
	providerOpenAI := GovernanceProviderOpenAI
	snapshot := &ModelRegistrySnapshot{
		Version: 42,
		Entries: map[string]ModelRegistryEntry{
			"claude-3-5-sonnet": {
				CanonicalID: "claude-3-5-sonnet",
				Provider:    providerAnthropic,
				Modality:    ModelModalityText,
				Status:      ModelLifecycleActive,
			},
			"gpt-4o": {
				CanonicalID: "gpt-4o",
				Provider:    providerOpenAI,
				Modality:    ModelModalityText,
				Status:      ModelLifecycleActive,
			},
			"dall-e-3": {
				CanonicalID: "dall-e-3",
				Provider:    providerOpenAI,
				Modality:    ModelModalityImage,
				Status:      ModelLifecycleActive,
			},
			"claude-legacy": {
				CanonicalID: "claude-legacy",
				Provider:    providerAnthropic,
				Modality:    ModelModalityText,
				Status:      ModelLifecycleDeprecated,
			},
			"claude-retired": {
				CanonicalID: "claude-retired",
				Provider:    providerAnthropic,
				Modality:    ModelModalityText,
				Status:      ModelLifecycleRetired,
			},
		},
		Aliases: map[string]string{
			"claude-sonnet-alias": "claude-3-5-sonnet",
			"gpt-4o-alias":        "gpt-4o",
		},
	}

	classifier := NewModelClassifier()

	tests := []struct {
		name              string
		accountProvider   GovernanceProvider
		upstreamID        string
		wantClassification string
		wantReason        string
		wantCanonical     *string
		wantProvider      *GovernanceProvider
	}{
		{
			name:              "exact canonical match approved",
			accountProvider:   anthropic,
			upstreamID:        "claude-3-5-sonnet",
			wantClassification: ModelClassificationApproved,
			wantReason:        "registry_approved",
			wantCanonical:     modelClassifierStrPtr("claude-3-5-sonnet"),
			wantProvider:      &providerAnthropic,
		},
		{
			name:              "reviewed alias approved",
			accountProvider:   anthropic,
			upstreamID:        "claude-sonnet-alias",
			wantClassification: ModelClassificationApproved,
			wantReason:        "registry_approved",
			wantCanonical:     modelClassifierStrPtr("claude-3-5-sonnet"),
			wantProvider:      &providerAnthropic,
		},
		{
			name:              "cross-provider via canonical",
			accountProvider:   anthropic,
			upstreamID:        "gpt-4o",
			wantClassification: ModelClassificationCrossProvider,
			wantReason:        "provider_mismatch",
			wantCanonical:     modelClassifierStrPtr("gpt-4o"),
			wantProvider:      &providerOpenAI,
		},
		{
			name:              "cross-provider via alias",
			accountProvider:   anthropic,
			upstreamID:        "gpt-4o-alias",
			wantClassification: ModelClassificationCrossProvider,
			wantReason:        "provider_mismatch",
			wantCanonical:     modelClassifierStrPtr("gpt-4o"),
			wantProvider:      &providerOpenAI,
		},
		{
			name:              "unknown unresolved",
			accountProvider:   anthropic,
			upstreamID:        "unknown-model-xyz",
			wantClassification: ModelClassificationUnknown,
			wantReason:        "unknown_identity",
			wantCanonical:     nil,
			wantProvider:      nil,
		},
		{
			name:              "wildcard ignored",
			accountProvider:   anthropic,
			upstreamID:        "gpt-*",
			wantClassification: ModelClassificationIgnored,
			wantReason:        "wildcard",
			wantCanonical:     nil,
			wantProvider:      nil,
		},
		{
			name:              "wildcard exact star ignored",
			accountProvider:   openai,
			upstreamID:        "*",
			wantClassification: ModelClassificationIgnored,
			wantReason:        "wildcard",
			wantCanonical:     nil,
			wantProvider:      nil,
		},
		{
			name:              "unsupported modality image ignored",
			accountProvider:   openai,
			upstreamID:        "dall-e-3",
			wantClassification: ModelClassificationIgnored,
			wantReason:        "unsupported_modality",
			wantCanonical:     modelClassifierStrPtr("dall-e-3"),
			wantProvider:      &providerOpenAI,
		},
		{
			name:              "deprecated not approved",
			accountProvider:   anthropic,
			upstreamID:        "claude-legacy",
			wantClassification: ModelClassificationIgnored,
			wantReason:        "registry_not_active",
			wantCanonical:     modelClassifierStrPtr("claude-legacy"),
			wantProvider:      &providerAnthropic,
		},
		{
			name:              "retired identified but ignored",
			accountProvider:   anthropic,
			upstreamID:        "claude-retired",
			wantClassification: ModelClassificationIgnored,
			wantReason:        "registry_retired",
			wantCanonical:     modelClassifierStrPtr("claude-retired"),
			wantProvider:      &providerAnthropic,
		},
		{
			name:              "canonical precedence over alias",
			accountProvider:   anthropic,
			upstreamID:        "claude-3-5-sonnet", // exists as canonical and also as alias target
			wantClassification: ModelClassificationApproved,
			wantReason:        "registry_approved",
			wantCanonical:     modelClassifierStrPtr("claude-3-5-sonnet"),
			wantProvider:      &providerAnthropic,
		},
		{
			name:              "trimming surrounding whitespace",
			accountProvider:   anthropic,
			upstreamID:        "  claude-3-5-sonnet  ",
			wantClassification: ModelClassificationApproved,
			wantReason:        "registry_approved",
			wantCanonical:     modelClassifierStrPtr("claude-3-5-sonnet"),
			wantProvider:      &providerAnthropic,
		},
		{
			name:              "case-sensitive after trim",
			accountProvider:   anthropic,
			upstreamID:        "Claude-3-5-Sonnet",
			wantClassification: ModelClassificationUnknown,
			wantReason:        "unknown_identity",
			wantCanonical:     nil,
			wantProvider:      nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			decision := classifier.Classify(snapshot, tc.accountProvider, tc.upstreamID)
			if decision.Classification != tc.wantClassification {
				t.Errorf("classification = %q, want %q", decision.Classification, tc.wantClassification)
			}
			if decision.Reason != tc.wantReason {
				t.Errorf("reason = %q, want %q", decision.Reason, tc.wantReason)
			}
			if (decision.CanonicalID == nil) != (tc.wantCanonical == nil) {
				t.Errorf("canonical nil mismatch: got %v want %v", decision.CanonicalID, tc.wantCanonical)
			} else if decision.CanonicalID != nil && *decision.CanonicalID != *tc.wantCanonical {
				t.Errorf("canonical = %q, want %q", *decision.CanonicalID, *tc.wantCanonical)
			}
			if (decision.Provider == nil) != (tc.wantProvider == nil) {
				t.Errorf("provider nil mismatch: got %v want %v", decision.Provider, tc.wantProvider)
			} else if decision.Provider != nil && *decision.Provider != *tc.wantProvider {
				t.Errorf("provider = %q, want %q", *decision.Provider, *tc.wantProvider)
			}
			if decision.UpstreamModelID != tc.upstreamID {
				t.Errorf("UpstreamModelID = %q, want %q", decision.UpstreamModelID, tc.upstreamID)
			}
		})
	}
}

func TestModelClassifier_SubstringsDoNotDetermineModalityOrProvider(t *testing.T) {
	// Registry says text, even though ID contains image/audio substrings, classification must be based on registry modality.
	provider := GovernanceProviderOpenAI
	snapshot := &ModelRegistrySnapshot{
		Version: 1,
		Entries: map[string]ModelRegistryEntry{
			"gpt-image-audio-video-trick": {
				CanonicalID: "gpt-image-audio-video-trick",
				Provider:    provider,
				Modality:    ModelModalityText,
				Status:      ModelLifecycleActive,
			},
			"claude-audio-hidden": {
				CanonicalID: "claude-audio-hidden",
				Provider:    GovernanceProviderAnthropic,
				Modality:    ModelModalityText,
				Status:      ModelLifecycleActive,
			},
		},
		Aliases: map[string]string{},
	}
	classifier := NewModelClassifier()

	// Even though model ID contains "image", registry modality is text => approved if provider matches
	decision := classifier.Classify(snapshot, GovernanceProviderOpenAI, "gpt-image-audio-video-trick")
	if decision.Classification != ModelClassificationApproved {
		t.Errorf("substring 'image' should not make modality image, got classification %q", decision.Classification)
	}
	// Cross-provider check is registry provider vs account, not substring
	decision = classifier.Classify(snapshot, GovernanceProviderAnthropic, "gpt-image-audio-video-trick")
	if decision.Classification != ModelClassificationCrossProvider {
		t.Errorf("provider should be from registry, not substring, got %q", decision.Classification)
	}

	decision = classifier.Classify(snapshot, GovernanceProviderAnthropic, "claude-audio-hidden")
	if decision.Classification != ModelClassificationApproved {
		t.Errorf("audio substring should not make modality audio, got %q", decision.Classification)
	}
}

func modelClassifierStrPtr(s string) *string { return &s }
