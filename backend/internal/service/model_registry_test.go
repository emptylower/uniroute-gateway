package service

import "testing"

func TestModelRegistry_ParseGovernanceProvider_AcceptsGovernedProviders(t *testing.T) {
	valid := []string{"anthropic", "openai", "gemini", "grok"}
	for _, provider := range valid {
		if _, err := ParseGovernanceProvider(provider); err != nil {
			t.Errorf("ParseGovernanceProvider(%q) should succeed, got %v", provider, err)
		}
		// trimming surrounding whitespace should also succeed, case-sensitive
		trimmed := "  " + provider + "  "
		if _, err := ParseGovernanceProvider(trimmed); err != nil {
			t.Errorf("ParseGovernanceProvider(%q) should succeed after trim, got %v", trimmed, err)
		}
	}
}

func TestModelRegistry_ParseGovernanceProvider_RejectsNonGovernedProviders(t *testing.T) {
	// Note: "anthropic " with trim should actually succeed (case-sensitive after trim), so it must not be in rejected list.
	// Explicit rejected list per spec: antigravity, composite, google, xai plus case mismatches.
	rejected := []string{"antigravity", "composite", "google", "xai", "Anthropic", "OPENAI", "Gemini", "XAI", "azure-openai", "unknown", "text"}
	for _, provider := range rejected {
		if _, err := ParseGovernanceProvider(provider); err == nil {
			t.Errorf("ParseGovernanceProvider(%q) should be rejected", provider)
		}
	}
}

func TestModelRegistry_Constants(t *testing.T) {
	if !ValidGovernanceProvider(GovernanceProviderAnthropic) {
		t.Error("anthropic should be valid")
	}
	if ValidGovernanceProvider(GovernanceProvider("antigravity")) {
		t.Error("antigravity should not be valid governance provider")
	}
	if ValidGovernanceProvider(GovernanceProvider("composite")) {
		t.Error("composite should not be valid governance provider")
	}
	if ValidGovernanceProvider(GovernanceProvider("google")) {
		t.Error("google should not be valid governance provider")
	}
	if ValidGovernanceProvider(GovernanceProvider("xai")) {
		t.Error("xai should not be valid governance provider")
	}

	if !ValidClassification(ModelClassificationDiscovered) {
		t.Error("discovered should be valid classification")
	}
	if !ValidClassification(ModelClassificationApproved) {
		t.Error("approved should be valid classification")
	}
	if ValidClassification("invalid") {
		t.Error("invalid classification should be rejected")
	}

	if !ValidEligibility(PublicationEligibilityEligible) {
		t.Error("eligible should be valid")
	}
	if !ValidEligibility(PublicationEligibilityBlocked) {
		t.Error("blocked should be valid")
	}
	if !ValidEligibility(PublicationEligibilityQuarantined) {
		t.Error("quarantined should be valid")
	}
	if !ValidEligibility(PublicationEligibilityRetired) {
		t.Error("retired should be valid")
	}

	if !ValidModelModality(ModelModalityText) {
		t.Error("text should be valid modality")
	}
	if ValidModelModality("unknown_modality") {
		t.Error("unknown modality should be rejected")
	}

	if !ValidModelLifecycle(ModelLifecycleActive) {
		t.Error("active should be valid lifecycle")
	}
	if !ValidModelLifecycle(ModelLifecycleDeprecated) {
		t.Error("deprecated should be valid lifecycle")
	}
	if !ValidModelLifecycle(ModelLifecycleRetired) {
		t.Error("retired should be valid lifecycle")
	}
}

func TestModelRegistry_NormalizeModelID(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"  claude-3-5-sonnet  ", "claude-3-5-sonnet"},
		{"gpt-4", "gpt-4"},
		{"GPT-4", "GPT-4"}, // case-sensitive, no lowercasing
		{"  ", ""},
		{"", ""},
	}
	for _, tc := range tests {
		got := NormalizeModelID(tc.input)
		if got != tc.want {
			t.Errorf("NormalizeModelID(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestModelRegistry_Wildcard(t *testing.T) {
	if !IsWildcardModelID("gpt-*") {
		t.Error("gpt-* should be wildcard")
	}
	if !IsWildcardModelID("*") {
		t.Error("* should be wildcard")
	}
	if IsWildcardModelID("gpt-4") {
		t.Error("gpt-4 should not be wildcard")
	}
}
