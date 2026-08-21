package service

import (
	"fmt"
	"strings"
)

// Governed providers are exactly anthropic, openai, gemini, grok.
// Protocol/original provider separation: antigravity, composite, google, xai are rejected as original-provider IDs.
const (
	GovernanceProviderAnthropic GovernanceProvider = "anthropic"
	GovernanceProviderOpenAI    GovernanceProvider = "openai"
	GovernanceProviderGemini    GovernanceProvider = "gemini"
	GovernanceProviderGrok      GovernanceProvider = "grok"
)

// Classification values exactly as approved specification.
const (
	ModelClassificationDiscovered   = "discovered"
	ModelClassificationApproved     = "approved"
	ModelClassificationCrossProvider = "cross_provider"
	ModelClassificationUnknown      = "unknown"
	ModelClassificationIgnored      = "ignored"
)

// Publication eligibility values.
const (
	PublicationEligibilityEligible    = "eligible"
	PublicationEligibilityBlocked     = "blocked"
	PublicationEligibilityQuarantined = "quarantined"
	PublicationEligibilityRetired     = "retired"
)

// Modality values.
const (
	ModelModalityText      = "text"
	ModelModalityImage     = "image"
	ModelModalityAudio     = "audio"
	ModelModalityVideo     = "video"
	ModelModalityEmbedding = "embedding"
	ModelModalityOther     = "other"
)

// Lifecycle values.
const (
	ModelLifecycleActive     = "active"
	ModelLifecycleDeprecated = "deprecated"
	ModelLifecycleRetired    = "retired"
)

// ModelRegistryEntry is the sole source of production provider authorization.
// Status uses lifecycle values: active, deprecated, retired.
type ModelRegistryEntry struct {
	CanonicalID string
	Provider    GovernanceProvider
	Modality    string
	Status      string
	Version     int64
	Aliases     []string
	DecidedBy   string
	EvidenceRef *string
}

// ModelRegistrySnapshot is an immutable versioned projection.
type ModelRegistrySnapshot struct {
	Version int64
	Entries map[string]ModelRegistryEntry
	Aliases map[string]string
}

// ClassificationDecision is the deterministic ownership classification result.
type ClassificationDecision struct {
	UpstreamModelID string
	CanonicalID     *string
	Provider        *GovernanceProvider
	Classification  string
	Reason          string
}

// PublicationInput is the eight-condition gate input.
type PublicationInput struct {
	RegistryEntry             *ModelRegistryEntry
	AccountProvider           GovernanceProvider
	ModelAllowed              bool
	MultiplierPresent         bool
	ExactChannelPricePresent  bool
	BillingMappingUnambiguous bool
	EndpointProbeValid        bool
	ConnectionEnabled         bool
	AccountEnabled            bool
	GroupEnabled              bool
	ChannelEnabled            bool
	UpstreamPresent           bool
}

// ValidGovernanceProvider reports whether provider is one of the four governed providers.
func ValidGovernanceProvider(provider GovernanceProvider) bool {
	switch provider {
	case GovernanceProviderAnthropic, GovernanceProviderOpenAI, GovernanceProviderGemini, GovernanceProviderGrok:
		return true
	default:
		return false
	}
}

// ParseGovernanceProvider validates and returns a governed provider.
// Exact IDs are case-sensitive after trimming surrounding whitespace.
func ParseGovernanceProvider(raw string) (GovernanceProvider, error) {
	trimmed := strings.TrimSpace(raw)
	provider := GovernanceProvider(trimmed)
	if !ValidGovernanceProvider(provider) {
		return "", fmt.Errorf("invalid governance provider %q", raw)
	}
	return provider, nil
}

// ValidModelModality reports whether modality is allowed.
func ValidModelModality(modality string) bool {
	switch modality {
	case ModelModalityText, ModelModalityImage, ModelModalityAudio, ModelModalityVideo, ModelModalityEmbedding, ModelModalityOther:
		return true
	default:
		return false
	}
}

// ValidModelLifecycle reports whether lifecycle is allowed.
func ValidModelLifecycle(lifecycle string) bool {
	switch lifecycle {
	case ModelLifecycleActive, ModelLifecycleDeprecated, ModelLifecycleRetired:
		return true
	default:
		return false
	}
}

// ValidClassification reports whether classification is allowed.
func ValidClassification(classification string) bool {
	switch classification {
	case ModelClassificationDiscovered, ModelClassificationApproved, ModelClassificationCrossProvider, ModelClassificationUnknown, ModelClassificationIgnored:
		return true
	default:
		return false
	}
}

// ValidEligibility reports whether eligibility is allowed.
func ValidEligibility(eligibility string) bool {
	switch eligibility {
	case PublicationEligibilityEligible, PublicationEligibilityBlocked, PublicationEligibilityQuarantined, PublicationEligibilityRetired:
		return true
	default:
		return false
	}
}

// NormalizeModelID trims surrounding whitespace without changing case.
func NormalizeModelID(modelID string) string {
	return strings.TrimSpace(modelID)
}

// IsWildcardModelID reports whether modelID contains a wildcard character.
func IsWildcardModelID(modelID string) bool {
	return strings.Contains(modelID, "*")
}
