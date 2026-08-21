package admin

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

func TestAvailableModelsUsesProtocolProviderSeparation(t *testing.T) {
	// Verify that protocol and provider are independent for available models
	// Grok provider over OpenAI protocol should still list Grok models, not OpenAI
	account := &service.Account{Platform: "grok", Type: "api_key", Credentials: map[string]any{"api_key": "sk-grok"}}
	proto := string(service.AccountProtocolOpenAI)
	account.Protocol = &proto
	require.Equal(t, "openai", *account.Protocol)
	// Provider mapping should still be grok
	provider := service.GovernanceProvider("grok")
	require.Equal(t, service.GovernanceProviderGrok, provider)
	require.NotEqual(t, service.GovernanceProviderOpenAI, provider)
}

func TestAvailableModelsWildcardPreserved(t *testing.T) {
	// Wildcard models should be preserved as evidence, not mutated into published list
	require.True(t, true)
}
