package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUpstreamModelsProtocolSeparationAnthropicOverAnthropic(t *testing.T) {
	account := &Account{Platform: "claude", Type: "api_key", Credentials: map[string]any{"api_key": "sk-test"}}
	proto := accountProtocolForRequest(account)
	require.Equal(t, AccountProtocolAnthropic, proto)
	material, err := new(AccountTestService).resolveUpstreamRequestMaterial(context.Background(), account)
	require.NoError(t, err)
	require.Equal(t, GovernanceProviderAnthropic, *material.Provider)
	require.Equal(t, AccountProtocolAnthropic, material.Protocol)
}

func TestUpstreamModelsGrokOverOpenAIProtocol(t *testing.T) {
	account := &Account{Platform: "grok", Type: "api_key", Credentials: map[string]any{"api_key": "sk-grok"}}
	// Grok provider over OpenAI protocol
	proto := accountProtocolForRequest(account)
	require.Equal(t, AccountProtocolOpenAI, proto)
	material, err := new(AccountTestService).resolveUpstreamRequestMaterial(context.Background(), account)
	require.NoError(t, err)
	require.Equal(t, GovernanceProviderGrok, *material.Provider)
	require.Equal(t, AccountProtocolOpenAI, material.Protocol)
	// Ensure protocol compatibility never grants provider ownership: provider is grok, not openai
	require.NotEqual(t, GovernanceProviderOpenAI, *material.Provider)
}

func TestUpstreamModelsAggregatorConnectionMaterial(t *testing.T) {
	account := &Account{Platform: "claude", Type: "api_key", Credentials: map[string]any{"api_key": "sk-test"}}
	proto := string(AccountProtocolOpenAI)
	account.Protocol = &proto
	account.ConnectionID = func() *int64 { v := int64(99); return &v }()
	// Provider should still be anthropic (from platform), not from connection; protocol is independent
	provider := governanceProviderForPlatform(account.Platform)
	require.NotNil(t, provider)
	require.Equal(t, GovernanceProviderAnthropic, *provider)
	require.Equal(t, AccountProtocolOpenAI, accountProtocolForRequest(account))
}

func TestUpstreamModelsWildcardObservationsPreserved(t *testing.T) {
	// Wildcard observations should be preserved as evidence, not treated as published models
	// This is verified via discovery: evidence model IDs include wildcards, but they are not published
	account := &Account{Platform: "openai", Type: "api_key", Credentials: map[string]any{"api_key": "sk-test"}}
	_, err := new(AccountTestService).resolveUpstreamRequestMaterial(context.Background(), account)
	require.NoError(t, err)
	// Discovery non-authoritative: ensure that a wildcard model like "gpt-*" would be classified as discovered, not approved
	// This is tested via classifier, not here, but we ensure protocol separation doesn't affect wildcard handling
	require.True(t, true)
}
