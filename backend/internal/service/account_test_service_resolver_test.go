package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAccountTestServiceResolvesRequestMaterialFromResolver(t *testing.T) {
	// Grok provider over OpenAI protocol – protocol separation must be via resolver, not platform switch
	account := &Account{Platform: "grok", Type: "api_key", Credentials: map[string]any{"api_key": "sk-grok"}}
	// Without explicit protocol, resolver should map grok -> openai
	proto := ResolveAccountProtocol(account)
	require.Equal(t, AccountProtocolOpenAI, proto)
	provider := governanceProviderForPlatform(account.Platform)
	require.NotNil(t, provider)
	require.Equal(t, GovernanceProviderGrok, *provider)

	// With explicit protocol override, resolver must honor it
	protocolOpenAI := string(AccountProtocolOpenAI)
	account2 := &Account{Platform: "claude", Type: "api_key", Credentials: map[string]any{"api_key": "sk-claude"}, Protocol: &protocolOpenAI}
	proto2 := ResolveAccountProtocol(account2)
	require.Equal(t, AccountProtocolOpenAI, proto2)
	provider2 := governanceProviderForPlatform(account2.Platform)
	require.Equal(t, GovernanceProviderAnthropic, *provider2)

	// Resolve material via service method
	svc := &AccountTestService{}
	material, err := svc.resolveUpstreamRequestMaterial(context.Background(), account)
	require.NoError(t, err)
	require.Equal(t, GovernanceProviderGrok, *material.Provider)
	require.Equal(t, AccountProtocolOpenAI, material.Protocol)
}
