package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestModelPublicationInputLoaderCombinesRealStateWithoutDeciding(t *testing.T) {
	// Use fake probe that returns valid
	now := time.Now()
	repo := &fakeProbeRepo{latest: &AccountEndpointProbe{AccountID: 1, Status: "success", ProbedAt: now, ExpiresAt: now.Add(24 * time.Hour)}}
	probeSvc := NewAccountEndpointProbeService(repo, nil)
	loader := NewModelPublicationInputLoader(probeSvc, nil)
	input, err := loader.Load(context.Background(), 1, 1, "claude-3-5-sonnet-latest")
	require.NoError(t, err)
	require.NotNil(t, input)
	require.True(t, input.EndpointProbeValid)
	require.Nil(t, input.RegistryEntry)
	require.Equal(t, GovernanceProviderAnthropic, input.AccountProvider)
}
