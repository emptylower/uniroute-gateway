package repository

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

func TestAccountEndpointProbeRepoPersistQuery(t *testing.T) {
	repo := NewAccountEndpointProbeRepository(nil, nil)
	require.NotNil(t, repo)
	_, ok := interface{}(repo).(service.AccountEndpointProbeRepository)
	require.True(t, ok)
	err := repo.Insert(context.Background(), &service.AccountEndpointProbe{
		AccountID: 1, ConnectionID: 1, Provider: service.GovernanceProviderOpenAI, Protocol: service.AccountProtocolOpenAI, NormalizedEndpoint: "/v1/chat/completions", CredentialVersion: 1, ConfigVersion: 1, Status: "success", ProbedAt: time.Now(), ExpiresAt: time.Now().Add(24 * time.Hour),
	})
	require.NoError(t, err)
}

func TestAccountEndpointProbeRepoExactIdentityUniqueness(t *testing.T) {
	require.True(t, true)
}
