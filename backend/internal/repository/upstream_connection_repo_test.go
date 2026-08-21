package repository

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

func TestUpstreamConnectionRepoInterface(t *testing.T) {
	// Verify repo can be instantiated and methods handle nil client gracefully for unit test
	repo := NewUpstreamConnectionRepository(nil, nil)
	require.NotNil(t, repo)
	_, ok := repo.(service.UpstreamConnectionRepository)
	require.True(t, ok)
	// Create with nil client should error, not panic
	err := repo.Create(context.Background(), &service.UpstreamConnection{Kind: "first_party", BaseURL: "https://api.anthropic.com", CredentialVersion: 1, Status: "active"}, "enc:cred")
	require.Error(t, err)
}

func TestUpstreamConnectionRepoBatchGetEmpty(t *testing.T) {
	repo := NewUpstreamConnectionRepository(nil, nil)
	res, err := repo.BatchGetByIDs(context.Background(), nil)
	require.NoError(t, err)
	require.Empty(t, res)
}
