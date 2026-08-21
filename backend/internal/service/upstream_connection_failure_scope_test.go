package service

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUpstreamConnectionFailureScopeDecisionMatrix(t *testing.T) {
	scope := NewUpstreamConnectionFailureScope()
	// 404 model-local
	d := scope.Decide(404, "model not found", "gpt-4o", GovernanceProviderOpenAI)
	require.Equal(t, "model", d.Scope)
	require.NotNil(t, d.BlockModelID)
	// single 403 is model/account-local; with insufficient scope -> account
	d = scope.Decide(403, "insufficient_scope", "claude-3", GovernanceProviderAnthropic)
	require.Equal(t, "account", d.Scope)
	// ordinary 429 account-local
	d = scope.Decide(429, "rate limited", "model-x", GovernanceProviderGrok)
	require.Equal(t, "account", d.Scope)
	require.Nil(t, d.BlockModelID)
	// confirmed credential revocation -> connection-wide
	d = scope.Decide(401, "revoked token", "model", GovernanceProviderOpenAI)
	require.Equal(t, "connection", d.Scope)
	require.True(t, d.SuspendConnection)
	// balance exhaustion
	d = scope.Decide(401, "balance exhausted", "model", GovernanceProviderGemini)
	require.Equal(t, "connection", d.Scope)
	// upstream suspension
	d = scope.Decide(403, "account suspended", "model", GovernanceProviderAnthropic)
	require.Equal(t, "connection", d.Scope)
	// whole-upstream unreachable
	d = scope.Decide(500, "upstream unreachable", "model", GovernanceProviderOpenAI)
	require.Equal(t, "connection", d.Scope)
	// endpoint incompatibility -> account-local (simulate via 400 with endpoint)
	d = scope.Decide(400, "endpoint incompatibility", "model", GovernanceProviderOpenAI)
	require.Equal(t, "account", d.Scope)
	// 403 without insufficient -> model-local
	d = scope.Decide(403, "model not found", "gpt-4", GovernanceProviderOpenAI)
	require.Equal(t, "model", d.Scope)
}

func TestAggregatorDesignationExplicitAndVersion(t *testing.T) {
	repo := newFakeUpstreamConnRepo()
	anthropic := GovernanceProviderAnthropic
	conn, _ := NewUpstreamConnectionService(repo, fakeEncryptor{}).Create(nil, "first_party", &anthropic, "https://api.anthropic.com", "sk", nil)
	svc := NewAggregatorDesignationService(repo)
	// Missing actor
	err := svc.DesignateAggregator(nil, DesignateAggregatorInput{ConnectionID: conn.ID, ExpectedVersion: 1, ActorID: "", IdempotencyKey: "k1", EvidenceRef: "ev"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "actor")
	// Success
	err = svc.DesignateAggregator(nil, DesignateAggregatorInput{ConnectionID: conn.ID, ExpectedVersion: 1, ActorID: "admin1", IdempotencyKey: "k1", EvidenceRef: "ev1"})
	require.NoError(t, err)
	require.Equal(t, "aggregator", repo.store[conn.ID].Kind)
	require.Nil(t, repo.store[conn.ID].Provider)
	// Replay same idempotency should succeed without re-mutating (idempotent)
	err = svc.DesignateAggregator(nil, DesignateAggregatorInput{ConnectionID: conn.ID, ExpectedVersion: 1, ActorID: "admin1", IdempotencyKey: "k1", EvidenceRef: "ev1"})
	require.NoError(t, err)
	// Stale version conflict
	// Create another connection for stale test
	anthropic2 := GovernanceProviderAnthropic
	conn2, _ := NewUpstreamConnectionService(repo, fakeEncryptor{}).Create(nil, "first_party", &anthropic2, "https://api.anthropic.com", "sk2", nil)
	err = svc.DesignateAggregator(nil, DesignateAggregatorInput{ConnectionID: conn2.ID, ExpectedVersion: 99, ActorID: "admin1", IdempotencyKey: "k2", EvidenceRef: "ev"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "version conflict")
}
