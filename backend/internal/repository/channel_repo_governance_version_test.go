//go:build integration

package repository

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChannelRepo_VerifyGovernanceVersion(t *testing.T) {
	ctx := context.Background()
	// Create a channel
	var channelID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `INSERT INTO channels (name, status) VALUES ($1, 'active') RETURNING id`, "gov-version-test-"+t.Name()).Scan(&channelID))
	defer func() { _, _ = integrationDB.ExecContext(ctx, `DELETE FROM channels WHERE id = $1`, channelID) }()

	repo := &channelRepository{db: integrationDB}
	// Initially governance_version is 1
	var version int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT governance_version FROM channels WHERE id = $1`, channelID).Scan(&version))
	require.Equal(t, int64(1), version)
	require.NoError(t, repo.VerifyGovernanceVersion(ctx, channelID, 1))
	// Stale version should conflict
	err := repo.VerifyGovernanceVersion(ctx, channelID, 999)
	require.Error(t, err)
	require.Contains(t, err.Error(), "stale channel version")
	// After increment, old version should fail
	_, err = integrationDB.ExecContext(ctx, `UPDATE channels SET governance_version = governance_version + 1 WHERE id = $1`, channelID)
	require.NoError(t, err)
	require.Error(t, repo.VerifyGovernanceVersion(ctx, channelID, 1))
	require.NoError(t, repo.VerifyGovernanceVersion(ctx, channelID, 2))
}
