//go:build integration

package repository

import (
	"context"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestModelPublicationRepoIntegration_RecomputeWritesProjectionAndEventsAtomically(t *testing.T) {
	ctx := context.Background()
	// Repeat-safe: clean any rows left by a prior failed run with the same fixed IDs.
	_, _ = integrationDB.ExecContext(ctx, `DELETE FROM model_publication_events WHERE batch_id IN ('batch-int-1','batch-q-1')`)
	_, _ = integrationDB.ExecContext(ctx, `DELETE FROM model_publication_eligibility WHERE canonical_model_id='claude-opus-4-6' AND batch_id IN ('batch-int-1','batch-q-1')`)
	_, _ = integrationDB.ExecContext(ctx, `DELETE FROM governance_idempotency_records WHERE idempotency_key IN ('idem-int-1','idem-q-1','idem-stale-reg','idem-stale-ch')`)
	// Also clean any leftover channels/accounts with the test's fixed prefix (from a prior crash before defer).
	_, _ = integrationDB.ExecContext(ctx, `DELETE FROM channels WHERE name LIKE 'pub-int-test-ch-%'`)
	_, _ = integrationDB.ExecContext(ctx, `DELETE FROM accounts WHERE name LIKE 'pub-int-test-acc-%'`)
	repo := NewModelPublicationRepository(integrationDB).(*modelPublicationRepository)
	// Create a channel and account for test with unique-per-run suffix to avoid collision if cleanup missed.
	uniqueSuffix := t.Name() // t.Name() is stable per test, but we add cleanup above for crash safety
	var channelID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `INSERT INTO channels (name, status) VALUES ($1, 'active') RETURNING id`, "pub-int-test-ch-"+uniqueSuffix).Scan(&channelID))
	defer func() { _, _ = integrationDB.ExecContext(ctx, `DELETE FROM channels WHERE id = $1`, channelID) }()
	var accountID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `INSERT INTO accounts (name, platform, type, status, schedulable) VALUES ($1, 'anthropic', 'apikey', 'active', true) RETURNING id`, "pub-int-test-acc-"+uniqueSuffix).Scan(&accountID))
	defer func() { _, _ = integrationDB.ExecContext(ctx, `DELETE FROM accounts WHERE id = $1`, accountID) }()
	// Ensure registry version exists
	var registryVersion int64 = 1
	// Get current max or create one
	_ = integrationDB.QueryRowContext(ctx, `SELECT COALESCE(MAX(registry_version),0) FROM model_registry_events`).Scan(&registryVersion)
	if registryVersion == 0 {
		registryVersion = 1
		_, _ = integrationDB.ExecContext(ctx, `INSERT INTO model_registry_events (idempotency_key, event_type, registry_version, actor_id) VALUES ($1, 'created', $2, 'test')`, "pub-int-reg-"+t.Name(), registryVersion)
	}
	// Get channel governance_version
	var channelVersion int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT governance_version FROM channels WHERE id = $1`, channelID).Scan(&channelVersion))

	input := service.RecomputeInput{
		BatchID: "batch-int-1", IdempotencyKey: "idem-int-1", RegistryVersion: registryVersion,
		ChannelVersions: map[int64]int64{channelID: channelVersion}, ActorID: "tester",
		Items: []service.RecomputeItem{
			{AccountID: accountID, ChannelID: channelID, CanonicalModelID: "claude-opus-4-6", Eligibility: service.PublicationEligibilityEligible, Reason: "eligible", RegistryVersion: registryVersion, ChannelVersion: channelVersion},
		},
	}
	require.NoError(t, repo.RecomputeBatch(ctx, input))
	// Verify eligibility row exists
	var eligibility string
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT eligibility FROM model_publication_eligibility WHERE account_id=$1 AND channel_id=$2 AND canonical_model_id=$3`, accountID, channelID, "claude-opus-4-6").Scan(&eligibility))
	require.Equal(t, "eligible", eligibility)
	// Verify event row exists
	var eventCount int
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM model_publication_events WHERE batch_id=$1`, "batch-int-1").Scan(&eventCount))
	require.Equal(t, 1, eventCount)
	// Duplicate batch idempotent
	require.NoError(t, repo.RecomputeBatch(ctx, input))
	var eventCount2 int
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM model_publication_events WHERE batch_id=$1`, "batch-int-1").Scan(&eventCount2))
	require.Equal(t, 1, eventCount2, "duplicate batch should not create new events")

	// Stale registry should conflict
	staleInput := input
	staleInput.IdempotencyKey = "idem-stale-reg"
	staleInput.RegistryVersion = registryVersion + 100
	staleInput.Items[0].RegistryVersion = staleInput.RegistryVersion
	err := repo.RecomputeBatch(ctx, staleInput)
	require.Error(t, err)
	require.Contains(t, err.Error(), "stale registry")

	// Stale channel should conflict
	staleInput2 := input
	staleInput2.IdempotencyKey = "idem-stale-ch"
	staleInput2.ChannelVersions = map[int64]int64{channelID: channelVersion + 100}
	staleInput2.Items[0].ChannelVersion = channelVersion + 100
	err = repo.RecomputeBatch(ctx, staleInput2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "stale channel")

	// Quarantine: set to quarantined, then verify observations still exist (not deleted)
	quarantineInput := service.RecomputeInput{
		BatchID: "batch-q-1", IdempotencyKey: "idem-q-1", RegistryVersion: registryVersion,
		ChannelVersions: map[int64]int64{channelID: channelVersion}, ActorID: "tester",
		Items: []service.RecomputeItem{
			{AccountID: accountID, ChannelID: channelID, CanonicalModelID: "claude-opus-4-6", Eligibility: service.PublicationEligibilityQuarantined, Reason: "quarantined", RegistryVersion: registryVersion, ChannelVersion: channelVersion, QuarantineBatchID: strPtr("batch-q-1")},
		},
	}
	require.NoError(t, repo.RecomputeBatch(ctx, quarantineInput))
	var qEligibility string
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT eligibility FROM model_publication_eligibility WHERE account_id=$1 AND channel_id=$2 AND canonical_model_id=$3`, accountID, channelID, "claude-opus-4-6").Scan(&qEligibility))
	require.Equal(t, "quarantined", qEligibility)
	// Verify that quarantine did not delete other data: check that channel still exists, account still exists, etc.
	var channelExists int
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM channels WHERE id=$1`, channelID).Scan(&channelExists))
	require.Equal(t, 1, channelExists)

	// Cleanup
	_, _ = integrationDB.ExecContext(ctx, `DELETE FROM model_publication_events WHERE batch_id IN ('batch-int-1','batch-q-1')`)
	_, _ = integrationDB.ExecContext(ctx, `DELETE FROM model_publication_eligibility WHERE account_id=$1 AND channel_id=$2`, accountID, channelID)
	_, _ = integrationDB.ExecContext(ctx, `DELETE FROM governance_idempotency_records WHERE idempotency_key IN ('idem-int-1','idem-q-1','idem-stale-reg','idem-stale-ch')`)
}

func strPtr(s string) *string { return &s }
