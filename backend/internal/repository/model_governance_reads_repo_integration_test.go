//go:build integration

package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestModelGovernanceReadRepoIntegration_QuarantinePoolAndEvents(t *testing.T) {
	ctx := context.Background()
	repo := NewModelGovernanceReadRepository(integrationDB)

	run := fmt.Sprintf("r%d", time.Now().UnixNano())
	channelName := "gov-read-ch-" + run
	accountName := "gov-read-acc-" + run

	var channelID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx,
		`INSERT INTO channels (name, status) VALUES ($1, 'active') RETURNING id`, channelName).Scan(&channelID))
	defer func() { _, _ = integrationDB.ExecContext(ctx, `DELETE FROM channels WHERE id = $1`, channelID) }()
	var accountID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx,
		`INSERT INTO accounts (name, platform, type, status, schedulable) VALUES ($1, 'anthropic', 'apikey', 'active', true) RETURNING id`,
		accountName).Scan(&accountID))
	defer func() { _, _ = integrationDB.ExecContext(ctx, `DELETE FROM accounts WHERE id = $1`, accountID) }()

	var registryVersion int64 = 1
	require.NoError(t, integrationDB.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(registry_version),0) FROM model_registry_events`).Scan(&registryVersion))
	if registryVersion == 0 {
		require.NoError(t, integrationDB.QueryRowContext(ctx,
			`INSERT INTO model_registry_events (idempotency_key, event_type, registry_version, actor_id, payload)
			 VALUES ($1, 'created', 1, 'gov-read-test', '{}'::jsonb) RETURNING registry_version`,
			"gov-read-reg-"+run).Scan(&registryVersion))
	}
	var channelVersion int64
	require.NoError(t, integrationDB.QueryRowContext(ctx,
		`SELECT governance_version FROM channels WHERE id = $1`, channelID).Scan(&channelVersion))

	publication := NewModelPublicationRepository(integrationDB)
	batch := "gov-read-batch-" + run
	idemEligible := "gov-read-idem-e-" + run
	idemQuarantined := "gov-read-idem-q-" + run

	baseInput := service.RecomputeInput{
		BatchID: batch, RegistryVersion: registryVersion,
		ChannelVersions: map[int64]int64{channelID: channelVersion}, ActorID: "gov-read-test",
	}

	// Seed one eligible row.
	eligible := baseInput
	eligible.IdempotencyKey = idemEligible
	eligible.Items = []service.RecomputeItem{{
		AccountID: accountID, ChannelID: channelID, CanonicalModelID: "claude-opus-4-6",
		Eligibility: service.PublicationEligibilityEligible, Reason: "all gates passed",
		RegistryVersion: registryVersion, ChannelVersion: channelVersion,
	}}
	require.NoError(t, publication.RecomputeBatch(ctx, eligible))

	// Seed one quarantined row.
	quarantinedBatchID := "gov-read-qbatch-" + run
	quarantined := baseInput
	quarantined.IdempotencyKey = idemQuarantined
	quarantined.Items = []service.RecomputeItem{{
		AccountID: accountID, ChannelID: channelID, CanonicalModelID: "gpt-5.2",
		Eligibility: service.PublicationEligibilityQuarantined, Reason: "quarantined_by_admin",
		RegistryVersion: registryVersion, ChannelVersion: channelVersion,
		QuarantineBatchID: &quarantinedBatchID,
	}}
	require.NoError(t, publication.RecomputeBatch(ctx, quarantined))

	// Quarantine pool: only the quarantined identity appears, joined for display.
	pool, err := repo.ListQuarantinePool(ctx, 1, 200)
	require.NoError(t, err)
	var found *service.QuarantinePoolItem
	for i := range pool.Items {
		if pool.Items[i].CanonicalModelID == "gpt-5.2" && pool.Items[i].AccountID == accountID {
			found = &pool.Items[i]
			break
		}
	}
	require.NotNil(t, found, "quarantined row must appear in the pool")
	require.Equal(t, accountName, found.AccountName)
	require.Equal(t, channelName, found.ChannelName)
	require.Equal(t, "quarantined_by_admin", found.Reason)
	require.NotNil(t, found.QuarantineBatchID)
	require.Equal(t, quarantinedBatchID, *found.QuarantineBatchID)

	// Pagination is honored.
	page, err := repo.ListQuarantinePool(ctx, 1, 1)
	require.NoError(t, err)
	require.Len(t, page.Items, 1)
	require.GreaterOrEqual(t, page.Total, int64(1))

	// Publication events stream contains both seeded decisions; no payload key.
	events, err := repo.ListGovernanceEvents(ctx, service.GovernanceEventStreamPublication, 1, 200)
	require.NoError(t, err)
	sawQuarantineEvent := false
	for _, item := range events.Items {
		require.NotEmpty(t, item.EventType)
		require.Equal(t, service.GovernanceEventStreamPublication, item.Stream)
		if item.BatchID == batch && item.CanonicalModelID == "gpt-5.2" && item.Eligibility == service.PublicationEligibilityQuarantined {
			sawQuarantineEvent = true
			require.NotNil(t, item.AccountID)
			require.NotNil(t, item.ChannelID)
			require.NotNil(t, item.QuarantineBatchID)
			require.Equal(t, quarantinedBatchID, *item.QuarantineBatchID)
		}
	}
	require.True(t, sawQuarantineEvent, "publication event for the quarantined row must be listed")

	// Registry events stream surfaces registry history.
	registryEvents, err := repo.ListGovernanceEvents(ctx, service.GovernanceEventStreamRegistry, 1, 200)
	require.NoError(t, err)
	require.NotEmpty(t, registryEvents.Items)
	for _, item := range registryEvents.Items {
		require.Equal(t, service.GovernanceEventStreamRegistry, item.Stream)
	}
}
