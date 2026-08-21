//go:build integration

package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

var registryIntegrationMu sync.Mutex

func TestModelRegistry_CreateDecision_IncrementsVersionAndEvent(t *testing.T) {
	registryIntegrationMu.Lock()
	defer registryIntegrationMu.Unlock()
	ctx := context.Background()
	repo := NewModelRegistryRepository(integrationDB)
	snapshot, err := repo.GetSnapshot(ctx)
	require.NoError(t, err)
	baseVersion := snapshot.Version

	input := service.RegistryDecisionInput{
		IdempotencyKey:  fmt.Sprintf("test-increment-%d", time.Now().UnixNano()),
		ExpectedVersion: baseVersion,
		CanonicalID:     fmt.Sprintf("increment-model-%d", time.Now().UnixNano()),
		Provider:        service.GovernanceProviderAnthropic,
		Modality:        service.ModelModalityText,
		Status:          service.ModelLifecycleActive,
		Aliases:         []string{fmt.Sprintf("alias-%d", time.Now().UnixNano())},
		ActorID:         "tester",
	}
	entry, version, err := repo.CreateDecision(ctx, input)
	require.NoError(t, err)
	require.NotNil(t, entry)
	require.Equal(t, baseVersion+1, version)
	require.Equal(t, input.CanonicalID, entry.CanonicalID)
	require.Equal(t, int64(baseVersion+1), entry.Version)

	// Verify exactly one event
	var count int
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM model_registry_events WHERE idempotency_key = $1`, input.IdempotencyKey).Scan(&count))
	require.Equal(t, 1, count)

	// Verify snapshot version updated
	snapshot2, err := repo.GetSnapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, baseVersion+1, snapshot2.Version)
	require.Contains(t, snapshot2.Entries, input.CanonicalID)
	require.Equal(t, input.CanonicalID, snapshot2.Aliases[input.Aliases[0]])
}

func TestModelRegistry_CreateDecision_IdempotentReplayReturnsPriorWithoutNewVersion(t *testing.T) {
	registryIntegrationMu.Lock()
	defer registryIntegrationMu.Unlock()
	ctx := context.Background()
	repo := NewModelRegistryRepository(integrationDB)
	snapshot, err := repo.GetSnapshot(ctx)
	require.NoError(t, err)
	baseVersion := snapshot.Version

	idempotencyKey := fmt.Sprintf("test-idempotent-%d", time.Now().UnixNano())
	canonical := fmt.Sprintf("idempotent-model-%d", time.Now().UnixNano())
	input := service.RegistryDecisionInput{
		IdempotencyKey:  idempotencyKey,
		ExpectedVersion: baseVersion,
		CanonicalID:     canonical,
		Provider:        service.GovernanceProviderOpenAI,
		Modality:        service.ModelModalityText,
		Status:          service.ModelLifecycleActive,
		Aliases:         []string{fmt.Sprintf("idem-alias-%d", time.Now().UnixNano())},
		ActorID:         "tester",
	}

	entry1, version1, err := repo.CreateDecision(ctx, input)
	require.NoError(t, err)
	require.Equal(t, baseVersion+1, version1)

	// Replay with same key but different expected version (should still return prior, not conflict)
	input2 := input
	input2.ExpectedVersion = 9999 // stale, but idempotent should return prior without conflict
	entry2, version2, err := repo.CreateDecision(ctx, input2)
	require.NoError(t, err)
	require.Equal(t, entry1.CanonicalID, entry2.CanonicalID)
	require.Equal(t, version1, version2)

	// Verify no new event
	var eventCount int
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM model_registry_events WHERE idempotency_key = $1`, idempotencyKey).Scan(&eventCount))
	require.Equal(t, 1, eventCount)

	snapshot2, err := repo.GetSnapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, baseVersion+1, snapshot2.Version)
}

func TestModelRegistry_CreateDecision_StaleVersionConflict(t *testing.T) {
	registryIntegrationMu.Lock()
	defer registryIntegrationMu.Unlock()
	ctx := context.Background()
	repo := NewModelRegistryRepository(integrationDB)
	snapshot, err := repo.GetSnapshot(ctx)
	require.NoError(t, err)
	baseVersion := snapshot.Version

	input := service.RegistryDecisionInput{
		IdempotencyKey:  fmt.Sprintf("test-stale-%d", time.Now().UnixNano()),
		ExpectedVersion: baseVersion - 1, // stale
		CanonicalID:     fmt.Sprintf("stale-model-%d", time.Now().UnixNano()),
		Provider:        service.GovernanceProviderGemini,
		Modality:        service.ModelModalityText,
		Status:          service.ModelLifecycleActive,
		Aliases:         nil,
		ActorID:         "tester",
	}
	_, _, err = repo.CreateDecision(ctx, input)
	require.Error(t, err)
	require.Contains(t, err.Error(), "stale version")
}

func TestModelRegistry_CreateDecision_AliasConflicts(t *testing.T) {
	registryIntegrationMu.Lock()
	defer registryIntegrationMu.Unlock()
	ctx := context.Background()
	repo := NewModelRegistryRepository(integrationDB)
	snapshot, err := repo.GetSnapshot(ctx)
	require.NoError(t, err)
	baseVersion := snapshot.Version

	// Create first entry with alias
	canonicalA := fmt.Sprintf("alias-conflict-a-%d", time.Now().UnixNano())
	alias := fmt.Sprintf("shared-alias-%d", time.Now().UnixNano())
	inputA := service.RegistryDecisionInput{
		IdempotencyKey:  fmt.Sprintf("alias-a-%d", time.Now().UnixNano()),
		ExpectedVersion: baseVersion,
		CanonicalID:     canonicalA,
		Provider:        service.GovernanceProviderAnthropic,
		Modality:        service.ModelModalityText,
		Status:          service.ModelLifecycleActive,
		Aliases:         []string{alias},
		ActorID:         "tester",
	}
	_, _, err = repo.CreateDecision(ctx, inputA)
	require.NoError(t, err)

	snapshot, err = repo.GetSnapshot(ctx)
	require.NoError(t, err)

	// Alias owned by another canonical -> conflict
	canonicalB := fmt.Sprintf("alias-conflict-b-%d", time.Now().UnixNano())
	inputB := service.RegistryDecisionInput{
		IdempotencyKey:  fmt.Sprintf("alias-b-%d", time.Now().UnixNano()),
		ExpectedVersion: snapshot.Version,
		CanonicalID:     canonicalB,
		Provider:        service.GovernanceProviderOpenAI,
		Modality:        service.ModelModalityText,
		Status:          service.ModelLifecycleActive,
		Aliases:         []string{alias},
		ActorID:         "tester",
	}
	_, _, err = repo.CreateDecision(ctx, inputB)
	require.Error(t, err)
	require.Contains(t, err.Error(), "alias conflict")

	// Alias equal to existing canonical -> conflict
	snapshot, err = repo.GetSnapshot(ctx)
	require.NoError(t, err)
	inputC := service.RegistryDecisionInput{
		IdempotencyKey:  fmt.Sprintf("alias-c-%d", time.Now().UnixNano()),
		ExpectedVersion: snapshot.Version,
		CanonicalID:     fmt.Sprintf("new-canonical-%d", time.Now().UnixNano()),
		Provider:        service.GovernanceProviderAnthropic,
		Modality:        service.ModelModalityText,
		Status:          service.ModelLifecycleActive,
		Aliases:         []string{canonicalA},
		ActorID:         "tester",
	}
	_, _, err = repo.CreateDecision(ctx, inputC)
	require.Error(t, err)
	require.Contains(t, err.Error(), "alias conflict")

	// Canonical equal to existing alias -> conflict
	snapshot, err = repo.GetSnapshot(ctx)
	require.NoError(t, err)
	inputD := service.RegistryDecisionInput{
		IdempotencyKey:  fmt.Sprintf("alias-d-%d", time.Now().UnixNano()),
		ExpectedVersion: snapshot.Version,
		CanonicalID:     alias,
		Provider:        service.GovernanceProviderAnthropic,
		Modality:        service.ModelModalityText,
		Status:          service.ModelLifecycleActive,
		Aliases:         nil,
		ActorID:         "tester",
	}
	_, _, err = repo.CreateDecision(ctx, inputD)
	require.Error(t, err)
	require.Contains(t, err.Error(), "canonical conflict")
}

func TestModelRegistry_CreateDecision_UpdateReplacesAliasesAtomically(t *testing.T) {
	registryIntegrationMu.Lock()
	defer registryIntegrationMu.Unlock()
	ctx := context.Background()
	repo := NewModelRegistryRepository(integrationDB)
	snapshot, err := repo.GetSnapshot(ctx)
	require.NoError(t, err)
	baseVersion := snapshot.Version

	canonical := fmt.Sprintf("replace-alias-%d", time.Now().UnixNano())
	alias1 := fmt.Sprintf("replace-alias1-%d", time.Now().UnixNano())
	input1 := service.RegistryDecisionInput{
		IdempotencyKey:  fmt.Sprintf("replace-1-%d", time.Now().UnixNano()),
		ExpectedVersion: baseVersion,
		CanonicalID:     canonical,
		Provider:        service.GovernanceProviderAnthropic,
		Modality:        service.ModelModalityText,
		Status:          service.ModelLifecycleActive,
		Aliases:         []string{alias1},
		ActorID:         "tester",
	}
	_, _, err = repo.CreateDecision(ctx, input1)
	require.NoError(t, err)

	snapshot, err = repo.GetSnapshot(ctx)
	require.NoError(t, err)

	alias2 := fmt.Sprintf("replace-alias2-%d", time.Now().UnixNano())
	input2 := service.RegistryDecisionInput{
		IdempotencyKey:  fmt.Sprintf("replace-2-%d", time.Now().UnixNano()),
		ExpectedVersion: snapshot.Version,
		CanonicalID:     canonical,
		Provider:        service.GovernanceProviderAnthropic,
		Modality:        service.ModelModalityText,
		Status:          service.ModelLifecycleActive,
		Aliases:         []string{alias2},
		ActorID:         "tester",
	}
	_, _, err = repo.CreateDecision(ctx, input2)
	require.NoError(t, err)

	snapshot, err = repo.GetSnapshot(ctx)
	require.NoError(t, err)
	require.NotContains(t, snapshot.Aliases, alias1)
	require.Contains(t, snapshot.Aliases, alias2)
	require.Equal(t, canonical, snapshot.Aliases[alias2])
}

func TestModelRegistry_SeparateAuthorizedEvents(t *testing.T) {
	registryIntegrationMu.Lock()
	defer registryIntegrationMu.Unlock()
	ctx := context.Background()
	repo := NewModelRegistryRepository(integrationDB)
	snapshot, err := repo.GetSnapshot(ctx)
	require.NoError(t, err)

	canonical := fmt.Sprintf("authorized-events-%d", time.Now().UnixNano())
	// Initial active
	input := service.RegistryDecisionInput{
		IdempotencyKey:  fmt.Sprintf("auth-init-%d", time.Now().UnixNano()),
		ExpectedVersion: snapshot.Version,
		CanonicalID:     canonical,
		Provider:        service.GovernanceProviderOpenAI,
		Modality:        service.ModelModalityText,
		Status:          service.ModelLifecycleActive,
		Aliases:         []string{fmt.Sprintf("auth-alias-%d", time.Now().UnixNano())},
		ActorID:         "tester",
	}
	_, _, err = repo.CreateDecision(ctx, input)
	require.NoError(t, err)

	// Provider change
	snapshot, err = repo.GetSnapshot(ctx)
	require.NoError(t, err)
	input = service.RegistryDecisionInput{
		IdempotencyKey:  fmt.Sprintf("auth-provider-%d", time.Now().UnixNano()),
		ExpectedVersion: snapshot.Version,
		CanonicalID:     canonical,
		Provider:        service.GovernanceProviderGemini,
		Modality:        service.ModelModalityText,
		Status:          service.ModelLifecycleActive,
		Aliases:         []string{},
		ActorID:         "tester",
	}
	entry, _, err := repo.CreateDecision(ctx, input)
	require.NoError(t, err)
	require.Equal(t, service.GovernanceProviderGemini, entry.Provider)

	// Deprecation
	snapshot, err = repo.GetSnapshot(ctx)
	require.NoError(t, err)
	input = service.RegistryDecisionInput{
		IdempotencyKey:  fmt.Sprintf("auth-deprecate-%d", time.Now().UnixNano()),
		ExpectedVersion: snapshot.Version,
		CanonicalID:     canonical,
		Provider:        service.GovernanceProviderGemini,
		Modality:        service.ModelModalityText,
		Status:          service.ModelLifecycleDeprecated,
		Aliases:         []string{},
		ActorID:         "tester",
	}
	entry, _, err = repo.CreateDecision(ctx, input)
	require.NoError(t, err)
	require.Equal(t, service.ModelLifecycleDeprecated, entry.Status)

	// Retirement
	snapshot, err = repo.GetSnapshot(ctx)
	require.NoError(t, err)
	input = service.RegistryDecisionInput{
		IdempotencyKey:  fmt.Sprintf("auth-retire-%d", time.Now().UnixNano()),
		ExpectedVersion: snapshot.Version,
		CanonicalID:     canonical,
		Provider:        service.GovernanceProviderGemini,
		Modality:        service.ModelModalityText,
		Status:          service.ModelLifecycleRetired,
		Aliases:         []string{},
		ActorID:         "tester",
	}
	entry, _, err = repo.CreateDecision(ctx, input)
	require.NoError(t, err)
	require.Equal(t, service.ModelLifecycleRetired, entry.Status)

	// Tombstone/deletion via status deleted
	snapshot, err = repo.GetSnapshot(ctx)
	require.NoError(t, err)
	input = service.RegistryDecisionInput{
		IdempotencyKey:  fmt.Sprintf("auth-delete-%d", time.Now().UnixNano()),
		ExpectedVersion: snapshot.Version,
		CanonicalID:     canonical,
		Provider:        service.GovernanceProviderGemini,
		Modality:        service.ModelModalityText,
		Status:          "deleted",
		Aliases:         []string{},
		ActorID:         "tester",
	}
	// This bypasses service validation, so we test via direct DB event insertion and rebuild?
	// For Phase 3, we verify that a deleted status via direct event and rebuild removes projection.
	// Insert a tombstone event directly
	var registryID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT id FROM model_registry WHERE canonical_id = $1`, canonical).Scan(&registryID))
	payload, _ := json.Marshal(map[string]any{
		"canonical_id": canonical,
		"provider":     string(service.GovernanceProviderGemini),
		"modality":     service.ModelModalityText,
		"status":       "deleted",
		"aliases":      []string{},
		"actor_id":     "tester",
	})
	_, err = integrationDB.ExecContext(ctx, `INSERT INTO model_registry_events (registry_id, idempotency_key, event_type, registry_version, actor_id, payload) VALUES ($1, $2, $3, $4, $5, $6::jsonb)`, registryID, fmt.Sprintf("tombstone-%d", time.Now().UnixNano()), "decision", snapshot.Version+1, "tester", string(payload))
	require.NoError(t, err)
	require.NoError(t, repo.RebuildProjection(ctx))
	snapshot, err = repo.GetSnapshot(ctx)
	require.NoError(t, err)
	require.NotContains(t, snapshot.Entries, canonical)
}

func TestModelRegistry_RebuildRestoresProjection(t *testing.T) {
	registryIntegrationMu.Lock()
	defer registryIntegrationMu.Unlock()
	ctx := context.Background()
	repo := NewModelRegistryRepository(integrationDB)
	snapshot, err := repo.GetSnapshot(ctx)
	require.NoError(t, err)
	baseVersion := snapshot.Version

	entries := make([]service.RegistryDecisionInput, 3)
	for i := 0; i < 3; i++ {
		entries[i] = service.RegistryDecisionInput{
			IdempotencyKey:  fmt.Sprintf("rebuild-%d-%d", i, time.Now().UnixNano()),
			ExpectedVersion: baseVersion + int64(i),
			CanonicalID:     fmt.Sprintf("rebuild-model-%d-%d", i, time.Now().UnixNano()),
			Provider:        service.GovernanceProviderAnthropic,
			Modality:        service.ModelModalityText,
			Status:          service.ModelLifecycleActive,
			Aliases:         []string{fmt.Sprintf("rebuild-alias-%d-%d", i, time.Now().UnixNano())},
			ActorID:         "tester",
		}
		_, _, err := repo.CreateDecision(ctx, entries[i])
		require.NoError(t, err)
	}

	snapshotBefore, err := repo.GetSnapshot(ctx)
	require.NoError(t, err)

	// Corrupt projection: delete one registry entry and one alias directly
	var toDelete string
	for k := range snapshotBefore.Entries {
		toDelete = k
		break
	}
	_, err = integrationDB.ExecContext(ctx, `DELETE FROM model_registry WHERE canonical_id = $1`, toDelete)
	require.NoError(t, err)
	// Snapshot now missing one entry
	snapshotCorrupted, err := repo.GetSnapshot(ctx)
	require.NoError(t, err)
	require.NotContains(t, snapshotCorrupted.Entries, toDelete)

	// Rebuild
	require.NoError(t, repo.RebuildProjection(ctx))

	snapshotAfter, err := repo.GetSnapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, snapshotBefore.Version, snapshotAfter.Version)
	require.Equal(t, len(snapshotBefore.Entries), len(snapshotAfter.Entries))
	for k, v := range snapshotBefore.Entries {
		av, ok := snapshotAfter.Entries[k]
		require.True(t, ok)
		require.Equal(t, v.CanonicalID, av.CanonicalID)
		require.Equal(t, v.Provider, av.Provider)
		require.Equal(t, v.Modality, av.Modality)
		require.Equal(t, v.Status, av.Status)
	}
	for alias, canonical := range snapshotBefore.Aliases {
		require.Equal(t, canonical, snapshotAfter.Aliases[alias])
	}
}
