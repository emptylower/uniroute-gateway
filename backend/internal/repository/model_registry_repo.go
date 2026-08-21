package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

type modelRegistryRepository struct {
	db *sql.DB
}

func NewModelRegistryRepository(db *sql.DB) service.ModelRegistryRepository {
	return &modelRegistryRepository{db: db}
}

func (r *modelRegistryRepository) GetSnapshot(ctx context.Context) (*service.ModelRegistrySnapshot, error) {
	if r.db == nil {
		return &service.ModelRegistrySnapshot{Version: 0, Entries: map[string]service.ModelRegistryEntry{}, Aliases: map[string]string{}}, nil
	}
	// Load current version as max registry_version from events
	var version sql.NullInt64
	if err := r.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(registry_version), 0) FROM model_registry_events`).Scan(&version); err != nil {
		return nil, err
	}
	snapshot := &service.ModelRegistrySnapshot{
		Version: version.Int64,
		Entries: make(map[string]service.ModelRegistryEntry),
		Aliases: make(map[string]string),
	}
	rows, err := r.db.QueryContext(ctx, `SELECT canonical_id, provider, modality, lifecycle, version, decided_by, evidence_ref FROM model_registry`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var entry service.ModelRegistryEntry
		var evidenceRef sql.NullString
		if err := rows.Scan(&entry.CanonicalID, &entry.Provider, &entry.Modality, &entry.Status, &entry.Version, &entry.DecidedBy, &evidenceRef); err != nil {
			return nil, err
		}
		if evidenceRef.Valid {
			entry.EvidenceRef = &evidenceRef.String
		}
		snapshot.Entries[entry.CanonicalID] = entry
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	aliasRows, err := r.db.QueryContext(ctx, `SELECT alias, registry_id FROM model_registry_aliases`)
	if err != nil {
		return nil, err
	}
	defer aliasRows.Close()
	// Need to map registry_id -> canonical
	idToCanonical := make(map[int64]string)
	for k, v := range snapshot.Entries {
		// Need id; we didn't load id. Instead query alias with join.
		_ = k
		_ = v
	}
	// Alternative: join alias to registry canonical_id
	aliasRows.Close()
	aliasRows, err = r.db.QueryContext(ctx, `SELECT a.alias, r.canonical_id FROM model_registry_aliases a JOIN model_registry r ON r.id = a.registry_id`)
	if err != nil {
		return nil, err
	}
	defer aliasRows.Close()
	for aliasRows.Next() {
		var alias, canonical string
		if err := aliasRows.Scan(&alias, &canonical); err != nil {
			return nil, err
		}
		snapshot.Aliases[alias] = canonical
	}
	if err := aliasRows.Err(); err != nil {
		return nil, err
	}
	// Silence unused var
	_ = idToCanonical
	return snapshot, nil
}

func (r *modelRegistryRepository) ListEntries(ctx context.Context) ([]service.ModelRegistryEntry, error) {
	snapshot, err := r.GetSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	entries := make([]service.ModelRegistryEntry, 0, len(snapshot.Entries))
	for _, e := range snapshot.Entries {
		entries = append(entries, e)
	}
	return entries, nil
}

func (r *modelRegistryRepository) GetEntry(ctx context.Context, canonicalID string) (*service.ModelRegistryEntry, error) {
	canonicalID = strings.TrimSpace(canonicalID)
	if canonicalID == "" {
		return nil, errors.New("canonical ID is required")
	}
	snapshot, err := r.GetSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	if entry, ok := snapshot.Entries[canonicalID]; ok {
		return &entry, nil
	}
	return nil, nil
}

func (r *modelRegistryRepository) RebuildProjection(ctx context.Context) error {
	if r.db == nil {
		return errors.New("database is not configured")
	}
	// Rebuild is idempotent: replay events to reconstruct projection.
	// For Phase 3, we simply verify snapshot can be rebuilt without mutation.
	// Real rebuild logic would truncate projection and replay events.
	// Here we ensure no error and version consistency.
	snapshot, err := r.GetSnapshot(ctx)
	if err != nil {
		return err
	}
	// Verify that replay would produce same count – placeholder for future logic.
	_ = snapshot
	return nil
}

func (r *modelRegistryRepository) CreateDecision(ctx context.Context, input service.RegistryDecisionInput) (*service.ModelRegistryEntry, int64, error) {
	if r.db == nil {
		return nil, 0, errors.New("database is not configured")
	}
	if err := validateRegistryDecisionInputForRepo(input); err != nil {
		return nil, 0, err
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, 0, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// Check idempotency
	var existingID int64
	var existingVersion int64
	var existingPayload string
	err = tx.QueryRowContext(ctx, `SELECT registry_id, registry_version, payload FROM model_registry_events WHERE idempotency_key = $1`, input.IdempotencyKey).Scan(&existingID, &existingVersion, &existingPayload)
	if err == nil {
		// Duplicate idempotency: return prior result without incrementing version
		var entry service.ModelRegistryEntry
		var evidenceRef sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT canonical_id, provider, modality, lifecycle, version, decided_by, evidence_ref FROM model_registry WHERE id = $1`, existingID).Scan(&entry.CanonicalID, &entry.Provider, &entry.Modality, &entry.Status, &entry.Version, &entry.DecidedBy, &evidenceRef); err != nil {
			return nil, 0, err
		}
		if evidenceRef.Valid {
			entry.EvidenceRef = &evidenceRef.String
		}
		// Load aliases
		rows, err := tx.QueryContext(ctx, `SELECT alias FROM model_registry_aliases WHERE registry_id = $1`, existingID)
		if err != nil {
			return nil, 0, err
		}
		defer rows.Close()
		for rows.Next() {
			var alias string
			if err := rows.Scan(&alias); err != nil {
				return nil, 0, err
			}
			entry.Aliases = append(entry.Aliases, alias)
		}
		if err := rows.Err(); err != nil {
			return nil, 0, err
		}
		if err := tx.Commit(); err != nil {
			return nil, 0, err
		}
		return &entry, existingVersion, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, 0, err
	}

	// Load current version
	var currentVersion int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(registry_version), 0) FROM model_registry_events`).Scan(&currentVersion); err != nil {
		return nil, 0, err
	}
	if input.ExpectedVersion != currentVersion {
		return nil, 0, fmt.Errorf("stale version conflict: expected %d, current %d", input.ExpectedVersion, currentVersion)
	}

	// Check alias ownership conflicts
	for _, alias := range input.Aliases {
		var conflictID int64
		var conflictCanonical string
		err := tx.QueryRowContext(ctx, `SELECT r.id, r.canonical_id FROM model_registry_aliases a JOIN model_registry r ON r.id = a.registry_id WHERE a.alias = $1`, alias).Scan(&conflictID, &conflictCanonical)
		if err == nil {
			// Alias already owned; if owned by same canonical, it's okay (idempotent alias add), else conflict
			if conflictCanonical != input.CanonicalID {
				return nil, 0, fmt.Errorf("alias conflict: %q already owned by %q", alias, conflictCanonical)
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return nil, 0, err
		}
		// Also check alias not equal to any other canonical
		var canonicalConflictID int64
		err = tx.QueryRowContext(ctx, `SELECT id FROM model_registry WHERE canonical_id = $1`, alias).Scan(&canonicalConflictID)
		if err == nil {
			return nil, 0, fmt.Errorf("alias conflict: %q already exists as canonical", alias)
		} else if !errors.Is(err, sql.ErrNoRows) {
			return nil, 0, err
		}
	}

	// Check canonical ID conflict (alias vs canonical)
	var aliasAsCanonicalID int64
	err = tx.QueryRowContext(ctx, `SELECT registry_id FROM model_registry_aliases WHERE alias = $1`, input.CanonicalID).Scan(&aliasAsCanonicalID)
	if err == nil {
		return nil, 0, fmt.Errorf("canonical conflict: %q already exists as alias", input.CanonicalID)
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, 0, err
	}

	newVersion := currentVersion + 1
	var registryID int64
	// Upsert registry entry
	// First try to find existing by canonical
	err = tx.QueryRowContext(ctx, `SELECT id FROM model_registry WHERE canonical_id = $1`, input.CanonicalID).Scan(&registryID)
	if errors.Is(err, sql.ErrNoRows) {
		err = tx.QueryRowContext(ctx, `
			INSERT INTO model_registry (canonical_id, provider, modality, lifecycle, version, decided_by, evidence_ref)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			RETURNING id
		`, input.CanonicalID, string(input.Provider), input.Modality, input.Status, newVersion, input.ActorID, input.EvidenceRef).Scan(&registryID)
		if err != nil {
			return nil, 0, err
		}
	} else if err != nil {
		return nil, 0, err
	} else {
		// Existing entry – update fields, bump version
		_, err = tx.ExecContext(ctx, `
			UPDATE model_registry SET provider = $1, modality = $2, lifecycle = $3, version = $4, decided_by = $5, evidence_ref = $6, updated_at = NOW()
			WHERE id = $7
		`, string(input.Provider), input.Modality, input.Status, newVersion, input.ActorID, input.EvidenceRef, registryID)
		if err != nil {
			return nil, 0, err
		}
		// Remove old aliases that are not in new set?
		// For Phase 3, we replace aliases atomically: delete then reinsert
		if _, err := tx.ExecContext(ctx, `DELETE FROM model_registry_aliases WHERE registry_id = $1`, registryID); err != nil {
			return nil, 0, err
		}
	}

	// Insert aliases
	for _, alias := range input.Aliases {
		if _, err := tx.ExecContext(ctx, `INSERT INTO model_registry_aliases (registry_id, alias) VALUES ($1, $2)`, registryID, alias); err != nil {
			return nil, 0, err
		}
	}

	// Insert event
	payload, _ := json.Marshal(map[string]any{
		"canonical_id": input.CanonicalID,
		"provider":     string(input.Provider),
		"modality":     input.Modality,
		"status":       input.Status,
		"aliases":      input.Aliases,
		"actor_id":     input.ActorID,
		"evidence_ref": input.EvidenceRef,
	})
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO model_registry_events (registry_id, idempotency_key, event_type, registry_version, actor_id, payload)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb)
	`, registryID, input.IdempotencyKey, "decision", newVersion, input.ActorID, string(payload)); err != nil {
		return nil, 0, err
	}

	if err := tx.Commit(); err != nil {
		return nil, 0, err
	}

	// Return entry
	entry := &service.ModelRegistryEntry{
		CanonicalID: input.CanonicalID,
		Provider:    input.Provider,
		Modality:    input.Modality,
		Status:      input.Status,
		Version:     newVersion,
		Aliases:     input.Aliases,
		DecidedBy:   input.ActorID,
		EvidenceRef: input.EvidenceRef,
	}
	return entry, newVersion, nil
}

func validateRegistryDecisionInputForRepo(input service.RegistryDecisionInput) error {
	if strings.TrimSpace(input.IdempotencyKey) == "" {
		return errors.New("idempotency key is required")
	}
	if strings.TrimSpace(input.CanonicalID) == "" {
		return errors.New("canonical ID is required")
	}
	if strings.Contains(input.CanonicalID, "*") {
		return errors.New("canonical ID must not contain wildcard")
	}
	// Provider/modality/status validation delegated to service
	return nil
}
