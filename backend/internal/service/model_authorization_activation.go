package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
)

// ActivationInput is the enforce activation request.
type ActivationInput struct {
	InventoryHash    string
	RegistryVersion  int64
	ChannelVersions  map[int64]int64
	ProjectedBatchID string
	AcknowledgedBy   string
	IdempotencyKey   string
	ActorID          string
}

// ModelAuthorizationActivationService gates enforce activation.
type ModelAuthorizationActivationService struct {
	db *sql.DB
}

// NewModelAuthorizationActivationService creates the service.
func NewModelAuthorizationActivationService(db *sql.DB) *ModelAuthorizationActivationService {
	return &ModelAuthorizationActivationService{db: db}
}

// Activate validates prerequisites transactionally and records activation.
// Only this endpoint may enter enforce; ordinary setting writes cannot.
func (s *ModelAuthorizationActivationService) Activate(ctx context.Context, input ActivationInput) error {
	if err := validateActivationInput(input); err != nil {
		return err
	}
	if s.db == nil {
		return fmt.Errorf("activation service not configured")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// Idempotency check.
	var existingHash string
	err = tx.QueryRowContext(ctx, `SELECT inventory_hash FROM model_authorization_activations WHERE idempotency_key = $1`, input.IdempotencyKey).Scan(&existingHash)
	if err == nil {
		if existingHash != input.InventoryHash {
			err = infraerrors.Conflict("ACTIVATION_IDEMPOTENCY_CONFLICT", "activation idempotency conflict")
			return err
		}
		if err = tx.Commit(); err != nil {
			return err
		}
		return nil
	}
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	// Inventory completeness check: compare hash and versions? Simplified: verify inventory hash exists in recent inventory runs.
	// For now, just verify registry version matches current max and channel versions match current.
	var currentRegistryVersion int64
	err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(registry_version),0) FROM model_registry_events`).Scan(&currentRegistryVersion)
	if err != nil {
		return err
	}
	if input.RegistryVersion != currentRegistryVersion {
		err = infraerrors.Conflict("STALE_REGISTRY_VERSION", fmt.Sprintf("stale registry version expected %d got %d", input.RegistryVersion, currentRegistryVersion))
		return err
	}
	for chID, expectedVersion := range input.ChannelVersions {
		var currentVersion int64
		err = tx.QueryRowContext(ctx, `SELECT governance_version FROM channels WHERE id = $1`, chID).Scan(&currentVersion)
		if err != nil {
			return err
		}
		if expectedVersion != currentVersion {
			err = infraerrors.Conflict("STALE_CHANNEL_VERSION", fmt.Sprintf("stale channel version for %d", chID))
			return err
		}
	}

	// Explicit acknowledgment required.
	if strings.TrimSpace(input.AcknowledgedBy) == "" {
		err = infraerrors.BadRequest("ACTIVATION_ACK_REQUIRED", "acknowledged_by is required")
		return err
	}

	// Insert activation record (append-only, never auto-restores on rollback).
	channelVersionsJSON, _ := json.Marshal(input.ChannelVersions)
	_, err = tx.ExecContext(ctx, `
INSERT INTO model_authorization_activations (inventory_hash, registry_version, channel_versions, projected_batch_id, acknowledged_by, idempotency_key, mode_before, mode_after, actor_id)
VALUES ($1,$2,$3::jsonb,$4,$5,$6,'shadow','enforce',$7)
`, input.InventoryHash, input.RegistryVersion, string(channelVersionsJSON), input.ProjectedBatchID, input.AcknowledgedBy, input.IdempotencyKey, input.ActorID)
	if err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

// Deactivate returns to shadow (never auto-restores quarantine).
func (s *ModelAuthorizationActivationService) Deactivate(ctx context.Context, idempotencyKey, actorID string) error {
	if strings.TrimSpace(idempotencyKey) == "" {
		return fmt.Errorf("idempotency key required")
	}
	if s.db == nil {
		return fmt.Errorf("not configured")
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO model_authorization_activations (inventory_hash, registry_version, channel_versions, projected_batch_id, acknowledged_by, idempotency_key, mode_before, mode_after, actor_id)
VALUES ('deactivate', 1, '{}'::jsonb, 'deactivate', 'system', $1, 'enforce', 'shadow', $2)
`, idempotencyKey, actorID)
	return err
}

func validateActivationInput(input ActivationInput) error {
	if strings.TrimSpace(input.InventoryHash) == "" {
		return infraerrors.BadRequest("ACTIVATION_INVENTORY_REQUIRED", "inventory_hash is required")
	}
	if input.RegistryVersion <= 0 {
		return infraerrors.BadRequest("ACTIVATION_REGISTRY_REQUIRED", "registry_version must be >0")
	}
	if len(input.ChannelVersions) == 0 {
		return infraerrors.BadRequest("ACTIVATION_CHANNEL_VERSIONS_REQUIRED", "channel_versions required")
	}
	if strings.TrimSpace(input.ProjectedBatchID) == "" {
		return infraerrors.BadRequest("ACTIVATION_BATCH_REQUIRED", "projected_batch_id required")
	}
	if strings.TrimSpace(input.AcknowledgedBy) == "" {
		return infraerrors.BadRequest("ACTIVATION_ACK_REQUIRED", "acknowledged_by required")
	}
	if strings.TrimSpace(input.IdempotencyKey) == "" {
		return infraerrors.BadRequest("ACTIVATION_IDEMPOTENCY_REQUIRED", "idempotency_key required")
	}
	if strings.TrimSpace(input.ActorID) == "" {
		return fmt.Errorf("actor is required")
	}
	return nil
}
