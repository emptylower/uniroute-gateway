package repository

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/service"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
)

type modelPublicationRepository struct {
	db *sql.DB
}

func NewModelPublicationRepository(db *sql.DB) service.ModelAuthorizationStore {
	return &modelPublicationRepository{db: db}
}

func (r *modelPublicationRepository) Decision(ctx context.Context, key service.ModelAuthorizationKey) (service.ModelAuthorizationDecision, error) {
	if r == nil || r.db == nil {
		return service.ModelAuthorizationDecision{}, errors.New("model publication repository is not configured")
	}
	if key.AccountID <= 0 || key.ChannelID <= 0 || strings.TrimSpace(key.CanonicalModelID) == "" {
		return service.ModelAuthorizationDecision{}, fmt.Errorf("invalid publication key")
	}
	var dec service.ModelAuthorizationDecision
	err := r.db.QueryRowContext(ctx, `
SELECT eligibility, reason, registry_version, channel_version
FROM model_publication_eligibility
WHERE account_id = $1 AND channel_id = $2 AND canonical_model_id = $3
`, key.AccountID, key.ChannelID, key.CanonicalModelID).Scan(&dec.Eligibility, &dec.Reason, &dec.RegistryVersion, &dec.ChannelVersion)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Fail closed: unknown model => blocked/quarantined? Return not found as blocked.
			return service.ModelAuthorizationDecision{}, infraerrors.NotFound("MODEL_PUBLICATION_NOT_FOUND", "model publication not found")
		}
		return service.ModelAuthorizationDecision{}, err
	}
	return dec, nil
}

func (r *modelPublicationRepository) IsChannelModelEligible(ctx context.Context, channelID int64, canonicalModelID string) (bool, error) {
	if r == nil || r.db == nil {
		return false, errors.New("model publication repository is not configured")
	}
	if channelID <= 0 || strings.TrimSpace(canonicalModelID) == "" {
		return false, fmt.Errorf("invalid channel eligibility key")
	}
	var exists bool
	err := r.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM model_publication_eligibility WHERE channel_id = $1 AND canonical_model_id = $2 AND eligibility = 'eligible')`, channelID, canonicalModelID).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (r *modelPublicationRepository) IsCanonicalEligible(ctx context.Context, canonicalModelID string) (bool, error) {
	if r == nil || r.db == nil {
		return false, errors.New("model publication repository is not configured")
	}
	if strings.TrimSpace(canonicalModelID) == "" {
		return false, fmt.Errorf("canonical model id is required")
	}
	var exists bool
	err := r.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM model_publication_eligibility WHERE canonical_model_id = $1 AND eligibility = 'eligible')`, strings.ToLower(strings.TrimSpace(canonicalModelID))).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (r *modelPublicationRepository) RecomputeBatch(ctx context.Context, input service.RecomputeInput) (err error) {
	if r == nil || r.db == nil {
		return errors.New("model publication repository is not configured")
	}
	if strings.TrimSpace(input.BatchID) == "" || strings.TrimSpace(input.IdempotencyKey) == "" {
		return errors.New("batch and idempotency key are required")
	}
	if input.RegistryVersion <= 0 {
		return errors.New("registry version must be > 0")
	}
	if len(input.Items) == 0 {
		return errors.New("recompute items required")
	}

	// Compute deterministic request hash for idempotency deduplication.
	requestHash := recomputeRequestHash(input)

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// Idempotency check: governance_idempotency_records with FOR UPDATE.
	var existingHash string
	err = tx.QueryRowContext(ctx, `SELECT request_hash FROM governance_idempotency_records WHERE idempotency_key = $1 FOR UPDATE`, input.IdempotencyKey).Scan(&existingHash)
	if err == nil {
		if existingHash != requestHash {
			err = infraerrors.Conflict("GOVERNANCE_IDEMPOTENCY_CONFLICT", "idempotency key collision with different payload")
			return err
		}
		// Idempotent replay: already done, return without new writes.
		if err = tx.Commit(); err != nil {
			return err
		}
		return nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	// Registry version check: must be current max.
	var currentRegistryVersion int64
	err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(registry_version), 0) FROM model_registry_events`).Scan(&currentRegistryVersion)
	if err != nil {
		return err
	}
	if input.RegistryVersion != currentRegistryVersion && currentRegistryVersion != 0 {
		// Stale registry input cannot restore quarantine; fail.
		err = infraerrors.Conflict("STALE_REGISTRY_VERSION", fmt.Sprintf("stale registry version: expected %d got %d", input.RegistryVersion, currentRegistryVersion))
		return err
	}

	// Channel version checks.
	for channelID, expectedVersion := range input.ChannelVersions {
		var currentChannelVersion int64
		err = tx.QueryRowContext(ctx, `SELECT governance_version FROM channels WHERE id = $1 FOR UPDATE`, channelID).Scan(&currentChannelVersion)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				err = infraerrors.NotFound("CHANNEL_NOT_FOUND", fmt.Sprintf("channel %d not found", channelID))
				return err
			}
			return err
		}
		if expectedVersion != currentChannelVersion {
			err = infraerrors.Conflict("STALE_CHANNEL_VERSION", fmt.Sprintf("stale channel version for channel %d: expected %d got %d", channelID, expectedVersion, currentChannelVersion))
			return err
		}
	}
	// Also verify each item's channel version matches current.
	for _, it := range input.Items {
		// If batch ChannelVersions does not contain this channel, fetch current and compare.
		if _, ok := input.ChannelVersions[it.ChannelID]; !ok {
			var currentChannelVersion int64
			if qerr := tx.QueryRowContext(ctx, `SELECT governance_version FROM channels WHERE id = $1 FOR UPDATE`, it.ChannelID).Scan(&currentChannelVersion); qerr != nil {
				if errors.Is(qerr, sql.ErrNoRows) {
					err = infraerrors.NotFound("CHANNEL_NOT_FOUND", fmt.Sprintf("channel %d not found", it.ChannelID))
					return err
				}
				err = qerr
				return err
			}
			if it.ChannelVersion != currentChannelVersion {
				err = infraerrors.Conflict("STALE_CHANNEL_VERSION", fmt.Sprintf("stale channel version for channel %d", it.ChannelID))
				return err
			}
		}
		// Quarantine restoration guard: cannot restore a quarantined row with stale channel input.
		// Check existing eligibility; if quarantined and new is non-quarantined but versions stale already handled above,
		// but if caller tries to restore quarantine via stale version, the version check already blocks.
		// Additional guard: if existing is quarantined and incoming is eligible but quarantine_batch mismatch, require fresh recompute.
		var existingEligibility string
		var existingQuarantineBatch sql.NullString
		_ = tx.QueryRowContext(ctx, `SELECT eligibility, quarantine_batch_id FROM model_publication_eligibility WHERE account_id=$1 AND channel_id=$2 AND canonical_model_id=$3 FOR UPDATE`, it.AccountID, it.ChannelID, it.CanonicalModelID).Scan(&existingEligibility, &existingQuarantineBatch)
		if existingEligibility == service.PublicationEligibilityQuarantined && it.Eligibility != service.PublicationEligibilityQuarantined {
			// Restoration must have current versions: already checked. If quarantined batch still active, disallow stale restore.
			// The version check above guarantees freshness; additional quarantine_batch check ensures not restoring via replay.
			if existingQuarantineBatch.Valid && existingQuarantineBatch.String != "" && it.QuarantineBatchID != nil && *it.QuarantineBatchID != existingQuarantineBatch.String {
				// Mismatch implies stale restoration attempt.
				err = infraerrors.Conflict("QUARANTINE_STALE_RESTORE", "cannot restore quarantine with stale batch")
				return err
			}
		}
	}

	// Write projections + events atomically.
	for _, it := range input.Items {
		var quarantineBatchID sql.NullString
		if it.QuarantineBatchID != nil {
			quarantineBatchID = sql.NullString{String: *it.QuarantineBatchID, Valid: true}
		}
		_, err = tx.ExecContext(ctx, `
INSERT INTO model_publication_eligibility (account_id, canonical_model_id, channel_id, eligibility, reason, registry_version, channel_version, quarantine_batch_id, batch_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (account_id, canonical_model_id, channel_id)
DO UPDATE SET eligibility=EXCLUDED.eligibility, reason=EXCLUDED.reason, registry_version=EXCLUDED.registry_version, channel_version=EXCLUDED.channel_version, quarantine_batch_id=EXCLUDED.quarantine_batch_id, batch_id=EXCLUDED.batch_id, updated_at=NOW()
`, it.AccountID, it.CanonicalModelID, it.ChannelID, it.Eligibility, it.Reason, it.RegistryVersion, it.ChannelVersion, nullableString(quarantineBatchID), input.BatchID)
		if err != nil {
			return err
		}
		// Append-only event.
		eventIDempotencyKey := fmt.Sprintf("%s:%d:%d:%s", input.IdempotencyKey, it.AccountID, it.ChannelID, it.CanonicalModelID)
		payloadBytes, _ := json.Marshal(map[string]any{
			"batch_id": input.BatchID, "actor_id": input.ActorID, "registry_version": it.RegistryVersion, "channel_version": it.ChannelVersion,
		})
		_, err = tx.ExecContext(ctx, `
INSERT INTO model_publication_events (account_id, canonical_model_id, channel_id, eligibility, reason, registry_version, channel_version, quarantine_batch_id, batch_id, idempotency_key, event_type, actor_id, payload)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'recomputed',$11,$12::jsonb)
`, it.AccountID, it.CanonicalModelID, it.ChannelID, it.Eligibility, it.Reason, it.RegistryVersion, it.ChannelVersion, nullableString(quarantineBatchID), input.BatchID, eventIDempotencyKey, input.ActorID, string(payloadBytes))
		if err != nil {
			return err
		}
	}

	// Governance idempotency record for replay safety.
	channelVersionsJSON, _ := json.Marshal(input.ChannelVersions)
	_, err = tx.ExecContext(ctx, `
INSERT INTO governance_idempotency_records (idempotency_key, operation, request_hash, actor_id, registry_version, channel_versions)
VALUES ($1, 'recompute_batch', $2, $3, $4, $5::jsonb)
`, input.IdempotencyKey, requestHash, input.ActorID, input.RegistryVersion, string(channelVersionsJSON))
	if err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}
	// Cache invalidation is published only after commit by the service wrapper.
	return nil
}

func recomputeRequestHash(input service.RecomputeInput) string {
	type itemHash struct {
		AccountID        int64  `json:"account_id"`
		ChannelID        int64  `json:"channel_id"`
		CanonicalModelID string `json:"canonical_model_id"`
		Eligibility      string `json:"eligibility"`
		Reason           string `json:"reason"`
		RegistryVersion  int64  `json:"registry_version"`
		ChannelVersion   int64  `json:"channel_version"`
	}
	hashItems := make([]itemHash, len(input.Items))
	for i, it := range input.Items {
		hashItems[i] = itemHash{
			AccountID: it.AccountID, ChannelID: it.ChannelID, CanonicalModelID: it.CanonicalModelID,
			Eligibility: it.Eligibility, Reason: it.Reason, RegistryVersion: it.RegistryVersion, ChannelVersion: it.ChannelVersion,
		}
	}
	payload, _ := json.Marshal(map[string]any{
		"batch_id":         input.BatchID,
		"registry_version": input.RegistryVersion,
		"channel_versions": input.ChannelVersions,
		"items":            hashItems,
	})
	sum := sha256.Sum256(payload)
	return fmt.Sprintf("sha256:%x", sum)
}

func nullableString(ns sql.NullString) any {
	if ns.Valid {
		return ns.String
	}
	return nil
}
