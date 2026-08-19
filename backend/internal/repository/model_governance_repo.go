package repository

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
)

type modelObservationRepository struct {
	db *sql.DB
}

type storedModelObservation struct {
	id             int64
	classification string
	reason         string
	presence       string
	missStreak     int
}

func NewModelObservationRepository(db *sql.DB) service.ModelObservationRepository {
	return &modelObservationRepository{db: db}
}

func (r *modelObservationRepository) RecordDiscovery(ctx context.Context, input service.DiscoveryBatchInput) (batchID string, err error) {
	if r == nil || r.db == nil {
		return "", errors.New("model observation repository is not configured")
	}
	if strings.TrimSpace(input.IdempotencyKey) == "" {
		return "", errors.New("discovery idempotency key is required")
	}
	if input.AccountID <= 0 {
		return "", errors.New("discovery account ID is required")
	}
	if input.ObservedAt.IsZero() {
		return "", errors.New("discovery observation time is required")
	}
	if input.AccountProvider != nil && !validGovernanceProvider(*input.AccountProvider) {
		return "", fmt.Errorf("invalid account provider %q", *input.AccountProvider)
	}
	var rawSnapshotObject map[string]json.RawMessage
	if err := json.Unmarshal(input.RawSnapshot, &rawSnapshotObject); err != nil || rawSnapshotObject == nil {
		return "", errors.New("discovery raw snapshot must be a JSON object")
	}
	persistedSnapshot, provenanceDigest, err := discoveryPersistenceEnvelope(input)
	if err != nil {
		return "", err
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	batchID = uuid.NewString()
	result, err := tx.ExecContext(ctx, `
		INSERT INTO model_classification_batches (
			batch_id, idempotency_key, account_id, connection_id, raw_snapshot, observed_at
		)
		VALUES ($1, $2, $3, $4, $5::jsonb, $6)
		ON CONFLICT (idempotency_key) DO NOTHING
	`, batchID, input.IdempotencyKey, input.AccountID, input.ConnectionID, string(persistedSnapshot), input.ObservedAt.UTC())
	if err != nil {
		return "", err
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return "", err
	}
	if inserted == 0 {
		var existingDigest string
		if err = tx.QueryRowContext(ctx, `
			SELECT batch_id, raw_snapshot->'provenance'->>'digest'
			FROM model_classification_batches WHERE idempotency_key = $1
		`, input.IdempotencyKey).Scan(&batchID, &existingDigest); err != nil {
			return "", err
		}
		if existingDigest != provenanceDigest {
			return "", errors.New("discovery idempotency key collision")
		}
		if err = tx.Commit(); err != nil {
			return "", err
		}
		return batchID, nil
	}
	var lockedAccountID int64
	if err = tx.QueryRowContext(ctx, `
		SELECT id FROM accounts WHERE id = $1 FOR UPDATE
	`, input.AccountID).Scan(&lockedAccountID); err != nil {
		return "", err
	}
	var projectionWatermark sql.NullTime
	if err = tx.QueryRowContext(ctx, `
		SELECT MAX(observed_at)
		FROM model_classification_batches
		WHERE account_id = $1 AND batch_id <> $2
	`, input.AccountID, batchID).Scan(&projectionWatermark); err != nil {
		return "", err
	}
	if projectionWatermark.Valid && input.ObservedAt.Before(projectionWatermark.Time) {
		if err = tx.Commit(); err != nil {
			return "", err
		}
		return batchID, nil
	}

	existing, err := lockModelObservations(ctx, tx, input.AccountID)
	if err != nil {
		return "", err
	}
	seen := make(map[string]struct{}, len(input.ModelIDs))
	for _, modelID := range input.ModelIDs {
		if _, duplicate := seen[modelID]; duplicate {
			continue
		}
		seen[modelID] = struct{}{}

		observation, exists := existing[modelID]
		if !exists {
			classification := "discovered"
			reason := "awaiting_registry_classification"
			if input.AccountProvider == nil {
				classification = "ignored"
				reason = "unsupported_routing_platform"
			}
			err = tx.QueryRowContext(ctx, `
				INSERT INTO model_observations (
					connection_id, account_id, upstream_model_id, classification,
					classification_reason, upstream_presence, miss_streak,
					first_seen_at, last_seen_at, raw_snapshot
				)
				VALUES ($1, $2, $3, $4, $5, 'present', 0, $6, $6, $7::jsonb)
				RETURNING id
			`, input.ConnectionID, input.AccountID, modelID, classification, reason, input.ObservedAt.UTC(), string(persistedSnapshot)).Scan(&observation.id)
			if err != nil {
				return "", err
			}
			observation.classification = classification
			observation.reason = reason
			observation.presence = "present"
			observation.missStreak = 0
			if err = appendObservationEvent(ctx, tx, observation, batchID, "discovered", input); err != nil {
				return "", err
			}
			continue
		}

		eventType := "observed"
		if observation.presence == "missing" {
			eventType = "reappeared"
		}
		observation.presence = "present"
		observation.missStreak = 0
		_, err = tx.ExecContext(ctx, `
			UPDATE model_observations
			SET connection_id = $1, upstream_presence = 'present', miss_streak = 0,
			    last_seen_at = GREATEST(last_seen_at, $2), raw_snapshot = $3::jsonb,
			    updated_at = NOW()
			WHERE id = $4
		`, input.ConnectionID, input.ObservedAt.UTC(), string(persistedSnapshot), observation.id)
		if err != nil {
			return "", err
		}
		if err = appendObservationEvent(ctx, tx, observation, batchID, eventType, input); err != nil {
			return "", err
		}
	}

	missingModelIDs := make([]string, 0, len(existing))
	for modelID := range existing {
		if _, present := seen[modelID]; present {
			continue
		}
		missingModelIDs = append(missingModelIDs, modelID)
	}
	sort.Strings(missingModelIDs)
	for _, modelID := range missingModelIDs {
		observation := existing[modelID]
		observation.presence = "missing"
		observation.missStreak++
		_, err = tx.ExecContext(ctx, `
			UPDATE model_observations
			SET upstream_presence = 'missing', miss_streak = $1, updated_at = NOW()
			WHERE id = $2
		`, observation.missStreak, observation.id)
		if err != nil {
			return "", err
		}
		if err = appendObservationEvent(ctx, tx, observation, batchID, "missing", input); err != nil {
			return "", err
		}
	}

	if err = tx.Commit(); err != nil {
		return "", err
	}
	return batchID, nil
}

func discoveryPersistenceEnvelope(input service.DiscoveryBatchInput) ([]byte, string, error) {
	modelIDs := dedupeModelIDsInOrder(input.ModelIDs)
	canonicalInput := struct {
		AccountID       int64                       `json:"account_id"`
		ConnectionID    *int64                      `json:"connection_id"`
		AccountProvider *service.GovernanceProvider `json:"account_provider"`
		RoutingPlatform string                      `json:"routing_platform"`
		ModelIDs        []string                    `json:"model_ids"`
		ObservedAt      string                      `json:"observed_at"`
		RawSnapshot     string                      `json:"raw_snapshot_base64"`
	}{
		AccountID:       input.AccountID,
		ConnectionID:    input.ConnectionID,
		AccountProvider: input.AccountProvider,
		RoutingPlatform: input.RoutingPlatform,
		ModelIDs:        modelIDs,
		ObservedAt:      input.ObservedAt.UTC().Format(time.RFC3339Nano),
		RawSnapshot:     base64.StdEncoding.EncodeToString(input.RawSnapshot),
	}
	canonicalJSON, err := json.Marshal(canonicalInput)
	if err != nil {
		return nil, "", fmt.Errorf("marshal discovery provenance: %w", err)
	}
	digest := fmt.Sprintf("sha256:%x", sha256.Sum256(canonicalJSON))

	var evidence map[string]json.RawMessage
	if err := json.Unmarshal(input.RawSnapshot, &evidence); err != nil {
		return nil, "", fmt.Errorf("decode discovery evidence: %w", err)
	}
	envelope, err := json.Marshal(map[string]any{
		"evidence": evidence,
		"provenance": map[string]any{
			"digest": digest,
		},
	})
	if err != nil {
		return nil, "", fmt.Errorf("marshal discovery persistence envelope: %w", err)
	}
	return envelope, digest, nil
}

func dedupeModelIDsInOrder(modelIDs []string) []string {
	seen := make(map[string]struct{}, len(modelIDs))
	result := make([]string, 0, len(modelIDs))
	for _, modelID := range modelIDs {
		if _, exists := seen[modelID]; exists {
			continue
		}
		seen[modelID] = struct{}{}
		result = append(result, modelID)
	}
	return result
}

func validGovernanceProvider(provider service.GovernanceProvider) bool {
	switch provider {
	case "anthropic", "openai", "gemini", "grok":
		return true
	default:
		return false
	}
}

func lockModelObservations(ctx context.Context, tx *sql.Tx, accountID int64) (map[string]storedModelObservation, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT id, upstream_model_id, classification, classification_reason,
		       upstream_presence, miss_streak
		FROM model_observations
		WHERE account_id = $1
		FOR UPDATE
	`, accountID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	result := make(map[string]storedModelObservation)
	for rows.Next() {
		var modelID string
		var observation storedModelObservation
		if err := rows.Scan(
			&observation.id,
			&modelID,
			&observation.classification,
			&observation.reason,
			&observation.presence,
			&observation.missStreak,
		); err != nil {
			return nil, err
		}
		result[modelID] = observation
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func appendObservationEvent(
	ctx context.Context,
	tx *sql.Tx,
	observation storedModelObservation,
	batchID string,
	eventType string,
	input service.DiscoveryBatchInput,
) error {
	payload, err := json.Marshal(map[string]any{
		"account_provider": input.AccountProvider,
		"routing_platform": input.RoutingPlatform,
		"observed_at":      input.ObservedAt.UTC(),
	})
	if err != nil {
		return fmt.Errorf("marshal observation event payload: %w", err)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO model_observation_events (
			observation_id, batch_id, event_type, classification,
			classification_reason, upstream_presence, miss_streak, payload
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
	`, observation.id, batchID, eventType, observation.classification,
		observation.reason, observation.presence, observation.missStreak, string(payload))
	return err
}
