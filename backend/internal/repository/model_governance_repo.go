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
	id                 int64
	modelID            string
	connectionID       sql.NullInt64
	resolvedRegistryID sql.NullInt64
	classification     string
	reason             string
	presence           string
	missStreak         int
	firstSeenAt        time.Time
	lastSeenAt         time.Time
	rawSnapshot        json.RawMessage
}

type discoveryProjectionWinner struct {
	batchID    string
	digest     string
	observedAt time.Time
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
	var lockedAccountID int64
	if err = tx.QueryRowContext(ctx, `
		SELECT id FROM accounts WHERE id = $1 FOR UPDATE
	`, input.AccountID).Scan(&lockedAccountID); err != nil {
		return "", err
	}
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
	var currentWinner discoveryProjectionWinner
	err = tx.QueryRowContext(ctx, `
		SELECT b.batch_id, b.observed_at, COALESCE(b.raw_snapshot->'provenance'->>'digest', '')
		FROM model_classification_batches b
		WHERE b.account_id = $1 AND b.batch_id <> $2
		ORDER BY b.observed_at DESC, COALESCE(b.raw_snapshot->'provenance'->>'digest', '') DESC,
		         EXISTS (SELECT 1 FROM model_observation_events e WHERE e.batch_id = b.batch_id) DESC,
		         b.id ASC
		LIMIT 1
	`, input.AccountID, batchID).Scan(&currentWinner.batchID, &currentWinner.observedAt, &currentWinner.digest)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	hasCurrentWinner := err == nil
	// A strictly older batch is durable evidence only. Projection history is not
	// retroactively replayed after a newer observation watermark has committed.
	if hasCurrentWinner && (input.ObservedAt.Before(currentWinner.observedAt) ||
		(input.ObservedAt.Equal(currentWinner.observedAt) && provenanceDigest <= currentWinner.digest)) {
		if err = tx.Commit(); err != nil {
			return "", err
		}
		return batchID, nil
	}
	replacesEqualWatermarkWinner := hasCurrentWinner && input.ObservedAt.Equal(currentWinner.observedAt) && provenanceDigest > currentWinner.digest
	var committedBefore map[string]storedModelObservation
	if replacesEqualWatermarkWinner {
		committedBefore, err = lockModelObservations(ctx, tx, input.AccountID)
		if err != nil {
			return "", err
		}
		if err = rollbackDiscoveryProjection(ctx, tx, currentWinner.batchID, batchID, input, committedBefore); err != nil {
			return "", err
		}
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
			classification, reason := "discovered", "awaiting_registry_classification"
			var resolvedRegistryID sql.NullInt64
			if input.AccountProvider == nil {
				classification = "ignored"
				reason = "unsupported_routing_platform"
			}
			if restored, restoreErr := loadRemovedAdminState(ctx, tx, input.AccountID, modelID); restoreErr != nil {
				return "", restoreErr
			} else if restored != nil {
				classification = restored.classification
				reason = restored.reason
				resolvedRegistryID = restored.resolvedRegistryID
			}
			err = tx.QueryRowContext(ctx, `
				INSERT INTO model_observations (
					connection_id, account_id, upstream_model_id, resolved_registry_id, classification,
					classification_reason, upstream_presence, miss_streak,
					first_seen_at, last_seen_at, raw_snapshot
				)
				VALUES ($1, $2, $3, $4, $5, $6, 'present', 0, $7, $7, $8::jsonb)
				RETURNING id
			`, input.ConnectionID, input.AccountID, modelID, resolvedRegistryID, classification, reason,
				input.ObservedAt.UTC(), string(persistedSnapshot)).Scan(&observation.id)
			if err != nil {
				return "", err
			}
			observation.classification = classification
			observation.modelID = modelID
			observation.reason = reason
			observation.resolvedRegistryID = resolvedRegistryID
			observation.presence = "present"
			observation.missStreak = 0
			transition := replacementTransition(committedBefore, modelID, "discovered")
			if err = appendObservationEvent(ctx, tx, observation, batchID, projectionEventType(transition, replacesEqualWatermarkWinner), transition, input); err != nil {
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
		transition := replacementTransition(committedBefore, modelID, eventType)
		if err = appendObservationEvent(ctx, tx, observation, batchID, projectionEventType(transition, replacesEqualWatermarkWinner), transition, input); err != nil {
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
		transition := replacementTransition(committedBefore, modelID, "missing")
		if err = appendObservationEvent(ctx, tx, observation, batchID, projectionEventType(transition, replacesEqualWatermarkWinner), transition, input); err != nil {
			return "", err
		}
	}

	if err = tx.Commit(); err != nil {
		return "", err
	}
	return batchID, nil
}

// Equal-watermark replacement first restores the projection that existed before
// the superseded batch, then applies the higher-digest catalog. Prior events stay
// append-only and continue to describe the projection history that actually ran.
func rollbackDiscoveryProjection(
	ctx context.Context,
	tx *sql.Tx,
	supersededBatchID string,
	replacementBatchID string,
	input service.DiscoveryBatchInput,
	committedBefore map[string]storedModelObservation,
) error {
	seen := make(map[string]struct{}, len(input.ModelIDs))
	for _, modelID := range input.ModelIDs {
		seen[modelID] = struct{}{}
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT id, observation_id, upstream_model_id, classification,
		       classification_reason, upstream_presence, miss_streak,
		       COALESCE(payload->>'transition', '')
		FROM model_observation_events
		WHERE batch_id = $1
		ORDER BY id
	`, supersededBatchID)
	if err != nil {
		return err
	}
	type supersededEvent struct {
		id          int64
		transition  string
		observation storedModelObservation
	}
	var events []supersededEvent
	for rows.Next() {
		var event supersededEvent
		if err := rows.Scan(
			&event.id, &event.observation.id, &event.observation.modelID,
			&event.observation.classification, &event.observation.reason,
			&event.observation.presence, &event.observation.missStreak, &event.transition,
		); err != nil {
			_ = rows.Close()
			return err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}

	for _, event := range events {
		if event.transition == "removed" {
			continue
		}
		var previous storedModelObservation
		var previousTransition string
		err := tx.QueryRowContext(ctx, `
			SELECT e.observation_id, e.upstream_model_id, e.classification,
			       e.classification_reason, e.upstream_presence, e.miss_streak,
			       COALESCE(e.payload->>'transition', ''),
			       NULLIF(e.payload->>'connection_id', '')::bigint,
			       mr.id,
			       (e.payload->>'first_seen_at')::timestamptz,
			       (e.payload->>'last_seen_at')::timestamptz,
			       e.payload->'raw_snapshot'
			FROM model_observation_events e
			JOIN model_classification_batches b ON b.batch_id = e.batch_id
			LEFT JOIN model_registry mr
			  ON mr.id = NULLIF(e.payload->>'resolved_registry_id', '')::bigint
			WHERE e.account_id = $1 AND e.upstream_model_id = $2 AND b.observed_at < $3
			ORDER BY b.observed_at DESC, e.id DESC
			LIMIT 1
		`, input.AccountID, event.observation.modelID, input.ObservedAt.UTC()).Scan(
			&previous.id, &previous.modelID, &previous.classification,
			&previous.reason, &previous.presence, &previous.missStreak, &previousTransition,
			&previous.connectionID, &previous.resolvedRegistryID, &previous.firstSeenAt,
			&previous.lastSeenAt, &previous.rawSnapshot,
		)
		if err == nil && previousTransition == "removed" {
			err = sql.ErrNoRows
		}
		if errors.Is(err, sql.ErrNoRows) {
			if _, remainsPresent := seen[event.observation.modelID]; remainsPresent {
				continue
			}
			removed := committedBefore[event.observation.modelID]
			removed.presence = "missing"
			if err := appendObservationEvent(ctx, tx, removed, replacementBatchID, "projection_replaced", "removed", input); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM model_observations WHERE id = $1`, event.observation.id); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		if current, existed := committedBefore[event.observation.modelID]; existed {
			previous.classification = current.classification
			previous.reason = current.reason
			previous.resolvedRegistryID = current.resolvedRegistryID
		}

		if _, err := tx.ExecContext(ctx, `
			UPDATE model_observations
			SET connection_id = $1, resolved_registry_id = $2, classification = $3,
			    classification_reason = $4, upstream_presence = $5, miss_streak = $6,
			    first_seen_at = $7, last_seen_at = $8, raw_snapshot = $9::jsonb,
			    updated_at = NOW()
			WHERE id = $10
		`, previous.connectionID, previous.resolvedRegistryID, previous.classification,
			previous.reason, previous.presence, previous.missStreak, previous.firstSeenAt,
			previous.lastSeenAt, string(previous.rawSnapshot), event.observation.id); err != nil {
			return err
		}
	}
	return nil
}

func replacementTransition(committedBefore map[string]storedModelObservation, modelID, fallback string) string {
	if committedBefore == nil {
		return fallback
	}
	before, existed := committedBefore[modelID]
	if !existed {
		return "discovered"
	}
	if fallback != "missing" && before.presence == "missing" {
		return "reappeared"
	}
	if fallback != "missing" {
		return "observed"
	}
	return "missing"
}

func loadRemovedAdminState(ctx context.Context, tx *sql.Tx, accountID int64, modelID string) (*storedModelObservation, error) {
	var restored storedModelObservation
	err := tx.QueryRowContext(ctx, `
		SELECT e.classification, e.classification_reason, mr.id
		FROM model_observation_events e
		LEFT JOIN model_registry mr
		  ON mr.id = NULLIF(e.payload->>'resolved_registry_id', '')::bigint
		WHERE e.account_id = $1 AND e.upstream_model_id = $2
		  AND e.event_type = 'projection_replaced'
		  AND e.payload->>'transition' = 'removed'
		ORDER BY e.id DESC
		LIMIT 1
	`, accountID, modelID).Scan(&restored.classification, &restored.reason, &restored.resolvedRegistryID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &restored, nil
}

func projectionEventType(transition string, replacesEqualWatermarkWinner bool) string {
	if replacesEqualWatermarkWinner {
		return "projection_replaced"
	}
	return transition
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
		SELECT id, upstream_model_id, connection_id, resolved_registry_id,
		       classification, classification_reason, upstream_presence, miss_streak,
		       first_seen_at, last_seen_at, raw_snapshot
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
			&observation.connectionID,
			&observation.resolvedRegistryID,
			&observation.classification,
			&observation.reason,
			&observation.presence,
			&observation.missStreak,
			&observation.firstSeenAt,
			&observation.lastSeenAt,
			&observation.rawSnapshot,
		); err != nil {
			return nil, err
		}
		observation.modelID = modelID
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
	transition string,
	input service.DiscoveryBatchInput,
) error {
	postState := observation
	err := tx.QueryRowContext(ctx, `
		SELECT connection_id, resolved_registry_id, classification, classification_reason,
		       upstream_presence, miss_streak, first_seen_at, last_seen_at, raw_snapshot
		FROM model_observations
		WHERE id = $1
	`, observation.id).Scan(
		&postState.connectionID, &postState.resolvedRegistryID, &postState.classification,
		&postState.reason, &postState.presence, &postState.missStreak,
		&postState.firstSeenAt, &postState.lastSeenAt, &postState.rawSnapshot,
	)
	if err != nil {
		return err
	}
	postState.id = observation.id
	postState.modelID = observation.modelID
	if transition == "removed" {
		postState.presence = "missing"
		postState.missStreak = observation.missStreak
	}
	payloadValues := map[string]any{
		"account_provider":     input.AccountProvider,
		"routing_platform":     input.RoutingPlatform,
		"observed_at":          input.ObservedAt.UTC(),
		"connection_id":        nullInt64Value(postState.connectionID),
		"resolved_registry_id": nullInt64Value(postState.resolvedRegistryID),
		"first_seen_at":        postState.firstSeenAt.UTC(),
		"last_seen_at":         postState.lastSeenAt.UTC(),
		"raw_snapshot":         postState.rawSnapshot,
	}
	if eventType == "projection_replaced" {
		payloadValues["transition"] = transition
	}
	payload, err := json.Marshal(payloadValues)
	if err != nil {
		return fmt.Errorf("marshal observation event payload: %w", err)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO model_observation_events (
			observation_id, batch_id, account_id, upstream_model_id, event_type, classification,
			classification_reason, upstream_presence, miss_streak, payload
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
	`, postState.id, batchID, input.AccountID, postState.modelID, eventType, postState.classification,
		postState.reason, postState.presence, postState.missStreak, string(payload))
	return err
}

func nullInt64Value(value sql.NullInt64) any {
	if !value.Valid {
		return nil
	}
	return value.Int64
}
