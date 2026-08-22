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

// Stable readiness prerequisite codes surfaced to administrators. They mirror
// the conflicts Activate raises when the same condition fails in-transaction.
const (
	ReadinessPrerequisiteRegistryEmpty         = "REGISTRY_EMPTY"
	ReadinessPrerequisiteInventoryNotCompleted = "INVENTORY_NOT_COMPLETED"
	ReadinessPrerequisiteShadowCoverage        = "SHADOW_COVERAGE_INCOMPLETE"
	ReadinessPrerequisiteUnresolvedImpacts     = "UNRESOLVED_IMPACT_REPORTS"
)

// ActivationReadiness is the read-only evaluation of every enforce activation
// prerequisite (Phase 7 Task 0 G3). The values are exactly what an
// administrator needs to prefill an activation request; Activate re-checks
// everything inside its transaction, so the GET can never be a bypass.
type ActivationReadiness struct {
	Ready       bool   `json:"ready"`
	CurrentMode string `json:"current_mode"`

	CurrentRegistryVersion       int64           `json:"current_registry_version"`
	LatestCompletedInventoryHash string          `json:"latest_completed_inventory_hash"`
	ChannelVersions              map[int64]int64 `json:"channel_versions"`
	ProjectedBatchID             string          `json:"projected_batch_id"`

	EnabledGovernedAccounts    int      `json:"enabled_governed_accounts"`
	AccountsWithShadowDecision int      `json:"accounts_with_shadow_decision"`
	UnresolvedImpactAccounts   int      `json:"unresolved_impact_accounts"`
	UnmetPrerequisites         []string `json:"unmet_prerequisites"`
}

// querier abstracts the single-statement reads shared by EvaluateReadiness
// (*sql.DB) and the transactional re-check inside Activate (*sql.Tx).
type querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func currentRegistryVersion(ctx context.Context, q querier) (int64, error) {
	var version int64
	err := q.QueryRowContext(ctx, `SELECT COALESCE(MAX(registry_version),0) FROM model_registry_events`).Scan(&version)
	return version, err
}

func channelGovernanceVersion(ctx context.Context, q querier, channelID int64) (int64, error) {
	var version int64
	err := q.QueryRowContext(ctx, `SELECT governance_version FROM channels WHERE id = $1`, channelID).Scan(&version)
	return version, err
}

func completedInventoryRunCount(ctx context.Context, q querier, inventoryHash string) (int, error) {
	var count int
	err := q.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM model_inventory_runs WHERE inventory_hash = $1 AND status = 'completed'`,
		inventoryHash).Scan(&count)
	return count, err
}

func enabledGovernedAccountCount(ctx context.Context, q querier) (int, error) {
	var count int
	err := q.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM accounts WHERE platform IN ('anthropic','openai','gemini','grok') AND status = 'active'`).Scan(&count)
	return count, err
}

func shadowCoverageAccountCount(ctx context.Context, q querier, registryVersion int64) (int, error) {
	var count int
	err := q.QueryRowContext(ctx, `SELECT COUNT(DISTINCT batch.account_id)
FROM model_shadow_decisions d
JOIN model_classification_batches batch ON batch.batch_id = d.batch_id
WHERE batch.registry_version = $1`, registryVersion).Scan(&count)
	return count, err
}

func unresolvedImpactAccountCount(ctx context.Context, q querier) (int, error) {
	var count int
	err := q.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM accounts WHERE platform IN ('anthropic','openai','gemini','grok') AND status = 'active' AND connection_id IS NULL`).Scan(&count)
	return count, err
}

// EvaluateReadiness computes every activation prerequisite read-only so the
// administrator dialog prefills truthfully and stays disabled until the
// backend reports ready. It shares its checks with Activate via the querier
// helpers above; Activate remains the authoritative gate.
func (s *ModelAuthorizationActivationService) EvaluateReadiness(ctx context.Context) (*ActivationReadiness, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("activation service not configured")
	}
	readiness := &ActivationReadiness{
		CurrentMode:        "shadow",
		ChannelVersions:    map[int64]int64{},
		UnmetPrerequisites: []string{},
	}

	if s.modeProvider != nil {
		readiness.CurrentMode = s.modeProvider.CurrentMode(ctx)
	}

	registryVersion, err := currentRegistryVersion(ctx, s.db)
	if err != nil {
		return nil, fmt.Errorf("read registry version: %w", err)
	}
	readiness.CurrentRegistryVersion = registryVersion
	if registryVersion <= 0 {
		readiness.UnmetPrerequisites = append(readiness.UnmetPrerequisites, ReadinessPrerequisiteRegistryEmpty)
	}

	err = s.db.QueryRowContext(ctx,
		`SELECT COALESCE(inventory_hash, '') FROM model_inventory_runs WHERE status = 'completed' ORDER BY created_at DESC LIMIT 1`,
	).Scan(&readiness.LatestCompletedInventoryHash)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("read latest completed inventory: %w", err)
	}
	if readiness.LatestCompletedInventoryHash == "" {
		readiness.UnmetPrerequisites = append(readiness.UnmetPrerequisites, ReadinessPrerequisiteInventoryNotCompleted)
	}

	rows, err := s.db.QueryContext(ctx, `SELECT id, governance_version FROM channels`)
	if err != nil {
		return nil, fmt.Errorf("read channel versions: %w", err)
	}
	for rows.Next() {
		var id, version int64
		if err := rows.Scan(&id, &version); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan channel versions: %w", err)
		}
		readiness.ChannelVersions[id] = version
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate channel versions: %w", err)
	}
	// Truthful prefill for projected_batch_id: the batch of the most recent
	// applied publication projection. Activate itself does not validate this
	// field beyond requiring it to be non-empty.
	_ = s.db.QueryRowContext(ctx,
		`SELECT batch_id FROM model_publication_events ORDER BY created_at DESC, id DESC LIMIT 1`,
	).Scan(&readiness.ProjectedBatchID)

	enabledAccounts, err := enabledGovernedAccountCount(ctx, s.db)
	if err != nil {
		return nil, fmt.Errorf("count enabled governed accounts: %w", err)
	}
	readiness.EnabledGovernedAccounts = enabledAccounts

	if enabledAccounts > 0 {
		covered, err := shadowCoverageAccountCount(ctx, s.db, registryVersion)
		if err != nil {
			return nil, fmt.Errorf("read shadow coverage: %w", err)
		}
		readiness.AccountsWithShadowDecision = covered
		if covered < enabledAccounts {
			readiness.UnmetPrerequisites = append(readiness.UnmetPrerequisites, ReadinessPrerequisiteShadowCoverage)
		}
	}

	unresolved, err := unresolvedImpactAccountCount(ctx, s.db)
	if err != nil {
		return nil, fmt.Errorf("count unresolved impact accounts: %w", err)
	}
	readiness.UnresolvedImpactAccounts = unresolved
	if unresolved > 0 {
		readiness.UnmetPrerequisites = append(readiness.UnmetPrerequisites, ReadinessPrerequisiteUnresolvedImpacts)
	}

	readiness.Ready = len(readiness.UnmetPrerequisites) == 0 &&
		readiness.LatestCompletedInventoryHash != ""
	return readiness, nil
}

// ModelAuthorizationActivationService gates enforce activation.
type ModelAuthorizationActivationService struct {
	db           *sql.DB
	modeProvider GovernanceModeProvider
}

func (s *ModelAuthorizationActivationService) SetModeProvider(p GovernanceModeProvider) {
	if s != nil {
		s.modeProvider = p
	}
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

	// Inventory completeness: verify inventory hash exists in recent completed inventory runs.
	currentRegistryVersionValue, err := currentRegistryVersion(ctx, tx)
	if err != nil {
		return err
	}
	if input.RegistryVersion != currentRegistryVersionValue {
		err = infraerrors.Conflict("STALE_REGISTRY_VERSION", fmt.Sprintf("stale registry version expected %d got %d", input.RegistryVersion, currentRegistryVersionValue))
		return err
	}
	for chID, expectedVersion := range input.ChannelVersions {
		currentVersion, err := channelGovernanceVersion(ctx, tx, chID)
		if err != nil {
			return err
		}
		if expectedVersion != currentVersion {
			err = infraerrors.Conflict("STALE_CHANNEL_VERSION", fmt.Sprintf("stale channel version for %d", chID))
			return err
		}
	}

	// Inventory completeness: hash must match a completed inventory run.
	inventoryCount, err := completedInventoryRunCount(ctx, tx, input.InventoryHash)
	if err != nil {
		return err
	}
	if inventoryCount == 0 {
		err = infraerrors.Conflict("INVENTORY_NOT_COMPLETED", "inventory hash does not match a completed inventory run")
		return err
	}

	// Post-registry shadow coverage: every enabled governed account must have a shadow decision at current registry version.
	enabledAccounts, err := enabledGovernedAccountCount(ctx, tx)
	if err != nil {
		return err
	}
	if enabledAccounts > 0 {
		shadowAccounts, err := shadowCoverageAccountCount(ctx, tx, input.RegistryVersion)
		if err != nil {
			return err
		}
		if shadowAccounts < enabledAccounts {
			err = infraerrors.Conflict("SHADOW_COVERAGE_INCOMPLETE", fmt.Sprintf("shadow coverage incomplete: %d/%d", shadowAccounts, enabledAccounts))
			return err
		}
	}

	// Unresolved impact reports: for now, check that no unsupported routing-only accounts remain without connection.
	// If any account has connection_id IS NULL and platform in governed set, it is an unresolved impact.
	unresolved, err := unresolvedImpactAccountCount(ctx, tx)
	if err != nil {
		return err
	}
	if unresolved > 0 {
		err = infraerrors.Conflict("UNRESOLVED_IMPACT_REPORTS", fmt.Sprintf("unresolved impact reports: %d accounts without connection", unresolved))
		return err
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
	if s.modeProvider != nil {
		if p, ok := s.modeProvider.(*dbGovernanceModeProvider); ok {
			p.Invalidate()
		}
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
	// Use current registry and channel versions for deactivate record.
	var currentRegistry int64
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(registry_version),1) FROM model_registry_events`).Scan(&currentRegistry)
	if err != nil {
		currentRegistry = 1
	}
	channelVersions := map[int64]int64{}
	rows, err := s.db.QueryContext(ctx, `SELECT id, governance_version FROM channels`)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var id, ver int64
			if err := rows.Scan(&id, &ver); err == nil {
				channelVersions[id] = ver
			}
		}
	}
	cvJSON, _ := json.Marshal(channelVersions)
	var inventoryHash = "deactivate"
	// Use a completed inventory hash if exists, otherwise use placeholder.
	var existingHash sql.NullString
	_ = s.db.QueryRowContext(ctx, `SELECT inventory_hash FROM model_inventory_runs WHERE status='completed' ORDER BY created_at DESC LIMIT 1`).Scan(&existingHash)
	if existingHash.Valid {
		inventoryHash = existingHash.String
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO model_authorization_activations (inventory_hash, registry_version, channel_versions, projected_batch_id, acknowledged_by, idempotency_key, mode_before, mode_after, actor_id)
VALUES ($1,$2,$3::jsonb,$4,'system',$5,'enforce','shadow',$6)
`, inventoryHash, currentRegistry, string(cvJSON), "deactivate", idempotencyKey, actorID)
	if err == nil && s.modeProvider != nil {
		if p, ok := s.modeProvider.(*dbGovernanceModeProvider); ok {
			p.Invalidate()
		}
	}
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
