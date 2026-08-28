package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type liveProvisionalStore struct {
	db *sql.DB
}

func newLiveProvisionalStore(db *sql.DB) LiveProvisionalStore {
	if db == nil {
		return nil
	}
	return &liveProvisionalStore{db: db}
}

func (s *liveProvisionalStore) Save(ctx context.Context, rec *LiveProvisionalRecord) error {
	if s == nil || s.db == nil {
		return errors.New("live provisional store unavailable")
	}
	if rec == nil {
		return errors.New("nil live provisional record")
	}
	windowsJSON, err := json.Marshal(rec.Windows)
	if err != nil {
		return fmt.Errorf("marshal windows: %w", err)
	}
	createdAt := rec.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	query := `
		INSERT INTO wallet_live_provisional (
			token, authorization_id, call_hash, platform_user_id,
			user_id, api_key_id, account_id, billing_currency,
			billing_snapshot_id, estimated_units, status, windows,
			settlement_event_id, created_at, activated_at, terminal_at
		) VALUES (
			$1, $2, $3, $4,
			$5, $6, $7, $8,
			$9, $10, $11, $12,
			$13, $14, $15, $16
		) ON CONFLICT (token) DO NOTHING
	`
	_, err = s.db.ExecContext(ctx, query,
		rec.Token, rec.AuthorizationID, rec.CallHash, rec.PlatformUserID,
		rec.UserID, rec.APIKeyID, rec.AccountID, rec.BillingCurrency,
		rec.BillingSnapshotID, rec.EstimatedUnits, string(rec.Status), windowsJSON,
		rec.SettlementEventID, createdAt, rec.ActivatedAt, rec.TerminalAt,
	)
	if err != nil {
		return fmt.Errorf("save live provisional record: %w", err)
	}
	return nil
}

func (s *liveProvisionalStore) Activate(ctx context.Context, token, callHash string, at time.Time) error {
	if s == nil || s.db == nil {
		return errors.New("live provisional store unavailable")
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	query := `
		UPDATE wallet_live_provisional
		SET status = 'active', call_hash = $2, activated_at = $3
		WHERE token = $1 AND status = 'provisional'
	`
	res, err := s.db.ExecContext(ctx, query, token, callHash, at)
	if err != nil {
		return fmt.Errorf("activate live provisional record: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("activate rows affected: %w", err)
	}
	if rows == 0 {
		var currentStatus string
		readErr := s.db.QueryRowContext(ctx, "SELECT status FROM wallet_live_provisional WHERE token = $1", token).Scan(&currentStatus)
		if errors.Is(readErr, sql.ErrNoRows) {
			return ErrLiveProvisionalNotFound
		}
		if readErr == nil {
			return fmt.Errorf("%w: status is %s", ErrLiveProvisionalNotFound, currentStatus)
		}
		return ErrLiveProvisionalNotFound
	}
	return nil
}

func (s *liveProvisionalStore) Abort(ctx context.Context, token string, at time.Time) error {
	if s == nil || s.db == nil {
		return errors.New("live provisional store unavailable")
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	query := `
		UPDATE wallet_live_provisional
		SET status = 'aborted', terminal_at = $2
		WHERE token = $1 AND status = 'provisional'
	`
	res, err := s.db.ExecContext(ctx, query, token, at)
	if err != nil {
		return fmt.Errorf("abort live provisional record: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("abort rows affected: %w", err)
	}
	if rows == 0 {
		var currentStatus string
		readErr := s.db.QueryRowContext(ctx, "SELECT status FROM wallet_live_provisional WHERE token = $1", token).Scan(&currentStatus)
		if errors.Is(readErr, sql.ErrNoRows) {
			return ErrLiveProvisionalNotFound
		}
		if readErr == nil {
			return fmt.Errorf("%w: status is %s", ErrLiveProvisionalNotFound, currentStatus)
		}
		return ErrLiveProvisionalNotFound
	}
	return nil
}

func (s *liveProvisionalStore) ClaimFinalization(ctx context.Context, token string, _ time.Time) (bool, error) {
	if s == nil || s.db == nil {
		return false, errors.New("live provisional store unavailable")
	}
	query := `
		UPDATE wallet_live_provisional
		SET status = 'finalizing'
		WHERE token = $1 AND status = 'active'
	`
	res, err := s.db.ExecContext(ctx, query, token)
	if err != nil {
		return false, fmt.Errorf("claim finalization: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("claim finalization rows affected: %w", err)
	}
	return rows == 1, nil
}

func (s *liveProvisionalStore) CompleteFinalization(ctx context.Context, token, settlementEventID string, settledUnits int64, at time.Time) error {
	if s == nil || s.db == nil {
		return errors.New("live provisional store unavailable")
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	query := `
		UPDATE wallet_live_provisional
		SET status = 'finalized', settlement_event_id = $2, terminal_at = $4,
		    windows = jsonb_set(windows, '{0,settled_units}', to_jsonb($3::bigint))
		WHERE token = $1 AND status = 'finalizing'
	`
	res, err := s.db.ExecContext(ctx, query, token, settlementEventID, settledUnits, at)
	if err != nil {
		return fmt.Errorf("complete finalization: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("complete finalization rows affected: %w", err)
	}
	if rows == 0 {
		var currentStatus string
		readErr := s.db.QueryRowContext(ctx, "SELECT status FROM wallet_live_provisional WHERE token = $1", token).Scan(&currentStatus)
		if errors.Is(readErr, sql.ErrNoRows) {
			return ErrLiveProvisionalNotFound
		}
		if readErr == nil {
			return fmt.Errorf("%w: status is %s", ErrLiveProvisionalNotFound, currentStatus)
		}
		return ErrLiveProvisionalNotFound
	}
	return nil
}

func (s *liveProvisionalStore) ReleaseFinalizationClaim(ctx context.Context, token string) error {
	if s == nil || s.db == nil {
		return errors.New("live provisional store unavailable")
	}
	query := `
		UPDATE wallet_live_provisional
		SET status = 'active'
		WHERE token = $1 AND status = 'finalizing'
	`
	res, err := s.db.ExecContext(ctx, query, token)
	if err != nil {
		return fmt.Errorf("release finalization claim: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("release finalization claim rows affected: %w", err)
	}
	if rows == 0 {
		var currentStatus string
		readErr := s.db.QueryRowContext(ctx, "SELECT status FROM wallet_live_provisional WHERE token = $1", token).Scan(&currentStatus)
		if errors.Is(readErr, sql.ErrNoRows) {
			return ErrLiveProvisionalNotFound
		}
		if readErr == nil {
			return fmt.Errorf("%w: status is %s", ErrLiveProvisionalNotFound, currentStatus)
		}
		return ErrLiveProvisionalNotFound
	}
	return nil
}

func (s *liveProvisionalStore) scanRow(row *sql.Row) (*LiveProvisionalRecord, error) {
	var rec LiveProvisionalRecord
	var statusStr string
	var windowsJSON []byte
	err := row.Scan(
		&rec.Token,
		&rec.AuthorizationID,
		&rec.CallHash,
		&rec.PlatformUserID,
		&rec.UserID,
		&rec.APIKeyID,
		&rec.AccountID,
		&rec.BillingCurrency,
		&rec.BillingSnapshotID,
		&rec.EstimatedUnits,
		&statusStr,
		&windowsJSON,
		&rec.SettlementEventID,
		&rec.CreatedAt,
		&rec.ActivatedAt,
		&rec.TerminalAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrLiveProvisionalNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan live provisional record: %w", err)
	}
	rec.Status = LiveProvisionalStatus(statusStr)
	if len(windowsJSON) > 0 {
		if err := json.Unmarshal(windowsJSON, &rec.Windows); err != nil {
			return nil, fmt.Errorf("unmarshal windows: %w", err)
		}
	}
	return &rec, nil
}

func (s *liveProvisionalStore) Get(ctx context.Context, token string) (*LiveProvisionalRecord, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("live provisional store unavailable")
	}
	query := `
		SELECT token, authorization_id, call_hash, platform_user_id,
		       user_id, api_key_id, account_id, billing_currency,
		       billing_snapshot_id, estimated_units, status, windows,
		       settlement_event_id, created_at, activated_at, terminal_at
		FROM wallet_live_provisional
		WHERE token = $1
	`
	return s.scanRow(s.db.QueryRowContext(ctx, query, token))
}

func (s *liveProvisionalStore) GetByCallHash(ctx context.Context, callHash string) (*LiveProvisionalRecord, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("live provisional store unavailable")
	}
	query := `
		SELECT token, authorization_id, call_hash, platform_user_id,
		       user_id, api_key_id, account_id, billing_currency,
		       billing_snapshot_id, estimated_units, status, windows,
		       settlement_event_id, created_at, activated_at, terminal_at
		FROM wallet_live_provisional
		WHERE call_hash = $1
	`
	return s.scanRow(s.db.QueryRowContext(ctx, query, callHash))
}
