package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

type liveProvisionalStore struct {
	db                    *sql.DB
	protectWalletAttempts bool
}

func ProvideLiveProvisionalStore(cfg *config.Config, db *sql.DB) LiveProvisionalStore {
	store := newLiveProvisionalStore(db)
	if store != nil && cfg != nil {
		store.(*liveProvisionalStore).protectWalletAttempts = cfg.CanonicalWallet.USDWalletEnabled
	}
	return store
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

func (s *liveProvisionalStore) CompleteFinalization(ctx context.Context, token, settlementEventID string, settledUnits int64, seq int, at time.Time) error {
	if s == nil || s.db == nil {
		return errors.New("live provisional store unavailable")
	}
	if seq < 1 {
		return fmt.Errorf("complete finalization: invalid window seq %d", seq)
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	// Phase 3.7b (§13.2.2): the finalization writes the FINAL window
	// (windows[seq-1]), not index 0. A pre-3.7 record (one window, seq 1)
	// writes index 0 exactly as before — that generalisation is what makes
	// the last-window rule backward compatible.
	query := `
		UPDATE wallet_live_provisional
		SET status = 'finalized', settlement_event_id = $2, terminal_at = $5,
		    windows = jsonb_set(windows, ARRAY[($4-1)::text, 'settled_units'], to_jsonb($3::bigint))
		WHERE token = $1 AND status = 'finalizing'
	`
	res, err := s.db.ExecContext(ctx, query, token, settlementEventID, settledUnits, seq, at)
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

// SetLiveWindowPending (§13.2.2, Phase 3.7b): persists the window's amount
// FIRST, as pending_units, in one statement. The jsonb path is derived from
// the seq parameter itself (nothing can drift), and the guard — status
// 'active', the window list exactly seq long, and the last window's
// window_seq equal to seq — makes this a true compare-and-set: 0 rows means
// the CAS lost and the observer reloads and retries on its next tick.
// The ::text cast is required: a mixed integer/text ARRAY[…] does not prepare.
func (s *liveProvisionalStore) SetLiveWindowPending(ctx context.Context, token string, seq int, units int64) error {
	if s == nil || s.db == nil {
		return errors.New("live provisional store unavailable")
	}
	if seq < 1 {
		return fmt.Errorf("set live window pending: invalid window seq %d", seq)
	}
	query := `
		UPDATE wallet_live_provisional
		SET windows = jsonb_set(windows, ARRAY[($2-1)::text, 'pending_units'], to_jsonb($3::bigint))
		WHERE token = $1 AND status = 'active'
		  AND jsonb_array_length(windows) = $2
		  AND (windows->($2-1)->>'window_seq')::int = $2
	`
	return s.execWindowCAS(ctx, "set live window pending", query, token, seq, units)
}

// AdvanceLiveWindow (§13.2.2, Phase 3.7b): settles window seq (settled_units,
// pending cleared) and appends the next window in ONE statement — a lost CAS
// can never leave a partially-applied window. After an advance the list is
// seq+1 long, so the length guard makes a repeat (or a second racing
// observer) lose: two observers can never both advance.
func (s *liveProvisionalStore) AdvanceLiveWindow(ctx context.Context, token string, seq int, settledUnits int64, next LiveWindow) error {
	if s == nil || s.db == nil {
		return errors.New("live provisional store unavailable")
	}
	if seq < 1 {
		return fmt.Errorf("advance live window: invalid window seq %d", seq)
	}
	nextJSON, err := json.Marshal([]LiveWindow{next})
	if err != nil {
		return fmt.Errorf("advance live window: marshal next window: %w", err)
	}
	// $4 is the next window marshalled as a one-element JSON array: || on two
	// jsonb arrays concatenates, so the result is […updated_window_seq, next].
	query := `
		UPDATE wallet_live_provisional
		SET windows = jsonb_set(
		      jsonb_set(windows, ARRAY[($2-1)::text, 'settled_units'], to_jsonb($3::bigint)),
		      ARRAY[($2-1)::text, 'pending_units'], to_jsonb(0::bigint)
		    ) || $4::jsonb
		WHERE token = $1 AND status = 'active'
		  AND jsonb_array_length(windows) = $2
		  AND (windows->($2-1)->>'window_seq')::int = $2
	`
	if !s.protectWalletAttempts {
		return s.execWindowCAS(ctx, "advance live window", query, token, seq, settledUnits, string(nextJSON))
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	count, err := lockWalletAttempt(ctx, tx, next.Token)
	if err != nil {
		return err
	}
	if count > 0 {
		res, err := tx.ExecContext(ctx, `UPDATE wallet_authorization_segment SET state='indeterminate',authorization_token=$2,updated_at=now() WHERE parent_authorization_id=$1 AND kind='live' AND state='held' AND authorization_token IS NULL AND platform_user_id=(SELECT platform_user_id FROM wallet_live_provisional WHERE token=$3) AND billing_snapshot_id=(SELECT billing_snapshot_id FROM wallet_live_provisional WHERE token=$3)`, next.Token, next.Token+":live-window", token)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n != int64(count) {
			return ErrLiveWindowCASLost
		}
	}
	res, err := tx.ExecContext(ctx, query, token, seq, settledUnits, string(nextJSON))
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrLiveWindowCASLost
	}
	return tx.Commit()
}

// execWindowCAS runs one of the two single-statement window CASes and maps
// 0 affected rows to ErrLiveWindowCASLost (§13.2.2).
func (s *liveProvisionalStore) execWindowCAS(ctx context.Context, op, query string, args ...any) error {
	res, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s rows affected: %w", op, err)
	}
	if rows == 0 {
		return ErrLiveWindowCASLost
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

// ListLiveProvisionalByUser (Phase 4.1-G, redesign §15.3 leg 3) is the
// reconciliation summary's per-user Live read: every record of ONE platform
// user created inside the half-open [since, until) window, windows decoded
// from the JSON column as LiveWindow, with the billing snapshot's fx rate
// folded in through a LEFT JOIN on wallet_billing_snapshot — a missing
// snapshot (or an empty billing_snapshot_id) degrades billing_fx to nil
// and never drops the record (round-3 fx fold; the durable fix — a
// billing_snapshot_id column on the outbox — is 4.2's). Read-only.
func (s *liveProvisionalStore) ListLiveProvisionalByUser(ctx context.Context, platformUserID string, since, until time.Time) ([]LiveProvisionalSummaryRecord, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("live provisional store unavailable")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT p.token, p.authorization_id, p.call_hash, p.platform_user_id,
		       p.billing_currency, p.billing_snapshot_id, b.payload->'fx'->>'rate',
		       p.status, p.estimated_units, p.settlement_event_id, p.windows, p.created_at
		FROM wallet_live_provisional p
		LEFT JOIN wallet_billing_snapshot b ON b.id = p.billing_snapshot_id
		WHERE p.platform_user_id = $1 AND p.created_at >= $2 AND p.created_at < $3
		ORDER BY p.created_at, p.token`, platformUserID, since, until)
	if err != nil {
		return nil, fmt.Errorf("list live provisional by user: %w", err)
	}
	defer rows.Close()
	out := make([]LiveProvisionalSummaryRecord, 0, 4)
	for rows.Next() {
		var r LiveProvisionalSummaryRecord
		var billingFX sql.NullString
		var windowsJSON []byte
		var statusStr string
		if err := rows.Scan(&r.Token, &r.AuthorizationID, &r.CallHash, &r.PlatformUserID,
			&r.BillingCurrency, &r.BillingSnapshotID, &billingFX, &statusStr, &r.EstimatedUnits, &r.SettlementEventID, &windowsJSON, &r.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan live provisional summary: %w", err)
		}
		r.Status = statusStr
		if billingFX.Valid {
			v := billingFX.String
			r.BillingFX = &v
		}
		if len(windowsJSON) > 0 {
			if err := json.Unmarshal(windowsJSON, &r.Windows); err != nil {
				return nil, fmt.Errorf("unmarshal windows: %w", err)
			}
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate live provisional: %w", err)
	}
	return out, nil
}

// PruneTerminalOlderThan (Phase 4.2-G Task 3, redesign §15.4) deletes
// terminal live records (terminal_at IS NOT NULL AND terminal_at < cutoff)
// strictly older than cutoff in batches using SKIP LOCKED.
// It never touches active/provisional/finalizing rows (terminal_at IS NULL)
// and never touches rows inside the retention window.
func (s *liveProvisionalStore) PruneTerminalOlderThan(ctx context.Context, cutoff time.Time, batch int) (int64, error) {
	if s == nil || s.db == nil {
		return 0, nil
	}
	if batch <= 0 {
		batch = 5000
	}
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM wallet_live_provisional
		WHERE token IN (
			SELECT token FROM wallet_live_provisional
			WHERE terminal_at IS NOT NULL AND terminal_at < $1
			ORDER BY token
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)`, cutoff, batch)
	if err != nil {
		return 0, fmt.Errorf("prune terminal live provisional records: %w", err)
	}
	return res.RowsAffected()
}
