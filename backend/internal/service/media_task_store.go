package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type mediaTaskRecord struct {
	AuthorizationKind  string
	Segments           []AuthorizationSegment
	ID                 string
	UserID             int64
	PlatformUserID     string
	APIKeyID           int64
	IdempotencyKey     string
	RequestHash        string
	Model              string
	MediaType          string
	Option             string
	Prompt             string
	RequestPayload     map[string]any
	SnapshotID         string
	QuotedUnits        int64
	AuthorizationID    string
	LeaseID            string
	LeaseBasis         *CanonicalWalletLease
	HeldUnits          int64
	ActualUnits        *int64
	EventID            string
	AuthorizationToken string
	ProviderTaskID     string
	Status             string
	PinState           string
	Result             MediaProviderResult
	ErrorCode          string
	ErrorMessage       string
	ClaimedBy          string
	DeadlineAt         time.Time
	CreatedAt          time.Time
	UpdatedAt          time.Time
	SettledAt          *time.Time
	CapturedEventIDs   []string
}
type mediaTaskStore struct{ db *sql.DB }

func (s *mediaTaskStore) reschedule(ctx context.Context, r *mediaTaskRecord) error {
	_, err := s.db.ExecContext(ctx, `UPDATE gateway_media_task SET claimed_by=NULL,claim_until=NULL,next_poll_at=now()+interval '5 seconds' WHERE id=$1 AND claimed_by=$2`, r.ID, r.ClaimedBy)
	return err
}

const mediaTaskColumns = `id,user_id,platform_user_id,api_key_id,idempotency_key,request_hash,model,media_type,option,prompt,request_payload,billing_snapshot_id,quoted_units,authorization_id,COALESCE(lease_id,''),lease_basis,held_units,actual_units,settlement_event_id,COALESCE(authorization_token,''),COALESCE(provider_task_id,''),status,pin_state,result,COALESCE(error_code,''),COALESCE(error_message,''),COALESCE(claimed_by,''),deadline_at,created_at,updated_at,settled_at`

type mediaScanner interface{ Scan(...any) error }

func scanMediaTask(row mediaScanner) (*mediaTaskRecord, error) {
	var r mediaTaskRecord
	var payload, basis, result []byte
	err := row.Scan(&r.ID, &r.UserID, &r.PlatformUserID, &r.APIKeyID, &r.IdempotencyKey, &r.RequestHash, &r.Model, &r.MediaType, &r.Option, &r.Prompt, &payload, &r.SnapshotID, &r.QuotedUnits, &r.AuthorizationID, &r.LeaseID, &basis, &r.HeldUnits, &r.ActualUnits, &r.EventID, &r.AuthorizationToken, &r.ProviderTaskID, &r.Status, &r.PinState, &result, &r.ErrorCode, &r.ErrorMessage, &r.ClaimedBy, &r.DeadlineAt, &r.CreatedAt, &r.UpdatedAt, &r.SettledAt)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(payload, &r.RequestPayload); err != nil {
		return nil, err
	}
	if len(basis) > 0 {
		if err = json.Unmarshal(basis, &r.LeaseBasis); err != nil {
			return nil, err
		}
	}
	if err = json.Unmarshal(result, &r.Result); err != nil {
		return nil, err
	}
	if r.Result.URLs == nil {
		r.Result.URLs = []MediaURL{}
	}
	return &r, nil
}
func (s *mediaTaskStore) get(ctx context.Context, userID int64, id string) (*mediaTaskRecord, error) {
	return scanMediaTask(s.db.QueryRowContext(ctx, `SELECT `+mediaTaskColumns+` FROM gateway_media_task WHERE user_id=$1 AND id=$2`, userID, id))
}
func (s *mediaTaskStore) create(ctx context.Context, r *mediaTaskRecord, snap *BillingSnapshot) (*mediaTaskRecord, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	payload, err := snap.MarshalPayload()
	if err != nil {
		return nil, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO wallet_billing_snapshot(id,version,user_id,api_key_id,account_id,billing_model,pricing_mode,payload,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9) ON CONFLICT(id) DO NOTHING`, snap.ID, snap.Version, snap.UserID, snap.APIKeyID, snap.AccountID, snap.BillingModel, string(snap.Pricing.Mode), string(payload), snap.FrozenAt)
	if err != nil {
		return nil, err
	}
	input, err := json.Marshal(r.RequestPayload)
	if err != nil {
		return nil, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO gateway_media_task(id,user_id,platform_user_id,api_key_id,idempotency_key,request_hash,model,media_type,option,prompt,request_payload,billing_snapshot_id,quoted_units,authorization_id,settlement_event_id,deadline_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12,$13,$14,$15,$16) ON CONFLICT(user_id,idempotency_key) DO NOTHING`, r.ID, r.UserID, r.PlatformUserID, r.APIKeyID, r.IdempotencyKey, r.RequestHash, r.Model, r.MediaType, r.Option, r.Prompt, string(input), r.SnapshotID, r.QuotedUnits, r.AuthorizationID, r.EventID, r.DeadlineAt)
	if err != nil {
		return nil, err
	}
	stored, err := scanMediaTask(tx.QueryRowContext(ctx, `SELECT `+mediaTaskColumns+` FROM gateway_media_task WHERE user_id=$1 AND idempotency_key=$2`, r.UserID, r.IdempotencyKey))
	if err != nil {
		return nil, err
	}
	if stored.RequestHash != r.RequestHash {
		return nil, ErrMediaIdempotencyConflict
	}
	// Losing an idempotent insert must not accumulate unrelated snapshots.
	if stored.ID != r.ID {
		if _, err = tx.ExecContext(ctx, `DELETE FROM wallet_billing_snapshot WHERE id=$1`, r.SnapshotID); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return stored, nil
}
func (s *mediaTaskStore) claim(ctx context.Context, owner string) (*mediaTaskRecord, error) {
	return scanMediaTask(s.db.QueryRowContext(ctx, `UPDATE gateway_media_task SET claimed_by=$1,claim_until=now()+interval '90 seconds',updated_at=now() WHERE id=(SELECT id FROM gateway_media_task WHERE status NOT IN ('completed','failed') AND next_poll_at<=now() AND (claim_until IS NULL OR claim_until<now()) ORDER BY next_poll_at,id LIMIT 1 FOR UPDATE SKIP LOCKED) RETURNING `+mediaTaskColumns, owner))
}
func (s *mediaTaskStore) save(ctx context.Context, r *mediaTaskRecord, delay time.Duration) error {
	result, err := json.Marshal(r.Result)
	if err != nil {
		return err
	}
	var basis any
	if r.LeaseBasis != nil {
		raw, e := json.Marshal(r.LeaseBasis)
		if e != nil {
			return e
		}
		basis = string(raw)
	}
	res, err := s.db.ExecContext(ctx, `UPDATE gateway_media_task SET lease_id=NULLIF($3,''),lease_basis=$4::jsonb,held_units=$5,actual_units=$6,authorization_token=NULLIF($7,''),provider_task_id=NULLIF($8,''),status=$9,pin_state=$10,result=$11::jsonb,error_code=NULLIF($12,''),error_message=NULLIF($13,''),settled_at=$14,updated_at=now(),next_poll_at=now()+$15*interval '1 millisecond',claim_until=NULL,claimed_by=NULL WHERE id=$1 AND claimed_by=$2 AND claim_until>now()`, r.ID, r.ClaimedBy, r.LeaseID, basis, r.HeldUnits, r.ActualUnits, r.AuthorizationToken, r.ProviderTaskID, r.Status, r.PinState, string(result), r.ErrorCode, r.ErrorMessage, r.SettledAt, delay.Milliseconds())
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("media task claim lost")
	}
	return nil
}

// checkpoint keeps ownership while making the next irreversible stage durable.
func (s *mediaTaskStore) checkpoint(ctx context.Context, r *mediaTaskRecord) error {
	var basis any
	if r.LeaseBasis != nil {
		raw, err := json.Marshal(r.LeaseBasis)
		if err != nil {
			return err
		}
		basis = string(raw)
	}
	res, err := s.db.ExecContext(ctx, `UPDATE gateway_media_task SET lease_id=NULLIF($3,''),lease_basis=$4::jsonb,held_units=$5,authorization_token=NULLIF($6,''),status=$7,pin_state=$8,updated_at=now() WHERE id=$1 AND claimed_by=$2 AND claim_until>now()`, r.ID, r.ClaimedBy, r.LeaseID, basis, r.HeldUnits, r.AuthorizationToken, r.Status, r.PinState)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return errors.New("media task claim lost")
	}
	return nil
}
func (s *mediaTaskStore) list(ctx context.Context, userID int64, limit int, before time.Time, beforeID string) ([]*mediaTaskRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+mediaTaskColumns+` FROM gateway_media_task WHERE user_id=$1 AND ($2::timestamptz IS NULL OR (created_at,id)<($2,$3)) ORDER BY created_at DESC,id DESC LIMIT $4`, userID, nullableMediaTime(before), beforeID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*mediaTaskRecord{}
	for rows.Next() {
		r, e := scanMediaTask(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
func nullableMediaTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}
func (s *mediaTaskStore) active(ctx context.Context, authID string) (bool, error) {
	var yes bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM gateway_media_task WHERE (authorization_id=$1 OR authorization_id IN (SELECT parent_authorization_id FROM wallet_authorization_segment WHERE authorization_id=$1)) AND (pin_state<>'finished' OR status NOT IN ('completed','failed')))`, authID).Scan(&yes)
	return yes, err
}
func (s *mediaTaskStore) leasePinned(ctx context.Context, user, lease string) (bool, error) {
	var yes bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM gateway_media_task WHERE platform_user_id=$1 AND (lease_id=$2 OR authorization_id IN (SELECT parent_authorization_id FROM wallet_authorization_segment WHERE lease_id=$2)) AND held_units>0 AND pin_state<>'finished')`, user, lease).Scan(&yes)
	return yes, err
}
func mediaStoreError(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return ErrMediaTaskNotFound
	}
	return fmt.Errorf("media task storage: %w", err)
}

// Refresh only a still-owned claim before a financial side effect. A late worker
// can never renew an expired token or release a newer worker's reservation.
func (s *mediaTaskStore) fence(ctx context.Context, r *mediaTaskRecord) error {
	result, err := s.db.ExecContext(ctx, `UPDATE gateway_media_task SET claim_until=now()+interval '90 seconds' WHERE id=$1 AND claimed_by=$2 AND claim_until>now()`, r.ID, r.ClaimedBy)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("media task claim lost")
	}
	return nil
}
