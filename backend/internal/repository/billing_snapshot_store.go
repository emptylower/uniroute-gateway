package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

type BillingSnapshotStore struct {
	db *sql.DB
}

func NewBillingSnapshotStore(db *sql.DB) *BillingSnapshotStore {
	return &BillingSnapshotStore{db: db}
}

func ProvideBillingSnapshotStore(db *sql.DB) service.BillingSnapshotStore {
	return NewBillingSnapshotStore(db)
}

func (s *BillingSnapshotStore) InsertBillingSnapshot(ctx context.Context, snap *service.BillingSnapshot) error {
	if s == nil || s.db == nil || snap == nil {
		return errors.New("billing snapshot store is unavailable")
	}
	payload, err := snap.MarshalPayload()
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO wallet_billing_snapshot
			(id, version, user_id, api_key_id, group_id, account_id, billing_model, pricing_mode, payload, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10)
		ON CONFLICT (id) DO NOTHING`,
		snap.ID, snap.Version, snap.UserID, snap.APIKeyID, snap.GroupID, snap.AccountID,
		snap.BillingModel, string(snap.Pricing.Mode), string(payload), snap.FrozenAt)
	return err
}

func (s *BillingSnapshotStore) GetBillingSnapshot(ctx context.Context, id string) (*service.BillingSnapshot, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("billing snapshot store is unavailable")
	}
	var payload []byte
	err := s.db.QueryRowContext(ctx, `SELECT payload FROM wallet_billing_snapshot WHERE id = $1`, id).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrBillingSnapshotNotFound
	}
	if err != nil {
		return nil, err
	}
	return service.UnmarshalBillingSnapshotPayload(payload)
}

// PruneUnreferencedSnapshotsOlderThan (Phase 4.4-G Task 2, redesign §15.4) deletes
// billing snapshots strictly older than cutoff that have no referents in usage_logs,
// wallet_live_provisional, or wallet_settlement_outbox, in batches using SKIP LOCKED.
func (s *BillingSnapshotStore) PruneUnreferencedSnapshotsOlderThan(ctx context.Context, cutoff time.Time, batch int) (int64, error) {
	if s == nil || s.db == nil {
		return 0, nil
	}
	if batch <= 0 {
		batch = 5000
	}
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM wallet_billing_snapshot
		WHERE id IN (
			SELECT s.id FROM wallet_billing_snapshot s
			WHERE s.created_at < $1
			  AND NOT EXISTS (
				  SELECT 1 FROM usage_logs u WHERE u.billing_snapshot_id = s.id
			  )
			  AND NOT EXISTS (
				  SELECT 1 FROM wallet_live_provisional l WHERE l.billing_snapshot_id = s.id
			  )
			  AND NOT EXISTS (
				  SELECT 1 FROM wallet_settlement_outbox o WHERE o.billing_snapshot_id = s.id
			  )
			  AND NOT EXISTS (
				  SELECT 1 FROM gateway_media_task m WHERE m.billing_snapshot_id = s.id
			  )
              AND NOT EXISTS (SELECT 1 FROM wallet_authorization_segment a WHERE a.billing_snapshot_id = s.id)
			ORDER BY s.id
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)`, cutoff, batch)
	if err != nil {
		return 0, fmt.Errorf("prune unreferenced wallet billing snapshots: %w", err)
	}
	return res.RowsAffected()
}
