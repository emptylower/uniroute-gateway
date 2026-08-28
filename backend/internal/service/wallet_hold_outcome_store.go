package service

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// walletHoldOutcomeStore (Phase 3.4b, redesign §10.6) is the durable outcome
// row's raw-SQL store on the bridge's OWN *sql.DB — the live_provisional_store
// pattern: no provider signature change, no new provider (plan constraint 2).
// A nil receiver is a no-op on every method: bridges built with a nil outboxDB
// (most tests) must never fail an indeterminate classification.
type walletHoldOutcomeStore struct {
	db *sql.DB
}

func newWalletHoldOutcomeStore(db *sql.DB) walletHoldOutcomeSink {
	if db == nil {
		return nil
	}
	return &walletHoldOutcomeStore{db: db}
}

// InsertIndeterminate rows an open outcome (resolution NULL) at the
// indeterminate classification. ON CONFLICT DO NOTHING: two overlapping
// classifications must not error; the row's class is whatever landed first.
func (s *walletHoldOutcomeStore) InsertIndeterminate(ctx context.Context, hold CanonicalWalletHold, platformUserID string, at time.Time) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO wallet_hold_outcome (authorization_id, platform_user_id, lease_id, held_units, class, armed_at, classified_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (authorization_id) DO NOTHING`,
		hold.AuthorizationID, platformUserID, hold.LeaseID, hold.HeldUnits, hold.Class, hold.ArmedAt, at)
	if err != nil {
		return fmt.Errorf("insert indeterminate wallet hold outcome: %w", err)
	}
	return nil
}

// InsertAbandoned rows the reaper's abandoned release: resolution='abandoned'
// with the hold's stored class. ON CONFLICT DO NOTHING — a crash between the
// release and this insert loses the row while the money is already returned
// (§10.3's stated window); Phase 4 is where it surfaces.
func (s *walletHoldOutcomeStore) InsertAbandoned(ctx context.Context, hold CanonicalWalletHold, platformUserID string, at time.Time) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO wallet_hold_outcome (authorization_id, platform_user_id, lease_id, held_units, class, armed_at, classified_at, resolution, resolved_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 'abandoned', $8) ON CONFLICT (authorization_id) DO NOTHING`,
		hold.AuthorizationID, platformUserID, hold.LeaseID, hold.HeldUnits, hold.Class, hold.ArmedAt, at, at)
	if err != nil {
		return fmt.Errorf("insert abandoned wallet hold outcome: %w", err)
	}
	return nil
}

// MarkSettled resolves an OPEN row to settled — only ever called after the
// outbox transaction committed (a rolled-back settlement must never leave a
// settled row).
func (s *walletHoldOutcomeStore) MarkSettled(ctx context.Context, authorizationID, eventID string, at time.Time) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE wallet_hold_outcome
		SET resolution = 'settled', resolved_at = $2, settlement_event_id = $3
		WHERE authorization_id = $1 AND resolution IS NULL`,
		authorizationID, at, eventID)
	if err != nil {
		return fmt.Errorf("mark wallet hold outcome settled: %w", err)
	}
	return nil
}

// MarkExpiredOlderThan is the reaper's Postgres pass (§10.6): rows still open
// whose armed_at is older than the cutoff — the hold hash is gone by then;
// Phase 4 reconciles these against wallet_lease_close. Returns the count.
func (s *walletHoldOutcomeStore) MarkExpiredOlderThan(ctx context.Context, cutoff, at time.Time) (int64, error) {
	if s == nil || s.db == nil {
		return 0, nil
	}
	res, err := s.db.ExecContext(ctx, `
		UPDATE wallet_hold_outcome
		SET resolution = 'expired', resolved_at = $2
		WHERE resolution IS NULL AND armed_at < $1`,
		cutoff, at)
	if err != nil {
		return 0, fmt.Errorf("mark wallet hold outcomes expired: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count expired wallet hold outcomes: %w", err)
	}
	return n, nil
}
