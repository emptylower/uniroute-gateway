package service

import (
	"context"
	"database/sql"
	"errors"
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

// WalletHoldOutcome (Phase 4.1-G, redesign §15.3 leg 3) is the read model of
// one wallet_hold_outcome row — the row type migration 213 fixes, with
// resolution as *string over the documented domain {settled, abandoned,
// expired, NULL = open} and the nullable timestamps/ids as pointers. The
// store today has writers only; this type exists for the read path.
type WalletHoldOutcome struct {
	AuthorizationID   string
	PlatformUserID    string
	LeaseID           string
	HeldUnits         int64
	Class             string
	ArmedAt           time.Time
	ClassifiedAt      time.Time
	Resolution        *string
	ResolvedAt        *time.Time
	SettlementEventID *string
}

// ListHoldOutcomesByUser (Phase 4.1-G) is the reconciliation summary's
// per-user hold read: every wallet_hold_outcome row of ONE platform user
// armed inside the half-open [since, until) window, ordered by armed_at.
// Runs on 215's (platform_user_id, armed_at) index. Read-only.
func (s *walletHoldOutcomeStore) ListHoldOutcomesByUser(ctx context.Context, platformUserID string, since, until time.Time) ([]WalletHoldOutcome, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("wallet hold outcome store unavailable")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT authorization_id, platform_user_id, lease_id, held_units, class, armed_at, classified_at, resolution, resolved_at, settlement_event_id
		FROM wallet_hold_outcome
		WHERE platform_user_id = $1 AND armed_at >= $2 AND armed_at < $3
		ORDER BY armed_at, authorization_id`, platformUserID, since, until)
	if err != nil {
		return nil, fmt.Errorf("list wallet hold outcomes by user: %w", err)
	}
	defer rows.Close()
	out := make([]WalletHoldOutcome, 0, 8)
	for rows.Next() {
		var o WalletHoldOutcome
		var resolution, settlementEventID sql.NullString
		var resolvedAt sql.NullTime
		if err := rows.Scan(&o.AuthorizationID, &o.PlatformUserID, &o.LeaseID, &o.HeldUnits, &o.Class, &o.ArmedAt, &o.ClassifiedAt, &resolution, &resolvedAt, &settlementEventID); err != nil {
			return nil, fmt.Errorf("scan wallet hold outcome: %w", err)
		}
		if resolution.Valid {
			v := resolution.String
			o.Resolution = &v
		}
		if resolvedAt.Valid {
			v := resolvedAt.Time
			o.ResolvedAt = &v
		}
		if settlementEventID.Valid {
			v := settlementEventID.String
			o.SettlementEventID = &v
		}
		out = append(out, o)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate wallet hold outcomes: %w", err)
	}
	return out, nil
}
