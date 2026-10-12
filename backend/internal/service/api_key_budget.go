package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

var ErrAPIKeyBudgetExceeded = errors.New("API Key allowance exceeded including pending requests")
var ErrAPIKeyBudgetConflict = errors.New("API Key allowance authorization conflict or fenced write")
var ErrAPIKeyBudgetReconciliation = errors.New("historical API Key allowance requires reconciliation")

type APIKeyBudgetReservation struct {
	AuthorizationID string
	APIKeyID        int64
	UserID          int64
	SnapshotID      string
	ReservedUnits   int64
	Kind            string
}

// Callers lock segments and any media task before these functions. These
// functions then lock the Key followed by its allowance; never invert that order.
func lockAPIKeyBudget(ctx context.Context, tx *sql.Tx, id int64) error {
	var got int64
	if err := tx.QueryRowContext(ctx, `SELECT id FROM api_keys WHERE id=$1 FOR UPDATE`, id).Scan(&got); err != nil {
		return err
	}
	return nil
}

func ReserveAPIKeyBudgetTx(ctx context.Context, tx *sql.Tx, in APIKeyBudgetReservation) error {
	if tx == nil || in.AuthorizationID == "" || in.APIKeyID <= 0 || in.UserID <= 0 || in.SnapshotID == "" || in.ReservedUnits < 0 || (in.Kind != "llm" && in.Kind != "live" && in.Kind != "media") {
		return ErrAPIKeyBudgetConflict
	}
	if err := lockAPIKeyBudget(ctx, tx, in.APIKeyID); err != nil {
		return err
	}
	var same bool
	err := tx.QueryRowContext(ctx, `SELECT api_key_id=$2 AND user_id=$3 AND snapshot_id=$4 AND reserved_units=$5 AND kind=$6 AND state<>'released' FROM api_key_budget_authorization WHERE authorization_id=$1`, in.AuthorizationID, in.APIKeyID, in.UserID, in.SnapshotID, in.ReservedUnits, in.Kind).Scan(&same)
	if err == nil {
		if !same {
			return ErrAPIKeyBudgetConflict
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err = backfillAPIKeyBudgetTx(ctx, tx, in.APIKeyID, in.AuthorizationID); err != nil {
		return err
	}
	var active, allowed bool
	err = tx.QueryRowContext(ctx, `SELECT user_id=$2 AND deleted_at IS NULL AND status='active' AND (expires_at IS NULL OR expires_at>now()),
 (quota<=0 OR quota_used*100000000+p.units+$3::numeric<=quota*100000000)
 AND (rate_limit_5h<=0 OR (CASE WHEN window_5h_start IS NULL OR window_5h_start+interval '5 hours'<=now() THEN 0 ELSE usage_5h*100000000 END)+p.units+$3::numeric<=rate_limit_5h*100000000)
 AND (rate_limit_1d<=0 OR (CASE WHEN window_1d_start IS NULL OR window_1d_start+interval '24 hours'<=now() THEN 0 ELSE usage_1d*100000000 END)+p.units+$3::numeric<=rate_limit_1d*100000000)
 AND (rate_limit_7d<=0 OR (CASE WHEN window_7d_start IS NULL OR window_7d_start+interval '7 days'<=now() THEN 0 ELSE usage_7d*100000000 END)+p.units+$3::numeric<=rate_limit_7d*100000000)
 FROM api_keys CROSS JOIN LATERAL(SELECT COALESCE(sum(reserved_units::numeric),0) units FROM api_key_budget_authorization WHERE api_key_id=$1 AND state NOT IN ('captured','released'))p WHERE id=$1`, in.APIKeyID, in.UserID, in.ReservedUnits).Scan(&active, &allowed)
	if err != nil {
		return err
	}
	if !active {
		return ErrAPIKeyBudgetConflict
	}
	if !allowed {
		return ErrAPIKeyBudgetExceeded
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO api_key_budget_authorization(authorization_id,api_key_id,user_id,snapshot_id,reserved_units,kind) VALUES($1,$2,$3,$4,$5,$6)`, in.AuthorizationID, in.APIKeyID, in.UserID, in.SnapshotID, in.ReservedUnits, in.Kind)
	return err
}

func apiKeyBudgetIdentity(ctx context.Context, tx *sql.Tx, auth string) (int64, string, sql.NullInt64, error) {
	var key int64
	err := tx.QueryRowContext(ctx, `SELECT api_key_id FROM api_key_budget_authorization WHERE authorization_id=$1`, auth).Scan(&key)
	if errors.Is(err, sql.ErrNoRows) {
		// Old accepted media rows have a reliable immutable original Key and
		// exact frozen quote. Seed only that obligation, never mutable pricing.
		var tasks bool
		if e := tx.QueryRowContext(ctx, `SELECT to_regclass('gateway_media_task') IS NOT NULL`).Scan(&tasks); e != nil {
			return 0, "", sql.NullInt64{}, e
		}
		if tasks {
			var user, units int64
			var snapshot string
			err = tx.QueryRowContext(ctx, `SELECT api_key_id,user_id,billing_snapshot_id,quoted_units FROM gateway_media_task WHERE authorization_id=$1`, auth).Scan(&key, &user, &snapshot, &units)
			if err == nil {
				if err = lockAPIKeyBudget(ctx, tx, key); err != nil {
					return 0, "", sql.NullInt64{}, err
				}
				_, err = tx.ExecContext(ctx, `INSERT INTO api_key_budget_authorization(authorization_id,api_key_id,user_id,snapshot_id,reserved_units,kind) VALUES($1,$2,$3,$4,$5,'media') ON CONFLICT(authorization_id) DO NOTHING`, auth, key, user, snapshot, units)
			}
		}
	}
	if err != nil {
		return 0, "", sql.NullInt64{}, err
	}
	if err = lockAPIKeyBudget(ctx, tx, key); err != nil {
		return 0, "", sql.NullInt64{}, err
	}
	var state string
	var actual sql.NullInt64
	err = tx.QueryRowContext(ctx, `SELECT state,captured_units FROM api_key_budget_authorization WHERE authorization_id=$1 FOR UPDATE`, auth).Scan(&state, &actual)
	return key, state, actual, err
}

func MarkAPIKeyBudgetWrittenTx(ctx context.Context, tx *sql.Tx, auth string) error {
	key, state, _, err := apiKeyBudgetIdentity(ctx, tx, auth)
	if err != nil {
		return err
	}
	if state == "written" || state == "unknown" {
		return nil
	}
	if state != "reserved" && state != "prepared" {
		return ErrAPIKeyBudgetConflict
	}
	var active bool
	err = tx.QueryRowContext(ctx, `SELECT deleted_at IS NULL AND status='active' AND (expires_at IS NULL OR expires_at>now()) FROM api_keys WHERE id=$1`, key).Scan(&active)
	if err != nil {
		return err
	}
	if !active {
		return ErrAPIKeyBudgetConflict
	}
	_, err = tx.ExecContext(ctx, `UPDATE api_key_budget_authorization SET state='written',written_at=now(),updated_at=now() WHERE authorization_id=$1`, auth)
	return err
}

// Release is only called with durable no-write/zero evidence, never a wallet
// timeout. Its terminal state also fences all future provider writes.
func ReleaseAPIKeyBudgetTx(ctx context.Context, tx *sql.Tx, auth string) error {
	_, state, actual, err := apiKeyBudgetIdentity(ctx, tx, auth)
	if err != nil {
		return err
	}
	if state == "released" {
		return nil
	}
	if state == "captured" {
		// Strict provider zero may already have atomically applied its zero
		// usage receipt. It has no pending allowance; the signed wallet zero
		// finalizer must still finish its ACK/hold/pin/source cleanup.
		if actual.Valid && actual.Int64 == 0 {
			return nil
		}
		return ErrAPIKeyBudgetConflict
	}
	_, err = tx.ExecContext(ctx, `UPDATE api_key_budget_authorization SET state='released',updated_at=now() WHERE authorization_id=$1`, auth)
	return err
}

func CaptureAPIKeyBudgetTx(ctx context.Context, tx *sql.Tx, auth string, units int64) error {
	if units < 0 {
		return ErrAPIKeyBudgetConflict
	}
	key, state, actual, err := apiKeyBudgetIdentity(ctx, tx, auth)
	if err != nil {
		return err
	}
	if state == "captured" {
		if !actual.Valid || actual.Int64 != units {
			return ErrAPIKeyBudgetConflict
		}
		return nil
	}
	if state == "released" {
		return ErrAPIKeyBudgetConflict
	}
	// Accepted charges update retained deleted/revoked Keys without resurrecting
	// them. Actual usage may exceed estimates; record it and refuse future work.
	_, err = tx.ExecContext(ctx, `UPDATE api_keys SET quota_used=quota_used+$2::numeric/100000000,
 status=CASE WHEN status='active' AND quota>0 AND quota_used+$2::numeric/100000000>=quota THEN 'quota_exhausted' ELSE status END,
 usage_5h=(CASE WHEN window_5h_start IS NULL OR window_5h_start+interval '5 hours'<=now() THEN 0 ELSE usage_5h END)+$2::numeric/100000000,
 usage_1d=(CASE WHEN window_1d_start IS NULL OR window_1d_start+interval '24 hours'<=now() THEN 0 ELSE usage_1d END)+$2::numeric/100000000,
 usage_7d=(CASE WHEN window_7d_start IS NULL OR window_7d_start+interval '7 days'<=now() THEN 0 ELSE usage_7d END)+$2::numeric/100000000,
 window_5h_start=CASE WHEN window_5h_start IS NULL OR window_5h_start+interval '5 hours'<=now() THEN now() ELSE window_5h_start END,
 window_1d_start=CASE WHEN window_1d_start IS NULL OR window_1d_start+interval '24 hours'<=now() THEN date_trunc('day',now()) ELSE window_1d_start END,
 window_7d_start=CASE WHEN window_7d_start IS NULL OR window_7d_start+interval '7 days'<=now() THEN date_trunc('day',now()) ELSE window_7d_start END,updated_at=now() WHERE id=$1`, key, units)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE api_key_budget_authorization SET state='captured',captured_units=$2,updated_at=now() WHERE authorization_id=$1`, auth, units)
	return err
}

// Backfill is repeated under each Key's lock. No historical wallet or task row
// is changed. Old/new rolling deployment commits are discovered before admission.
func BackfillAPIKeyBudgetTx(ctx context.Context, tx *sql.Tx, key int64) error {
	return backfillAPIKeyBudgetTx(ctx, tx, key, "")
}

func backfillAPIKeyBudgetTx(ctx context.Context, tx *sql.Tx, key int64, exclude string) error {
	var available bool
	if err := tx.QueryRowContext(ctx, `SELECT to_regclass('wallet_authorization_segment') IS NOT NULL AND to_regclass('wallet_billing_snapshot') IS NOT NULL AND to_regclass('gateway_media_task') IS NOT NULL`).Scan(&available); err != nil {
		return err
	}
	if !available {
		return nil
	}
	// A missing original identity cannot safely be charged to an arbitrary Key.
	var ambiguous bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM wallet_authorization_segment a LEFT JOIN wallet_billing_snapshot s ON s.id=a.billing_snapshot_id WHERE (s.api_key_id IS NULL OR s.api_key_id<=0) AND a.zero_ack_at IS NULL AND (a.authorization_token IS NOT NULL OR a.state NOT IN ('released','finished')))
 AND EXISTS(SELECT 1 FROM api_keys WHERE id=$1 AND (quota>0 OR rate_limit_5h>0 OR rate_limit_1d>0 OR rate_limit_7d>0))`, key).Scan(&ambiguous)
	if err != nil {
		return err
	}
	if ambiguous {
		return ErrAPIKeyBudgetReconciliation
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO api_key_budget_authorization(authorization_id,api_key_id,user_id,snapshot_id,reserved_units,kind,state,written_at)
 SELECT a.parent_authorization_id,s.api_key_id,s.user_id,a.billing_snapshot_id,sum(a.held_units)::bigint,min(a.kind),
 CASE WHEN bool_or(a.authorization_token IS NOT NULL) THEN 'unknown' ELSE 'reserved' END,min(a.first_write_at)
 FROM wallet_authorization_segment a JOIN wallet_billing_snapshot s ON s.id=a.billing_snapshot_id
 WHERE s.api_key_id=$1 AND a.parent_authorization_id<>$2 AND a.kind IN ('llm','live','media') AND a.zero_ack_at IS NULL
 AND NOT EXISTS(SELECT 1 FROM usage_logs u WHERE u.billing_snapshot_id=s.id AND a.kind IN ('llm','live'))
 AND NOT EXISTS(SELECT 1 FROM gateway_media_task t WHERE t.authorization_id=a.parent_authorization_id AND t.financial_state IN ('charged','released_zero'))
 AND (a.authorization_token IS NOT NULL OR a.state NOT IN ('released','finished'))
 GROUP BY a.parent_authorization_id,s.api_key_id,s.user_id,a.billing_snapshot_id
 ON CONFLICT(authorization_id) DO NOTHING`, key, exclude)
	if err != nil {
		return fmt.Errorf("seed historical segments: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO api_key_budget_authorization(authorization_id,api_key_id,user_id,snapshot_id,reserved_units,kind,state,written_at)
 SELECT authorization_id,api_key_id,user_id,billing_snapshot_id,quoted_units,'media',CASE WHEN authorization_token IS NULL THEN 'reserved' ELSE 'unknown' END,CASE WHEN authorization_token IS NULL THEN NULL ELSE created_at END
 FROM gateway_media_task WHERE api_key_id=$1 AND authorization_id<>$2 AND financial_state NOT IN ('charged','released_zero')
 ON CONFLICT(authorization_id) DO NOTHING`, key, exclude)
	if err != nil {
		return err
	}
	// The pre-allowance media engine wrote usage but never incremented Key
	// counters. Reconcile exact historical charged fees once under the Key lock,
	// including unlimited Keys, retaining every existing spent amount.
	chargedRows, chargedErr := tx.QueryContext(ctx, `SELECT authorization_id,user_id,billing_snapshot_id,quoted_units,actual_units FROM gateway_media_task t WHERE api_key_id=$1 AND authorization_id<>$2 AND financial_state='charged' AND actual_units IS NOT NULL AND NOT EXISTS(SELECT 1 FROM api_key_budget_authorization a WHERE a.authorization_id=t.authorization_id)`, key, exclude)
	if chargedErr != nil {
		return chargedErr
	}
	type historicalCharge struct {
		auth, snapshot      string
		user, quote, actual int64
	}
	charges := []historicalCharge{}
	for chargedRows.Next() {
		var c historicalCharge
		if chargedErr = chargedRows.Scan(&c.auth, &c.user, &c.snapshot, &c.quote, &c.actual); chargedErr != nil {
			break
		}
		charges = append(charges, c)
	}
	if chargedErr == nil {
		chargedErr = chargedRows.Err()
	}
	if closeErr := chargedRows.Close(); chargedErr == nil {
		chargedErr = closeErr
	}
	if chargedErr != nil {
		return chargedErr
	}
	for _, c := range charges {
		if _, err = tx.ExecContext(ctx, `INSERT INTO api_key_budget_authorization(authorization_id,api_key_id,user_id,snapshot_id,reserved_units,kind) VALUES($1,$2,$3,$4,$5,'media')`, c.auth, key, c.user, c.snapshot, c.quote); err != nil {
			return err
		}
		// Historical fees belong to their original charge time. Reconcile total
		// spent only; do not put old fees into today's rolling-window buckets.
		if _, err = tx.ExecContext(ctx, `UPDATE api_keys SET quota_used=quota_used+$2::numeric/100000000,status=CASE WHEN status='active' AND quota>0 AND quota_used+$2::numeric/100000000>=quota THEN 'quota_exhausted' ELSE status END,updated_at=now() WHERE id=$1`, key, c.actual); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE api_key_budget_authorization SET state='captured',captured_units=$2,updated_at=now() WHERE authorization_id=$1`, c.auth, c.actual); err != nil {
			return err
		}
	}
	if err = reconcileHistoricalLLMKeyBudgetTx(ctx, tx, key, exclude); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO api_key_budget_epoch(api_key_id) VALUES($1) ON CONFLICT(api_key_id) DO UPDATE SET checked_at=now()`, key)
	return err
}

func apiKeyBudgetPresentTx(ctx context.Context, tx *sql.Tx, auth string) (bool, error) {
	var table bool
	if err := tx.QueryRowContext(ctx, `SELECT to_regclass('api_key_budget_authorization') IS NOT NULL`).Scan(&table); err != nil {
		return false, err
	}
	if !table {
		return false, nil
	}
	var present bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM api_key_budget_authorization WHERE authorization_id=$1)`, auth).Scan(&present)
	return present, err
}
func releaseAPIKeyBudgetIfPresentTx(ctx context.Context, tx *sql.Tx, auth string) error {
	present, err := apiKeyBudgetPresentTx(ctx, tx, auth)
	if err != nil || !present {
		return err
	}
	return ReleaseAPIKeyBudgetTx(ctx, tx, auth)
}
func captureAPIKeyBudgetIfPresentTx(ctx context.Context, tx *sql.Tx, auth string, units int64) error {
	present, err := apiKeyBudgetPresentTx(ctx, tx, auth)
	if err != nil || !present {
		return err
	}
	return CaptureAPIKeyBudgetTx(ctx, tx, auth, units)
}
func markAPIKeyBudgetWrittenIfPresentTx(ctx context.Context, tx *sql.Tx, auth string) error {
	present, err := apiKeyBudgetPresentTx(ctx, tx, auth)
	if err != nil || !present {
		return err
	}
	return MarkAPIKeyBudgetWrittenTx(ctx, tx, auth)
}

// CaptureAPIKeyBudgetUsageTx returns whether shared allowance accounting replaced
// the legacy quota effects. The caller retains its usage-dedup transaction.
func CaptureAPIKeyBudgetUsageTx(ctx context.Context, tx *sql.Tx, auth string, key, units int64) (bool, error) {
	if auth == "" {
		return false, nil
	}
	present, err := apiKeyBudgetPresentTx(ctx, tx, auth)
	if err != nil || !present {
		return false, err
	}
	var valid bool
	if err = tx.QueryRowContext(ctx, `SELECT api_key_id=$2 FROM api_key_budget_authorization WHERE authorization_id=$1`, auth, key).Scan(&valid); err != nil {
		return false, err
	}
	if !valid {
		return false, ErrAPIKeyBudgetConflict
	}
	return true, CaptureAPIKeyBudgetTx(ctx, tx, auth, units)
}

// USDToUnits shares the canonical checked USD conversion for repository effects.
func USDToUnits(usd float64) (int64, error) { return canonicalWalletUnitsFromUSD(usd) }
