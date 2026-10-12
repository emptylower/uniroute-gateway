package service

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"
)

// Recovery discovers outside its locking transaction, then reacquires segments,
// task, Key and allowance in that order. A timeout merely selects candidates;
// tokenless durable state and a terminal allowance fence prove no future write.
func RecoverAPIKeyBudgets(ctx context.Context, db *sql.DB, limit int) error {
	if db == nil {
		return nil
	}
	if limit <= 0 || limit > 128 {
		limit = 32
	}
	var present, segments, tasks, live bool
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('api_key_budget_authorization') IS NOT NULL,to_regclass('wallet_authorization_segment') IS NOT NULL,to_regclass('gateway_media_task') IS NOT NULL,to_regclass('wallet_live_provisional') IS NOT NULL`).Scan(&present, &segments, &tasks, &live); err != nil {
		return err
	}
	if !present {
		return nil
	}
	rows, err := db.QueryContext(ctx, `SELECT authorization_id FROM api_key_budget_authorization WHERE state NOT IN ('captured','released') AND updated_at<now()-interval '90 seconds' ORDER BY updated_at,authorization_id LIMIT $1`, limit)
	if err != nil {
		return err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			break
		}
		ids = append(ids, id)
	}
	if err == nil {
		err = rows.Err()
	}
	if closeErr := rows.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err = recoverAPIKeyBudget(ctx, db, id, segments, tasks, live); err != nil {
			return err
		}
		if _, err = db.ExecContext(ctx, `UPDATE api_key_budget_authorization SET updated_at=now() WHERE authorization_id=$1 AND state NOT IN ('captured','released')`, id); err != nil {
			return err
		}
	}
	return nil
}
func recoverAPIKeyBudget(ctx context.Context, db *sql.DB, id string, segments, tasks, live bool) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	n := 0
	zero := true
	notSent := true
	kind := ""
	actual := int64(0)
	settled := true
	if segments {
		rows, e := tx.QueryContext(ctx, `SELECT authorization_token,zero_ack_at,actual_units,kind,state,settlement_payload,event_id,remainder_payload,COALESCE(terminal_evidence->>'source','') FROM wallet_authorization_segment WHERE parent_authorization_id=$1 ORDER BY ordinal FOR UPDATE`, id)
		if e != nil {
			return e
		}
		type share struct {
			token                     sql.NullString
			ack                       sql.NullTime
			units                     int64
			kind, state               string
			payload, event, remainder sql.NullString
			zeroSource                string
		}
		shares := []share{}
		for rows.Next() {
			var s share
			if e = rows.Scan(&s.token, &s.ack, &s.units, &s.kind, &s.state, &s.payload, &s.event, &s.remainder, &s.zeroSource); e != nil {
				break
			}
			shares = append(shares, s)
		}
		if e == nil {
			e = rows.Err()
		}
		if closeErr := rows.Close(); e == nil {
			e = closeErr
		}
		if e != nil {
			return e
		}
		for _, s := range shares {
			n++
			kind = s.kind
			zero = zero && (s.ack.Valid || (s.kind == "live" && s.zeroSource == liveBudgetZeroSource)) && s.units == 0
			notSent = notSent && !s.token.Valid && s.units == 0 && !s.payload.Valid && !s.remainder.Valid
			if s.units > 0 {
				var addErr error
				actual, addErr = AddUnits(actual, s.units)
				if addErr != nil {
					return addErr
				}
				var delivered bool
				if e = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM wallet_settlement_outbox WHERE event_id=$1 AND status='delivered')`, s.event).Scan(&delivered); e != nil {
					return e
				}
				settled = settled && delivered
			} else {
				settled = settled && (s.ack.Valid || s.state == "finished")
			}
			if s.remainder.Valid {
				settled = false
			}
		}
	}
	if tasks {
		var taskID string
		e := tx.QueryRowContext(ctx, `SELECT id FROM gateway_media_task WHERE authorization_id=$1 FOR UPDATE`, id).Scan(&taskID)
		if e == nil {
			return nil
		}
		if !errors.Is(e, sql.ErrNoRows) {
			return e
		}
	}
	if live && kind == "live" && notSent {
		var active bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM wallet_live_provisional p WHERE status IN ('provisional','active','finalizing') AND EXISTS(SELECT 1 FROM jsonb_array_elements(p.windows) w WHERE w->>'token'=$1))`, id).Scan(&active); err != nil {
			return err
		}
		if active {
			return nil
		}
	}
	_, state, _, err := apiKeyBudgetIdentity(ctx, tx, id)
	if err != nil {
		return err
	}
	if state == "captured" || state == "released" {
		return nil
	}
	if kind == "live" && n > 0 && settled && actual > 0 {
		if err = CaptureAPIKeyBudgetTx(ctx, tx, id, actual); err != nil {
			return err
		}
		return tx.Commit()
	}
	if (n > 0 && zero) || (notSent && (state == "reserved" || state == "prepared")) {
		if err = ReleaseAPIKeyBudgetTx(ctx, tx, id); err != nil {
			return err
		}
	} else {
		if _, err = tx.ExecContext(ctx, `UPDATE api_key_budget_authorization SET state='unknown',updated_at=now() WHERE authorization_id=$1 AND state IN ('written','unknown')`, id); err != nil {
			return err
		}
	}
	return tx.Commit()
}
func (b *CanonicalWalletBridge) runAPIKeyBudgetRecovery() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-b.stop:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
			err := RecoverAPIKeyBudgets(ctx, b.outboxDB, 32)
			if err == nil {
				err = b.recoverAPIKeyBudgetNoWriteWallet(ctx)
			}
			cancel()
			if err != nil {
				slog.Warn("API Key allowance recovery deferred", "error", err)
			}
		}
	}
}

func InitializeAPIKeyBudgetEpoch(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `SELECT DISTINCT api_key_id FROM wallet_billing_snapshot WHERE api_key_id>0 ORDER BY api_key_id`)
	if err != nil {
		return err
	}
	keys := []int64{}
	for rows.Next() {
		var key int64
		if err = rows.Scan(&key); err != nil {
			break
		}
		keys = append(keys, key)
	}
	if err == nil {
		err = rows.Err()
	}
	if closeErr := rows.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	for _, key := range keys {
		tx, e := db.BeginTx(ctx, nil)
		if e != nil {
			return e
		}
		e = lockAPIKeyBudget(ctx, tx, key)
		if e == nil {
			e = BackfillAPIKeyBudgetTx(ctx, tx, key)
		}
		if errors.Is(e, ErrAPIKeyBudgetReconciliation) {
			_, e = tx.ExecContext(ctx, `INSERT INTO api_key_budget_epoch(api_key_id,blocked_reason) VALUES($1,'historical identity unavailable') ON CONFLICT(api_key_id) DO UPDATE SET checked_at=now(),blocked_reason=EXCLUDED.blocked_reason`, key)
		}
		if e != nil {
			_ = tx.Rollback()
			return e
		}
		if e = tx.Commit(); e != nil {
			return e
		}
	}
	return nil
}

// Wallet cleanup has its own durable discovery even when media admission and
// workers are disabled. It reuses the original pin/hold/source finalizers only
// for ledger-fenced, tokenless parents; unknown writes are untouched here.
func (b *CanonicalWalletBridge) recoverAPIKeyBudgetNoWriteWallet(ctx context.Context) error {
	var available bool
	if err := b.outboxDB.QueryRowContext(ctx, `SELECT to_regclass('api_key_budget_authorization') IS NOT NULL AND to_regclass('wallet_authorization_segment') IS NOT NULL`).Scan(&available); err != nil {
		return err
	}
	if !available {
		return nil
	}
	rows, err := b.outboxDB.QueryContext(ctx, `SELECT a.authorization_id FROM api_key_budget_authorization a WHERE ((a.kind='live' AND a.state='captured') OR (a.state='released' AND (a.written_at IS NULL OR a.kind='live' AND EXISTS(SELECT 1 FROM wallet_authorization_segment z WHERE z.parent_authorization_id=a.authorization_id AND z.terminal_evidence->>'source'='live_final_usage_zero')))) AND a.kind IN ('llm','live') AND EXISTS(SELECT 1 FROM wallet_authorization_segment s WHERE s.parent_authorization_id=a.authorization_id AND s.state<>'finished') AND (a.state='captured' OR NOT EXISTS(SELECT 1 FROM wallet_authorization_segment s WHERE s.parent_authorization_id=a.authorization_id AND s.authorization_token IS NOT NULL AND (s.kind<>'live' OR COALESCE(s.terminal_evidence->>'source','')<>'live_final_usage_zero'))) ORDER BY a.updated_at,a.authorization_id LIMIT 4`)
	if err != nil {
		return err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			break
		}
		ids = append(ids, id)
	}
	if err == nil {
		err = rows.Err()
	}
	if closeErr := rows.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	for _, id := range ids {
		b.recoverPoolAttempts(ctx, id)
	}
	return b.completeRecoveredLiveZeroFinalizations(ctx)
}
