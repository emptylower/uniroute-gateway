//go:build integration

package repository

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestBackfillOutboxBillingSnapshotGuardsAmbiguity (Phase 4.2-G Task 1c):
// the one-shot operator backfill that ships OUTSIDE migration 216 (round-1
// MAJOR-3b — the transactional runner would hold ACCESS EXCLUSIVE on the
// outbox for the whole join) is executed HERE against seeded data, straight
// from the shipped file, so the SQL that will run in production is the SQL
// this test proves:
//
//   - a row with exactly ONE usage_logs match (u.request_id =
//     o.gateway_request_id, u.billing_snapshot_id IS NOT NULL) gains the id;
//   - a row with TWO matching logs (two api keys, one request id — the
//     (request_id, api_key_id) uniqueness of migration 027) stays NULL —
//     ambiguous is never backfilled;
//   - a row with NO match (pre-210 or unlogged) stays NULL — the expected
//     NULL rate, not an error;
//   - a second run of the same file changes nothing (idempotent).
//
// usage_logs is the REAL table (the harness applies every migration; its
// api_key_id references api_keys, seeded through the same users→api_keys
// pattern the inventory tests use — one user, two keys, cascade-cleaned).
func TestBackfillOutboxBillingSnapshotGuardsAmbiguity(t *testing.T) {
	resetWalletOutboxTable(t)
	ctx := context.Background()
	suffix := fmt.Sprintf("76c-%d", time.Now().UnixNano())

	var userID, accountID int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO users (email, password_hash, billing_currency) VALUES ($1, 'hash', 'CNY') RETURNING id`,
		"backfill-76c-"+suffix+"@example.invalid").Scan(&userID))
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		INSERT INTO accounts (name, platform, type) VALUES ($1, 'anthropic', 'api_key') RETURNING id`,
		"backfill-76c-"+suffix).Scan(&accountID))
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(context.Background(), `DELETE FROM users WHERE id = $1`, userID)
		_, _ = integrationDB.ExecContext(context.Background(), `DELETE FROM accounts WHERE id = $1`, accountID)
	})
	newKey := func(name string) int64 {
		var id int64
		require.NoError(t, integrationDB.QueryRowContext(ctx, `
			INSERT INTO api_keys (user_id, key, name) VALUES ($1, $2, $3) RETURNING id`,
			userID, "sk-backfill-76c-"+name+"-"+suffix, name).Scan(&id))
		return id
	}
	keyOne := newKey("one")
	keyTwoA := newKey("two-a")
	keyTwoB := newKey("two-b")
	seedLog := func(keyID int64, requestID, snapshotID string) {
		var snap any
		if snapshotID != "" {
			snap = snapshotID
		}
		_, err := integrationDB.ExecContext(ctx, `
			INSERT INTO usage_logs (user_id, api_key_id, account_id, request_id, model, upstream_model, actual_cost, settlement_currency, billing_snapshot_id, created_at)
			VALUES ($1, $2, $3, $4, 'claude-sonnet-4', 'claude-sonnet-4', 1.0, 'CNY', $5, now())`,
			userID, keyID, accountID, requestID, snap)
		require.NoError(t, err)
	}

	occurred := time.Now().UTC().Add(-time.Hour)
	seedOutbox := func(eventID, requestID string) {
		_, err := integrationDB.ExecContext(ctx, `
			INSERT INTO wallet_settlement_outbox
				(event_id, platform_user_id, gateway_request_id, currency, amount_units, payload_hash, status, occurred_at)
			VALUES ($1, 'shipany-user-76c', $2, 'CNY', 1000, 'hash-76c', 'delivered', $3)`,
			eventID, requestID, occurred)
		require.NoError(t, err)
	}
	seedOutbox("gwusg_76c_one", "req-76c-one-"+suffix)
	seedOutbox("gwusg_76c_two", "req-76c-two-"+suffix)
	seedOutbox("gwusg_76c_none", "req-76c-none-"+suffix)

	// Exactly one match for req-…-one.
	seedLog(keyOne, "req-76c-one-"+suffix, "wbs_76c_single")
	// TWO matches for req-…-two — different api keys, the same request id:
	// both unique under 027's (request_id, api_key_id) index, and jointly
	// ambiguous.
	seedLog(keyTwoA, "req-76c-two-"+suffix, "wbs_76c_a")
	seedLog(keyTwoB, "req-76c-two-"+suffix, "wbs_76c_b")
	// No match for req-…-none.

	// The shipped script, executed as the operator runs it. Each
	// semicolon-terminated statement is applied in file order.
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "deploy", "scripts", "wallet-outbox-backfill-billing-snapshot.sql"))
	require.NoError(t, err, "the operator backfill script ships at deploy/scripts/wallet-outbox-backfill-billing-snapshot.sql")
	runScript := func() {
		for _, stmt := range strings.Split(string(raw), ";") {
			if strings.TrimSpace(stmt) == "" {
				continue
			}
			_, err := integrationDB.ExecContext(ctx, stmt)
			require.NoError(t, err, "backfill statement must apply: %s", truncateForLog(stmt))
		}
	}
	runScript()

	snapFor := func(eventID string) string {
		var snapID sql.NullString
		require.NoError(t, integrationDB.QueryRowContext(ctx,
			`SELECT billing_snapshot_id FROM wallet_settlement_outbox WHERE event_id = $1`, eventID).Scan(&snapID))
		if !snapID.Valid {
			return ""
		}
		return snapID.String
	}
	require.Equal(t, "wbs_76c_single", snapFor("gwusg_76c_one"), "exactly one match: backfilled")
	require.Equal(t, "", snapFor("gwusg_76c_two"), "two matches: ambiguous, never backfilled")
	require.Equal(t, "", snapFor("gwusg_76c_none"), "no match: expected NULL rate, not an error")

	// Idempotence: a second execution of the same file changes nothing
	// (the guard's o.billing_snapshot_id IS NULL clause).
	runScript()
	require.Equal(t, "wbs_76c_single", snapFor("gwusg_76c_one"))
	require.Equal(t, "", snapFor("gwusg_76c_two"))
	require.Equal(t, "", snapFor("gwusg_76c_none"))
}

func truncateForLog(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 120 {
		return s[:120] + "…"
	}
	return s
}
