//go:build unit

package service

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestWalletImmediateLegacyShadowRequiresDurableReaderHandoff(t *testing.T) {
	dsn := os.Getenv("UNIROUTE_MEDIA_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("isolated PostgreSQL fixture is not configured")
	}
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	require.Contains(t, []string{"127.0.0.1", "localhost", "::1"}, u.Hostname())
	require.True(t, strings.Contains(u.Path, "test"), "refuse a non-test database")
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(1) // session-local temporary tables, no fixture schema changes
	ctx := context.Background()
	_, err = db.ExecContext(ctx, `CREATE TEMP TABLE wallet_authorization_segment(
		authorization_id text,parent_authorization_id text,ordinal integer,kind text,state text,
		terminal_sealed_at timestamptz,reader_started boolean,reader_handoff_at timestamptz,
		evidence_pending boolean,fee_pending boolean,known_fee_units bigint,expiry_deadline timestamptz,
		write_ended_at timestamptz,first_write_at timestamptz,legacy_completion_proof text,
		write_active_until timestamptz,actual_units bigint,settlement_payload text,remainder_payload text,
		event_id text,held_units bigint,expiry_intent_version integer,updated_at timestamptz);
		CREATE TEMP TABLE wallet_risk_admission(parent_authorization_id text);
		CREATE TEMP TABLE wallet_settlement_outbox(event_id text)`)
	require.NoError(t, err)
	bridge := &CanonicalWalletBridge{outboxDB: db, cfg: config.CanonicalWalletConfig{PoolExpiryMode: "enabled", LLMImmediateReleaseMode: "shadow"}}
	for _, tc := range []struct {
		name                                                            string
		reader, sealed, handedOff, admitted, pending, feePending, known bool
		eligible                                                        bool
	}{
		{name: "legacy_no_reader", eligible: true},
		{name: "shadow_started_unjoined", reader: true},
		{name: "shadow_sealed_without_handoff", reader: true, sealed: true},
		{name: "shadow_handoff", reader: true, sealed: true, handedOff: true, eligible: true},
		{name: "enabled_ownership_survives_flag_off", reader: true, sealed: true, handedOff: true, admitted: true},
		{name: "evidence_pending", reader: true, sealed: true, handedOff: true, pending: true},
		{name: "fee_pending", reader: true, sealed: true, handedOff: true, feePending: true},
		{name: "known_fee", reader: true, sealed: true, handedOff: true, known: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.ExecContext(ctx, `TRUNCATE wallet_authorization_segment,wallet_risk_admission`)
			require.NoError(t, err)
			_, err = db.ExecContext(ctx, `INSERT INTO wallet_authorization_segment VALUES(
				'auth','parent',0,'llm','indeterminate',CASE WHEN $1 THEN now() END,$2,CASE WHEN $3 THEN now() END,
				$4,$5,CASE WHEN $6 THEN 1 END,now()-interval '1 hour',now()-interval '25 hours',now()-interval '26 hours',NULL,
				NULL,0,NULL,NULL,'event',100,NULL,now())`, tc.sealed, tc.reader, tc.handedOff, tc.pending, tc.feePending, tc.known)
			require.NoError(t, err)
			if tc.admitted {
				_, err = db.ExecContext(ctx, `INSERT INTO wallet_risk_admission VALUES('parent')`)
				require.NoError(t, err)
			}
			claimed, err := bridge.claimPoolExpiry(ctx, "parent")
			require.NoError(t, err)
			require.Equal(t, tc.eligible, claimed)
			var state string
			require.NoError(t, db.QueryRowContext(ctx, `SELECT state FROM wallet_authorization_segment`).Scan(&state))
			if tc.eligible {
				require.Equal(t, "expiry_pending", state)
			} else {
				require.Equal(t, "indeterminate", state)
			}
		})
	}
}
