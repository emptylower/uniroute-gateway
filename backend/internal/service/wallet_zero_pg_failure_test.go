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

type walletZeroCleanupProbe struct {
	CanonicalWalletLeaseStore
	calls int
}

func (s *walletZeroCleanupProbe) ReleaseCanonicalWalletHold(context.Context, string, string, string, string) (int64, error) {
	s.calls++
	return 1, nil
}

func TestWalletKnownZeroActualPGOwnershipReadFailureCannotRelease(t *testing.T) {
	dsn := os.Getenv("UNIROUTE_MEDIA_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("isolated PostgreSQL fixture is not configured")
	}
	endpoint, err := url.Parse(dsn)
	require.NoError(t, err)
	require.Contains(t, []string{"localhost", "127.0.0.1", "::1"}, endpoint.Hostname())
	require.Contains(t, endpoint.Path, "test")
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(1)
	// Session-local schema damage creates a genuine PG ownership-query error
	// without changing shared test tables, connections, or financial rows.
	_, err = db.Exec(`CREATE TEMP TABLE wallet_authorization_segment(parent_authorization_id text,kind text,state text); INSERT INTO wallet_authorization_segment VALUES('auth-pg-fault','llm','indeterminate')`)
	require.NoError(t, err)
	store := &walletZeroCleanupProbe{}
	bridge := &CanonicalWalletBridge{outboxDB: db, store: store, cfg: config.CanonicalWalletConfig{LLMImmediateReleaseMode: "off"}}
	err = bridge.releasePoolAttempt(context.Background(), "auth-pg-fault", "user", []AuthorizationSegment{{Kind: "llm", AuthorizationID: "auth-pg-fault"}}, "auth-pg-fault.1")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "does not exist"), err)
	require.Zero(t, store.calls, "an actual PG read error must not select legacy Redis cleanup")
	var state string
	require.NoError(t, db.QueryRow(`SELECT state FROM wallet_authorization_segment`).Scan(&state))
	require.Equal(t, "indeterminate", state)
}
