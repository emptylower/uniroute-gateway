//go:build integration

package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// Phase 3.7c: the integration suite shares ONE Postgres container per run.
// Isolation is per-database: every helper call CREATEs a database on the
// shared container (~50 ms) and applies the caller's DDL, so each caller
// still sees a fresh *sql.DB with exactly the tables it expects — the
// helpers' contracts are unchanged. The databases are dropped by the
// container's teardown in TestMain, not per test (dropping would need to
// wait for the caller's connections and buys nothing).
//
// SharedTestPostgresDBForTest / SharedTestRedisClientForTest are exported
// (ForTest, the openai_live_export_test.go convention) because
// openai_live_restart_integration_test.go lives in the external service_test
// package and cannot call unexported identifiers of this one.
var sharedPG struct {
	once      sync.Once
	container *tcpostgres.PostgresContainer
	base      string
	admin     *sql.DB
	err       error
	seq       atomic.Int64
}

// SharedTestPostgresDBForTest returns a *sql.DB on a freshly created database
// of the shared Postgres container, closed at t.Cleanup. The database itself
// lives until the container terminates at process exit.
func SharedTestPostgresDBForTest(t testing.TB) *sql.DB {
	t.Helper()
	sharedPG.once.Do(func() {
		ctx := context.Background()
		c, err := tcpostgres.Run(ctx, "postgres:18.1-alpine3.23",
			tcpostgres.WithDatabase("postgres"),
			tcpostgres.WithUsername("postgres"),
			tcpostgres.WithPassword("postgres"),
			tcpostgres.BasicWaitStrategies(),
		)
		if err != nil {
			sharedPG.err = err
			return
		}
		sharedPG.container = c
		connStr, err := c.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			sharedPG.err = err
			return
		}
		sharedPG.base = connStr
		admin, err := sql.Open("postgres", connStr)
		if err != nil {
			sharedPG.err = err
			return
		}
		sharedPG.admin = admin
	})
	require.NoError(t, sharedPG.err)
	ctx := context.Background()
	name := fmt.Sprintf("t%d_%d", os.Getpid(), sharedPG.seq.Add(1))
	_, err := sharedPG.admin.ExecContext(ctx, "CREATE DATABASE "+name)
	require.NoError(t, err)
	u, err := url.Parse(sharedPG.base)
	require.NoError(t, err)
	u.Path = "/" + name
	db, err := sql.Open("postgres", u.String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.PingContext(ctx))
	return db
}

// Phase 3.7c: the ONE shared Redis container per run. Isolation is FLUSHDB
// per helper call — legal because every integration test in this package is
// sequential (Task 0 grep: no t.Parallel()) and every bridge is closed at
// its test's t.Cleanup (Task 0 inventory; 3.7a's Close waits for both loop
// goroutines), so no goroutine from a finished test can race the next
// test's FLUSHDB. The reaper's SETNX leader key and hold_users are global
// and are flushed with the rest, so each test sees exactly the empty
// global keyspace a fresh instance would present.
var sharedRedis struct {
	once      sync.Once
	container *tcredis.RedisContainer
	addr      string
	err       error
	live      atomic.Bool
}

var errSharedRedisInUse = errors.New("shared Redis is in use by another live test — integration tests must run sequentially")

// claimSharedRedis acquires the one live claim; releaseSharedRedis releases
// it (idempotent — a double release is a no-op). The guard's contract: one
// LIVE claim at a time, not one call per run — t.Cleanup releases it, so
// sequential subtests and sequential tests are fine and only genuine
// concurrency trips it (two simultaneously-live clients would let the
// second call's FLUSHDB wipe the first's keyspace mid-test).
func claimSharedRedis() error {
	if !sharedRedis.live.CompareAndSwap(false, true) {
		return errSharedRedisInUse
	}
	return nil
}

func releaseSharedRedis() {
	sharedRedis.live.Store(false)
}

// SharedTestRedisClientForTest returns a *redis.Client on the shared
// instance with an EMPTY keyspace (FLUSHDB before the client is handed
// out), closed and released at t.Cleanup.
func SharedTestRedisClientForTest(t testing.TB) *redis.Client {
	t.Helper()
	sharedRedis.once.Do(func() {
		ctx := context.Background()
		c, err := tcredis.Run(ctx, "redis:8.4-alpine")
		if err != nil {
			sharedRedis.err = err
			return
		}
		sharedRedis.container = c
		host, err := c.Host(ctx)
		if err != nil {
			sharedRedis.err = err
			return
		}
		port, err := c.MappedPort(ctx, "6379/tcp")
		if err != nil {
			sharedRedis.err = err
			return
		}
		sharedRedis.addr = fmt.Sprintf("%s:%d", host, port.Int())
	})
	require.NoError(t, sharedRedis.err)
	if err := claimSharedRedis(); err != nil {
		t.Fatalf("%s", err)
	}
	client := redis.NewClient(&redis.Options{Addr: sharedRedis.addr})
	t.Cleanup(func() { _ = client.Close(); releaseSharedRedis() })
	ctx := context.Background()
	require.NoError(t, client.FlushDB(ctx).Err())
	require.NoError(t, client.Ping(ctx).Err())
	return client
}

// TestMain terminates the shared containers after the whole run. The
// unit-tagged TestMain in billing_snapshot_record_path_test.go never
// compiles together with this file (disjoint build tags).
func TestMain(m *testing.M) {
	code := m.Run()
	ctx := context.Background()
	if sharedPG.admin != nil {
		_ = sharedPG.admin.Close()
	}
	if sharedPG.container != nil {
		_ = sharedPG.container.Terminate(ctx)
	}
	if sharedRedis.container != nil {
		_ = sharedRedis.container.Terminate(ctx)
	}
	os.Exit(code)
}

// Phase 3.7c: two calls to the Postgres helper in one test must land in two
// DIFFERENT databases on the ONE shared container — the outbox DDL present in
// both, a row visible in only one, and the container itself reused.
func TestPhase37cSharedPostgresIsolation(t *testing.T) {
	ctx := context.Background()
	db1 := startCanonicalWalletTestPostgres(t, ctx)
	id1 := sharedPG.container.GetContainerID()
	db2 := startCanonicalWalletTestPostgres(t, ctx)
	require.Equal(t, id1, sharedPG.container.GetContainerID(), "second call must reuse the shared container")

	var name1, name2 string
	require.NoError(t, db1.QueryRowContext(ctx, `SELECT current_database()`).Scan(&name1))
	require.NoError(t, db2.QueryRowContext(ctx, `SELECT current_database()`).Scan(&name2))
	require.NotEqual(t, name1, name2, "each helper call must get its own database")

	var n int
	require.NoError(t, db1.QueryRowContext(ctx, `SELECT count(*) FROM wallet_settlement_outbox`).Scan(&n))
	require.Equal(t, 0, n)
	require.NoError(t, db2.QueryRowContext(ctx, `SELECT count(*) FROM wallet_settlement_outbox`).Scan(&n))
	require.Equal(t, 0, n, "both databases must have the outbox table, empty")

	_, err := db1.ExecContext(ctx, `INSERT INTO wallet_settlement_outbox (event_id, platform_user_id, gateway_request_id, currency, amount_units, payload_hash, occurred_at)
		VALUES ('p37c-iso-1', 'p37c-user', 'p37c-req', 'CNY', 1, 'p37c-hash', now())`)
	require.NoError(t, err)
	require.NoError(t, db1.QueryRowContext(ctx, `SELECT count(*) FROM wallet_settlement_outbox`).Scan(&n))
	require.Equal(t, 1, n)
	require.NoError(t, db2.QueryRowContext(ctx, `SELECT count(*) FROM wallet_settlement_outbox`).Scan(&n))
	require.Equal(t, 0, n, "a row inserted in one database must be invisible in the other")
}

// Phase 3.7c: the shared Redis gives every helper call an EMPTY keyspace
// (FLUSHDB per call) on the ONE shared container, and the sequential-only
// guard trips on a second simultaneously-live claim. All subtests are
// sequential — no t.Parallel() anywhere in this package (Task 0 grep).
func TestPhase37cSharedRedisIsolation(t *testing.T) {
	ctx := context.Background()
	var containerID string

	t.Run("first client sees its own keyspace", func(t *testing.T) {
		c1 := startCanonicalWalletTestRedis(t, ctx)
		containerID = sharedRedis.container.GetContainerID()
		require.NoError(t, c1.Set(ctx, "p37c-sentinel", "1", 0).Err())
		v, err := c1.Get(ctx, "p37c-sentinel").Result()
		require.NoError(t, err)
		require.Equal(t, "1", v)
	})

	t.Run("second call flushed the keyspace and reused the container", func(t *testing.T) {
		c2 := startCanonicalWalletTestRedis(t, ctx)
		require.Equal(t, containerID, sharedRedis.container.GetContainerID(), "second call must reuse the shared container")
		v, err := c2.Get(ctx, "p37c-sentinel").Result()
		require.ErrorIs(t, err, redis.Nil, "the second call's FLUSHDB must have removed the sentinel")
		require.Empty(t, v)
	})

	t.Run("guard trips on a second live claim", func(t *testing.T) {
		// The guard's contract: one LIVE claim at a time — t.Cleanup
		// releases it, so sequential tests are fine and only genuine
		// concurrency trips it. Drive the claim state directly; the
		// second claim fails and holds nothing, so releasing once (the
		// deferred release is a no-op) lets the run continue.
		require.NoError(t, claimSharedRedis())
		defer releaseSharedRedis()
		err := claimSharedRedis()
		require.Error(t, err)
		require.Contains(t, err.Error(), "shared Redis is in use by another live test — integration tests must run sequentially")
		releaseSharedRedis()
	})
}
