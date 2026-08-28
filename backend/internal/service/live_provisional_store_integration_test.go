//go:build integration

package service

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

func startLiveProvisionalTestPostgres(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()
	container, err := tcpostgres.Run(ctx, "postgres:18.1-alpine3.23",
		tcpostgres.WithDatabase("live_provisional_test"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		tcpostgres.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(ctx) })
	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	db, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.PingContext(ctx))

	sqlContent, err := os.ReadFile(filepath.Join("..", "..", "migrations", "211_wallet_live_provisional.sql"))
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, string(sqlContent))
	require.NoError(t, err)
	return db
}

func TestLiveProvisionalStoreLifecycle(t *testing.T) {
	ctx := context.Background()
	db := startLiveProvisionalTestPostgres(t, ctx)
	store := newLiveProvisionalStore(db)
	require.NotNil(t, store)

	token := "auth_" + uuid.NewString()
	authID := token
	platformUserID := "shipany-user-" + uuid.NewString()
	now := time.Now().UTC().Truncate(time.Microsecond)

	rec := &LiveProvisionalRecord{
		Token:             token,
		AuthorizationID:   authID,
		CallHash:          "",
		PlatformUserID:    platformUserID,
		UserID:            101,
		APIKeyID:          202,
		AccountID:         303,
		BillingCurrency:   "CNY",
		BillingSnapshotID: "snap_123",
		EstimatedUnits:    50000,
		Status:            LiveProvisionalStatusProvisional,
		Windows: []LiveWindow{
			{WindowSeq: 1, LeaseID: "", Token: token, PendingUnits: 50000, SettledUnits: 0},
		},
		SettlementEventID: "",
		CreatedAt:         now,
	}

	// 1. Save -> Get round-trips every field incl. Windows
	require.NoError(t, store.Save(ctx, rec))
	got, err := store.Get(ctx, token)
	require.NoError(t, err)
	require.Equal(t, rec.Token, got.Token)
	require.Equal(t, rec.AuthorizationID, got.AuthorizationID)
	require.Equal(t, "", got.CallHash)
	require.Equal(t, rec.PlatformUserID, got.PlatformUserID)
	require.Equal(t, rec.UserID, got.UserID)
	require.Equal(t, rec.APIKeyID, got.APIKeyID)
	require.Equal(t, rec.AccountID, got.AccountID)
	require.Equal(t, rec.BillingCurrency, got.BillingCurrency)
	require.Equal(t, rec.BillingSnapshotID, got.BillingSnapshotID)
	require.Equal(t, rec.EstimatedUnits, got.EstimatedUnits)
	require.Equal(t, LiveProvisionalStatusProvisional, got.Status)
	require.Len(t, got.Windows, 1)
	require.Equal(t, 1, got.Windows[0].WindowSeq)
	require.Equal(t, token, got.Windows[0].Token)
	require.Equal(t, int64(50000), got.Windows[0].PendingUnits)
	require.Equal(t, int64(0), got.Windows[0].SettledUnits)
	require.WithinDuration(t, rec.CreatedAt, got.CreatedAt, time.Millisecond)

	// 2. Save twice with one token is a no-op
	require.NoError(t, store.Save(ctx, rec))

	// 3. Two different tokens with call_hash = '' both insert (the partial index)
	token2 := "auth_" + uuid.NewString()
	rec2 := &LiveProvisionalRecord{
		Token:             token2,
		AuthorizationID:   token2,
		CallHash:          "",
		PlatformUserID:    platformUserID,
		UserID:            101,
		APIKeyID:          202,
		AccountID:         303,
		BillingCurrency:   "CNY",
		BillingSnapshotID: "snap_123",
		EstimatedUnits:    50000,
		Status:            LiveProvisionalStatusProvisional,
		Windows: []LiveWindow{
			{WindowSeq: 1, LeaseID: "", Token: token2, PendingUnits: 50000, SettledUnits: 0},
		},
		CreatedAt: now,
	}
	require.NoError(t, store.Save(ctx, rec2))

	// 4. Activate then Abort fails (status)
	callHash := "call_hash_" + uuid.NewString()
	actTime := time.Now().UTC().Truncate(time.Microsecond)
	require.NoError(t, store.Activate(ctx, token, callHash, actTime))

	gotActive, err := store.Get(ctx, token)
	require.NoError(t, err)
	require.Equal(t, LiveProvisionalStatusActive, gotActive.Status)
	require.Equal(t, callHash, gotActive.CallHash)
	require.NotNil(t, gotActive.ActivatedAt)

	// Abort on active row must fail
	abortErr := store.Abort(ctx, token, time.Now().UTC())
	require.Error(t, abortErr)
	require.ErrorIs(t, abortErr, ErrLiveProvisionalNotFound)

	// 5. GetByCallHash finds the activated row
	byHash, err := store.GetByCallHash(ctx, callHash)
	require.NoError(t, err)
	require.Equal(t, token, byHash.Token)
	require.Equal(t, callHash, byHash.CallHash)

	// 6. ClaimFinalization on active -> true once, false the second time
	claim1, err := store.ClaimFinalization(ctx, token, time.Now().UTC())
	require.NoError(t, err)
	require.True(t, claim1)

	claim2, err := store.ClaimFinalization(ctx, token, time.Now().UTC())
	require.NoError(t, err)
	require.False(t, claim2, "second claim on finalizing row must return false")

	// 7. ReleaseFinalizationClaim returns the row to active and a second claim succeeds
	require.NoError(t, store.ReleaseFinalizationClaim(ctx, token))
	gotReleased, err := store.Get(ctx, token)
	require.NoError(t, err)
	require.Equal(t, LiveProvisionalStatusActive, gotReleased.Status)

	claim3, err := store.ClaimFinalization(ctx, token, time.Now().UTC())
	require.NoError(t, err)
	require.True(t, claim3)

	// 8. CompleteFinalization writes the event id and windows[0].settled_units
	eventID := "gwusg_" + uuid.NewString()
	termTime := time.Now().UTC().Truncate(time.Microsecond)
	require.NoError(t, store.CompleteFinalization(ctx, token, eventID, 42000, termTime))

	gotFinal, err := store.Get(ctx, token)
	require.NoError(t, err)
	require.Equal(t, LiveProvisionalStatusFinalized, gotFinal.Status)
	require.Equal(t, eventID, gotFinal.SettlementEventID)
	require.NotNil(t, gotFinal.TerminalAt)
	require.Len(t, gotFinal.Windows, 1)
	require.Equal(t, int64(42000), gotFinal.Windows[0].SettledUnits)

	// 9. Abort path on provisional rec2
	term2 := time.Now().UTC().Truncate(time.Microsecond)
	require.NoError(t, store.Abort(ctx, token2, term2))
	gotAborted, err := store.Get(ctx, token2)
	require.NoError(t, err)
	require.Equal(t, LiveProvisionalStatusAborted, gotAborted.Status)
	require.NotNil(t, gotAborted.TerminalAt)
}
