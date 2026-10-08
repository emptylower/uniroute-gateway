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
)

func startLiveProvisionalTestPostgres(t testing.TB, ctx context.Context) *sql.DB {
	t.Helper()
	// Phase 3.7c: a database on the one shared container per run; the
	// migration below is unchanged (this helper's contract is a fresh
	// *sql.DB with migration 211 applied).
	db := SharedTestPostgresDBForTest(t)

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
		BillingCurrency:   "USD",
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
		BillingCurrency:   "USD",
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
	require.NoError(t, store.CompleteFinalization(ctx, token, eventID, 42000, 1, termTime))

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

// TestLiveProvisionalWindowCAS (Phase 3.7b Task 2, redesign §13.2.2): the two
// single-statement window CASes and the seq-aware CompleteFinalization.
// SetLiveWindowPending writes pending_units only on the live window of an
// active record; AdvanceLiveWindow settles window seq and appends the next
// window in ONE statement, and its length guard makes a repeat (or two racing
// observers) lose the CAS; CompleteFinalization writes windows[seq-1], and a
// one-window record under seq 1 is byte-for-byte today's behaviour.
func TestLiveProvisionalWindowCAS(t *testing.T) {
	ctx := context.Background()
	db := startLiveProvisionalTestPostgres(t, ctx)
	store := newLiveProvisionalStore(db)
	require.NotNil(t, store)

	newRecord := func(status LiveProvisionalStatus, windows []LiveWindow) (*LiveProvisionalRecord, string) {
		t.Helper()
		token := "auth_" + uuid.NewString()
		callHash := "call_hash_" + uuid.NewString()
		rec := &LiveProvisionalRecord{
			Token: token, AuthorizationID: token,
			CallHash: callHash, PlatformUserID: "shipany-user-" + uuid.NewString(),
			UserID: 101, APIKeyID: 202, AccountID: 303,
			BillingCurrency: "USD", BillingSnapshotID: "snap_cas",
			EstimatedUnits: 50000, Status: status, Windows: windows,
			CreatedAt: time.Now().UTC().Truncate(time.Microsecond),
		}
		require.NoError(t, store.Save(ctx, rec))
		return rec, token
	}

	// (a) SetLiveWindowPending
	recA, tokenA := newRecord(LiveProvisionalStatusProvisional, []LiveWindow{{WindowSeq: 1, Token: "T1"}})
	require.NoError(t, store.Activate(ctx, tokenA, recA.CallHash, time.Now().UTC()))
	require.NoError(t, store.SetLiveWindowPending(ctx, tokenA, 1, 700))
	got, err := store.Get(ctx, tokenA)
	require.NoError(t, err)
	require.Equal(t, int64(700), got.Windows[0].PendingUnits)

	// seq 2 on a one-window record: the length guard loses the CAS, nothing written
	require.ErrorIs(t, store.SetLiveWindowPending(ctx, tokenA, 2, 700), ErrLiveWindowCASLost)
	got, err = store.Get(ctx, tokenA)
	require.NoError(t, err)
	require.Len(t, got.Windows, 1)
	require.Equal(t, int64(700), got.Windows[0].PendingUnits)

	// a finalizing record loses the CAS (status guard)
	recF, tokenF := newRecord(LiveProvisionalStatusProvisional, []LiveWindow{{WindowSeq: 1, Token: "TF"}})
	require.NoError(t, store.Activate(ctx, tokenF, recF.CallHash, time.Now().UTC()))
	claimed, err := store.ClaimFinalization(ctx, tokenF, time.Now().UTC())
	require.NoError(t, err)
	require.True(t, claimed)
	require.ErrorIs(t, store.SetLiveWindowPending(ctx, tokenF, 1, 700), ErrLiveWindowCASLost)
	gotF, err := store.Get(ctx, tokenF)
	require.NoError(t, err)
	require.Equal(t, int64(0), gotF.Windows[0].PendingUnits)

	// (b) AdvanceLiveWindow
	now := time.Now().UTC()
	next := LiveWindow{WindowSeq: 2, LeaseID: "L2", Token: "T2", OpenedAtMS: now.UnixMilli()}
	require.NoError(t, store.AdvanceLiveWindow(ctx, tokenA, 1, 700, next))
	got, err = store.Get(ctx, tokenA)
	require.NoError(t, err)
	require.Len(t, got.Windows, 2)
	require.Equal(t, int64(700), got.Windows[0].SettledUnits)
	require.Equal(t, int64(0), got.Windows[0].PendingUnits)
	require.Equal(t, 2, got.Windows[1].WindowSeq)
	require.Equal(t, "L2", got.Windows[1].LeaseID)
	require.Equal(t, "T2", got.Windows[1].Token)
	require.Equal(t, now.UnixMilli(), got.Windows[1].OpenedAtMS)

	// the repeat loses: the length guard is jsonb_array_length(windows) = seq
	require.ErrorIs(t, store.AdvanceLiveWindow(ctx, tokenA, 1, 700, next), ErrLiveWindowCASLost)
	got, err = store.Get(ctx, tokenA)
	require.NoError(t, err)
	require.Len(t, got.Windows, 2, "a lost advance must append nothing")

	// (c) CompleteFinalization writes windows[seq-1]
	claimed, err = store.ClaimFinalization(ctx, tokenA, time.Now().UTC())
	require.NoError(t, err)
	require.True(t, claimed)
	eventID := "gwusg_" + uuid.NewString()
	require.NoError(t, store.CompleteFinalization(ctx, tokenA, eventID, 900, 2, time.Now().UTC()))
	got, err = store.Get(ctx, tokenA)
	require.NoError(t, err)
	require.Equal(t, LiveProvisionalStatusFinalized, got.Status)
	require.Equal(t, int64(900), got.Windows[1].SettledUnits)
	require.Equal(t, int64(700), got.Windows[0].SettledUnits, "window 1 is untouched")
	require.Equal(t, eventID, got.SettlementEventID)
}

// BenchmarkLiveProvisionalSaveActivate measures the disclosed synchronous
// request-path cost of constraint 8: one provisional INSERT (Save) plus the
// provisional → active UPDATE (Activate) against real Postgres (testcontainers),
// fresh token and call hash per iteration so every Save is a real insert.
func BenchmarkLiveProvisionalSaveActivate(b *testing.B) {
	ctx := context.Background()
	db := startLiveProvisionalTestPostgres(b, ctx)
	store := newLiveProvisionalStore(db)

	rec := &LiveProvisionalRecord{
		UserID:            101,
		APIKeyID:          202,
		AccountID:         303,
		BillingCurrency:   "USD",
		BillingSnapshotID: "snap_bench",
		EstimatedUnits:    50000,
		CreatedAt:         time.Now().UTC().Truncate(time.Microsecond),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		token := "auth_" + uuid.NewString()
		rec.Token = token
		rec.AuthorizationID = token
		rec.CallHash = ""
		rec.Status = LiveProvisionalStatusProvisional
		rec.Windows = []LiveWindow{
			{WindowSeq: 1, LeaseID: "", Token: token, PendingUnits: 50000, SettledUnits: 0},
		}
		if err := store.Save(ctx, rec); err != nil {
			b.Fatal(err)
		}
		if err := store.Activate(ctx, token, "call_hash_"+token, time.Now().UTC()); err != nil {
			b.Fatal(err)
		}
	}
}
