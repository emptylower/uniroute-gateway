//go:build integration

package service

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// startWalletReconciliationTestPostgres (Phase 4.1-G) builds the summary's
// full read surface on one fresh database of the shared container by
// reading the REAL migration files — the outbox chain (208/212/214), the
// billing snapshot (209), Live provisional (211), hold outcomes (213), and
// 215's indexes last. Unlike startCanonicalWalletTestPostgres's inline DDL
// (which must be kept in lockstep with 215 by hand — see its comment),
// this helper executes the shipped SQL itself, so test 69's tables always
// carry exactly what production carries, indexes included.
func startWalletReconciliationTestPostgres(t testing.TB, ctx context.Context) *sql.DB {
	t.Helper()
	db := SharedTestPostgresDBForTest(t)
	for _, migration := range []string{
		"208_wallet_settlement_outbox.sql",
		"209_wallet_billing_snapshot.sql",
		"211_wallet_live_provisional.sql",
		"212_wallet_outbox_dead_letter_reason.sql",
		"213_wallet_hold_outcome.sql",
		"214_wallet_outbox_split_and_authorization.sql",
		"215_wallet_reconciliation_indexes.sql",
		"216_wallet_outbox_billing_snapshot.sql",
		"218_wallet_authorization_segments.sql",
		"219_wallet_attempt_protection.sql", "222_wallet_unknown_expiry.sql",
	} {
		applyMigrationOnceForTest(t, ctx, db, migration)
	}
	createWalletMediaTaskTableForTest(t, ctx, db) // see its comment: 217 itself needs users/api_keys
	applyWalletV5MigrationsForTest(t, ctx, db)
	return db
}

// TestWalletHoldOutcomeListByUser (Phase 4.1-G Task 1b): one row per
// resolution of migration 213's documented domain — settled (with its
// settlement_event_id), abandoned, expired, and NULL (open) — seeded
// through the store's own writers, all returned by ListHoldOutcomesByUser
// with resolution as *string.
func TestWalletHoldOutcomeListByUser(t *testing.T) {
	ctx := context.Background()
	db := startWalletHoldOutcomeTestPostgres(t, ctx)
	store := &walletHoldOutcomeStore{db: db}
	now := time.Now().UTC().Truncate(time.Microsecond)
	user := "shipany-user-hold-read"
	other := "shipany-user-hold-other"

	// settled: open row, then resolved by the settlement's event id.
	settledHold := CanonicalWalletHold{
		AuthorizationID: "auth_hold_settled", LeaseID: "lease-h1",
		HeldUnits: 11_000000, ArmedAt: now.Add(-10 * time.Minute), Class: "indeterminate", State: "armed",
	}
	require.NoError(t, store.InsertIndeterminate(ctx, settledHold, user, now.Add(-9*time.Minute)))
	require.NoError(t, store.MarkSettled(ctx, settledHold.AuthorizationID, "gwusg_hold_settled", now.Add(-8*time.Minute)))

	// abandoned: the reaper's release.
	abandonedHold := CanonicalWalletHold{
		AuthorizationID: "auth_hold_abandoned", LeaseID: "lease-h2",
		HeldUnits: 22_000000, ArmedAt: now.Add(-7 * time.Minute), Class: "", State: "armed",
	}
	require.NoError(t, store.InsertAbandoned(ctx, abandonedHold, user, now.Add(-6*time.Minute)))

	// expired: an open row older than the reaper's cutoff.
	expiredHold := CanonicalWalletHold{
		AuthorizationID: "auth_hold_expired", LeaseID: "lease-h3",
		HeldUnits: 33_000000, ArmedAt: now.Add(-5 * time.Minute), Class: "not_written", State: "armed",
	}
	require.NoError(t, store.InsertIndeterminate(ctx, expiredHold, user, now.Add(-4*time.Minute)))
	n, err := store.MarkExpiredOlderThan(ctx, now.Add(-2*time.Minute), now.Add(-2*time.Minute))
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	// open: resolution stays NULL.
	openHold := CanonicalWalletHold{
		AuthorizationID: "auth_hold_open", LeaseID: "lease-h4",
		HeldUnits: 44_000000, ArmedAt: now.Add(-1 * time.Minute), Class: "indeterminate", State: "armed",
	}
	require.NoError(t, store.InsertIndeterminate(ctx, openHold, user, now))

	// another user's row: never in this user's list.
	otherHold := CanonicalWalletHold{
		AuthorizationID: "auth_hold_other", LeaseID: "lease-h5",
		HeldUnits: 55_000000, ArmedAt: now.Add(-3 * time.Minute), Class: "indeterminate", State: "armed",
	}
	require.NoError(t, store.InsertIndeterminate(ctx, otherHold, other, now))

	rows, err := store.ListHoldOutcomesByUser(ctx, user, now.Add(-time.Hour), now.Add(time.Hour))
	require.NoError(t, err)
	require.Len(t, rows, 4, "exactly this user's four resolutions")

	byAuth := map[string]WalletHoldOutcome{}
	for _, r := range rows {
		byAuth[r.AuthorizationID] = r
	}
	require.Contains(t, byAuth, "auth_hold_settled")
	require.Contains(t, byAuth, "auth_hold_abandoned")
	require.Contains(t, byAuth, "auth_hold_expired")
	require.Contains(t, byAuth, "auth_hold_open")

	s := byAuth["auth_hold_settled"]
	require.Equal(t, user, s.PlatformUserID)
	require.Equal(t, "lease-h1", s.LeaseID)
	require.Equal(t, int64(11_000000), s.HeldUnits)
	require.Equal(t, "indeterminate", s.Class)
	require.NotNil(t, s.Resolution)
	require.Equal(t, "settled", *s.Resolution)
	require.NotNil(t, s.ResolvedAt)
	require.NotNil(t, s.SettlementEventID)
	require.Equal(t, "gwusg_hold_settled", *s.SettlementEventID)
	require.WithinDuration(t, now.Add(-10*time.Minute), s.ArmedAt, time.Second)
	require.WithinDuration(t, now.Add(-9*time.Minute), s.ClassifiedAt, time.Second)

	a := byAuth["auth_hold_abandoned"]
	require.NotNil(t, a.Resolution)
	require.Equal(t, "abandoned", *a.Resolution)
	require.NotNil(t, a.ResolvedAt)
	require.Nil(t, a.SettlementEventID)
	require.Empty(t, a.Class, "the unclassified orphan's class is stored as it was")

	e := byAuth["auth_hold_expired"]
	require.NotNil(t, e.Resolution)
	require.Equal(t, "expired", *e.Resolution)
	require.Nil(t, e.SettlementEventID)

	o := byAuth["auth_hold_open"]
	require.Nil(t, o.Resolution, "an open hold's resolution is NULL")
	require.Nil(t, o.ResolvedAt)
	require.Nil(t, o.SettlementEventID)

	// the window is over armed_at, half-open
	onlyLate, err := store.ListHoldOutcomesByUser(ctx, user, now.Add(-90*time.Second), now.Add(time.Hour))
	require.NoError(t, err)
	require.Len(t, onlyLate, 1)
	require.Equal(t, "auth_hold_open", onlyLate[0].AuthorizationID)

	otherRows, err := store.ListHoldOutcomesByUser(ctx, other, now.Add(-time.Hour), now.Add(time.Hour))
	require.NoError(t, err)
	require.Len(t, otherRows, 1)
	require.Equal(t, "auth_hold_other", otherRows[0].AuthorizationID)
}

// TestLiveProvisionalListByUser (Phase 4.1-G Task 1c): a record with two
// windows seeded by a direct INSERT mirroring migration 211's shape
// (saveLiveProvisional is a gateway-service method needing an
// authorization handle — not a seeding path), returned with both windows
// decoded and the fx LEFT JOIN degrading to nil for an empty
// billing_snapshot_id (round-3 fx fold; the snapshot-backed leg is test
// 69's).
func TestLiveProvisionalListByUser(t *testing.T) {
	ctx := context.Background()
	db := startWalletReconciliationTestPostgres(t, ctx)
	// the concrete store, not the LiveProvisionalStore port: the read
	// method is part of the reconciliation read model, not the gateway's
	// durable-port surface (the port's callers never list by user).
	store := &liveProvisionalStore{db: db}
	require.NotNil(t, store)

	user := "shipany-user-live-read"
	callHash := "live_callhash_read01"
	now := time.Now().UTC().Truncate(time.Microsecond)

	_, err := db.ExecContext(ctx, `
		INSERT INTO wallet_live_provisional
			(token, authorization_id, call_hash, platform_user_id, user_id, api_key_id, account_id,
			 billing_currency, billing_snapshot_id, estimated_units, status, windows, settlement_event_id, created_at, activated_at)
		VALUES ($1, $2, $3, $4, 101, 202, 303, 'USD', '', 7_000000, 'active', $5::jsonb, '', $6, $6)`,
		"auth_live_read", "auth_live_read", callHash, user,
		`[{"window_seq":1,"lease_id":"lease-l1","token":"auth_live_read","pending_units":0,"settled_units":3000000,"opened_at_ms":1000},
		   {"window_seq":2,"lease_id":"lease-l2","token":"auth_live_read","pending_units":5,"settled_units":4000000,"opened_at_ms":2000}]`,
		now,
	)
	require.NoError(t, err)

	// another user's record stays out.
	_, err = db.ExecContext(ctx, `
		INSERT INTO wallet_live_provisional
			(token, authorization_id, call_hash, platform_user_id, user_id, api_key_id, account_id,
			 billing_currency, billing_snapshot_id, estimated_units, status, windows, settlement_event_id, created_at)
		VALUES ('auth_live_other', 'auth_live_other', 'live_callhash_other', $1, 1, 2, 3, 'USD', '', 100, 'finalized', '[]'::jsonb, 'gwusg_other', now())`,
		"shipany-user-live-other",
	)
	require.NoError(t, err)

	rows, err := store.ListLiveProvisionalByUser(ctx, user, now.Add(-time.Hour), now.Add(time.Hour))
	require.NoError(t, err)
	require.Len(t, rows, 1)

	rec := rows[0]
	require.Equal(t, "auth_live_read", rec.Token)
	require.Equal(t, "auth_live_read", rec.AuthorizationID)
	require.Equal(t, callHash, rec.CallHash)
	require.Equal(t, user, rec.PlatformUserID)
	require.Equal(t, "USD", rec.BillingCurrency)
	require.Equal(t, "", rec.BillingSnapshotID)
	require.Nil(t, rec.BillingFX, "an empty billing_snapshot_id degrades to null fx — never drops the record")
	require.Equal(t, "active", rec.Status)
	require.Equal(t, int64(7_000000), rec.EstimatedUnits)
	require.Empty(t, rec.SettlementEventID)
	require.WithinDuration(t, now, rec.CreatedAt, time.Second)
	require.Len(t, rec.Windows, 2, "both windows decoded from the JSON column")
	require.Equal(t, 1, rec.Windows[0].WindowSeq)
	require.Equal(t, "lease-l1", rec.Windows[0].LeaseID)
	require.Equal(t, int64(3_000000), rec.Windows[0].SettledUnits)
	require.Equal(t, int64(0), rec.Windows[0].PendingUnits)
	require.Equal(t, int64(1000), rec.Windows[0].OpenedAtMS)
	require.Equal(t, 2, rec.Windows[1].WindowSeq)
	require.Equal(t, "lease-l2", rec.Windows[1].LeaseID)
	require.Equal(t, int64(4_000000), rec.Windows[1].SettledUnits)
	require.Equal(t, int64(5), rec.Windows[1].PendingUnits)
	require.Equal(t, int64(2000), rec.Windows[1].OpenedAtMS)
}

// TestWalletReconciliationSummaryEqualsDirectSums is test 69 (redesign
// §15.5): the summary's figures equal the direct-table sums on real
// Postgres, and a Redis loss is diagnostic only. One shared-Redis claim
// for the whole test; the FlushDB leg reuses the SAME client (no second
// claim, no helper re-entry — round-1 MAJOR-5).
func TestWalletReconciliationSummaryEqualsDirectSums(t *testing.T) {
	ctx := context.Background()
	db := startWalletReconciliationTestPostgres(t, ctx)
	rdb := startCanonicalWalletTestRedis(t, ctx) // the ONE claim

	user := "shipany-user-recon69"
	now := time.Now().UTC().Truncate(time.Microsecond)
	since, until := now.Add(-time.Hour), now.Add(time.Hour)

	// --- seed the outbox: one row per status and reason (direct SQL —
	// test seeding with full control of status/reason/delivered_at).
	type seeded struct {
		id                               int64
		eventID, leaseID, status, reason string
		amount                           int64
	}
	seeds := []seeded{
		{eventID: "gwusg_69_d1", leaseID: "lease-69-a", status: "delivered", amount: 1_000000},
		{eventID: "gwusg_69_d2", leaseID: "lease-69-b", status: "delivered", amount: 2_000000},
		{eventID: "gwusg_69_d3", leaseID: "lease-69-c", status: "delivered", amount: 4_000000},
		{eventID: "gwusg_69_p1", leaseID: "lease-69-a", status: "pending", amount: 8_000000},
		{eventID: "gwusg_69_f1", leaseID: "lease-69-b", status: "in_flight", amount: 16_000000},
		{eventID: "gwusg_69_bs", leaseID: "lease-69-a", status: "dead_letter", reason: "balance_shortfall", amount: 32_000000},
		{eventID: "gwusg_69_se", leaseID: "lease-69-c", status: "dead_letter", reason: "split_exhausted", amount: 64_000000},
	}
	for i, s := range seeds {
		var deliveredAt any
		if s.status == "delivered" {
			deliveredAt = now.Add(-time.Duration(i+1) * time.Minute)
		}
		var reason any
		if s.reason != "" {
			reason = s.reason
		}
		_, err := db.ExecContext(ctx, `
			INSERT INTO wallet_settlement_outbox
				(event_id, platform_user_id, lease_id, gateway_request_id, currency, amount_units, payload_hash, status, occurred_at, delivered_at, dead_letter_reason, attempt_count, authorization_id)
			VALUES ($1, $2, $3, $4, 'USD', $5, 'hash-69', $6, $7, $8, $9, 1, $10)`,
			s.eventID, user, s.leaseID, "req-"+s.eventID, s.amount, s.status, now.Add(-30*time.Minute), deliveredAt, reason, "auth-69-"+s.eventID)
		require.NoError(t, err)
		require.NoError(t, db.QueryRowContext(ctx, `SELECT id FROM wallet_settlement_outbox WHERE event_id = $1`, s.eventID).Scan(&seeds[i].id))
	}
	t.Logf("test 69 seeded outbox rows: %d (user %s)", len(seeds), user)

	// --- seed hold outcomes: one row per resolution of 213's domain.
	holdStore := &walletHoldOutcomeStore{db: db}
	holdSeeds := []CanonicalWalletHold{
		{AuthorizationID: "auth-69-settled", LeaseID: "lease-69-a", HeldUnits: 11_000000, ArmedAt: now.Add(-20 * time.Minute), Class: "indeterminate", State: "armed"},
		{AuthorizationID: "auth-69-abandoned", LeaseID: "lease-69-b", HeldUnits: 12_000000, ArmedAt: now.Add(-19 * time.Minute), Class: "", State: "armed"},
		{AuthorizationID: "auth-69-expired", LeaseID: "lease-69-c", HeldUnits: 13_000000, ArmedAt: now.Add(-18 * time.Minute), Class: "not_written", State: "armed"},
		{AuthorizationID: "auth-69-open", LeaseID: "lease-69-a", HeldUnits: 14_000000, ArmedAt: now.Add(-17 * time.Minute), Class: "indeterminate", State: "armed"},
	}
	require.NoError(t, holdStore.InsertIndeterminate(ctx, holdSeeds[0], user, now.Add(-20*time.Minute)))
	require.NoError(t, holdStore.MarkSettled(ctx, holdSeeds[0].AuthorizationID, "gwusg_69_d1", now.Add(-15*time.Minute)))
	require.NoError(t, holdStore.InsertAbandoned(ctx, holdSeeds[1], user, now.Add(-19*time.Minute)))
	require.NoError(t, holdStore.InsertIndeterminate(ctx, holdSeeds[2], user, now.Add(-18*time.Minute)))
	n, err := holdStore.MarkExpiredOlderThan(ctx, now.Add(-17*time.Minute).Add(time.Second), now.Add(-10*time.Minute))
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	require.NoError(t, holdStore.InsertIndeterminate(ctx, holdSeeds[3], user, now.Add(-17*time.Minute)))

	// --- seed the billing snapshot + two Live records: one whose
	// billing_snapshot_id resolves (fx = the snapshot's rate — round-3 fx
	// fold), one with billing_snapshot_id='' (fx = null).
	_, err = db.ExecContext(ctx, `
		INSERT INTO wallet_billing_snapshot (id, version, user_id, api_key_id, account_id, billing_model, pricing_mode, payload)
		VALUES ('snap-69', 1, 101, 202, 303, 'paygo', 'per_call', '{"fx":{"rate":"7.2451","base":"USD"}}'::jsonb)`)
	require.NoError(t, err)
	liveCallHash := "live_callhash_69"
	_, err = db.ExecContext(ctx, `
		INSERT INTO wallet_live_provisional
			(token, authorization_id, call_hash, platform_user_id, user_id, api_key_id, account_id,
			 billing_currency, billing_snapshot_id, estimated_units, status, windows, settlement_event_id, created_at)
		VALUES ('auth-69-live-a', 'auth-69-live-a', $1, $2, 101, 202, 303, 'USD', 'snap-69', 7_000000, 'finalized',
			$3::jsonb, 'gwusg_69_live_final', $4)`,
		liveCallHash, user,
		`[{"window_seq":1,"lease_id":"lease-69-a","token":"auth-69-live-a","pending_units":0,"settled_units":3,"opened_at_ms":1000},
		   {"window_seq":2,"lease_id":"lease-69-c","token":"auth-69-live-a","pending_units":5,"settled_units":4,"opened_at_ms":2000}]`,
		now.Add(-25*time.Minute))
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `
		INSERT INTO wallet_live_provisional
			(token, authorization_id, call_hash, platform_user_id, user_id, api_key_id, account_id,
			 billing_currency, billing_snapshot_id, estimated_units, status, windows, settlement_event_id, created_at)
		VALUES ('auth-69-live-b', 'auth-69-live-b', 'live_callhash_69b', $1, 101, 202, 303, 'USD', '', 500, 'finalized', '[]'::jsonb, 'gwusg_69_live_b_final', $2)`,
		user, now.Add(-24*time.Minute))
	require.NoError(t, err)
	t.Logf("test 69 seeded: holds=%d live=2 (1 with resolvable fx, 1 without)", len(holdSeeds))

	// --- seed Redis: the current pointer + lease hash for U (the
	// ensure-shaped seeding: InstallCanonicalWalletLease is exactly what a
	// successful ensure leaves behind) + two armed holds.
	leaseStore := &gatewayCacheAdapterForTest{rdb: rdb}
	leaseID := "lease-69-current"
	lease := CanonicalWalletLease{
		LeaseID: leaseID, PlatformUserID: user, Currency: "USD",
		BudgetUnits: 500_000000, ConsumedUnits: 9_000000, ExpiresAt: now.Add(10 * time.Minute),
	}
	require.NoError(t, leaseStore.InstallCanonicalWalletLease(ctx, lease))
	for _, authID := range []string{"auth-69-open", "auth-69-open2"} {
		outLease, held, _, armErr := leaseStore.ArmCanonicalWalletHold(ctx, user, leaseID, "USD", authID, 14_000000, 60_000, now)
		require.NoError(t, armErr)
		require.Equal(t, leaseID, outLease)
		require.Equal(t, int64(14_000000), held)
	}

	// --- the service under test, composed exactly as production does.
	outbox := &outboxStoreForTest{db: db}
	svc := &WalletReconciliationReadService{
		outbox: outbox,
		holds:  holdStore,
		live:   &liveProvisionalStore{db: db},
		leases: leaseStore,
	}

	summary, err := svc.Summary(ctx, user, since, until, 0)
	require.NoError(t, err)
	require.Equal(t, user, summary.PlatformUserID)

	// Σ delivered amount_units per lease == the direct SQL GROUP BY.
	require.Len(t, summary.Outbox, len(seeds))
	deliveredByLease := map[string]int64{}
	for _, row := range summary.Outbox {
		if row.Status == "delivered" {
			deliveredByLease[row.LeaseID] += row.AmountUnits
		}
	}
	dbRows, err := db.QueryContext(ctx, `SELECT lease_id, SUM(amount_units) FROM wallet_settlement_outbox WHERE platform_user_id = $1 AND status = 'delivered' GROUP BY lease_id`, user)
	require.NoError(t, err)
	for dbRows.Next() {
		var lease string
		var sum int64
		require.NoError(t, dbRows.Scan(&lease, &sum))
		require.Equal(t, sum, deliveredByLease[lease], "lease %s", lease)
		delete(deliveredByLease, lease)
	}
	require.NoError(t, dbRows.Err())
	dbRows.Close()
	require.Empty(t, deliveredByLease, "every delivered lease appears in the direct sums")

	// The receivable == the direct sums == SumDeadLetterUnits (this
	// database holds only U's dead letters).
	require.Equal(t, int64(32_000000), summary.Receivable.BalanceShortfallUnits)
	require.Equal(t, int64(64_000000), summary.Receivable.SplitExhaustedUnits)
	require.Equal(t, 2, summary.Receivable.Rows)
	for reason, want := range map[string]int64{"balance_shortfall": 32_000000, "split_exhausted": 64_000000} {
		sum, sumErr := outbox.SumDeadLetterUnits(ctx, reason)
		require.NoError(t, sumErr)
		require.Equal(t, want, sum, "SumDeadLetterUnits(%s)", reason)
	}

	// Every hold row, with its resolution.
	require.Len(t, summary.Holds, 4)
	holdResolutions := map[string]string{}
	for _, h := range summary.Holds {
		if h.Resolution != nil {
			holdResolutions[h.AuthorizationID] = *h.Resolution
		} else {
			holdResolutions[h.AuthorizationID] = ""
		}
	}
	require.Equal(t, map[string]string{
		"auth-69-settled":   "settled",
		"auth-69-abandoned": "abandoned",
		"auth-69-expired":   "expired",
		"auth-69-open":      "",
	}, holdResolutions)
	settled := summary.Holds[0]
	for _, h := range summary.Holds {
		if h.AuthorizationID == "auth-69-settled" {
			settled = h
		}
	}
	require.NotNil(t, settled.SettlementEventID)
	require.Equal(t, "gwusg_69_d1", *settled.SettlementEventID)

	// Both windows with their DERIVED event ids; the fx fold.
	require.Len(t, summary.Live, 2)
	var liveA, liveB *WalletReconciliationLiveRow
	for i := range summary.Live {
		if summary.Live[i].CallHash == liveCallHash {
			liveA = &summary.Live[i]
		} else {
			liveB = &summary.Live[i]
		}
	}
	require.NotNil(t, liveA, "the snapshot-backed Live record")
	require.NotNil(t, liveB)
	require.Len(t, liveA.Windows, 2)
	require.Equal(t, LiveWindowSettlementEventID(liveCallHash, 1, user, "USD"), liveA.Windows[0].EventID)
	require.Equal(t, LiveWindowSettlementEventID(liveCallHash, 2, user, "USD"), liveA.Windows[1].EventID)
	require.NotEqual(t, liveA.Windows[0].EventID, liveA.Windows[1].EventID)
	require.NotNil(t, liveA.BillingFX, "the resolvable snapshot reports its rate")
	require.Equal(t, "7.2451", *liveA.BillingFX)
	require.Equal(t, "snap-69", liveA.BillingSnapshotID)
	require.Nil(t, liveB.BillingFX, "an empty billing_snapshot_id degrades to null fx")
	require.Equal(t, "", liveB.BillingSnapshotID)

	// The Redis view: available, the current lease, two armed holds.
	require.True(t, summary.Redis.Available)
	require.NotNil(t, summary.Redis.CurrentLeaseID)
	require.Equal(t, leaseID, *summary.Redis.CurrentLeaseID)
	require.NotNil(t, summary.Redis.Lease)
	require.Equal(t, int64(500_000000), summary.Redis.Lease.BudgetUnits)
	require.Equal(t, 2, summary.Redis.OpenHolds)

	// The watermark counts match the seeded statuses and the global
	// receivable equals SumDeadLetterUnits for both reasons.
	wm, err := svc.Watermark(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), wm.Pending)
	require.Equal(t, int64(1), wm.InFlight)
	require.Equal(t, int64(2), wm.DeadLetter)
	require.NotNil(t, wm.DeliveredAtMax)
	require.WithinDuration(t, now.Add(-time.Minute), *wm.DeliveredAtMax, time.Second, "the first delivered seed carries the latest delivered_at")
	var maxID int64
	require.NoError(t, db.QueryRowContext(ctx, `SELECT max(id) FROM wallet_settlement_outbox`).Scan(&maxID))
	require.Equal(t, maxID, wm.OutboxIDMax)
	require.Equal(t, int64(32_000000), wm.Receivable.BalanceShortfallUnits)
	require.Equal(t, int64(64_000000), wm.Receivable.SplitExhaustedUnits)

	// --- the empty-keyspace leg (round-1 MAJOR-5): FLUSHDB on the
	// ALREADY-CLAIMED client — no second claim, no helper re-entry. An
	// empty keyspace is not an outage (test 63's recoverable shape): the
	// summary stays 200-shaped with redis.available == true, no current
	// lease, zero open holds, and the Postgres parts identical.
	require.NoError(t, rdb.FlushDB(ctx).Err())
	afterFlush, err := svc.Summary(ctx, user, since, until, 0)
	require.NoError(t, err)
	require.True(t, afterFlush.Redis.Available, "an empty keyspace is not an outage")
	require.Nil(t, afterFlush.Redis.CurrentLeaseID)
	require.Nil(t, afterFlush.Redis.Lease)
	require.Zero(t, afterFlush.Redis.OpenHolds)
	require.Equal(t, summary.Outbox, afterFlush.Outbox, "the Postgres parts are identical")
	require.Equal(t, summary.Receivable, afterFlush.Receivable)
	require.Equal(t, summary.Holds, afterFlush.Holds)
	require.Equal(t, summary.Live, afterFlush.Live)
}
