//go:build media_integration

package media_integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/migrations"
	"github.com/stretchr/testify/require"
)

func expiredUnknownFixture(t *testing.T, mode string, ackLoss bool, configure ...func(*config.Config)) (*mediaFixture, *service.AuthorizationHandle, *service.BillingSnapshot, *http.Response) {
	options := append([]func(*config.Config){func(cfg *config.Config) { cfg.CanonicalWallet.PoolExpiryMode = mode }}, configure...)
	f := newMediaFixture(t, options...)
	f.poolFunds(t, 5000000, 5000000, 10000000)
	h, snap := f.authorize(t, 8000000, service.BillingFamilyOpenAI)
	past := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	// Age trusted fixture basis BEFORE the durable write; the production
	// deadline is immutable after that boundary, and tests never wait 30 min.
	_, err := f.db.Exec(`UPDATE wallet_authorization_segment SET lease_basis=jsonb_set(lease_basis,'{expires_at}',to_jsonb($2::text)) WHERE parent_authorization_id=$1`, h.ID, past.Format(time.RFC3339Nano))
	require.NoError(t, err)
	f.control.mu.Lock()
	for _, l := range f.control.pool {
		l.expires = past
	}
	f.control.expiryAckLost = ackLoss
	f.control.mu.Unlock()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
		_, _ = io.WriteString(w, "fixture unavailable")
	}))
	t.Cleanup(upstream.Close)
	req, err := http.NewRequestWithContext(service.WithAuthorizationHandle(context.Background(), h), "POST", upstream.URL, strings.NewReader(`{"input":"fixture"}`))
	require.NoError(t, err)
	port := service.NewAuthorizingHTTPUpstream(&mediaHTTP{client: upstream.Client()}, f.cfg)
	resp, err := port.Do(req, "", 1, 1)
	require.NoError(t, err)
	require.Equal(t, 503, resp.StatusCode)
	return f, h, snap, resp
}

func TestExternalUnknownExpiryShadowDoesNotChangeFundsOrPins(t *testing.T) {
	f, h, _, resp := expiredUnknownFixture(t, "shadow", false)
	require.NoError(t, resp.Body.Close())
	f.svc = f.newService(t)
	time.Sleep(1500 * time.Millisecond)
	var pending, expired int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FILTER (WHERE state='indeterminate'),count(*) FILTER (WHERE state IN ('expiry_pending','expired_unknown')) FROM wallet_authorization_segment WHERE parent_authorization_id=$1`, h.ID).Scan(&pending, &expired))
	require.Equal(t, 2, pending)
	require.Zero(t, expired)
	f.control.mu.Lock()
	defer f.control.mu.Unlock()
	for _, s := range h.Segments {
		require.Equal(t, "active", f.control.pinStatus[s.AuthorizationID])
	}
}

func TestExternalUnknownExpiryLostAckAndLateUsagePreserveIdentity(t *testing.T) {
	f, h, snap, resp := expiredUnknownFixture(t, "enabled", true)
	require.NoError(t, resp.Body.Close())
	f.svc = f.newService(t)
	require.Eventually(t, func() bool {
		var n int
		_ = f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND state='expired_unknown' AND expiry_cleanup_at IS NOT NULL`, h.ID).Scan(&n)
		return n == 2
	}, 10*time.Second, 50*time.Millisecond)
	var cost, outbox int64
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_settlement_outbox`).Scan(&outbox))
	require.Zero(t, outbox)
	wallet := repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore)
	for _, s := range h.Segments {
		hold, err := wallet.GetCanonicalWalletHold(context.Background(), "media-user", s.AuthorizationID)
		require.NoError(t, err)
		require.Equal(t, "released", hold.State)
		require.Equal(t, "expiry_unknown", hold.Class)
	}
	f.svc.Stop()
	event := service.CanonicalWalletSettlementEvent{GatewayRequestID: "late-proven-usage", PlatformUserID: "media-user", Currency: "USD", AmountUnits: 7000000, AuthorizationID: h.ID, BillingSnapshotID: snap.ID, OccurredAt: time.Now()}
	require.True(t, f.bridge.ObserveSettlement(event))
	f.svc = f.newService(t)
	require.Eventually(t, func() bool {
		var n int
		_ = f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND state='finished'`, h.ID).Scan(&n)
		return n == 2
	}, 15*time.Second, 50*time.Millisecond)
	require.NoError(t, f.db.QueryRow(`SELECT sum(amount_units) FROM wallet_settlement_outbox WHERE gateway_request_id='late-proven-usage' AND status='delivered'`).Scan(&cost))
	require.Equal(t, int64(7000000), cost)
	for _, s := range h.Segments {
		var snapshot, eventID, lease string
		require.NoError(t, f.db.QueryRow(`SELECT billing_snapshot_id,event_id,lease_id FROM wallet_settlement_outbox WHERE authorization_id=$1`, s.AuthorizationID).Scan(&snapshot, &eventID, &lease))
		require.Equal(t, snap.ID, snapshot)
		require.Equal(t, s.EventID, eventID)
		require.NotEqual(t, s.LeaseID, lease)
	}
	// Same cost is a replay, never a second customer charge.
	_ = f.bridge.ObserveSettlement(event)
	require.NoError(t, f.db.QueryRow(`SELECT sum(amount_units) FROM wallet_settlement_outbox WHERE gateway_request_id='late-proven-usage'`).Scan(&cost))
	require.Equal(t, int64(7000000), cost)
}

func TestExternalUnknownExpiryKeepsActiveBodyUntilClose(t *testing.T) {
	f, h, _, resp := expiredUnknownFixture(t, "enabled", false)
	f.svc = f.newService(t)
	time.Sleep(1500 * time.Millisecond)
	var count int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND state='indeterminate' AND write_active_until>now()`, h.ID).Scan(&count))
	require.Equal(t, 2, count)
	require.NoError(t, resp.Body.Close())
	require.Eventually(t, func() bool {
		_ = f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND state='expired_unknown'`, h.ID).Scan(&count)
		return count == 2
	}, 10*time.Second, 50*time.Millisecond)
}

func TestExternalUnknownExpiryPendingKeepsProvenPayloadBehindFence(t *testing.T) {
	f, h, snap, resp := expiredUnknownFixture(t, "enabled", false)
	f.control.mu.Lock()
	f.control.expiryRefused = true
	f.control.mu.Unlock()
	require.NoError(t, resp.Body.Close())
	f.svc = f.newService(t)
	require.Eventually(t, func() bool {
		var n int
		_ = f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND state='expiry_pending'`, h.ID).Scan(&n)
		return n == 2
	}, 10*time.Second, 50*time.Millisecond)
	event := service.CanonicalWalletSettlementEvent{GatewayRequestID: "pending-proven", PlatformUserID: "media-user", Currency: "USD", AmountUnits: 7000000, AuthorizationID: h.ID, BillingSnapshotID: snap.ID, OccurredAt: time.Now()}
	require.True(t, f.bridge.ObserveSettlement(event))
	var outbox int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_settlement_outbox`).Scan(&outbox))
	require.Zero(t, outbox)
	var raw []byte
	require.NoError(t, f.db.QueryRow(`SELECT settlement_payload FROM wallet_authorization_segment WHERE authorization_id=$1`, h.ID).Scan(&raw))
	var saved service.CanonicalWalletSettlementEvent
	require.NoError(t, json.Unmarshal(raw, &saved))
	require.Empty(t, saved.LeaseID)
	require.Positive(t, saved.AmountUnits)
	f.control.mu.Lock()
	f.control.expiryRefused = false
	f.control.mu.Unlock()
	require.Eventually(t, func() bool {
		var n int
		_ = f.db.QueryRow(`SELECT count(*) FROM wallet_settlement_outbox WHERE gateway_request_id='pending-proven' AND status='delivered'`).Scan(&n)
		return n == 2
	}, 15*time.Second, 50*time.Millisecond)
}

func TestExternalLegacyUnknownNeedsBoundTerminalProof(t *testing.T) {
	f := newMediaFixture(t, func(cfg *config.Config) { cfg.CanonicalWallet.PoolExpiryMode = "enabled" })
	f.poolFunds(t, 5000000, 5000000, 0)
	h, _ := f.authorize(t, 8000000, service.BillingFamilyOpenAI)
	past := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	_, err := f.db.Exec(`UPDATE wallet_authorization_segment SET state='indeterminate',authorization_token='legacy-ended',lease_basis=jsonb_set(lease_basis,'{expires_at}',to_jsonb($2::text)),expiry_deadline=$3 WHERE parent_authorization_id=$1`, h.ID, past.Format(time.RFC3339Nano), past.Add(1800*time.Second))
	require.NoError(t, err)
	f.control.mu.Lock()
	for _, l := range f.control.pool {
		l.expires = past
	}
	f.control.mu.Unlock()
	f.svc = f.newService(t)
	time.Sleep(1500 * time.Millisecond)
	var n int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND state='indeterminate' AND first_write_at IS NULL AND write_active_until IS NULL`, h.ID).Scan(&n))
	require.Equal(t, 2, n)
	_, err = f.db.Exec(`UPDATE wallet_authorization_segment SET write_ended_at=now()-interval '1 hour',legacy_completion_proof=$2 WHERE parent_authorization_id=$1 AND first_write_at IS NULL AND write_active_until IS NULL`, h.ID, strings.Repeat("a", 64))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_ = f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND state='expired_unknown' AND expiry_cleanup_at IS NOT NULL`, h.ID).Scan(&n)
		return n == 2
	}, 10*time.Second, 50*time.Millisecond)
	// A different metadata proof cannot replace the receipt-bound historical proof.
	_, err = f.db.Exec(`UPDATE wallet_authorization_segment SET legacy_completion_proof=$2 WHERE parent_authorization_id=$1`, h.ID, strings.Repeat("b", 64))
	require.Error(t, err)
}

func TestExternalExpiryPoisonGroupsDoNotStarveHealthyAfter32(t *testing.T) {
	f, h, snap, resp := expiredUnknownFixture(t, "enabled", false)
	require.NoError(t, resp.Body.Close())
	for i := 0; i < 40; i++ {
		parent := fmt.Sprintf("auth_poison_%032d", i)
		_, err := f.db.Exec(`INSERT INTO wallet_authorization_segment(parent_authorization_id,ordinal,authorization_id,platform_user_id,billing_snapshot_id,lease_id,held_units,lease_basis,event_id,kind,state,pin_state,authorization_token,expiry_deadline,expiry_intent_version,updated_at) SELECT $1,0,$1,platform_user_id,billing_snapshot_id,lease_id,held_units,lease_basis,$2,'llm','expiry_pending','active','poison-write',expiry_deadline,1,now()-interval '3 hours' FROM wallet_authorization_segment WHERE authorization_id=$3`, parent, service.CanonicalWalletSettlementEventID(parent, "media-user", "USD"), h.ID)
		require.NoError(t, err)
	}
	f.svc = f.newService(t)
	require.Eventually(t, func() bool {
		var n int
		_ = f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND state='expired_unknown'`, h.ID).Scan(&n)
		return n == 2
	}, 10*time.Second, 50*time.Millisecond)
	var poison int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE billing_snapshot_id=$1 AND parent_authorization_id LIKE 'auth_poison_%' AND state='expiry_pending'`, snap.ID).Scan(&poison))
	require.Equal(t, 40, poison)
}

func TestExternalExpiredPinnedCostKeepsWholeRootReceivableAcrossShortFunding(t *testing.T) {
	for _, caseName := range []string{"undergrant", "reactive_partial", "reactive_zero"} {
		t.Run(caseName, func(t *testing.T) {
			f, h, snap, resp := expiredUnknownFixture(t, "enabled", false)
			require.NoError(t, resp.Body.Close())
			f.svc = f.newService(t)
			require.Eventually(t, func() bool {
				var n int
				_ = f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND state='expired_unknown' AND expiry_cleanup_at IS NOT NULL`, h.ID).Scan(&n)
				return n == 2
			}, 10*time.Second, 50*time.Millisecond)
			f.svc.Stop()
			f.control.mu.Lock()
			f.control.unleased = 1000000
			f.control.forceOverCapture = caseName != "undergrant"
			if caseName == "reactive_partial" {
				f.control.overCaptureHeadroom = 1000000
			}
			f.control.mu.Unlock()
			proven := service.CanonicalWalletSettlementEvent{GatewayRequestID: "late-short-funding", PlatformUserID: "media-user", Currency: "USD", AmountUnits: 4000000, AuthorizationID: h.ID, BillingSnapshotID: snap.ID, OccurredAt: time.Now()}
			require.True(t, f.bridge.ObserveSettlement(proven))
			f.svc = f.newService(t)
			var id int64
			require.Eventually(t, func() bool {
				err := f.db.QueryRow(`SELECT id FROM wallet_settlement_outbox WHERE authorization_id=$1 AND status='dead_letter' AND dead_letter_reason='balance_shortfall'`, h.ID).Scan(&id)
				return err == nil
			}, 15*time.Second, 50*time.Millisecond)
			var amount int64
			var eventID, snapshot string
			var splits, count int
			require.NoError(t, f.db.QueryRow(`SELECT amount_units,event_id,billing_snapshot_id,split_depth FROM wallet_settlement_outbox WHERE id=$1`, id).Scan(&amount, &eventID, &snapshot, &splits))
			require.Equal(t, int64(4000000), amount)
			require.Equal(t, h.Segments[0].EventID, eventID)
			require.Equal(t, snap.ID, snapshot)
			require.Zero(t, splits)
			require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_settlement_outbox WHERE gateway_request_id='late-short-funding'`).Scan(&count))
			require.Equal(t, 1, count)
			f.control.mu.Lock()
			require.Zero(t, f.control.events[eventID])
			f.control.unleased += 4000000
			f.control.forceOverCapture = false
			f.control.mu.Unlock()
			require.NoError(t, repository.ProvideWalletOutboxStore(f.db).RequeueDeadLetter(context.Background(), id, "fixture-redrive"))
			require.Eventually(t, func() bool {
				var state string
				_ = f.db.QueryRow(`SELECT state FROM wallet_authorization_segment WHERE authorization_id=$1`, h.ID).Scan(&state)
				return state == "finished"
			}, 15*time.Second, 50*time.Millisecond)
			f.control.mu.Lock()
			require.Equal(t, int64(4000000), f.control.events[eventID])
			require.Equal(t, "settled", f.control.pinStatus[h.ID])
			f.control.mu.Unlock()
			require.NoError(t, f.db.QueryRow(`SELECT amount_units,split_depth FROM wallet_settlement_outbox WHERE id=$1 AND status='delivered'`, id).Scan(&amount, &splits))
			require.Equal(t, int64(4000000), amount)
			require.Zero(t, splits)
		})
	}
}

func TestExternalExpiredActualRemainsAvailableObligationWhenEnqueueFails(t *testing.T) {
	f, h, snap, resp := expiredUnknownFixture(t, "enabled", false)
	require.NoError(t, resp.Body.Close())
	f.svc = f.newService(t)
	require.Eventually(t, func() bool {
		var n int
		_ = f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND state='expired_unknown' AND expiry_cleanup_at IS NOT NULL`, h.ID).Scan(&n)
		return n == 2
	}, 10*time.Second, 50*time.Millisecond)
	f.svc.Stop()
	input := service.WalletAvailabilityInput{UnitVersion: "usd-e8-v1", USDPolicyVersion: "usd-wallet-v1"}
	for _, l := range f.control.pool {
		input.Leases = append(input.Leases, service.WalletAvailabilityLeaseInput{LeaseID: l.id, BudgetUnits: strconv.FormatInt(l.budget, 10), CapturedUnits: "0", ReleasedUnits: "0", ReservedUnits: "0", Status: "expired", ExpiresAt: l.expires})
	}
	view, err := f.svc.Availability(context.Background(), f.userID, input)
	require.NoError(t, err)
	require.Empty(t, view.Obligations, "unknown expired evidence is not a known actual debt")
	_, err = f.db.Exec(`CREATE FUNCTION refuse_fixture_outbox() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture enqueue unavailable'; END $$; CREATE TRIGGER refuse_fixture_outbox BEFORE INSERT ON wallet_settlement_outbox FOR EACH ROW EXECUTE FUNCTION refuse_fixture_outbox()`)
	require.NoError(t, err)
	event := service.CanonicalWalletSettlementEvent{GatewayRequestID: "proven-enqueue-failure", PlatformUserID: "media-user", Currency: "USD", AmountUnits: 7000000, AuthorizationID: h.ID, BillingSnapshotID: snap.ID, OccurredAt: time.Now()}
	require.False(t, f.bridge.ObserveSettlement(event), "enqueue failure must retain the already committed actual payload")
	var outbox int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_settlement_outbox`).Scan(&outbox))
	require.Zero(t, outbox)
	view, err = f.svc.Availability(context.Background(), f.userID, input)
	require.NoError(t, err)
	require.Len(t, view.Obligations, 2)
	var total int64
	for _, o := range view.Obligations {
		n, e := strconv.ParseInt(o.AmountUnits, 10, 64)
		require.NoError(t, e)
		total += n
		require.False(t, o.CoveredByRedis)
		require.False(t, o.Captured)
	}
	require.Equal(t, int64(7000000), total)
	_, err = f.db.Exec(`DROP TRIGGER refuse_fixture_outbox ON wallet_settlement_outbox; DROP FUNCTION refuse_fixture_outbox()`)
	require.NoError(t, err)
	f.svc = f.newService(t)
	require.Eventually(t, func() bool {
		var n int
		_ = f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND state='finished'`, h.ID).Scan(&n)
		return n == 2
	}, 15*time.Second, 50*time.Millisecond)
}

func TestExternalExpiryCrashWindowAndPendingSagaResumeInShadow(t *testing.T) {
	f, h, _, resp := expiredUnknownFixture(t, "enabled", false)
	// Simulate a paused/crashed owner: no terminal callback, but its persisted
	// 90-second lease is expired. No arbitrary request-duration assumption.
	_, err := f.db.Exec(`UPDATE wallet_authorization_segment SET write_active_until=now()-interval '1 second' WHERE parent_authorization_id=$1`, h.ID)
	require.NoError(t, err)
	f.control.mu.Lock()
	f.control.expiryRefused = true
	f.control.mu.Unlock()
	f.svc = f.newService(t)
	require.Eventually(t, func() bool {
		var n int
		_ = f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND state='expiry_pending'`, h.ID).Scan(&n)
		return n == 2
	}, 10*time.Second, 50*time.Millisecond)
	f.svc.Stop()
	f.cfg.CanonicalWallet.PoolExpiryMode = "shadow"
	f.bridge.Close()
	wallet := repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore)
	f.bridge = service.NewCanonicalWalletBridge(f.cfg, wallet, f.db, repository.ProvideWalletOutboxStore(f.db))
	t.Cleanup(f.bridge.Close)
	f.control.mu.Lock()
	f.control.expiryRefused = false
	f.control.mu.Unlock()
	f.svc = f.newService(t)
	require.Eventually(t, func() bool {
		var n int
		_ = f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND state='expired_unknown' AND expiry_cleanup_at IS NOT NULL`, h.ID).Scan(&n)
		return n == 2
	}, 10*time.Second, 50*time.Millisecond)
	require.NoError(t, resp.Body.Close())
}

func TestExternalPartialExpiryAckProtectsArmedHoldFromReaperAcrossRestart(t *testing.T) {
	f, h, _, resp := expiredUnknownFixture(t, "enabled", false, func(cfg *config.Config) {
		cfg.CanonicalWallet.OrphanGraceSeconds = 1
		cfg.CanonicalWallet.OrphanSweepIntervalSeconds = 1
	})
	f.control.mu.Lock()
	f.control.expiryRefusedAuth = h.Segments[1].AuthorizationID
	f.control.mu.Unlock()
	keys, err := f.rdb.Keys(context.Background(), "canonical_wallet:hold:*").Result()
	require.NoError(t, err)
	require.Len(t, keys, 2)
	for _, key := range keys {
		require.NoError(t, f.rdb.HSet(context.Background(), key, "armed_at_ms", time.Now().Add(-time.Hour).UnixMilli()).Err())
	}
	require.NoError(t, resp.Body.Close())
	f.svc = f.newService(t)
	require.Eventually(t, func() bool {
		var n int
		_ = f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND state='expiry_pending' AND expiry_ack_at IS NOT NULL`, h.ID).Scan(&n)
		return n == 1
	}, 10*time.Second, 50*time.Millisecond)
	// At least two normal reaper ticks occur with an ACKed sibling still armed.
	time.Sleep(2200 * time.Millisecond)
	wallet := repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore)
	for _, s := range h.Segments {
		hold, e := wallet.GetCanonicalWalletHold(context.Background(), "media-user", s.AuthorizationID)
		require.NoError(t, e)
		require.Equal(t, "armed", hold.State)
	}
	f.svc.Stop()
	f.control.mu.Lock()
	f.control.expiryRefusedAuth = ""
	f.control.mu.Unlock()
	f.svc = f.newService(t)
	require.Eventually(t, func() bool {
		var n int
		_ = f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND state='expired_unknown' AND expiry_cleanup_at IS NOT NULL`, h.ID).Scan(&n)
		return n == 2
	}, 10*time.Second, 50*time.Millisecond)
	for _, s := range h.Segments {
		hold, e := wallet.GetCanonicalWalletHold(context.Background(), "media-user", s.AuthorizationID)
		require.NoError(t, e)
		require.Equal(t, "released", hold.State)
		require.Equal(t, "expiry_unknown", hold.Class)
	}
}

func TestExternalExpiryMigrationPreservesHistoricalFinancialRowsAndGuards(t *testing.T) {
	f := newMediaFixture(t)
	f.svc.Stop()
	f.bridge.Close()
	// Stop all fixture loops before reconstructing the pre-222 additive schema.
	f.poolFunds(t, 5000000, 5000000, 0)
	// Use a separate still-operational authorizer bridge only to seed ordinary
	// held rows, then close it before testing the migration transaction.
	wallet := repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore)
	f.bridge = service.NewCanonicalWalletBridge(f.cfg, wallet, f.db, repository.ProvideWalletOutboxStore(f.db))
	h, _ := f.authorize(t, 8000000, service.BillingFamilyOpenAI)
	f.bridge.Close()
	tx, err := f.db.Begin()
	require.NoError(t, err)
	defer tx.Rollback()
	// A pre-222 schema has none of the later (233) segment triggers whose
	// bodies read the 222 columns; DROP COLUMN does not remove them, so drop
	// them here and restore them after 222 is re-applied.
	_, err = tx.Exec(`DROP TRIGGER wallet_unknown_expiry_guard ON wallet_authorization_segment;
 DROP TRIGGER wallet_funding_terminal_enqueue ON wallet_authorization_segment;
 DROP TRIGGER wallet_funding_terminal_cleanup_identity ON wallet_authorization_segment;
 ALTER TABLE wallet_authorization_segment DROP COLUMN first_write_at,DROP COLUMN write_active_until,DROP COLUMN write_ended_at,DROP COLUMN legacy_completion_proof,DROP COLUMN expiry_deadline,DROP COLUMN expiry_intent_version,DROP COLUMN expiry_ack_at,DROP COLUMN expiry_receipt_id,DROP COLUMN expiry_cleanup_at;
 ALTER TABLE wallet_authorization_segment DROP CONSTRAINT wallet_authorization_segment_state_check;
 ALTER TABLE wallet_authorization_segment ADD CONSTRAINT wallet_authorization_segment_state_check CHECK (state IN ('prepared','held','indeterminate','settling','released','finished'));
 UPDATE wallet_authorization_segment SET state=CASE WHEN ordinal=0 THEN 'released' ELSE 'settling' END`)
	require.NoError(t, err)
	var before []byte
	require.NoError(t, tx.QueryRow(`SELECT jsonb_agg(to_jsonb(a) ORDER BY authorization_id) FROM wallet_authorization_segment a`).Scan(&before))
	sql, err := migrations.FS.ReadFile("222_wallet_unknown_expiry.sql")
	require.NoError(t, err)
	_, err = tx.Exec(string(sql))
	require.NoError(t, err)
	var equal bool
	require.NoError(t, tx.QueryRow(`SELECT $1::jsonb=jsonb_agg(to_jsonb(a)-ARRAY['first_write_at','write_active_until','write_ended_at','legacy_completion_proof','expiry_deadline','expiry_intent_version','expiry_ack_at','expiry_receipt_id','expiry_cleanup_at'] ORDER BY authorization_id) FROM wallet_authorization_segment a`, string(before)).Scan(&equal))
	require.True(t, equal, "every historical identity, snapshot, amount, payload and state survives the additive migration")
	// Restore the later objects invalidated by the reconstruction (the 223
	// partial index went with the dropped columns); the guard checks below then
	// run with every current segment trigger present.
	_, err = tx.Exec(`CREATE TRIGGER wallet_funding_terminal_cleanup_identity BEFORE UPDATE ON wallet_authorization_segment FOR EACH ROW EXECUTE FUNCTION wallet_funding_terminal_cleanup_guard();
 CREATE TRIGGER wallet_funding_terminal_enqueue AFTER INSERT OR UPDATE ON wallet_authorization_segment FOR EACH ROW EXECUTE FUNCTION enqueue_wallet_funding_terminal();
 CREATE INDEX IF NOT EXISTS idx_wallet_immediate_expiry_pending ON wallet_authorization_segment(updated_at) WHERE expiry_intent_version=2 AND (expiry_ack_at IS NULL OR expiry_cleanup_at IS NULL)`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	// Guards reject incomplete intents, fabricated unknown-zero receipts, and
	// subsequent deadline mutation, using actual PostgreSQL trigger execution.
	_, err = f.db.Exec(`UPDATE wallet_authorization_segment SET state='expiry_pending' WHERE authorization_id=$1`, h.Segments[0].AuthorizationID)
	require.ErrorContains(t, err, "intent is incomplete")
	_, err = f.db.Exec(`UPDATE wallet_authorization_segment SET state='expired_unknown',expiry_intent_version=1,expiry_deadline=now() WHERE authorization_id=$1`, h.Segments[0].AuthorizationID)
	require.ErrorContains(t, err, "receipt is missing")
	_, err = f.db.Exec(`UPDATE wallet_authorization_segment SET expiry_deadline=now() WHERE authorization_id=$1`, h.Segments[0].AuthorizationID)
	require.NoError(t, err)
	_, err = f.db.Exec(`UPDATE wallet_authorization_segment SET expiry_deadline=now()+interval '1 second' WHERE authorization_id=$1`, h.Segments[0].AuthorizationID)
	require.ErrorContains(t, err, "evidence is immutable")
}
