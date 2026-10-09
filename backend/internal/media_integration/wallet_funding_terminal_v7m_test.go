//go:build media_integration

package media_integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// These cases pin the zero-share branch of the frozen-lease terminal cleanup
// (cleanupFundingTerminal: version 0, state released, actual 0, settlement
// payload amount 0). That branch runs only for a frozen llm/legacy lease with a
// selected cleanup row, so every case keeps the media service stopped while the
// cleanup is under test: its one-second tick drives recoverPoolAttempts, which
// would otherwise end the share before any freeze. The freeze itself always comes
// from the production shared-free reclaim (a media quote the free credits cannot
// cover).

type fundingV7mPin struct {
	AuthorizationID string  `json:"authorizationId"`
	GatewayJobID    string  `json:"gatewayJobId"`
	State           string  `json:"state"`
	Token           *string `json:"authorizationToken"`
}

type fundingV7mLease struct {
	ID       string `json:"id"`
	Status   string `json:"status"`
	Released string `json:"releasedUnits"`
}

// fundingV7mD1 reads the actual Worker/D1 snapshot without failing the test, so
// it can be polled from require.Eventually/Never condition goroutines.
func fundingV7mD1(x fundingV7Fixture) (map[string]fundingV7mPin, map[string]fundingV7mLease, bool) {
	raw, err := json.Marshal(map[string]string{"platform_user_id": x.f.platformUserID})
	if err != nil {
		return nil, nil, false
	}
	req, err := http.NewRequest(http.MethodPost, x.base+"/__fixture/snapshot", bytes.NewReader(raw))
	if err != nil {
		return nil, nil, false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-fixture-secret", x.secret)
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return nil, nil, false
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil || resp.StatusCode != http.StatusOK {
		return nil, nil, false
	}
	var snapshot struct {
		Pins   []fundingV7mPin   `json:"pins"`
		Leases []fundingV7mLease `json:"leases"`
	}
	if json.Unmarshal(body, &snapshot) != nil {
		return nil, nil, false
	}
	pins, leases := map[string]fundingV7mPin{}, map[string]fundingV7mLease{}
	for _, pin := range snapshot.Pins {
		pins[pin.AuthorizationID] = pin
	}
	for _, lease := range snapshot.Leases {
		leases[lease.ID] = lease
	}
	return pins, leases, true
}

func fundingV7mPinState(t *testing.T, x fundingV7Fixture, auth string) fundingV7mPin {
	t.Helper()
	pins, _, ok := fundingV7mD1(x)
	require.True(t, ok, "actual isolated Worker/D1 snapshot is required")
	pin, found := pins[auth]
	require.True(t, found, "the share has an actual D1 pin")
	return pin
}

func fundingV7mHoldState(t *testing.T, x fundingV7Fixture, auth string) string {
	t.Helper()
	hold, err := x.wallet.GetCanonicalWalletHold(context.Background(), x.f.platformUserID, auth)
	if err != nil {
		return "missing"
	}
	return hold.State
}

// fundingV7mFinishAudit records, in the test's own database, every transition of
// a segment to state=finished together with the state it left and whether its
// lease was frozen at that moment. finishPoolSegment is the only writer of
// state=finished for a released zero share, and with the media service stopped
// (and no zero finalizer for a charged group) its only caller that can reach such
// a share is the frozen-lease cleanup.
func fundingV7mFinishAudit(t *testing.T, x fundingV7Fixture) {
	t.Helper()
	_, err := x.f.db.Exec(`CREATE TABLE funding_v7m_finish_audit(authorization_id text NOT NULL,old_state text NOT NULL,old_pin_state text NOT NULL,lease_frozen boolean NOT NULL,at timestamptz NOT NULL DEFAULT clock_timestamp());
	 CREATE FUNCTION funding_v7m_finish_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
	  IF OLD.state IS DISTINCT FROM 'finished' AND NEW.state='finished' THEN
	   INSERT INTO funding_v7m_finish_audit(authorization_id,old_state,old_pin_state,lease_frozen) VALUES(NEW.authorization_id,OLD.state,OLD.pin_state,
	    EXISTS(SELECT 1 FROM wallet_funding_freeze f WHERE f.platform_user_id=NEW.platform_user_id AND f.lease_id=NEW.lease_id));
	  END IF; RETURN NEW; END $$;
	 CREATE TRIGGER z_funding_v7m_finish_audit AFTER UPDATE ON wallet_authorization_segment FOR EACH ROW EXECUTE FUNCTION funding_v7m_finish_audit()`)
	require.NoError(t, err)
}

// fundingV7mSplit spends 9.1 of the 10 source lease, seeds a second 0.9 legacy
// lease, then authorizes 1.5: the source lease's 0.9 free plus 0.6 on the second
// lease. It returns the split handle and the second lease id.
func fundingV7mSplit(t *testing.T, x fundingV7Fixture) (*service.AuthorizationHandle, string) {
	t.Helper()
	x.authorize(t, 910000000)
	short := "funding-v7m-short-" + uuid.NewString()
	code, raw := immediateV5WirePost(t, x.base, x.secret, "/__fixture/seed", map[string]string{"platform_user_id": x.f.platformUserID, "grant_units": "90000000", "lease_id": short, "budget_units": "90000000"}, true)
	require.Equal(t, http.StatusOK, code, string(raw))
	require.NoError(t, x.wallet.InstallCanonicalWalletLease(context.Background(), service.CanonicalWalletLease{LeaseID: short, PlatformUserID: x.f.platformUserID, Currency: "USD", FundedUnits: 90000000, BudgetUnits: 90000000, FundingScope: "legacy", ExpiresAt: time.Now().Add(65 * time.Minute)}))
	h := x.authorize(t, 150000000)
	require.Len(t, h.Segments, 2, "the 1.5 hold spans the source lease (0.9 free) and the second lease (0.6)")
	require.Equal(t, x.source, h.Segments[0].LeaseID)
	require.Equal(t, short, h.Segments[1].LeaseID)
	return h, short
}

// fundingV7mChargeSplit records one real trusted 0.8 usage for the split
// attempt. FIFO allocation charges the first share 0.8 and leaves the second
// share at zero (state released, settlement payload amount 0).
func fundingV7mChargeSplit(t *testing.T, x fundingV7Fixture, h *service.AuthorizationHandle) {
	t.Helper()
	billingRepository := repository.NewUsageBillingRepository(nil, x.f.db)
	x.f.bridge.SetBillingEvidenceRepository(billingRepository)
	usageService := service.NewOpenAIGatewayService(nil, nil, billingRepository, x.f.users, nil, nil, repository.NewGatewayCache(x.f.rdb), x.f.cfg, x.f.db, repository.ProvideWalletOutboxStore(x.f.db), nil, nil, service.NewBillingService(x.f.cfg, nil), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, x.f.snapshots)
	t.Cleanup(usageService.CloseOpenAIWSPool)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"funding-v7m-split","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":0}}`)
	}))
	defer provider.Close()
	request, err := http.NewRequestWithContext(service.WithAuthorizationHandle(context.Background(), h), http.MethodPost, provider.URL+"/v1/responses", strings.NewReader(`{"model":"gpt-5.1"}`))
	require.NoError(t, err)
	response, err := service.NewAuthorizingHTTPUpstream(&mediaHTTP{client: provider.Client()}, x.f.cfg).Do(request, "", 0, 1)
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	var usageErr error
	dispatch, err := h.PrepareUsageTask(context.Background(), func(ctx context.Context) {
		usageErr = usageService.RecordUsage(ctx, &service.OpenAIRecordUsageInput{Result: &service.OpenAIForwardResult{RequestID: "funding-v7m-split-" + h.ID, Model: "gpt-5.1", Usage: service.OpenAIUsage{InputTokens: 1}}, User: x.owner, APIKey: &service.APIKey{ID: x.snapshot.APIKeyID, Quota: 100}, Account: &service.Account{ID: x.snapshot.AccountID, Type: service.AccountTypeAPIKey, Platform: service.PlatformOpenAI}, BillingSnapshot: x.snapshot, AuthorizationID: h.ID, AuthorizationToken: h.LastWriteToken()})
	})
	require.NoError(t, usageErr)
	require.NoError(t, err)
	require.NotNil(t, dispatch)
	dispatch(context.Background())
	require.Eventually(t, func() bool {
		var ready bool
		return x.f.db.QueryRow(`SELECT bool_or(ordinal=0 AND actual_units=80000000 AND (settlement_payload->>'amount_units')::bigint=80000000)
		 AND bool_or(ordinal=1 AND state='released' AND actual_units=0 AND (settlement_payload->>'amount_units')::bigint=0)
		 AND bool_and(NOT fee_pending AND NOT evidence_pending)
		 AND EXISTS(SELECT 1 FROM wallet_settlement_outbox o WHERE o.event_id=$2 AND o.status='delivered')
		 FROM wallet_authorization_segment WHERE parent_authorization_id=$1`, h.ID, h.Segments[0].EventID).Scan(&ready) == nil && ready
	}, 8*time.Second, 20*time.Millisecond, "the first share is charged 0.8 and delivered; the second share is a released zero share")
}

// fundingV7mSegment is the persisted cleanup-relevant state of one share.
type fundingV7mSegment struct {
	state, pin           string
	version              int
	zeroAck, cleanup     bool
	actual, payloadUnits int64
}

func fundingV7mRead(x fundingV7Fixture, auth string) (fundingV7mSegment, bool) {
	var s fundingV7mSegment
	err := x.f.db.QueryRow(`SELECT state,pin_state,expiry_intent_version,zero_ack_at IS NOT NULL,funding_terminal_cleanup_at IS NOT NULL,actual_units,COALESCE((settlement_payload->>'amount_units')::bigint,-1)
	 FROM wallet_authorization_segment WHERE authorization_id=$1`, auth).Scan(&s.state, &s.pin, &s.version, &s.zeroAck, &s.cleanup, &s.actual, &s.payloadUnits)
	return s, err == nil
}

// fundingV7mFreezeByReclaim makes a media quote the free credits cannot cover.
// The production shared-free reclaim then freezes every shared lease with free
// principal (including the zero share's lease), which inserts the durable
// terminal work for it. Whether this particular quote is finally funded depends
// on how fast the background cleanup releases the second lease, so its outcome
// is only logged; the assertions are on the frozen lease itself.
func fundingV7mFreezeByReclaim(t *testing.T, x fundingV7Fixture, lease string, units int64) {
	t.Helper()
	media, err := x.mediaFunding(t, units)
	t.Logf("reclaiming media quote outcome: err=%v", err != nil)
	if media != nil {
		// Keep the helper's task row out of any later media service tick.
		_, updateErr := x.f.db.Exec(`UPDATE gateway_media_task SET status='failed',pin_state='finished',actual_units=0 WHERE authorization_id=$1 AND status NOT IN ('failed','completed') AND held_units=0`, media.ID)
		require.NoError(t, updateErr)
	}
	require.Eventually(t, func() bool {
		var frozen bool
		return x.f.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM wallet_funding_freeze f JOIN wallet_funding_terminal_work w USING(platform_user_id,lease_id) WHERE f.platform_user_id=$1 AND f.lease_id=$2)`, x.f.platformUserID, lease).Scan(&frozen) == nil && frozen
	}, 8*time.Second, 20*time.Millisecond, "the shared-free reclaim froze the lease and scheduled its terminal cleanup")
}

// fundingV7mAwaitEnded waits until the frozen-lease cleanup has ended the share
// (segment finished with its cleanup receipt, D1 pin released) and the lease has
// closed in PG and D1. On timeout it reports the last observed state.
func fundingV7mAwaitEnded(t *testing.T, x fundingV7Fixture, lease, auth string, timeout time.Duration, msg string) {
	t.Helper()
	var s fundingV7mSegment
	var pin fundingV7mPin
	var d1Lease fundingV7mLease
	var closed bool
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		current, ok := fundingV7mRead(x, auth)
		pins, leases, d1 := fundingV7mD1(x)
		if !ok || !d1 {
			continue
		}
		s, pin, d1Lease = current, pins[auth], leases[lease]
		closed = false
		if x.f.db.QueryRow(`SELECT closed FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, lease).Scan(&closed) != nil {
			continue
		}
		if s.state == "finished" && s.pin == "finished" && s.cleanup && pin.State == "released" && d1Lease.Status == "closed" && closed {
			return
		}
	}
	require.Failf(t, msg, "last observed: segment state=%s pin_state=%s cleanup_receipt=%v, D1 pin state=%s job=%s, D1 lease status=%s released=%s, PG freeze closed=%v, Redis hold=%s",
		s.state, s.pin, s.cleanup, pin.State, pin.GatewayJobID, d1Lease.Status, d1Lease.Released, closed, fundingV7mHoldState(t, x, auth))
}

// A. The zero share of a CHARGED split attempt, still released with its D1 pin
// active when its lease is frozen, is ended by the frozen-lease cleanup: its
// capacity hold is released, its D1 pin is finished (released), its segment is
// finished with its cleanup receipt, and the lease can then close and return its
// whole free principal. Without the branch the active pin blocks every return
// of that lease forever.
func TestExternalFundingV7mFrozenLeaseCleanupEndsZeroShareOfChargedSplit(t *testing.T) {
	x := newFundingV7Fixture(t)
	h, short := fundingV7mSplit(t, x)
	fundingV7mChargeSplit(t, x, h)
	zero := h.Segments[1].AuthorizationID
	before, ok := fundingV7mRead(x, zero)
	require.True(t, ok)
	require.Equal(t, fundingV7mSegment{state: "released", pin: "active", version: 0, actual: 0, payloadUnits: 0}, before, "exact branch shape before any freeze: released zero share, version 0, no zero ACK, pin not finished")
	require.Equal(t, "active", fundingV7mPinState(t, x, zero).State, "the zero share's D1 pin is still active")
	require.Equal(t, "released", fundingV7mHoldState(t, x, zero), "the settlement plan already released the zero share's Redis hold")
	var freezes int
	require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, short).Scan(&freezes))
	require.Zero(t, freezes, "the second lease is not frozen yet")
	fundingV7mFinishAudit(t, x)

	// Only $0.1 is free on the source lease; the second lease's $0.9 can come back
	// only after the cleanup ends the zero share's still-active pin.
	fundingV7mFreezeByReclaim(t, x, short, 50000000)
	fundingV7mAwaitEnded(t, x, short, zero, 12*time.Second, "the frozen-lease cleanup ends the zero share's pin and segment and the second lease closes")
	var oldState, oldPin string
	var frozen bool
	require.NoError(t, x.f.db.QueryRow(`SELECT old_state,old_pin_state,lease_frozen FROM funding_v7m_finish_audit WHERE authorization_id=$1`, zero).Scan(&oldState, &oldPin, &frozen))
	t.Logf("zero share finish transition: old_state=%s old_pin_state=%s lease_frozen=%v", oldState, oldPin, frozen)
	require.Equal(t, "released", oldState)
	require.Equal(t, "active", oldPin, "the D1 pin was finished by this finish, not before it")
	require.True(t, frozen, "the share was finished while its lease was frozen, i.e. by the frozen-lease cleanup")
	require.Equal(t, "released", fundingV7mHoldState(t, x, zero))
	pin := fundingV7mPinState(t, x, zero)
	require.Equal(t, h.ID, pin.GatewayJobID, "an LLM share's trusted gateway job id is its parent authorization")
	receipt := fundingV7PrimaryReceipt(t, x.f.db, x.f.platformUserID, short)
	fundingV7VerifyReturnSignature(t, x.secret, receipt)
	require.Equal(t, "close", receipt.Mode)
	require.Equal(t, "0", receipt.HeldUnits)
	require.Equal(t, "0", receipt.CapturedUnits)
	require.Equal(t, "90000000", receipt.ReturnedAfterUnits, "the second lease returns its whole principal once the zero share has ended")
}

// fundingV7mObserveUntouched holds the share under observation while the
// frozen lease's durable terminal work keeps being claimed. Every claim runs
// recoverFundingTerminal, which (once no first-winner return request is pending)
// runs cleanupFundingTerminal over a batch containing the share; the mutation
// runs of B and D show that this same harness does reach the share's branch.
func fundingV7mObserveUntouched(t *testing.T, x fundingV7Fixture, lease, auth, hold string, window time.Duration) {
	t.Helper()
	claims := map[time.Time]bool{}
	for deadline := time.Now().Add(window); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		var claimed time.Time
		if x.f.db.QueryRow(`SELECT updated_at FROM wallet_funding_terminal_work WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, lease).Scan(&claimed) == nil {
			claims[claimed] = true
		}
		s, ok := fundingV7mRead(x, auth)
		pins, _, d1 := fundingV7mD1(x)
		if !ok || !d1 {
			continue
		}
		holdState := fundingV7mHoldState(t, x, auth)
		if s.state != "released" || s.pin == "finished" || s.cleanup || pins[auth].State != "active" || holdState != hold {
			require.Failf(t, "the frozen-lease cleanup must leave this share, its D1 pin and its Redis hold untouched",
				"after %d terminal work claims: segment state=%s pin_state=%s cleanup_receipt=%v, D1 pin state=%s job=%s, Redis hold=%s (expected released/active/false, active, %s)",
				len(claims), s.state, s.pin, s.cleanup, pins[auth].State, pins[auth].GatewayJobID, holdState, hold)
		}
	}
	t.Logf("distinct terminal work claims observed while the share stayed untouched: %d", len(claims))
	require.GreaterOrEqual(t, len(claims), 3, "the frozen lease's cleanup really ran repeatedly over the untouched share")
}

// B. Version guard. A zero share of a CHARGED split attempt that carries an
// expiry intent (expiry_intent_version=2, no expiry ACK yet: the intent commits
// in PG before the Worker is contacted) must never be plain-released by the
// frozen-lease cleanup. Its pin belongs to the expiry protocol: a plain release
// either consumes the still-active D1 pin (after which the committed expiry can
// never be acknowledged) or, once D1 has expired it, is refused forever.
//
// Reachability: no production writer leaves an expiry-intent share in state
// released (a late settlement under an intent writes expiry_pending, an ACKed
// zero writes expired_unknown, and a released share with a payload can never
// take an intent: guard_wallet_unknown_expiry, migration 223 lines 59-62). The
// entry.version==0 guard is therefore defense in depth, and this case builds the
// exact excluded shape with schema-permitted PG surgery on the natural state of
// case A: only the expiry-intent columns are added (the payload and fee are
// lifted and restored around the intent so the 223 guard accepts it).
func TestExternalFundingV7mFrozenLeaseCleanupNeverPlainReleasesAnExpiryIntentShare(t *testing.T) {
	x := newFundingV7Fixture(t)
	h, short := fundingV7mSplit(t, x)
	fundingV7mChargeSplit(t, x, h)
	zero := h.Segments[1].AuthorizationID
	var payload []byte
	var fee *int64
	require.NoError(t, x.f.db.QueryRow(`SELECT settlement_payload,known_fee_units FROM wallet_authorization_segment WHERE authorization_id=$1`, zero).Scan(&payload, &fee))
	tx, err := x.f.db.Begin()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.Exec(`UPDATE wallet_authorization_segment SET settlement_payload=NULL,known_fee_units=NULL WHERE authorization_id=$1`, zero)
	require.NoError(t, err)
	_, err = tx.Exec(`UPDATE wallet_authorization_segment SET expiry_intent_version=2,expiry_v2_deadline=date_trunc('milliseconds',now()),expiry_policy_version='wallet-immediate-v5',
	 expiry_terminal_proof=COALESCE(expiry_terminal_proof,$2),terminal_sealed_at=COALESCE(terminal_sealed_at,now()),terminal_evidence=COALESCE(terminal_evidence,'{}'::jsonb) WHERE authorization_id=$1`, zero, strings.Repeat("a7", 32))
	require.NoError(t, err)
	_, err = tx.Exec(`UPDATE wallet_authorization_segment SET settlement_payload=$2::jsonb,known_fee_units=$3 WHERE authorization_id=$1`, zero, string(payload), fee)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	before, ok := fundingV7mRead(x, zero)
	require.True(t, ok)
	require.Equal(t, fundingV7mSegment{state: "released", pin: "active", version: 2, actual: 0, payloadUnits: 0}, before, "exact excluded shape: a released zero share of a charged group under an unacknowledged expiry intent")
	var acked bool
	require.NoError(t, x.f.db.QueryRow(`SELECT expiry_ack_at IS NOT NULL FROM wallet_authorization_segment WHERE authorization_id=$1`, zero).Scan(&acked))
	require.False(t, acked)
	require.Equal(t, "active", fundingV7mPinState(t, x, zero).State, "the expiry RPC has not reached D1: the pin is still active, so a plain release would be accepted")
	require.Equal(t, "released", fundingV7mHoldState(t, x, zero))

	fundingV7mFreezeByReclaim(t, x, short, 50000000)
	fundingV7mObserveUntouched(t, x, short, zero, "released", 3*time.Second)
	var closed bool
	require.NoError(t, x.f.db.QueryRow(`SELECT closed FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, short).Scan(&closed))
	require.False(t, closed, "the lease cannot close while the expiry-intent share is unresolved")
	after, ok := fundingV7mRead(x, zero)
	require.True(t, ok)
	require.Equal(t, before, after)
}

// C. Trusted gateway job id. A MEDIA share's D1 pin belongs to the media TASK id
// (MediaTaskService pins every share with gateway_job_id = task id), not to the
// parent authorization id. The frozen-lease cleanup must finish a media zero
// share's pin under the job id from walletExpiryGatewayJobID (the task id): the
// actual Worker refuses any finish whose gateway_job_id differs from the pin's
// (task-pin.ts samePin), so a parent-id finish would fail on every pass, the pin
// would stay active and the lease could never return or close.
//
// Reachability: in V5 a media share is only on a shared llm/legacy lease
// through a saved historical plan, and the media fee plan (persistMediaFeePlan)
// charges every share its whole hold because the plan's holds always sum to the
// quote (authorizePoolOnce refuses any other saved total). A media zero share of a
// charged group is therefore never produced by the current writers. This case
// keeps every production step that can be kept: the media task row, the saved
// shared-lease plan armed by the real authorizer, and D1 pins created through
// the actual Worker route exactly as MediaTaskService.pin sends them; only the
// fee plan's outcome (share 0 charged, share 1 zero) is written by PG surgery.
// The share's Redis hold is still armed (a crash between the fee plan commit and
// its zero-share hold release), so the cleanup's hold release is observable too.
func TestExternalFundingV7mFrozenLeaseCleanupFinishesMediaZeroSharePinUnderTaskID(t *testing.T) {
	x := newFundingV7Fixture(t)
	ctx := context.Background()
	user := x.f.platformUserID
	short := "funding-v7m-media-short-" + uuid.NewString()
	code, raw := immediateV5WirePost(t, x.base, x.secret, "/__fixture/seed", map[string]string{"platform_user_id": user, "grant_units": "90000000", "lease_id": short, "budget_units": "90000000"}, true)
	require.Equal(t, http.StatusOK, code, string(raw))
	require.NoError(t, x.wallet.InstallCanonicalWalletLease(ctx, service.CanonicalWalletLease{LeaseID: short, PlatformUserID: user, Currency: "USD", FundedUnits: 90000000, BudgetUnits: 90000000, FundingScope: "legacy", ExpiresAt: time.Now().Add(65 * time.Minute)}))

	// The media task, with the same immutable identity mediaFunding inserts.
	const quote, charged, zeroHeld = int64(50000000), int64(30000000), int64(20000000)
	template := x.f.create(t, "funding-v7m-media-template-"+uuid.NewString(), "google/nano-banana", "1:1")
	var templateSnapshot string
	require.NoError(t, x.f.db.QueryRow(`SELECT billing_snapshot_id FROM gateway_media_task WHERE id=$1`, template.TaskID).Scan(&templateSnapshot))
	snapshot, err := repository.ProvideBillingSnapshotStore(x.f.db).GetBillingSnapshot(ctx, templateSnapshot)
	require.NoError(t, err)
	snapshot.ID = "funding-v7m-media-snapshot-" + uuid.NewString()
	snapshot.FrozenAt = time.Now().UTC()
	snapshot.Pricing.DefaultPerRequestPrice = float64(quote) / 100000000
	require.NoError(t, repository.ProvideBillingSnapshotStore(x.f.db).InsertBillingSnapshot(ctx, snapshot))
	task := "media_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	parent := "auth_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	second := "auth_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	firstEvent := "funding-v7m-media-event-" + uuid.NewString()
	secondEvent := service.CanonicalWalletSettlementEventID(task+":"+second, user, "USD")
	_, err = x.f.db.Exec(`INSERT INTO gateway_media_task(id,user_id,platform_user_id,api_key_id,idempotency_key,request_hash,model,media_type,option,prompt,request_payload,billing_snapshot_id,quoted_units,authorization_id,settlement_event_id,deadline_at,financial_policy_version,financial_runtime_deadline)
	 SELECT $2,user_id,platform_user_id,api_key_id,$2,request_hash,model,media_type,option,prompt,request_payload,$5,$6,$3,$4,deadline_at,financial_policy_version,financial_runtime_deadline FROM gateway_media_task WHERE id=$1`, template.TaskID, task, parent, firstEvent, snapshot.ID, quote)
	require.NoError(t, err)
	_, err = x.f.db.Exec(`UPDATE gateway_media_task SET status='failed',pin_state='finished',actual_units=0 WHERE id=$1`, template.TaskID)
	require.NoError(t, err)

	// A saved historical shared binding: 0.3 on the source lease, 0.2 on the second.
	plan := []struct {
		auth, lease, event string
		held               int64
	}{{parent, x.source, firstEvent, charged}, {second, short, secondEvent, zeroHeld}}
	for ordinal, share := range plan {
		basis, basisErr := x.wallet.GetCanonicalWalletLeaseByID(ctx, user, share.lease)
		require.NoError(t, basisErr)
		basisRaw, marshalErr := json.Marshal(basis)
		require.NoError(t, marshalErr)
		_, err = x.f.db.Exec(`INSERT INTO wallet_authorization_segment(parent_authorization_id,ordinal,authorization_id,platform_user_id,billing_snapshot_id,lease_id,held_units,lease_basis,kind,event_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,'media',$9)`, parent, ordinal, share.auth, user, snapshot.ID, share.lease, share.held, string(basisRaw), share.event)
		require.NoError(t, err)
	}
	h, err := service.NewCanonicalWalletAuthorizer(x.f.cfg, x.f.bridge, x.f.snapshots).Authorize(service.WithMediaFundingOwner(ctx, task), service.AuthorizeInput{Snapshot: snapshot, User: x.owner, FixedEstimateUnits: quote, DurableAuthorizationID: parent})
	require.NoError(t, err)
	require.True(t, h.HoldArmed)
	require.Len(t, h.Segments, 2, "the real authorizer armed the saved shared-lease plan")
	require.Equal(t, short, h.Segments[1].LeaseID)
	for _, share := range plan {
		// Exactly the create request MediaTaskService.pin sends for each share.
		code, raw = immediateV5WirePost(t, x.base, x.secret, "/api/internal/v2/wallet/task-pins/create", map[string]any{"authorization_kind": "media", "authorization_id": share.auth, "gateway_job_id": task, "platform_user_id": user, "lease_id": share.lease, "held": amount(share.held), "billing_snapshot_id": snapshot.ID, "settlement_event_id": share.event, "usd_wallet_policy_version": "usd-wallet-v1"}, false)
		require.Equal(t, http.StatusOK, code, string(raw))
	}
	_, err = x.f.db.Exec(`UPDATE wallet_authorization_segment SET pin_state='active' WHERE parent_authorization_id=$1`, parent)
	require.NoError(t, err)
	// The fee plan's outcome: share 0 charged its whole hold, share 1 zero.
	for ordinal, share := range plan {
		actual, state := share.held, "settling"
		if ordinal == 1 {
			actual, state = 0, "released"
		}
		payload, marshalErr := json.Marshal(service.CanonicalWalletSettlementEvent{EventID: share.event, GatewayRequestID: task, PlatformUserID: user, LeaseID: share.lease, Currency: "USD", AmountUnits: actual, OccurredAt: time.Now().UTC()})
		require.NoError(t, marshalErr)
		_, err = x.f.db.Exec(`UPDATE wallet_authorization_segment SET state=$2,actual_units=$3,settlement_payload=$4::jsonb WHERE authorization_id=$1`, share.auth, state, actual, string(payload))
		require.NoError(t, err)
	}
	before, ok := fundingV7mRead(x, second)
	require.True(t, ok)
	require.Equal(t, fundingV7mSegment{state: "released", pin: "active", version: 0, actual: 0, payloadUnits: 0}, before)
	pin := fundingV7mPinState(t, x, second)
	require.Equal(t, "active", pin.State)
	require.Equal(t, task, pin.GatewayJobID, "the media share's D1 pin belongs to the task id")
	require.NotEqual(t, parent, pin.GatewayJobID)
	require.Equal(t, "armed", fundingV7mHoldState(t, x, second), "the zero share's Redis hold is still armed")
	fundingV7mFinishAudit(t, x)

	// No credit is free: any media quote reclaims both shared leases' free tails.
	fundingV7mFreezeByReclaim(t, x, short, 50000000)
	fundingV7mAwaitEnded(t, x, short, second, 12*time.Second, "the media zero share's pin is finished under its task id and the second lease closes")
	var oldState, oldPin string
	var frozen bool
	require.NoError(t, x.f.db.QueryRow(`SELECT old_state,old_pin_state,lease_frozen FROM funding_v7m_finish_audit WHERE authorization_id=$1`, second).Scan(&oldState, &oldPin, &frozen))
	t.Logf("media zero share finish transition: old_state=%s old_pin_state=%s lease_frozen=%v", oldState, oldPin, frozen)
	require.Equal(t, "released", oldState)
	require.Equal(t, "active", oldPin)
	require.True(t, frozen, "finished by the frozen-lease cleanup (the media service is stopped)")
	pin = fundingV7mPinState(t, x, second)
	require.Equal(t, task, pin.GatewayJobID)
	require.Equal(t, "released", fundingV7mHoldState(t, x, second), "the cleanup released the zero share's armed hold")
	receipt := fundingV7PrimaryReceipt(t, x.f.db, user, short)
	fundingV7VerifyReturnSignature(t, x.secret, receipt)
	require.Equal(t, "close", receipt.Mode)
	require.Equal(t, "0", receipt.HeldUnits)
	require.Equal(t, "90000000", receipt.ReturnedAfterUnits, "the whole second lease principal is back once the media zero share has ended")
}

// D. An all-zero attempt is not this branch's to end. When every share of a
// split attempt is zero, its pins end only through the signed zero finalizer,
// whose Worker finish carries the authorization token and returns the signed zero
// receipt. A plain cleanup release first would finish the D1 pin without that
// token, after which the Worker refuses the signed zero receipt forever (its
// zeroReceiptView requires the pin's token) and the attempt can never be
// acknowledged. The branch's charged-group query is what keeps it away.
//
// Reachability: in V5 an all-zero LLM group never carries a settlement payload
// (a zero fee goes to finishZeroPoolAttempt, never to ObserveSettlement), so
// the cleanup only selects it after its zero ACK. This case therefore keeps the
// natural all-zero attempt (a provider's explicit validation rejection, with the
// signed zero finish held back at a transparent proxy so the attempt stays
// unacknowledged), and adds by PG surgery only the zero settlement payload an
// all-zero observation would write, the exact shape the charged query excludes.
func TestExternalFundingV7mFrozenLeaseCleanupLeavesAllZeroGroupToSignedZero(t *testing.T) {
	x := newFundingV7Fixture(t)
	x.f.bridge.Close()
	target, err := url.Parse(x.base)
	require.NoError(t, err)
	holdSignedZero := &atomic.Bool{}
	holdSignedZero.Store(true)
	heldBack := &atomic.Int64{}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, readErr := io.ReadAll(r.Body)
		if readErr != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		if r.URL.Path == "/api/internal/v2/wallet/task-pins/finish" && holdSignedZero.Load() {
			var finish struct {
				Token string `json:"authorization_token"`
			}
			if json.Unmarshal(body, &finish) == nil && finish.Token != "" {
				heldBack.Add(1)
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
		}
		forwarded := r.Clone(r.Context())
		forwarded.URL.Scheme, forwarded.URL.Host, forwarded.RequestURI = target.Scheme, target.Host, ""
		forwarded.Body = io.NopCloser(bytes.NewReader(body))
		response, forwardErr := http.DefaultClient.Do(forwarded)
		if forwardErr != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		defer func() { _ = response.Body.Close() }()
		for key, values := range response.Header {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(response.StatusCode)
		_, _ = io.Copy(w, response.Body)
	}))
	t.Cleanup(proxy.Close)
	x.f.cfg.CanonicalWallet.ControlPlaneURL = proxy.URL
	x.f.bridge = service.NewCanonicalWalletBridge(x.f.cfg, x.wallet, x.f.db, repository.ProvideWalletOutboxStore(x.f.db))
	t.Cleanup(x.f.bridge.Close)

	h, short := fundingV7mSplit(t, x)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"error":{"message":"safe validation rejection"}}`)
	}))
	defer provider.Close()
	request, err := http.NewRequestWithContext(service.WithAuthorizationHandle(context.Background(), h), http.MethodPost, provider.URL+"/v1/responses", strings.NewReader(`{"model":"gpt-5.1"}`))
	require.NoError(t, err)
	response, err := service.NewAuthorizingHTTPUpstream(&mediaHTTP{client: provider.Client()}, x.f.cfg).Do(request, "", 0, 1)
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Positive(t, heldBack.Load(), "the signed zero finish reached the Worker boundary and was held back")
	var allZero bool
	require.NoError(t, x.f.db.QueryRow(`SELECT bool_and(state='released' AND actual_units=0 AND known_fee_units=0 AND zero_intent_at IS NOT NULL AND zero_ack_at IS NULL AND settlement_payload IS NULL AND remainder_payload IS NULL) AND count(*)=2
	 FROM wallet_authorization_segment WHERE parent_authorization_id=$1`, h.ID).Scan(&allZero))
	require.True(t, allZero, "a natural all-zero split attempt awaiting its signed zero receipt")
	// The zero settlement plan an all-zero observation writes for each share.
	for _, segment := range h.Segments {
		payload, marshalErr := json.Marshal(service.CanonicalWalletSettlementEvent{EventID: segment.EventID, GatewayRequestID: "funding-v7m-all-zero-" + h.ID, PlatformUserID: x.f.platformUserID, LeaseID: segment.LeaseID, Currency: "USD", AmountUnits: 0, OccurredAt: time.Now().UTC()})
		require.NoError(t, marshalErr)
		_, err = x.f.db.Exec(`UPDATE wallet_authorization_segment SET settlement_payload=$2::jsonb WHERE authorization_id=$1 AND settlement_payload IS NULL`, segment.AuthorizationID, string(payload))
		require.NoError(t, err)
	}
	zero := h.Segments[1].AuthorizationID
	before, ok := fundingV7mRead(x, zero)
	require.True(t, ok)
	require.Equal(t, fundingV7mSegment{state: "released", pin: "active", version: 0, actual: 0, payloadUnits: 0}, before, "the branch's shape, except that no sibling is charged")
	require.Equal(t, "active", fundingV7mPinState(t, x, zero).State)
	require.Equal(t, "armed", fundingV7mHoldState(t, x, zero), "the signed zero finalizer releases holds only after every receipt")

	fundingV7mFreezeByReclaim(t, x, short, 50000000)
	fundingV7mObserveUntouched(t, x, short, zero, "armed", 3*time.Second)

	// Let the signed zero finalizer run (the media service tick drives it).
	holdSignedZero.Store(false)
	x.f.svc = x.f.newService(t)
	require.Eventually(t, func() bool {
		var acked bool
		pins, leases, d1 := fundingV7mD1(x)
		var closed bool
		return x.f.db.QueryRow(`SELECT bool_and(zero_ack_at IS NOT NULL AND state='finished' AND pin_state='finished' AND funding_terminal_cleanup_at IS NOT NULL) FROM wallet_authorization_segment WHERE parent_authorization_id=$1`, h.ID).Scan(&acked) == nil && acked &&
			d1 && pins[zero].State == "released" && leases[short].Status == "closed" &&
			x.f.db.QueryRow(`SELECT closed FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, short).Scan(&closed) == nil && closed
	}, 15*time.Second, 50*time.Millisecond, "the signed zero finalizer acknowledges every share and only then does the second lease close")
	x.f.svc.Stop()
	pin := fundingV7mPinState(t, x, zero)
	require.NotNil(t, pin.Token)
	require.Equal(t, h.LastWriteToken(), *pin.Token, "the D1 pin was finished by the signed zero finish, carrying the attempt's token")
	var receiptRaw []byte
	require.NoError(t, x.f.db.QueryRow(`SELECT zero_receipt FROM wallet_authorization_segment WHERE authorization_id=$1`, zero).Scan(&receiptRaw))
	var receipt service.WalletTaskPinReceipt
	require.NoError(t, json.Unmarshal(receiptRaw, &receipt))
	_, err = service.VerifyWalletTaskPinReceipt(x.secret, service.WalletTaskPinReceiptExpected{GatewayJobID: h.ID, AuthorizationID: zero, PlatformUserID: x.f.platformUserID, LeaseID: short, BillingSnapshotID: x.snapshot.ID, SettlementEventID: h.Segments[1].EventID, HeldUnits: h.Segments[1].HeldUnits, AuthorizationKind: "llm", AuthorizationToken: h.LastWriteToken(), Status: "released"}, receipt)
	require.NoError(t, err, "the zero share holds a genuine signed zero receipt")
}
