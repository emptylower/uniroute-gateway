//go:build media_integration

package media_integration

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Final usage of a complete stream whose output count is not a number: positive
// (7 input tokens) but impossible to prove strictly. The reader evidence is
// untrusted for good.
const barrierMalformedFinal = `{"input_tokens":7,"output_tokens":"bad"}`

// Strict final usage: 1 input token, which the fixture prices at 0.8.
const barrierStrictFinal = `{"input_tokens":1,"output_tokens":0}`

type barrierFixture struct {
	x     fundingV7Fixture
	usage *service.OpenAIGatewayService
	repo  service.UsageBillingRepository
}

func newBarrierFixture(t *testing.T) barrierFixture {
	t.Helper()
	x := newFundingV7Fixture(t)
	billingRepository := repository.NewUsageBillingRepository(nil, x.f.db)
	x.f.bridge.SetBillingEvidenceRepository(billingRepository)
	usageService := service.NewOpenAIGatewayService(nil, nil, billingRepository, x.f.users, nil, nil, repository.NewGatewayCache(x.f.rdb), x.f.cfg, x.f.db, repository.ProvideWalletOutboxStore(x.f.db), nil, nil, service.NewBillingService(x.f.cfg, nil), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, x.f.snapshots)
	t.Cleanup(usageService.CloseOpenAIWSPool)
	return barrierFixture{x: x, usage: usageService, repo: billingRepository}
}

// streamCompleted reads a complete SSE response whose terminal usage is final.
func (b barrierFixture) streamCompleted(t *testing.T, h *service.AuthorizationHandle, final string) {
	t.Helper()
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, walletStreamPrelude)
		_, _ = io.WriteString(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_barrier_v5\",\"status\":\"completed\",\"usage\":"+final+"}}\n\n")
	}))
	defer provider.Close()
	response := b.x.streamRequest(t, h, provider)
	_, err := io.Copy(io.Discard, response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
}

// record hands the gateway's own usage to billing and returns what RecordUsage said.
func (b barrierFixture) record(t *testing.T, h *service.AuthorizationHandle, inputTokens int) error {
	t.Helper()
	var usageErr error
	_, prepareErr := h.PrepareUsageTask(context.Background(), func(ctx context.Context) {
		usageErr = b.usage.RecordUsage(ctx, &service.OpenAIRecordUsageInput{Result: &service.OpenAIForwardResult{RequestID: "barrier-" + h.ID, Model: "gpt-5.1", Usage: service.OpenAIUsage{InputTokens: inputTokens}}, User: b.x.owner, APIKey: &service.APIKey{ID: b.x.snapshot.APIKeyID, Quota: 100}, Account: &service.Account{ID: b.x.snapshot.AccountID, Type: service.AccountTypeAPIKey, Platform: service.PlatformOpenAI}, BillingSnapshot: b.x.snapshot, AuthorizationID: h.ID, AuthorizationToken: h.LastWriteToken()})
	})
	if usageErr != nil {
		require.Error(t, prepareErr, "a failed usage handoff is reported to the caller too")
	}
	return usageErr
}

// ageDeadline moves one segment's expiry_deadline into the past (ordinal < 0: all of
// them). expiry_deadline is immutable evidence in production, guarded by a trigger;
// the test database is the only place that trigger is disabled, for one statement.
func (b barrierFixture) ageDeadline(t *testing.T, h *service.AuthorizationHandle, ordinal, minutes int) {
	t.Helper()
	where := ""
	if ordinal >= 0 {
		where = fmt.Sprintf(" AND ordinal=%d", ordinal)
	}
	_, err := b.x.f.db.Exec(fmt.Sprintf(`ALTER TABLE wallet_authorization_segment DISABLE TRIGGER wallet_unknown_expiry_guard; UPDATE wallet_authorization_segment SET expiry_deadline=now()-interval '%d minutes' WHERE parent_authorization_id='%s'%s; ALTER TABLE wallet_authorization_segment ENABLE TRIGGER wallet_unknown_expiry_guard`, minutes, h.ID, where))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = b.x.f.db.Exec(`ALTER TABLE wallet_authorization_segment ENABLE TRIGGER wallet_unknown_expiry_guard`)
	})
}

func (b barrierFixture) flags(t *testing.T, h *service.AuthorizationHandle) (fee, evidence []bool) {
	t.Helper()
	rows, err := b.x.f.db.Query(`SELECT fee_pending,evidence_pending FROM wallet_authorization_segment WHERE parent_authorization_id=$1 ORDER BY ordinal`, h.ID)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var f, e bool
		require.NoError(t, rows.Scan(&f, &e))
		fee, evidence = append(fee, f), append(evidence, e)
	}
	require.NoError(t, rows.Err())
	return fee, evidence
}

func (b barrierFixture) unknownCounters(h *service.AuthorizationHandle) int {
	var n int
	_ = b.x.f.db.QueryRow(`SELECT count(*) FROM wallet_unknown_release_counter WHERE parent_authorization_id=$1`, h.ID).Scan(&n)
	return n
}

func (b barrierFixture) held(t *testing.T, h *service.AuthorizationHandle) bool {
	t.Helper()
	hold, err := b.x.wallet.GetCanonicalWalletHold(context.Background(), b.x.f.platformUserID, h.ID)
	return err == nil && hold.State == "armed"
}

func allTrue(values []bool) bool {
	for _, v := range values {
		if !v {
			return false
		}
	}
	return len(values) > 0
}

func allFalse(values []bool) bool {
	for _, v := range values {
		if v {
			return false
		}
	}
	return len(values) > 0
}

// A terminal usage that is positive but cannot be proven strictly sets the
// fee/evidence barrier, and no trusted fee can ever follow. The barrier is held
// until the segment's expiry deadline plus the grace has passed, then the hold is
// released ONCE as an unknown cost that the platform bears. Nothing is charged.
func TestExternalWalletUnresolvableFeeBarrierReleasesAsUnknownAfterDeadlinePlusGrace(t *testing.T) {
	b := newBarrierFixture(t)
	h := b.x.authorize(t, 200000000)
	b.streamCompleted(t, h, barrierMalformedFinal)
	require.Error(t, b.record(t, h, 7), "a positive terminal usage that cannot be proven strictly sets the barrier")
	fee, evidence := b.flags(t, h)
	require.True(t, allTrue(fee) && allTrue(evidence), "the unprovable positive usage latched the barrier")

	// The recovery lane of a fresh service owns the unresolved reader.
	b.x.f.svc = b.x.f.newService(t)

	// 29 minutes past the deadline is still inside the 30 minute grace: the barrier holds.
	b.ageDeadline(t, h, -1, 29)
	require.Never(t, func() bool { return b.unknownCounters(h) > 0 || !b.held(t, h) }, 3*time.Second, 100*time.Millisecond, "the barrier must hold until deadline plus grace")
	fee, evidence = b.flags(t, h)
	require.True(t, allTrue(fee) && allTrue(evidence))

	// 31 minutes past the deadline: released once, as an unknown cost.
	b.ageDeadline(t, h, -1, 31)
	require.Eventually(t, func() bool { return b.unknownCounters(h) == 1 }, 25*time.Second, 100*time.Millisecond, "the unresolved barrier is released as one unknown-cost event")
	fee, evidence = b.flags(t, h)
	require.True(t, allFalse(fee) && allFalse(evidence), "the barrier is cleared by the release")
	require.Eventually(t, func() bool { return !b.held(t, h) }, 10*time.Second, 100*time.Millisecond, "the hold is released")
	var reason string
	var version int
	var heldUnits int64
	require.NoError(t, b.x.f.db.QueryRow(`SELECT COALESCE(terminal_evidence->>'Reason',''),expiry_intent_version FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0`, h.ID).Scan(&reason, &version))
	require.Equal(t, "barrier_timeout_unknown", reason, "the seal records why it was released")
	require.Equal(t, 2, version, "released through the immediate unknown-release path")
	require.NoError(t, b.x.f.db.QueryRow(`SELECT held_units FROM wallet_unknown_release_counter WHERE parent_authorization_id=$1`, h.ID).Scan(&heldUnits))
	require.Equal(t, int64(200000000), heldUnits)
	var staged, charges int
	require.NoError(t, b.x.f.db.QueryRow(`SELECT count(*) FROM wallet_billing_pending WHERE parent_authorization_id=$1`, h.ID).Scan(&staged))
	require.Zero(t, staged, "an unknown cost is not charged")
	require.NoError(t, b.x.f.db.QueryRow(`SELECT count(*) FROM wallet_billing_charge_receipt c JOIN wallet_billing_pending p ON p.id=c.pending_id WHERE p.parent_authorization_id=$1`, h.ID).Scan(&charges))
	require.Zero(t, charges)
	require.Never(t, func() bool { return b.unknownCounters(h) > 1 }, 2*time.Second, 100*time.Millisecond, "never released twice")
}

// A barrier whose reader evidence IS provable (here the gateway's own number
// disagreed with the strict stream usage) must be CHARGED the reader's fee once it
// is overdue, never released as an unknown cost: the owner of the stream is still
// alive in this process, which is exactly the case recovery used to skip forever.
func TestExternalWalletOverdueBarrierWithProvableEvidenceIsChargedNotReleased(t *testing.T) {
	b := newBarrierFixture(t)
	h := b.x.authorize(t, 200000000)
	b.streamCompleted(t, h, barrierStrictFinal)
	// The gateway computed 2 tokens against the stream's strict 1: the numbers disagree.
	require.Error(t, b.record(t, h, 2))
	fee, evidence := b.flags(t, h)
	require.True(t, allTrue(fee) && allTrue(evidence))
	b.x.f.svc = b.x.f.newService(t)

	b.ageDeadline(t, h, -1, 29)
	require.Never(t, func() bool {
		var staged int
		_ = b.x.f.db.QueryRow(`SELECT count(*) FROM wallet_billing_pending WHERE parent_authorization_id=$1`, h.ID).Scan(&staged)
		return staged > 0 || b.unknownCounters(h) > 0
	}, 3*time.Second, 100*time.Millisecond, "nothing moves before deadline plus grace")

	b.ageDeadline(t, h, -1, 31)
	var fee80 int64
	var applied bool
	require.Eventually(t, func() bool {
		return b.x.f.db.QueryRow(`SELECT fee_units,apply_ack_at IS NOT NULL FROM wallet_billing_pending WHERE parent_authorization_id=$1`, h.ID).Scan(&fee80, &applied) == nil && applied
	}, 25*time.Second, 100*time.Millisecond, "the provable reader fee is staged and applied")
	require.Equal(t, int64(80000000), fee80, "the reader's own strict fee, not the gateway's disagreeing number")
	require.Zero(t, b.unknownCounters(h), "a provable fee is never released as an unknown cost")
	var charges int
	require.NoError(t, b.x.f.db.QueryRow(`SELECT count(*) FROM wallet_billing_charge_receipt c JOIN wallet_billing_pending p ON p.id=c.pending_id WHERE p.parent_authorization_id=$1`, h.ID).Scan(&charges))
	require.Equal(t, 1, charges)
}

// Shadow mode is the observation stage of the rollout: an overdue barrier is only
// reported, never cleared and never released.
func TestExternalWalletOverdueBarrierIsNotReleasedInShadowMode(t *testing.T) {
	b := newBarrierFixture(t)
	h := b.x.authorize(t, 200000000)
	b.streamCompleted(t, h, barrierMalformedFinal)
	require.Error(t, b.record(t, h, 7))
	b.x.f.bridge.Close()
	b.x.f.cfg.CanonicalWallet.LLMImmediateReleaseMode, b.x.f.cfg.CanonicalWallet.MediaImmediateReleaseMode = "shadow", "shadow"
	b.x.f.bridge = service.NewCanonicalWalletBridge(b.x.f.cfg, b.x.wallet, b.x.f.db, repository.ProvideWalletOutboxStore(b.x.f.db))
	b.x.f.bridge.SetBillingEvidenceRepository(b.repo)
	t.Cleanup(b.x.f.bridge.Close)
	b.x.f.svc = b.x.f.newService(t)
	b.ageDeadline(t, h, -1, 90)
	require.Never(t, func() bool {
		fee, evidence := b.flags(t, h)
		return b.unknownCounters(h) > 0 || !b.held(t, h) || !allTrue(fee) || !allTrue(evidence)
	}, 4*time.Second, 100*time.Millisecond, "shadow mode never clears a barrier or releases a hold")
}

// An attempt that spans two leases has one deadline per segment. The release is
// all-or-nothing: while any segment is inside its grace nothing is sealed or
// cleared, so no half-sealed group can be left behind; once every segment is
// overdue the whole group is released together.
func TestExternalWalletBarrierReleaseIsAllOrNothingAcrossSegments(t *testing.T) {
	b := newBarrierFixture(t)
	x := b.x
	x.authorize(t, 910000000)
	short := "funding-v7-short-" + uuid.NewString()
	code, raw := immediateV5WirePost(t, x.base, x.secret, "/__fixture/seed", map[string]string{"platform_user_id": x.f.platformUserID, "grant_units": "90000000", "lease_id": short, "budget_units": "90000000"}, true)
	require.Equal(t, http.StatusOK, code, string(raw))
	require.NoError(t, x.wallet.InstallCanonicalWalletLease(context.Background(), service.CanonicalWalletLease{LeaseID: short, PlatformUserID: x.f.platformUserID, Currency: "USD", FundedUnits: 90000000, BudgetUnits: 90000000, FundingScope: "legacy", ExpiresAt: time.Now().Add(time.Hour)}))
	h := x.authorize(t, 150000000)
	require.Len(t, h.Segments, 2)
	b.streamCompleted(t, h, barrierMalformedFinal)
	require.Error(t, b.record(t, h, 7))
	fee, evidence := b.flags(t, h)
	require.True(t, len(fee) == 2 && allTrue(fee) && allTrue(evidence))
	x.f.svc = x.f.newService(t)

	// Segment 0 is far overdue, segment 1 is still inside its grace.
	b.ageDeadline(t, h, 0, 90)
	b.ageDeadline(t, h, 1, 10)
	require.Never(t, func() bool {
		f, e := b.flags(t, h)
		var sealed int
		_ = x.f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND terminal_sealed_at IS NOT NULL`, h.ID).Scan(&sealed)
		return b.unknownCounters(h) > 0 || sealed > 0 || !allTrue(f) || !allTrue(e)
	}, 3*time.Second, 100*time.Millisecond, "no segment is sealed or cleared while another is inside its grace")

	b.ageDeadline(t, h, 1, 90)
	require.Eventually(t, func() bool { return b.unknownCounters(h) == len(h.Segments) }, 25*time.Second, 100*time.Millisecond, "every segment of the group is released together")
	fee, evidence = b.flags(t, h)
	require.True(t, allFalse(fee) && allFalse(evidence))
	require.Eventually(t, func() bool { return !b.held(t, h) }, 10*time.Second, 100*time.Millisecond)
}
