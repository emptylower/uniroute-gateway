//go:build media_integration

package media_integration

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func fundingV7VerifyReturnSignature(t *testing.T, secret string, r service.WalletFundingReturnReceipt) {
	t.Helper()
	sources, err := json.Marshal(r.Sources)
	require.NoError(t, err)
	payload, err := json.Marshal([]string{r.Protocol, r.ReceiptID, r.PlatformUserID, r.LeaseID, r.FundingScope, r.FundingOwnerID, r.FundingIssuanceKey, strconv.FormatInt(r.ReturnRevision, 10), strconv.FormatInt(r.BudgetRevision, 10), strconv.FormatInt(r.CaptureSeq, 10), r.FundedUnits, r.CapturedUnits, r.ReturnedBeforeUnits, r.ReturnedUnits, r.ReturnedAfterUnits, r.CreditedUnits, r.WriteOffUnits, r.HeldUnits, r.GatewayConsumedUnits, r.GatewayReleasedUnits, r.Mode, r.CommittedAt, string(sources)})
	require.NoError(t, err)
	mac := hmac.New(sha256.New, []byte(secret))
	_, err = mac.Write(payload)
	require.NoError(t, err)
	signature, err := hex.DecodeString(r.Signature)
	require.NoError(t, err)
	require.True(t, hmac.Equal(mac.Sum(nil), signature), "every source disposition comes from the original signed canonical ACK")
}

func TestExternalFundingV7OriginalSourcePolicyWriteoffNeverRecreditsTerminalResidual(t *testing.T) {
	for _, mode := range []string{"expired", "revoked"} {
		t.Run(mode, func(t *testing.T) {
			x := newFundingV7Fixture(t)
			h := x.authorize(t, 100000000)
			_, err := x.mediaFunding(t, 200000000)
			require.NoError(t, err)
			first := fundingV7PrimaryReceipt(t, x.f.db, x.f.platformUserID, x.source)
			require.Equal(t, int64(1), first.ReturnRevision)
			require.Equal(t, "900000000", first.CreditedUnits)
			require.Equal(t, "100000000", first.HeldUnits)
			code, raw := immediateV5WirePost(t, x.base, x.secret, "/__fixture/source-policy", map[string]string{"platform_user_id": x.f.platformUserID, "lease_id": x.source, "mode": mode}, true)
			require.Equal(t, http.StatusOK, code, "actual isolated original-grant policy endpoint is required: %s", raw)
			var policy struct {
				Count int `json:"affected_credit_count"`
			}
			require.NoError(t, json.Unmarshal(raw, &policy))
			require.Equal(t, 1, policy.Count)
			started := time.Now()
			x.signedZero(t, h)
			var closed bool
			var revision, returned, applied int64
			require.Eventually(t, func() bool {
				return x.f.db.QueryRow(`SELECT closed,return_revision,returned_units,redis_applied_revision FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, x.source).Scan(&closed, &revision, &returned, &applied) == nil && closed && revision == 2 && returned == 1000000000 && applied == revision
			}, 5*time.Second, 20*time.Millisecond)
			last := fundingV7PrimaryReceipt(t, x.f.db, x.f.platformUserID, x.source)
			fundingV7VerifyReturnSignature(t, x.secret, last)
			require.Equal(t, "close", last.Mode)
			require.Equal(t, "100000000", last.ReturnedUnits)
			require.Equal(t, "0", last.CreditedUnits)
			require.Equal(t, "100000000", last.WriteOffUnits)
			require.Equal(t, "0", last.CapturedUnits)
			require.Equal(t, "0", last.HeldUnits)
			require.Len(t, last.Sources, 1)
			require.Equal(t, "100000000", last.Sources[0].ReturnedUnits)
			require.Equal(t, "0", last.Sources[0].CreditedUnits)
			require.Equal(t, "source_grant_"+mode, last.Sources[0].WriteOffReason)
			require.Equal(t, int64(700000000), fundingV7Available(t, x), "stored remaining7 + original isolated media2 + policy writeoff1 conserves10; the stored7 is now policy-ineligible")
			require.Less(t, time.Since(started), 2500*time.Millisecond)
			fresh, err := service.NewCanonicalWalletAuthorizer(x.f.cfg, x.f.bridge, x.f.snapshots).Authorize(context.Background(), service.AuthorizeInput{Snapshot: x.snapshot, User: x.owner, FixedEstimateUnits: 1})
			require.Error(t, err, "fresh public LLM authorization cannot spend the now-ineligible original grant or recreate the written-off residual")
			// In enforce mode the authorizer deliberately returns the minted handle
			// together with the refusal so the refusal carries an authorization id.
			// What matters is that the refused handle armed nothing.
			require.NotNil(t, fresh)
			require.NotNil(t, fresh.Refusal)
			require.False(t, fresh.HoldArmed)
			require.Empty(t, fresh.Segments)
			require.Zero(t, fresh.HeldUnits)
			var refusal *service.AuthorizationRefusedError
			require.ErrorAs(t, err, &refusal)
			require.Equal(t, service.AuthorizationRefusalBalanceShortfall, refusal.Reason)
			require.ErrorIs(t, err, service.ErrCanonicalWalletBalanceShortfall)
			var positiveCharges, unknowns int
			require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_billing_charge_receipt c JOIN wallet_billing_pending p ON p.id=c.pending_id WHERE p.parent_authorization_id=$1 AND p.fee_units>0`, h.ID).Scan(&positiveCharges))
			require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_unknown_release_counter WHERE parent_authorization_id=$1`, h.ID).Scan(&unknowns))
			require.Zero(t, positiveCharges)
			require.Zero(t, unknowns)
		})
	}
}

// A trusted reader observes real positive usage whose fee (0.8) exceeds the
// frozen hold (0.5). The held part and the immutable overrun remainder are two
// stable events. While the remainder's delivered ACK is blocked, the original
// source's terminal generation must stay pending and the source must not be
// acknowledged or closed. After a restart with both release flags OFF, the
// durable generation must rediscover the remainder, and only then close.
//
// (An earlier draft of this case tried to charge a forwarder-computed number
// after the reader had sealed without usage. Production refuses that on
// purpose - see applyUsageBillingDetailed: a provider parser's number that is
// not backed by trusted reader evidence is never charged - so that scenario is
// not reachable and is intentionally not asserted here.)
func TestExternalFundingV7OverrunRemainderPendingKeepsSourceOpenUntilDeliveredAndResumesOff(t *testing.T) {
	x := newFundingV7Fixture(t)
	billingRepository := repository.NewUsageBillingRepository(nil, x.f.db)
	x.f.bridge.SetBillingEvidenceRepository(billingRepository)
	usageService := service.NewOpenAIGatewayService(nil, nil, billingRepository, x.f.users, nil, nil, repository.NewGatewayCache(x.f.rdb), x.f.cfg, x.f.db, repository.ProvideWalletOutboxStore(x.f.db), nil, nil, service.NewBillingService(x.f.cfg, nil), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, x.f.snapshots)
	t.Cleanup(usageService.CloseOpenAIWSPool)
	h := x.authorize(t, 50000000)
	_, err := x.mediaFunding(t, 200000000)
	require.NoError(t, err)
	require.Equal(t, "950000000", fundingV7PrimaryReceipt(t, x.f.db, x.f.platformUserID, x.source).ReturnedAfterUnits)
	segment := h.Segments[0]
	remainder := service.CanonicalWalletSettlementEventID(segment.AuthorizationID+":overrun", x.f.platformUserID, "USD")
	_, err = x.f.db.Exec(`CREATE FUNCTION funding_v7_remainder_delivery_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.event_id='` + remainder + `' AND NEW.status='delivered' THEN RAISE EXCEPTION 'isolated original late remainder delivered ACK failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER funding_v7_remainder_delivery_fault BEFORE UPDATE ON wallet_settlement_outbox FOR EACH ROW EXECUTE FUNCTION funding_v7_remainder_delivery_fault()`)
	require.NoError(t, err)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"funding-v7-overrun","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":0}}`)
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
		usageErr = usageService.RecordUsage(ctx, &service.OpenAIRecordUsageInput{Result: &service.OpenAIForwardResult{RequestID: "funding-v7-overrun-" + h.ID, Model: "gpt-5.1", Usage: service.OpenAIUsage{InputTokens: 1}}, User: x.owner, APIKey: &service.APIKey{ID: x.snapshot.APIKeyID, Quota: 100}, Account: &service.Account{ID: x.snapshot.AccountID, Type: service.AccountTypeAPIKey, Platform: service.PlatformOpenAI}, BillingSnapshot: x.snapshot, AuthorizationID: h.ID, AuthorizationToken: h.LastWriteToken()})
	})
	require.NoError(t, usageErr)
	require.NoError(t, err)
	require.NotNil(t, dispatch)
	dispatch(context.Background())
	var fee int64
	var applied bool
	require.Eventually(t, func() bool {
		return x.f.db.QueryRow(`SELECT fee_units,apply_ack_at IS NOT NULL FROM wallet_billing_pending WHERE parent_authorization_id=$1`, h.ID).Scan(&fee, &applied) == nil && fee == 80000000 && applied
	}, 5*time.Second, 20*time.Millisecond, "the original trusted reader fee is staged and applied exactly once")
	var originalPayload, originalRemainder []byte
	require.Eventually(t, func() bool {
		return x.f.db.QueryRow(`SELECT settlement_payload,remainder_payload FROM wallet_authorization_segment WHERE authorization_id=$1 AND actual_units=50000000`, h.ID).Scan(&originalPayload, &originalRemainder) == nil && len(originalPayload) > 0 && len(originalRemainder) > 0
	}, 5*time.Second, 20*time.Millisecond)
	var payload, extra service.CanonicalWalletSettlementEvent
	require.NoError(t, json.Unmarshal(originalPayload, &payload))
	require.NoError(t, json.Unmarshal(originalRemainder, &extra))
	require.Equal(t, int64(50000000), payload.AmountUnits, "the frozen hold is the held settlement")
	require.Equal(t, int64(30000000), extra.AmountUnits, "the overrun above the hold is an immutable remainder")
	require.Equal(t, remainder, extra.EventID)
	require.Empty(t, extra.AuthorizationID)
	var delivered, remainderDelivered bool
	require.Eventually(t, func() bool {
		return x.f.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM wallet_settlement_outbox WHERE event_id=$1 AND status='delivered'),EXISTS(SELECT 1 FROM wallet_settlement_outbox WHERE event_id=$2 AND status='delivered')`, segment.EventID, remainder).Scan(&delivered, &remainderDelivered) == nil && delivered && !remainderDelivered
	}, 8*time.Second, 20*time.Millisecond, "the held settlement delivers while the faulted remainder stays undelivered")
	var closed bool
	var revision int64
	var mode string
	require.Never(t, func() bool {
		_ = x.f.db.QueryRow(`SELECT closed,return_revision,requested_mode FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, x.source).Scan(&closed, &revision, &mode)
		return closed || mode == "close"
	}, 700*time.Millisecond, 20*time.Millisecond, "the original source cannot persist permanent close while its immutable remainder delivery is pending")
	var generation, appliedGeneration int64
	require.NoError(t, x.f.db.QueryRow(`SELECT generation,applied_generation FROM wallet_funding_terminal_work WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, x.source).Scan(&generation, &appliedGeneration))
	require.Greater(t, generation, appliedGeneration, "the generation stays pending, it is not acknowledged as a no-delta")
	// Restart with both release flags OFF while the remainder remains pending.
	// Its eventual delivery has no authorization_id wake and must be found by
	// the durable bounded retry of the original generation.
	x.f.bridge.Close()
	x.f.cfg.CanonicalWallet.LLMImmediateReleaseMode, x.f.cfg.CanonicalWallet.MediaImmediateReleaseMode = "off", "off"
	x.f.bridge = service.NewCanonicalWalletBridge(x.f.cfg, x.wallet, x.f.db, repository.ProvideWalletOutboxStore(x.f.db))
	x.f.bridge.SetBillingEvidenceRepository(billingRepository)
	t.Cleanup(x.f.bridge.Close)
	_, err = x.f.db.Exec(`DROP TRIGGER funding_v7_remainder_delivery_fault ON wallet_settlement_outbox; DROP FUNCTION funding_v7_remainder_delivery_fault()`)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return x.f.db.QueryRow(`SELECT f.closed,f.return_revision,w.generation,w.applied_generation FROM wallet_funding_freeze f JOIN wallet_funding_terminal_work w USING(platform_user_id,lease_id) WHERE f.platform_user_id=$1 AND f.lease_id=$2`, x.f.platformUserID, x.source).Scan(&closed, &revision, &generation, &appliedGeneration) == nil && closed && generation == appliedGeneration
	}, outboxStaleReclaimWindow(x.f.cfg.CanonicalWallet.RequestTimeoutMS), 20*time.Millisecond, "the durable generation rediscovers the remainder and closes the source only after it is delivered")
	var exact int
	require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_settlement_outbox WHERE platform_user_id=$1 AND status='delivered' AND billing_snapshot_id=$2 AND ((event_id=$3 AND amount_units=50000000) OR (event_id=$4 AND amount_units=30000000))`, x.f.platformUserID, x.snapshot.ID, segment.EventID, remainder).Scan(&exact))
	require.Equal(t, 2, exact, "both stable events are delivered exactly once")
	require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_billing_charge_receipt c JOIN wallet_billing_pending p ON p.id=c.pending_id WHERE p.parent_authorization_id=$1 AND p.fee_units=80000000`, h.ID).Scan(&exact))
	require.Equal(t, 1, exact, "one original charge receipt, never re-charged")
	last := fundingV7PrimaryReceipt(t, x.f.db, x.f.platformUserID, x.source)
	fundingV7VerifyReturnSignature(t, x.secret, last)
	require.Equal(t, "close", last.Mode)
	require.Equal(t, "0", last.HeldUnits)
	require.Equal(t, "50000000", last.CapturedUnits, "the held 0.5 is the only capture on the original source lease")
	require.Equal(t, "950000000", last.ReturnedAfterUnits)
	// Conservation of the original 10 across the whole D1 ledger. Every lease is
	// funded from, and its release returns to, the credit batches, so
	// remaining credits + sum(lease budget - released) must equal the grant. The
	// overrun remainder is settled on a separate settle-scope lease (fresh
	// funding), never on the original source lease.
	code, raw := immediateV5WirePost(t, x.base, x.secret, "/__fixture/snapshot", map[string]string{"platform_user_id": x.f.platformUserID}, true)
	require.Equal(t, http.StatusOK, code, string(raw))
	var snapshot struct {
		Credits []struct {
			Remaining string `json:"remainingUnits"`
		} `json:"credits"`
		Leases []struct {
			ID       string `json:"id"`
			Scope    string `json:"fundingScope"`
			Status   string `json:"status"`
			Budget   string `json:"budgetUnits"`
			Captured string `json:"capturedUnits"`
			Released string `json:"releasedUnits"`
		} `json:"leases"`
		Events []struct {
			ID      string `json:"id"`
			LeaseID string `json:"leaseId"`
			Amount  string `json:"amountUnits"`
		} `json:"events"`
	}
	require.NoError(t, json.Unmarshal(raw, &snapshot))
	number := func(v string) int64 {
		n, e := strconv.ParseInt(v, 10, 64)
		require.NoError(t, e)
		return n
	}
	ledger := int64(0)
	for _, credit := range snapshot.Credits {
		ledger += number(credit.Remaining)
	}
	var sourceClosed bool
	var settleCaptured int64
	for _, lease := range snapshot.Leases {
		ledger += number(lease.Budget) - number(lease.Released)
		if lease.ID == x.source {
			sourceClosed = lease.Status == "closed"
			require.Equal(t, int64(50000000), number(lease.Captured))
			require.Equal(t, int64(950000000), number(lease.Released))
		}
		if lease.Scope == "settle" {
			settleCaptured += number(lease.Captured)
		}
	}
	require.True(t, sourceClosed)
	require.Equal(t, int64(30000000), settleCaptured, "only the overrun remainder is settled on fresh funding")
	require.Equal(t, int64(1000000000), ledger, "credits plus lease backing conserve the original 10")
	var events int64
	for _, event := range snapshot.Events {
		events += number(event.Amount)
	}
	require.Equal(t, int64(80000000), events, "the charged fee is exactly the held 0.5 plus the 0.3 remainder")
}

func TestExternalFundingV7LowBalanceOverrunLeavesFreshLLMAuthorizable(t *testing.T) {
	for _, sourceUnits := range []int64{1000000, 133000000} {
		t.Run(strconv.FormatInt(sourceUnits, 10), func(t *testing.T) {
			// The source holds 0.005 of its 0.01 backing. A trusted 0.8 fee must
			// fund only the 0.795 remainder from the unleased 1.32, leaving spendable
			// funds for the next request while the remainder's delivered ACK is delayed.
			x := newFundingV7FixtureWithAmounts(t, 133000000, sourceUnits)
			billingRepository := repository.NewUsageBillingRepository(nil, x.f.db)
			x.f.bridge.SetBillingEvidenceRepository(billingRepository)
			usageService := service.NewOpenAIGatewayService(nil, nil, billingRepository, x.f.users, nil, nil, repository.NewGatewayCache(x.f.rdb), x.f.cfg, x.f.db, repository.ProvideWalletOutboxStore(x.f.db), nil, nil, service.NewBillingService(x.f.cfg, nil), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, x.f.snapshots)
			t.Cleanup(usageService.CloseOpenAIWSPool)
			h := x.authorize(t, 500000)
			var other *service.AuthorizationHandle
			if sourceUnits == 133000000 {
				other = x.authorize(t, 200000)
			}
			segment := h.Segments[0]
			require.Equal(t, x.source, segment.LeaseID)
			remainder := service.CanonicalWalletSettlementEventID(segment.AuthorizationID+":overrun", x.f.platformUserID, "USD")
			_, err := x.f.db.Exec(`CREATE FUNCTION funding_v7_availability_delivery_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.event_id='` + remainder + `' AND NEW.status='delivered' THEN RAISE EXCEPTION 'isolated remainder delivered ACK failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER funding_v7_availability_delivery_fault BEFORE UPDATE ON wallet_settlement_outbox FOR EACH ROW EXECUTE FUNCTION funding_v7_availability_delivery_fault()`)
			require.NoError(t, err)
			t.Cleanup(func() {
				_, _ = x.f.db.Exec(`DROP TRIGGER IF EXISTS funding_v7_availability_delivery_fault ON wallet_settlement_outbox; DROP FUNCTION IF EXISTS funding_v7_availability_delivery_fault()`)
			})
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"id":"funding-v7-availability","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":0}}`)
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
				usageErr = usageService.RecordUsage(ctx, &service.OpenAIRecordUsageInput{Result: &service.OpenAIForwardResult{RequestID: "funding-v7-availability-" + h.ID, Model: "gpt-5.1", Usage: service.OpenAIUsage{InputTokens: 1}}, User: x.owner, APIKey: &service.APIKey{ID: x.snapshot.APIKeyID, Quota: 100}, Account: &service.Account{ID: x.snapshot.AccountID, Type: service.AccountTypeAPIKey, Platform: service.PlatformOpenAI}, BillingSnapshot: x.snapshot, AuthorizationID: h.ID, AuthorizationToken: h.LastWriteToken()})
			})
			require.NoError(t, err)
			require.NotNil(t, dispatch)
			dispatch(context.Background())
			require.NoError(t, usageErr)
			var heldDelivered, remainderPending bool
			var settleLeaseID string
			require.Eventually(t, func() bool {
				return x.f.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM wallet_settlement_outbox WHERE event_id=$1 AND status='delivered'),status<>'delivered',lease_id FROM wallet_settlement_outbox WHERE event_id=$2`, segment.EventID, remainder).Scan(&heldDelivered, &remainderPending, &settleLeaseID) == nil && heldDelivered && remainderPending && settleLeaseID != ""
			}, 8*time.Second, 20*time.Millisecond)
			settle, err := x.wallet.GetCanonicalWalletLeaseByID(context.Background(), x.f.platformUserID, settleLeaseID)
			require.NoError(t, err)
			require.Equal(t, "settle", settle.FundingScope)
			require.Equal(t, int64(79500000), settle.BudgetUnits, "settlement funding reserves only the actual overrun, not the whole available wallet")
			source, err := x.wallet.GetCanonicalWalletLeaseByID(context.Background(), x.f.platformUserID, x.source)
			require.NoError(t, err)
			require.False(t, source.Sealed, "separate settlement funding must not seal the original LLM source")
			expectedConsumed := int64(500000)
			if other != nil {
				expectedConsumed += other.HeldUnits
				partial := fundingV7PrimaryReceipt(t, x.f.db, x.f.platformUserID, x.source)
				fundingV7VerifyReturnSignature(t, x.secret, partial)
				require.Equal(t, "partial", partial.Mode)
				require.Equal(t, "200000", partial.HeldUnits)
				require.Equal(t, "500000", partial.CapturedUnits)
				require.Equal(t, "132300000", partial.ReturnedAfterUnits)
				require.True(t, source.FundingFrozen)
				var closed bool
				var mode string
				require.NoError(t, x.f.db.QueryRow(`SELECT closed,requested_mode FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, x.source).Scan(&closed, &mode))
				require.False(t, closed, "pending original holds and the undelivered remainder cannot close their source")
				require.Equal(t, "partial", mode)
				hold, err := x.wallet.GetCanonicalWalletHold(context.Background(), x.f.platformUserID, other.ID)
				require.NoError(t, err)
				require.Equal(t, "armed", hold.State)
				require.Equal(t, int64(200000), hold.HeldUnits)
			}
			require.Equal(t, expectedConsumed, source.ConsumedUnits, "a separate settlement cannot fabricate consumption on the original source")
			require.Zero(t, source.ReleasedUnits)
			started := time.Now()
			require.NoError(t, x.f.bridge.EnsureCanonicalWalletLeaseForAdmission(context.Background(), x.f.platformUserID, "USD"), "the eligibility bootstrap must not top up or spend the current settlement lease")
			fresh, err := service.NewCanonicalWalletAuthorizer(x.f.cfg, x.f.bridge, x.f.snapshots).Authorize(context.Background(), service.AuthorizeInput{Snapshot: x.snapshot, User: x.owner, FixedEstimateUnits: 1000000})
			require.NoError(t, err, "the next LLM authorization succeeds without waiting for a settle TTL or sweep")
			require.True(t, fresh.HoldArmed)
			require.Equal(t, int64(1000000), fresh.HeldUnits)
			authorizationElapsed := time.Since(started)
			require.Less(t, authorizationElapsed, 2*time.Second)
			t.Logf("source budget=%d settlement budget=%d original C=%d R=%d next LLM authorization=%s", sourceUnits, settle.BudgetUnits, source.ConsumedUnits, source.ReleasedUnits, authorizationElapsed)
			_, err = x.f.db.Exec(`DROP TRIGGER funding_v7_availability_delivery_fault ON wallet_settlement_outbox; DROP FUNCTION funding_v7_availability_delivery_fault()`)
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				var count int
				return x.f.db.QueryRow(`SELECT count(*) FROM wallet_settlement_outbox WHERE platform_user_id=$1 AND status='delivered' AND ((event_id=$2 AND amount_units=500000) OR (event_id=$3 AND amount_units=79500000))`, x.f.platformUserID, segment.EventID, remainder).Scan(&count) == nil && count == 2
			}, outboxStaleReclaimWindow(x.f.cfg.CanonicalWallet.RequestTimeoutMS), 20*time.Millisecond)
			code, raw := immediateV5WirePost(t, x.base, x.secret, "/__fixture/snapshot", map[string]string{"platform_user_id": x.f.platformUserID}, true)
			require.Equal(t, http.StatusOK, code, string(raw))
			var snapshot struct {
				Credits []struct {
					Remaining string `json:"remainingUnits"`
				} `json:"credits"`
				Leases []struct {
					ID       string `json:"id"`
					Budget   string `json:"budgetUnits"`
					Captured string `json:"capturedUnits"`
					Released string `json:"releasedUnits"`
				} `json:"leases"`
				Events []struct {
					Amount string `json:"amountUnits"`
				} `json:"events"`
			}
			require.NoError(t, json.Unmarshal(raw, &snapshot))
			number := func(v string) int64 {
				n, err := strconv.ParseInt(v, 10, 64)
				require.NoError(t, err)
				return n
			}
			ledger := int64(0)
			for _, credit := range snapshot.Credits {
				ledger += number(credit.Remaining)
			}
			for _, lease := range snapshot.Leases {
				ledger += number(lease.Budget) - number(lease.Released)
				if lease.ID == settleLeaseID {
					require.Equal(t, int64(79500000), number(lease.Budget))
					require.Equal(t, int64(79500000), number(lease.Captured))
				}
			}
			require.Equal(t, int64(133000000), ledger, "every remaining credit and lease allocation still has its original backing")
			var charged int64
			for _, event := range snapshot.Events {
				charged += number(event.Amount)
			}
			require.Equal(t, int64(80000000), charged, "the held fee and remainder capture the trusted cost exactly once")
		})
	}
}

// An LLM attempt that spans two shared leases and is charged less than its first
// share leaves the second share at zero. That share moves no money, but its
// capacity hold and its D1 pin must still end; otherwise the lease it sits on can
// never return its free principal again (the Worker refuses every return while an
// unmatched pin is active).
func TestExternalFundingV7ZeroShareOfChargedSplitAttemptEndsItsPinAndLeavesTheLeaseReturnable(t *testing.T) {
	x := newFundingV7Fixture(t)
	billingRepository := repository.NewUsageBillingRepository(nil, x.f.db)
	x.f.bridge.SetBillingEvidenceRepository(billingRepository)
	usageService := service.NewOpenAIGatewayService(nil, nil, billingRepository, x.f.users, nil, nil, repository.NewGatewayCache(x.f.rdb), x.f.cfg, x.f.db, repository.ProvideWalletOutboxStore(x.f.db), nil, nil, service.NewBillingService(x.f.cfg, nil), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, x.f.snapshots)
	t.Cleanup(usageService.CloseOpenAIWSPool)
	// A first live attempt holds 9.1 of the source lease, leaving 0.9 free there.
	x.authorize(t, 910000000)
	short := "funding-v7-short-" + uuid.NewString()
	code, raw := immediateV5WirePost(t, x.base, x.secret, "/__fixture/seed", map[string]string{"platform_user_id": x.f.platformUserID, "grant_units": "90000000", "lease_id": short, "budget_units": "90000000"}, true)
	require.Equal(t, http.StatusOK, code, string(raw))
	require.NoError(t, x.wallet.InstallCanonicalWalletLease(context.Background(), service.CanonicalWalletLease{LeaseID: short, PlatformUserID: x.f.platformUserID, Currency: "USD", FundedUnits: 90000000, BudgetUnits: 90000000, FundingScope: "legacy", ExpiresAt: time.Now().Add(65 * time.Minute)}))
	h := x.authorize(t, 150000000)
	require.Len(t, h.Segments, 2, "the 1.5 hold spans the source lease (0.9 free) and the second lease (0.6)")
	require.Equal(t, x.source, h.Segments[0].LeaseID)
	require.Equal(t, short, h.Segments[1].LeaseID)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"funding-v7-split","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":0}}`)
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
		usageErr = usageService.RecordUsage(ctx, &service.OpenAIRecordUsageInput{Result: &service.OpenAIForwardResult{RequestID: "funding-v7-split-" + h.ID, Model: "gpt-5.1", Usage: service.OpenAIUsage{InputTokens: 1}}, User: x.owner, APIKey: &service.APIKey{ID: x.snapshot.APIKeyID, Quota: 100}, Account: &service.Account{ID: x.snapshot.AccountID, Type: service.AccountTypeAPIKey, Platform: service.PlatformOpenAI}, BillingSnapshot: x.snapshot, AuthorizationID: h.ID, AuthorizationToken: h.LastWriteToken()})
	})
	require.NoError(t, usageErr)
	require.NoError(t, err)
	require.NotNil(t, dispatch)
	dispatch(context.Background())
	// The 0.8 fee is wholly absorbed by the first share; the second share is zero.
	// The attempt-recovery loop that ends delivered and zero shares is driven by
	// the media service's one-second tick; the fixture stopped it to stay quiet.
	x.f.svc = x.f.newService(t)
	require.Eventually(t, func() bool {
		var charged, zero bool
		return x.f.db.QueryRow(`SELECT bool_or(ordinal=0 AND actual_units=80000000 AND state='finished'),bool_or(ordinal=1 AND actual_units=0 AND state='finished' AND pin_state='finished') FROM wallet_authorization_segment WHERE parent_authorization_id=$1`, h.ID).Scan(&charged, &zero) == nil && charged && zero
	}, 8*time.Second, 20*time.Millisecond, "both shares end: the charged one by its delivered settlement and the zero one by finishing its pin")
	zeroShare := h.Segments[1].AuthorizationID
	require.Eventually(t, func() bool {
		code, raw := immediateV5WirePost(t, x.base, x.secret, "/__fixture/snapshot", map[string]string{"platform_user_id": x.f.platformUserID}, true)
		if code != http.StatusOK {
			return false
		}
		var snapshot struct {
			Pins []struct {
				AuthorizationID string `json:"authorizationId"`
				State           string `json:"state"`
			} `json:"pins"`
		}
		if json.Unmarshal(raw, &snapshot) != nil {
			return false
		}
		for _, pin := range snapshot.Pins {
			if pin.AuthorizationID == zeroShare {
				return pin.State == "released"
			}
		}
		return false
	}, 8*time.Second, 20*time.Millisecond, "the zero share's D1 pin is released, not left active")
	// Both shares have ended. Stop the service loop again so it cannot claim the
	// task row that the media-funding helper inserts and spend the credit itself.
	x.f.svc.Stop()
	// The second lease must now be able to give back its free principal. Only
	// $0.1 is free on the source lease, so a $0.5 media quote can be funded only if
	// the second lease's $0.9 is reclaimed through a signed partial return, which
	// the Worker accepts only when no unmatched pin is active on that lease.
	_, err = x.mediaFunding(t, 50000000)
	require.NoError(t, err, "reclaiming the second lease's free principal is not blocked by a leaked pin")
}

// A user running short of funds must not mint a signed revision per shared lease
// on every attempt: a lease with nothing free has nothing to give back.
func TestExternalFundingV7ReclaimSkipsSharedLeasesWithNothingFree(t *testing.T) {
	x := newFundingV7Fixture(t)
	x.authorize(t, 1000000000)
	for attempt := 0; attempt < 3; attempt++ {
		_, err := x.mediaFunding(t, 200000000)
		require.Error(t, err)
		var refusal *service.AuthorizationRefusedError
		require.ErrorAs(t, err, &refusal)
		require.Equal(t, service.AuthorizationRefusalBalanceShortfall, refusal.Reason)
	}
	var intents, freezes, works int
	require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_funding_source_intent WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, x.source).Scan(&intents))
	require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, x.source).Scan(&freezes))
	require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_funding_terminal_work WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, x.source).Scan(&works))
	require.Zero(t, intents, "no source fence was requested for a lease with nothing free")
	require.Zero(t, freezes, "no empty return revision was minted")
	require.Zero(t, works)
}
