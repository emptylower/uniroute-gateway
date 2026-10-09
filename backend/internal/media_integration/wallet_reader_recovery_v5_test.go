//go:build media_integration

package media_integration

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func walletRecoveryV5Snapshot(t *testing.T, f *mediaFixture) (*service.AuthorizationHandle, *service.BillingSnapshot, *service.User) {
	t.Helper()
	_, original := f.authorize(t, 1000000, service.BillingFamilyOpenAI)
	snapshot := *original
	snapshot.ID = "snapshot-reader-recovery-v5-" + uuid.NewString()
	snapshot.Family = service.BillingFamilyOpenAI
	snapshot.RequestedModel, snapshot.BillingModel = "gpt-5.1", "gpt-5.1"
	snapshot.Candidates = []string{"gpt-5.1"}
	snapshot.Flags.WalletImmediateReleasePolicyVersion = service.WalletImmediateReleasePolicyVersion
	snapshot.Pricing = service.BillingSnapshotPricing{Mode: service.BillingModeToken, Source: service.PricingSourceLiteLLM, Base: &service.ModelPricing{InputPricePerToken: .000001, OutputPricePerToken: .000002}}
	snapshot.Multipliers = service.BillingSnapshotMultipliers{Base: 1, Text: 1, Image: 1, Video: 1, WebSearch: 1, Account: 1}
	require.NoError(t, f.snapshots.Persist(context.Background(), &snapshot))
	f.bridge.Close()
	f.cfg.CanonicalWallet.LLMImmediateReleaseMode = "enabled"
	f.bridge = service.NewCanonicalWalletBridge(f.cfg, repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore), f.db, repository.ProvideWalletOutboxStore(f.db))
	t.Cleanup(f.bridge.Close)
	user, err := f.users.GetByID(context.Background(), f.userID)
	require.NoError(t, err)
	h, err := service.NewCanonicalWalletAuthorizer(f.cfg, f.bridge, f.snapshots).Authorize(context.Background(), service.AuthorizeInput{Snapshot: &snapshot, User: user, FixedEstimateUnits: 1000000})
	require.NoError(t, err)
	require.Len(t, h.Segments, 1)
	return h, &snapshot, user
}

func walletRecoveryV5JoinedJournal(t *testing.T, f *mediaFixture, h *service.AuthorizationHandle) {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(f.readerJournalDir, "*.json"))
	require.NoError(t, err)
	found := false
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		var record struct {
			AuthorizationID string                             `json:"authorization_id"`
			Token           string                             `json:"token"`
			SnapshotID      string                             `json:"snapshot_id"`
			Joined          bool                               `json:"joined"`
			Normalization   *service.WalletReaderNormalization `json:"normalization"`
		}
		if json.Unmarshal(raw, &record) == nil && record.AuthorizationID == h.ID {
			require.True(t, record.Joined)
			require.Equal(t, h.LastWriteToken(), record.Token)
			require.Equal(t, h.SnapshotID, record.SnapshotID)
			require.NotNil(t, record.Normalization)
			require.True(t, record.Normalization.Ready)
			found = true
		}
	}
	require.True(t, found, "the actual reader must have fsynced its owning normalized handoff")
}

func TestExternalWalletReaderExactZeroCrashBeforeStagingRecoversSignedZeroOff(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"strict-zero", `{"id":"zero-response","usage":{"input_tokens":0,"output_tokens":0}}`},
		{"missing", `{"id":"missing-response"}`},
		{"malformed", `{"usage":{"input_tokens":"0","output_tokens":0}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, denyFinish := mediaV5WireFixture(t, "off")
			h, snapshot, user := walletRecoveryV5Snapshot(t, f)
			var balanceBefore, quotaBefore float64
			require.NoError(t, f.db.QueryRow(`SELECT balance FROM users WHERE id=$1`, user.ID).Scan(&balanceBefore))
			require.NoError(t, f.db.QueryRow(`SELECT quota_used FROM api_keys WHERE id=$1`, snapshot.APIKeyID).Scan(&quotaBefore))
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, tc.body) }))
			defer provider.Close()
			request, err := http.NewRequestWithContext(service.WithAuthorizationHandle(context.Background(), h), "POST", provider.URL+"/v1/responses", strings.NewReader(`{"model":"gpt-5.1"}`))
			require.NoError(t, err)
			response, err := service.NewAuthorizingHTTPUpstream(&mediaHTTP{client: provider.Client()}, f.cfg).Do(request, "", 0, 1)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, response.StatusCode)
			_, err = io.Copy(io.Discard, response.Body)
			require.NoError(t, err)
			require.NoError(t, response.Body.Close())
			walletRecoveryV5JoinedJournal(t, f, h)
			var joined, intent, ack, sealed bool
			var pending int
			require.NoError(t, f.db.QueryRow(`SELECT reader_handoff_at IS NOT NULL,zero_intent_at IS NOT NULL,zero_ack_at IS NOT NULL,terminal_sealed_at IS NOT NULL FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0`, h.ID).Scan(&joined, &intent, &ack, &sealed))
			require.True(t, joined)
			require.False(t, intent || ack || sealed)
			require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_billing_pending WHERE parent_authorization_id=$1`, h.ID).Scan(&pending))
			require.Zero(t, pending, "crash occurs before RecordUsage and billing staging")
			if tc.name == "strict-zero" {
				_, err = f.db.Exec(`CREATE FUNCTION reader_zero_stage_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'isolated strict zero pending persistence fault'; END $$; CREATE TRIGGER reader_zero_stage_fault BEFORE INSERT ON wallet_billing_pending FOR EACH ROW EXECUTE FUNCTION reader_zero_stage_fault()`)
				require.NoError(t, err)
			}
			f.bridge.Close()
			f.cfg.CanonicalWallet.LLMImmediateReleaseMode = "off"
			denyFinish.Store(true)
			wallet := repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore)
			f.bridge = service.NewCanonicalWalletBridge(f.cfg, wallet, f.db, repository.ProvideWalletOutboxStore(f.db))
			f.bridge.SetBillingEvidenceRepository(repository.NewUsageBillingRepository(nil, f.db))
			t.Cleanup(f.bridge.Close)
			f.svc = f.newService(t)
			if tc.name != "strict-zero" {
				require.Eventually(t, func() bool {
					_ = f.db.QueryRow(`SELECT terminal_sealed_at IS NOT NULL FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0`, h.ID).Scan(&sealed)
					return sealed
				}, 8*time.Second, 20*time.Millisecond)
				var zero bool
				require.NoError(t, f.db.QueryRow(`SELECT known_fee_units IS NOT NULL OR zero_intent_at IS NOT NULL OR zero_ack_at IS NOT NULL FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0`, h.ID).Scan(&zero))
				require.False(t, zero, "missing/malformed usage cannot become a fee from its advisory estimate")
				hold, err := wallet.GetCanonicalWalletHold(context.Background(), f.platformUserID, h.ID)
				require.NoError(t, err)
				require.Equal(t, "armed", hold.State)
				return
			}
			require.Never(t, func() bool {
				_ = f.db.QueryRow(`SELECT terminal_sealed_at IS NOT NULL OR known_fee_units IS NOT NULL OR zero_intent_at IS NOT NULL FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0`, h.ID).Scan(&sealed)
				return sealed
			}, 2*time.Second, 20*time.Millisecond, "failed fee persistence must retain the reader's financial barrier")
			hold, err := wallet.GetCanonicalWalletHold(context.Background(), f.platformUserID, h.ID)
			require.NoError(t, err)
			require.Equal(t, "armed", hold.State)
			_, err = f.db.Exec(`DROP TRIGGER reader_zero_stage_fault ON wallet_billing_pending; DROP FUNCTION reader_zero_stage_fault()`)
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				_ = f.db.QueryRow(`SELECT zero_intent_at IS NOT NULL,zero_ack_at IS NOT NULL FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0`, h.ID).Scan(&intent, &ack)
				return intent
			}, 8*time.Second, 20*time.Millisecond)
			require.False(t, ack)
			hold, err = wallet.GetCanonicalWalletHold(context.Background(), f.platformUserID, h.ID)
			require.NoError(t, err)
			require.Equal(t, "armed", hold.State, "a missing real Worker finish must not release Redis")
			denyFinish.Store(false)
			var raw []byte
			var signedAt time.Time
			require.Eventually(t, func() bool {
				var state string
				err := f.db.QueryRow(`SELECT state,zero_ack_at IS NOT NULL,zero_receipt,zero_released_at FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0`, h.ID).Scan(&state, &ack, &raw, &signedAt)
				return err == nil && ack && state == "finished"
			}, 8*time.Second, 20*time.Millisecond)
			var receipt service.WalletTaskPinReceipt
			require.NoError(t, json.Unmarshal(raw, &receipt))
			segment := h.Segments[0]
			released, err := service.VerifyWalletTaskPinReceipt(f.cfg.CanonicalWallet.Secret, service.WalletTaskPinReceiptExpected{GatewayJobID: h.ID, AuthorizationID: segment.AuthorizationID, PlatformUserID: f.platformUserID, LeaseID: segment.LeaseID, BillingSnapshotID: snapshot.ID, SettlementEventID: segment.EventID, HeldUnits: segment.HeldUnits, AuthorizationKind: "llm", AuthorizationToken: h.LastWriteToken(), Status: "released"}, receipt)
			require.NoError(t, err)
			require.True(t, signedAt.Equal(released))
			hold, err = wallet.GetCanonicalWalletHold(context.Background(), f.platformUserID, h.ID)
			require.NoError(t, err)
			require.Equal(t, "released", hold.State)
			var counters, charges int
			require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_unknown_release_counter WHERE parent_authorization_id=$1`, h.ID).Scan(&counters))
			require.Zero(t, counters)
			require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_billing_charge_receipt c JOIN wallet_billing_pending p ON p.id=c.pending_id WHERE p.parent_authorization_id=$1 AND p.fee_units<>0`, h.ID).Scan(&charges))
			require.Zero(t, charges)
			var zeroFee bool
			require.NoError(t, f.db.QueryRow(`SELECT fee_units=0 AND (command->>'WalletCostUSD')::numeric=0 FROM wallet_billing_pending WHERE parent_authorization_id=$1`, h.ID).Scan(&zeroFee))
			require.True(t, zeroFee, "the successful zero is calculated from the strict frozen proof")
			var balanceAfter, quotaAfter float64
			require.NoError(t, f.db.QueryRow(`SELECT balance FROM users WHERE id=$1`, user.ID).Scan(&balanceAfter))
			require.NoError(t, f.db.QueryRow(`SELECT quota_used FROM api_keys WHERE id=$1`, snapshot.APIKeyID).Scan(&quotaAfter))
			require.Equal(t, balanceBefore, balanceAfter)
			require.Equal(t, quotaBefore, quotaAfter)
		})
	}
}

func TestExternalWalletWSSealedPositivePendingStageFaultRecoversOriginalFeeOff(t *testing.T) {
	walletWSSealedPendingStageRecoveryV5(t, false)
}

func TestExternalWalletWSSealedStrictZeroPendingStageFaultRecoversSignedZeroOff(t *testing.T) {
	walletWSSealedPendingStageRecoveryV5(t, true)
}

func walletWSSealedPendingStageRecoveryV5(t *testing.T, zero bool) {
	t.Helper()
	f, denyFinish := mediaV5WireFixture(t, "off")
	h, snapshot, user := walletRecoveryV5Snapshot(t, f)
	var balanceBefore, quotaBefore float64
	require.NoError(t, f.db.QueryRow(`SELECT balance FROM users WHERE id=$1`, user.ID).Scan(&balanceBefore))
	require.NoError(t, f.db.QueryRow(`SELECT quota_used FROM api_keys WHERE id=$1`, snapshot.APIKeyID).Scan(&quotaBefore))
	tokens := service.UsageTokens{InputTokens: 1000, OutputTokens: 100}
	terminal := `{"type":"response.completed","response":{"id":"sealed-positive-response","model":"gpt-5.1","usage":{"input_tokens":1000,"output_tokens":100}}}`
	if zero {
		tokens = service.UsageTokens{}
		terminal = `{"type":"response.completed","response":{"id":"sealed-zero-response","model":"gpt-5.1","usage":{"input_tokens":0,"output_tokens":0}}}`
	}
	f.cfg.Security.URLAllowlist.Enabled = false
	f.cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	f.cfg.Gateway.OpenAIWS.Enabled, f.cfg.Gateway.OpenAIWS.APIKeyEnabled, f.cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true, true, true
	f.cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
	f.cfg.Gateway.OpenAIWS.IngressModeDefault = service.OpenAIWSIngressModePassthrough
	f.cfg.Gateway.OpenAIWS.DialTimeoutSeconds, f.cfg.Gateway.OpenAIWS.ReadTimeoutSeconds, f.cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3, 3, 3
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if _, _, err = conn.Read(ctx); err != nil {
			return
		}
		_ = conn.Write(ctx, websocket.MessageText, []byte(terminal))
		_, _, _ = conn.Read(ctx)
	}))
	defer provider.Close()
	account := &service.Account{ID: snapshot.AccountID, Type: service.AccountTypeAPIKey, Platform: service.PlatformOpenAI, Status: service.StatusActive, Concurrency: 1, Credentials: map[string]any{"api_key": "sk-test", "base_url": provider.URL}, Extra: map[string]any{"openai_apikey_responses_websockets_v2_mode": service.OpenAIWSIngressModePassthrough}}
	usageService := service.NewOpenAIGatewayService(nil, nil, repository.NewUsageBillingRepository(nil, f.db), f.users, nil, nil, repository.NewGatewayCache(f.rdb), f.cfg, f.db, repository.ProvideWalletOutboxStore(f.db), nil, nil, service.NewBillingService(f.cfg, nil), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, f.snapshots)
	t.Cleanup(usageService.CloseOpenAIWSPool)
	_, err := f.db.Exec(`CREATE FUNCTION reader_ws_stage_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'isolated WS pending persistence fault'; END $$; CREATE TRIGGER reader_ws_stage_fault BEFORE INSERT ON wallet_billing_pending FOR EACH ROW EXECUTE FUNCTION reader_ws_stage_fault()`)
	require.NoError(t, err)
	stageErrors, ended := make(chan error, 1), make(chan error, 1)
	ingress := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			ended <- err
			return
		}
		defer conn.CloseNow()
		_, first, err := conn.Read(r.Context())
		if err != nil {
			ended <- err
			return
		}
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = r
		hooks := &service.OpenAIWSIngressHooks{
			AuthorizeTurn: func(_ int, _ service.EstimateInput) (*service.AuthorizationHandle, error) { return h, nil },
			AfterTurn: func(_ int, result *service.OpenAIForwardResult, turnErr error) {
				if result == nil || turnErr != nil {
					stageErrors <- turnErr
					return
				}
				_, stageErr := h.PrepareUsageTask(r.Context(), func(ctx context.Context) {
					_ = usageService.RecordUsage(ctx, &service.OpenAIRecordUsageInput{Result: result, User: user, APIKey: &service.APIKey{ID: snapshot.APIKeyID, Quota: 100}, Account: account, BillingSnapshot: snapshot, AuthorizationID: h.ID, AuthorizationToken: h.LastWriteToken()})
				})
				stageErrors <- stageErr
			},
		}
		ended <- usageService.ProxyResponsesWebSocketFromClient(r.Context(), c, conn, account, "sk-test", first, hooks)
	}))
	defer ingress.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	client, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(ingress.URL, "http"), nil)
	require.NoError(t, err)
	defer client.CloseNow()
	require.NoError(t, client.Write(ctx, websocket.MessageText, []byte(`{"type":"response.create","model":"gpt-5.1","input":[]}`)))
	_, _, err = client.Read(ctx)
	require.NoError(t, err)
	select {
	case stageErr := <-stageErrors:
		require.ErrorContains(t, stageErr, "WS pending persistence fault")
	case <-ctx.Done():
		t.Fatal("actual WS billing stage did not run")
	}
	var sealed, joined, feePending bool
	require.Eventually(t, func() bool {
		_ = f.db.QueryRow(`SELECT terminal_sealed_at IS NOT NULL,reader_handoff_at IS NOT NULL,fee_pending FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0`, h.ID).Scan(&sealed, &joined, &feePending)
		return sealed && joined && feePending
	}, 3*time.Second, 20*time.Millisecond)
	walletRecoveryV5JoinedJournal(t, f, h)
	var sealBefore []byte
	var pending int
	require.NoError(t, f.db.QueryRow(`SELECT terminal_evidence FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0`, h.ID).Scan(&sealBefore))
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_billing_pending WHERE parent_authorization_id=$1`, h.ID).Scan(&pending))
	require.Zero(t, pending)
	_ = client.CloseNow()
	select {
	case <-ended:
	case <-time.After(3 * time.Second):
		t.Fatal("WS reader actors did not join")
	}
	usageService.CloseOpenAIWSPool()
	f.bridge.Close()
	if !zero {
		_, err = f.db.Exec(`DROP TRIGGER reader_ws_stage_fault ON wallet_billing_pending; DROP FUNCTION reader_ws_stage_fault()`)
		require.NoError(t, err)
	}
	f.cfg.CanonicalWallet.LLMImmediateReleaseMode = "off"
	denyFinish.Store(zero)
	f.bridge = service.NewCanonicalWalletBridge(f.cfg, repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore), f.db, repository.ProvideWalletOutboxStore(f.db))
	f.bridge.SetBillingEvidenceRepository(repository.NewUsageBillingRepository(nil, f.db))
	t.Cleanup(f.bridge.Close)
	f.svc = f.newService(t)
	cost, err := (&service.BillingService{}).CalculateCostFromSnapshot(snapshot, service.SnapshotSettlementInput{Tokens: tokens})
	require.NoError(t, err)
	units := int64(math.Round(cost.ActualCost * 100000000))
	if zero {
		require.Zero(t, units)
		wallet := repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore)
		require.Never(t, func() bool {
			var unsafe bool
			_ = f.db.QueryRow(`SELECT known_fee_units IS NOT NULL OR zero_intent_at IS NOT NULL OR zero_ack_at IS NOT NULL FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0`, h.ID).Scan(&unsafe)
			return unsafe
		}, 2*time.Second, 20*time.Millisecond, "sealed strict zero retains its fee barrier while pending INSERT fails")
		hold, err := wallet.GetCanonicalWalletHold(context.Background(), f.platformUserID, h.ID)
		require.NoError(t, err)
		require.Equal(t, "armed", hold.State)
		_, err = f.db.Exec(`DROP TRIGGER reader_ws_stage_fault ON wallet_billing_pending; DROP FUNCTION reader_ws_stage_fault()`)
		require.NoError(t, err)
		var intent, ack bool
		require.Eventually(t, func() bool {
			_ = f.db.QueryRow(`SELECT zero_intent_at IS NOT NULL,zero_ack_at IS NOT NULL FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0`, h.ID).Scan(&intent, &ack)
			return intent
		}, 8*time.Second, 20*time.Millisecond)
		require.False(t, ack)
		hold, err = wallet.GetCanonicalWalletHold(context.Background(), f.platformUserID, h.ID)
		require.NoError(t, err)
		require.Equal(t, "armed", hold.State, "a missing actual signed finish must preserve Redis")
		denyFinish.Store(false)
		var raw []byte
		var releasedAt time.Time
		require.Eventually(t, func() bool {
			var finished bool
			err := f.db.QueryRow(`SELECT state='finished',zero_ack_at IS NOT NULL,zero_receipt,zero_released_at FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0`, h.ID).Scan(&finished, &ack, &raw, &releasedAt)
			return err == nil && ack && finished
		}, 8*time.Second, 20*time.Millisecond)
		var receipt service.WalletTaskPinReceipt
		require.NoError(t, json.Unmarshal(raw, &receipt))
		segment := h.Segments[0]
		signedAt, err := service.VerifyWalletTaskPinReceipt(f.cfg.CanonicalWallet.Secret, service.WalletTaskPinReceiptExpected{GatewayJobID: h.ID, AuthorizationID: segment.AuthorizationID, PlatformUserID: f.platformUserID, LeaseID: segment.LeaseID, BillingSnapshotID: snapshot.ID, SettlementEventID: segment.EventID, HeldUnits: segment.HeldUnits, AuthorizationKind: "llm", AuthorizationToken: h.LastWriteToken(), Status: "released"}, receipt)
		require.NoError(t, err)
		require.True(t, releasedAt.Equal(signedAt))
		hold, err = wallet.GetCanonicalWalletHold(context.Background(), f.platformUserID, h.ID)
		require.NoError(t, err)
		require.Equal(t, "released", hold.State)
		var zeroFee bool
		require.NoError(t, f.db.QueryRow(`SELECT fee_units=0 AND (command->>'WalletCostUSD')::numeric=0 FROM wallet_billing_pending WHERE parent_authorization_id=$1`, h.ID).Scan(&zeroFee))
		require.True(t, zeroFee)
		var balanceAfter, quotaAfter float64
		require.NoError(t, f.db.QueryRow(`SELECT balance FROM users WHERE id=$1`, user.ID).Scan(&balanceAfter))
		require.NoError(t, f.db.QueryRow(`SELECT quota_used FROM api_keys WHERE id=$1`, snapshot.APIKeyID).Scan(&quotaAfter))
		require.Equal(t, balanceBefore, balanceAfter)
		require.Equal(t, quotaBefore, quotaAfter)
		var nonzeroCharges int
		require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_billing_charge_receipt c JOIN wallet_billing_pending p ON p.id=c.pending_id WHERE p.parent_authorization_id=$1 AND p.fee_units<>0`, h.ID).Scan(&nonzeroCharges))
		require.Zero(t, nonzeroCharges)
	} else {
		require.Greater(t, units, int64(0))
		require.Eventually(t, func() bool {
			var actual int64
			var done bool
			err := f.db.QueryRow(`SELECT a.actual_units,a.state='finished' AND p.apply_ack_at IS NOT NULL AND p.canonical_ack_at IS NOT NULL FROM wallet_authorization_segment a JOIN wallet_billing_pending p ON p.parent_authorization_id=a.parent_authorization_id WHERE a.parent_authorization_id=$1 AND a.ordinal=0`, h.ID).Scan(&actual, &done)
			return err == nil && done && actual == units
		}, 8*time.Second, 20*time.Millisecond)
	}
	var receipts, counters int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_billing_charge_receipt c JOIN wallet_billing_pending p ON p.id=c.pending_id WHERE p.parent_authorization_id=$1 AND p.fee_units=$2`, h.ID, units).Scan(&receipts))
	require.Equal(t, 1, receipts)
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_unknown_release_counter WHERE parent_authorization_id=$1`, h.ID).Scan(&counters))
	require.Zero(t, counters)
	var sealAfter []byte
	require.NoError(t, f.db.QueryRow(`SELECT terminal_evidence FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0`, h.ID).Scan(&sealAfter))
	require.JSONEq(t, string(sealBefore), string(sealAfter), "recovery preserves the immutable original WS terminal proof")
	base, secret := immediateV5WireConfig(t)
	code, body := immediateV5WirePost(t, base, secret, "/__fixture/snapshot", map[string]string{"platform_user_id": f.platformUserID}, true)
	require.Less(t, code, 300, string(body))
	var wire struct {
		Events []struct {
			ID     string `json:"id"`
			Amount string `json:"amountUnits"`
		} `json:"events"`
	}
	require.NoError(t, json.Unmarshal(body, &wire))
	matches := 0
	for _, event := range wire.Events {
		if event.ID == h.Segments[0].EventID {
			matches++
			require.Equal(t, strconv.FormatInt(units, 10), event.Amount)
		}
	}
	if zero {
		require.Zero(t, matches, "a signed zero never emits a user debit settlement event")
	} else {
		require.Equal(t, 1, matches, "actual Worker D1 records the original sealed WS charge exactly once")
	}
}
