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
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const providerGrokReasoningUsage = `{"id":"provider-grok-proof","usage":{"prompt_tokens":1247,"completion_tokens":1,"total_tokens":1324,"prompt_tokens_details":{"cached_tokens":1152},"completion_tokens_details":{"reasoning_tokens":76}}}`

func providerUsageFixture(t *testing.T, platform string, family service.BillingFamily) fundingV7Fixture {
	t.Helper()
	x := newFundingV7Fixture(t)
	snapshot := *x.snapshot
	snapshot.ID = "provider-usage-" + uuid.NewString()
	snapshot.ProviderPlatform, snapshot.Family = platform, family
	snapshot.Pricing = service.BillingSnapshotPricing{Mode: service.BillingModeToken, Source: service.PricingSourceLiteLLM, SupportsCacheBreakdown: true, Base: &service.ModelPricing{InputPricePerToken: .00001, OutputPricePerToken: .00002, CacheReadPricePerToken: .000001}}
	require.NoError(t, x.f.snapshots.Persist(context.Background(), &snapshot))
	x.snapshot = &snapshot
	x.f.bridge.SetBillingEvidenceRepository(repository.NewUsageBillingRepository(nil, x.f.db))
	x.f.bridge.SetBillingEvidenceDependencies(repository.NewUsageLogRepository(nil, x.f.db), nil)
	return x
}

func providerUsageRead(t *testing.T, x fundingV7Fixture, h *service.AuthorizationHandle, status int, stream bool, raw string) {
	t.Helper()
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if stream {
			w.Header().Set("Content-Type", "text/event-stream")
		} else {
			w.Header().Set("Content-Type", "application/json")
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, raw)
	}))
	defer provider.Close()
	request, err := http.NewRequestWithContext(service.WithAuthorizationHandle(context.Background(), h), http.MethodPost, provider.URL+"/selected-provider", strings.NewReader(`{"model":"gpt-5.1"}`))
	require.NoError(t, err)
	response, err := service.NewAuthorizingHTTPUpstream(&mediaHTTP{client: provider.Client()}, x.f.cfg).Do(request, "", x.snapshot.AccountID, 1)
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
}

// The original retail snapshot and selected provider survive a process restart.
// Apply/charge/Redis conversion/outbox/Worker D1 remain the production paths.
func TestExternalWalletProviderUsageRecoveryAndHTTPErrorChargeFrozenFeeExactlyOnce(t *testing.T) {
	geminiFinal := `{"responseId":"provider-gemini-proof","candidates":[{"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":8,"thoughtsTokenCount":3,"totalTokenCount":11,"cachedContentTokenCount":4}}`
	for _, tc := range []struct {
		name, platform, raw string
		family              service.BillingFamily
		status              int
		stream              bool
		want                service.UsageTokens
	}{
		{"grok-restart-json", service.PlatformGrok, providerGrokReasoningUsage, service.BillingFamilyOpenAI, 200, false, service.UsageTokens{InputTokens: 95, OutputTokens: 77, CacheReadTokens: 1152}},
		{"grok-error-positive", service.PlatformGrok, providerGrokReasoningUsage, service.BillingFamilyOpenAI, 401, false, service.UsageTokens{InputTokens: 95, OutputTokens: 77, CacheReadTokens: 1152}},
		{"grok-inclusive-restart", service.PlatformGrok, strings.Replace(providerGrokReasoningUsage, `"completion_tokens":1`, `"completion_tokens":77`, 1), service.BillingFamilyOpenAI, 200, false, service.UsageTokens{InputTokens: 95, OutputTokens: 77, CacheReadTokens: 1152}},
		{"openai-same-shape-inclusive", service.PlatformOpenAI, providerGrokReasoningUsage, service.BillingFamilyOpenAI, 200, false, service.UsageTokens{InputTokens: 95, OutputTokens: 1, CacheReadTokens: 1152}},
		{"gemini-blocked-input-is-positive", service.PlatformGemini, `{"promptFeedback":{"blockReason":"SAFETY"},"usageMetadata":{"promptTokenCount":8,"totalTokenCount":8}}`, service.BillingFamilyGeneric, 401, false, service.UsageTokens{InputTokens: 8}},
		{"gemini-sse-thoughts-restart", service.PlatformGemini, "data: {\"usageMetadata\":{\"promptTokenCount\":8,\"totalTokenCount\":8}}\n\ndata: " + geminiFinal + "\n\ndata: " + geminiFinal + "\n\n", service.BillingFamilyGeneric, 200, true, service.UsageTokens{InputTokens: 4, OutputTokens: 3, CacheReadTokens: 4}},
		{"gemini-sse-positive-before-zero-terminal", service.PlatformGemini, "data: {\"usageMetadata\":{\"promptTokenCount\":8,\"thoughtsTokenCount\":3,\"totalTokenCount\":11}}\n\ndata: {\"candidates\":[{\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":0,\"totalTokenCount\":0}}\n\n", service.BillingFamilyGeneric, 200, true, service.UsageTokens{InputTokens: 8, OutputTokens: 3}},
		{"antigravity-wrapped-native-restart", service.PlatformAntigravity, `{"response":` + geminiFinal + `}`, service.BillingFamilyGeneric, 200, false, service.UsageTokens{InputTokens: 4, OutputTokens: 3, CacheReadTokens: 4}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x := providerUsageFixture(t, tc.platform, tc.family)
			h := x.authorize(t, 100000000)
			journalFault := tc.name == "grok-restart-json"
			if journalFault {
				_, err := x.f.db.Exec(`CREATE FUNCTION provider_handoff_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.reader_evidence IS NOT NULL THEN RAISE EXCEPTION 'isolated provider reader PG handoff fault'; END IF; RETURN NEW; END $$; CREATE TRIGGER provider_handoff_fault BEFORE UPDATE ON wallet_authorization_segment FOR EACH ROW EXECUTE FUNCTION provider_handoff_fault()`)
				require.NoError(t, err)
			}
			providerUsageRead(t, x, h, tc.status, tc.stream, tc.raw)
			var evidenceRaw, factsRaw []byte
			require.NoError(t, x.f.db.QueryRow(`SELECT reader_evidence,reader_fee_normalization FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0`, h.ID).Scan(&evidenceRaw, &factsRaw))
			if journalFault {
				require.Empty(t, evidenceRaw, "PG has no positive evidence before journal recovery")
				paths, err := filepath.Glob(filepath.Join(x.f.readerJournalDir, "*.json"))
				require.NoError(t, err)
				found := false
				for _, path := range paths {
					raw, err := os.ReadFile(path)
					require.NoError(t, err)
					var record struct {
						AuthorizationID   string          `json:"authorization_id"`
						Joined            bool            `json:"joined"`
						PersistenceFailed bool            `json:"persistence_failed"`
						Evidence          json.RawMessage `json:"evidence"`
						Normalization     json.RawMessage `json:"normalization"`
					}
					require.NoError(t, json.Unmarshal(raw, &record))
					if record.AuthorizationID == h.ID {
						require.True(t, record.Joined && record.PersistenceFailed)
						evidenceRaw, factsRaw = record.Evidence, record.Normalization
						found = true
					}
				}
				require.True(t, found, "actual fsynced WAL retains selected provider and the full reasoning fee")
			}
			var evidence service.WalletReaderEvidence
			var facts service.WalletReaderNormalization
			require.NoError(t, json.Unmarshal(evidenceRaw, &evidence))
			require.NoError(t, json.Unmarshal(factsRaw, &facts))
			require.Equal(t, tc.platform, facts.ProviderPlatform)
			require.True(t, evidence.Complete && evidence.Present && evidence.Valid && !evidence.Malformed && evidence.ObservedPositive)
			require.Equal(t, tc.want, evidence.Tokens)
			if tc.status == 200 {
				var pending int
				require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_billing_pending WHERE parent_authorization_id=$1`, h.ID).Scan(&pending))
				require.Zero(t, pending, "crash boundary precedes ordinary RecordUsage")
			}
			x.f.bridge.Close()
			if journalFault {
				_, err := x.f.db.Exec(`DROP TRIGGER provider_handoff_fault ON wallet_authorization_segment; DROP FUNCTION provider_handoff_fault()`)
				require.NoError(t, err)
			}
			x.f.cfg.CanonicalWallet.LLMImmediateReleaseMode = "off"
			x.f.bridge = service.NewCanonicalWalletBridge(x.f.cfg, x.wallet, x.f.db, repository.ProvideWalletOutboxStore(x.f.db))
			x.f.bridge.SetBillingEvidenceRepository(repository.NewUsageBillingRepository(nil, x.f.db))
			x.f.bridge.SetBillingEvidenceDependencies(repository.NewUsageLogRepository(nil, x.f.db), nil)
			t.Cleanup(x.f.bridge.Close)
			x.f.svc = x.f.newService(t)
			expected, err := (&service.BillingService{}).CalculateCostFromSnapshot(x.snapshot, service.SnapshotSettlementInput{Tokens: tc.want})
			require.NoError(t, err)
			fee := int64(math.Round(expected.ActualCost * 100000000))
			require.Greater(t, fee, int64(0))
			var applied, canonical, finished bool
			var feeUnits, actualUnits int64
			require.Eventually(t, func() bool {
				return x.f.db.QueryRow(`SELECT p.fee_units,p.apply_ack_at IS NOT NULL,p.canonical_ack_at IS NOT NULL,a.actual_units,a.state='finished' FROM wallet_billing_pending p JOIN wallet_authorization_segment a ON a.parent_authorization_id=p.parent_authorization_id AND a.ordinal=0 WHERE p.parent_authorization_id=$1`, h.ID).Scan(&feeUnits, &applied, &canonical, &actualUnits, &finished) == nil && applied && canonical && finished
			}, 10*time.Second, 20*time.Millisecond)
			require.Equal(t, fee, feeUnits)
			require.Equal(t, fee, actualUnits)
			var receipts, unknown, zero, logs, output int
			require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_billing_charge_receipt c JOIN wallet_billing_pending p ON p.id=c.pending_id WHERE p.parent_authorization_id=$1`, h.ID).Scan(&receipts))
			require.Equal(t, 1, receipts)
			require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_unknown_release_counter WHERE parent_authorization_id=$1`, h.ID).Scan(&unknown))
			require.Zero(t, unknown)
			require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND zero_ack_at IS NOT NULL`, h.ID).Scan(&zero))
			require.Zero(t, zero, "input/thoughts positive cannot acquire a signed zero")
			require.NoError(t, x.f.db.QueryRow(`SELECT count(*),COALESCE(max(output_tokens),0) FROM usage_logs WHERE request_id=$1`, "wallet:"+h.ID).Scan(&logs, &output))
			require.Equal(t, 1, logs)
			require.Equal(t, tc.want.OutputTokens, output)
			var receiptRaw []byte
			require.NoError(t, x.f.db.QueryRow(`SELECT c.receipt FROM wallet_billing_charge_receipt c JOIN wallet_billing_pending p ON p.id=c.pending_id WHERE p.parent_authorization_id=$1`, h.ID).Scan(&receiptRaw))
			var receipt service.WalletBillingChargeReceipt
			require.NoError(t, json.Unmarshal(receiptRaw, &receipt))
			require.NotNil(t, receipt.Command.WalletUsageLog)
			require.Equal(t, float64(1), receipt.Command.WalletUsageLog.ExchangeRate)
			require.Equal(t, service.CanonicalWalletUnitVersion, receipt.Command.WalletUsageLog.ExchangeRateSource)
			require.Equal(t, service.CurrencyUSD, receipt.Command.WalletUsageLog.SourceCurrency)
			require.Equal(t, service.CurrencyUSD, receipt.Command.WalletUsageLog.SettlementCurrency)
			require.Equal(t, expected.TotalCost, receipt.Command.WalletUsageLog.SourceCost)
			require.Equal(t, expected.TotalCost, receipt.Command.WalletUsageLog.BaseCost)
			require.NotNil(t, receipt.Command.WalletUsageLog.ExchangeRateAsOf)
			require.True(t, x.snapshot.FrozenAt.Equal(*receipt.Command.WalletUsageLog.ExchangeRateAsOf), "the USD audit instant comes from the original frozen snapshot, never recovery-now")
			require.NoError(t, x.f.bridge.ApplyPendingWalletBilling(context.Background(), h.ID+":"+h.LastWriteToken()))
			var replayReceipt []byte
			require.NoError(t, x.f.db.QueryRow(`SELECT c.receipt FROM wallet_billing_charge_receipt c JOIN wallet_billing_pending p ON p.id=c.pending_id WHERE p.parent_authorization_id=$1`, h.ID).Scan(&replayReceipt))
			require.Equal(t, receiptRaw, replayReceipt, "replay preserves the original command, USD audit metadata, token buckets and fee")
			require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM usage_logs WHERE request_id=$1`, "wallet:"+h.ID).Scan(&logs))
			require.Equal(t, 1, logs, "replay cannot duplicate the original usage log")
			require.Eventually(t, func() bool {
				hold, err := x.wallet.GetCanonicalWalletHold(context.Background(), x.f.platformUserID, h.ID)
				return err != nil || hold.State != "armed"
			}, 5*time.Second, 20*time.Millisecond)
			providerUsageAssertLedger(t, x, h, fee)
		})
	}
}

func providerUsageAssertLedger(t *testing.T, x fundingV7Fixture, h *service.AuthorizationHandle, fee int64) {
	t.Helper()
	code, raw := immediateV5WirePost(t, x.base, x.secret, "/__fixture/snapshot", map[string]string{"platform_user_id": x.f.platformUserID}, true)
	require.Equal(t, http.StatusOK, code)
	var snapshot struct {
		Credits []struct {
			Remaining string `json:"remainingUnits"`
		} `json:"credits"`
		Leases []struct {
			Budget   string `json:"budgetUnits"`
			Captured string `json:"capturedUnits"`
			Released string `json:"releasedUnits"`
		} `json:"leases"`
		Events []struct {
			ID     string `json:"id"`
			Amount string `json:"amountUnits"`
		} `json:"events"`
	}
	require.NoError(t, json.Unmarshal(raw, &snapshot))
	number := func(value string) int64 { n, err := strconv.ParseInt(value, 10, 64); require.NoError(t, err); return n }
	var ledger, captured, events int64
	for _, credit := range snapshot.Credits {
		ledger += number(credit.Remaining)
	}
	for _, lease := range snapshot.Leases {
		ledger += number(lease.Budget) - number(lease.Released)
		captured += number(lease.Captured)
	}
	for _, event := range snapshot.Events {
		if event.ID == h.Segments[0].EventID {
			events++
			require.Equal(t, fee, number(event.Amount))
		}
	}
	require.Equal(t, int64(1000000000), ledger, "all credits and lease backing conserve the original grant")
	require.Equal(t, fee, captured)
	if fee > 0 {
		require.Equal(t, int64(1), events)
	} else {
		require.Zero(t, events)
	}
}

func TestExternalWalletGrokLiveBillingMatchesFrozenReaderReasoningFee(t *testing.T) {
	x := providerUsageFixture(t, service.PlatformGrok, service.BillingFamilyOpenAI)
	logs := repository.NewUsageLogRepository(nil, x.f.db)
	billing := repository.NewUsageBillingRepository(nil, x.f.db)
	usage := service.NewOpenAIGatewayService(nil, logs, billing, x.f.users, nil, nil, repository.NewGatewayCache(x.f.rdb), x.f.cfg, x.f.db, repository.ProvideWalletOutboxStore(x.f.db), nil, nil, service.NewBillingService(x.f.cfg, nil), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, x.f.snapshots)
	t.Cleanup(usage.CloseOpenAIWSPool)
	h := x.authorize(t, 100000000)
	providerUsageRead(t, x, h, 200, false, providerGrokReasoningUsage)
	var recordErr error
	dispatch, err := h.PrepareUsageTask(context.Background(), func(ctx context.Context) {
		recordErr = usage.RecordUsage(ctx, &service.OpenAIRecordUsageInput{Result: &service.OpenAIForwardResult{RequestID: "grok-live-" + h.ID, Model: "gpt-5.1", Usage: service.OpenAIUsage{InputTokens: 1247, OutputTokens: 77, CacheReadInputTokens: 1152}}, User: x.owner, APIKey: &service.APIKey{ID: x.snapshot.APIKeyID}, Account: &service.Account{ID: x.snapshot.AccountID, Type: service.AccountTypeAPIKey, Platform: service.PlatformGrok}, BillingSnapshot: x.snapshot, AuthorizationID: h.ID, AuthorizationToken: h.LastWriteToken()})
	})
	require.NoError(t, recordErr, "the real handler fee77 must agree with the strict selected reader")
	require.NoError(t, err)
	require.NotNil(t, dispatch)
	dispatch(context.Background())
	// The funding fixture stops its recovery worker during setup. Resume the
	// production worker after live handoff so delivered attempts finish normally.
	x.f.svc = x.f.newService(t)
	expected, err := (&service.BillingService{}).CalculateCostFromSnapshot(x.snapshot, service.SnapshotSettlementInput{Tokens: service.UsageTokens{InputTokens: 95, OutputTokens: 77, CacheReadTokens: 1152}})
	require.NoError(t, err)
	fee := int64(math.Round(expected.ActualCost * 100000000))
	var actual int64
	var receiptCount, logCount, output int
	require.Eventually(t, func() bool {
		return x.f.db.QueryRow(`SELECT actual_units FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND ordinal=0 AND state='finished'`, h.ID).Scan(&actual) == nil && actual == fee
	}, 10*time.Second, 20*time.Millisecond)
	require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_billing_charge_receipt c JOIN wallet_billing_pending p ON p.id=c.pending_id WHERE p.parent_authorization_id=$1`, h.ID).Scan(&receiptCount))
	require.Equal(t, 1, receiptCount)
	require.NoError(t, x.f.db.QueryRow(`SELECT count(*),COALESCE(max(output_tokens),0) FROM usage_logs WHERE request_id=$1`, "grok-live-"+h.ID).Scan(&logCount, &output))
	require.Equal(t, 1, logCount)
	require.Equal(t, 77, output)
	var unknown, zero int
	require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_unknown_release_counter WHERE parent_authorization_id=$1`, h.ID).Scan(&unknown))
	require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND zero_ack_at IS NOT NULL`, h.ID).Scan(&zero))
	require.Zero(t, unknown)
	require.Zero(t, zero)
	providerUsageAssertLedger(t, x, h, fee)
}

func TestExternalWalletHistoricalOrUnprovableProviderFactsRemainUnknownUntilGrace(t *testing.T) {
	for _, tc := range []struct {
		name, platform, raw string
		family              service.BillingFamily
	}{
		{"legacy-missing-provider", "", providerGrokReasoningUsage, service.BillingFamilyOpenAI},
		{"grok-missing-total", service.PlatformGrok, strings.Replace(providerGrokReasoningUsage, `,"total_tokens":1324`, "", 1), service.BillingFamilyOpenAI},
		{"gemini-terminal-total-mismatch", service.PlatformGemini, `{"promptFeedback":{"blockReason":"SAFETY"},"usageMetadata":{"promptTokenCount":8,"totalTokenCount":9}}`, service.BillingFamilyGeneric},
		{"gemini-partial-only-eof", service.PlatformGemini, `{"usageMetadata":{"promptTokenCount":8,"thoughtsTokenCount":3,"totalTokenCount":11}}`, service.BillingFamilyGeneric},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x := providerUsageFixture(t, tc.platform, tc.family)
			h := x.authorize(t, 100000000)
			providerUsageRead(t, x, h, 200, false, tc.raw)
			b := barrierFixture{x: x}
			require.True(t, b.held(t, h))
			x.f.bridge.Close()
			b.ageDeadline(t, h, -1, 29)
			x.f.bridge = service.NewCanonicalWalletBridge(x.f.cfg, x.wallet, x.f.db, repository.ProvideWalletOutboxStore(x.f.db))
			x.f.bridge.SetBillingEvidenceRepository(repository.NewUsageBillingRepository(nil, x.f.db))
			t.Cleanup(x.f.bridge.Close)
			x.f.svc = x.f.newService(t)
			b.x = x
			require.Never(t, func() bool { return b.unknownCounters(h) > 0 || !b.held(t, h) }, time.Second, 20*time.Millisecond, "no release before the unchanged deadline plus grace")
			// Altering the immutable deadline is test-only. Stop both recovery
			// actors before this one statement so trigger DDL cannot deadlock sweep.
			x.f.svc.Stop()
			x.f.bridge.Close()
			b.ageDeadline(t, h, -1, 31)
			x.f.bridge = service.NewCanonicalWalletBridge(x.f.cfg, x.wallet, x.f.db, repository.ProvideWalletOutboxStore(x.f.db))
			x.f.bridge.SetBillingEvidenceRepository(repository.NewUsageBillingRepository(nil, x.f.db))
			t.Cleanup(x.f.bridge.Close)
			x.f.svc = x.f.newService(t)
			b.x = x
			require.Eventually(t, func() bool { return b.unknownCounters(h) == 1 && !b.held(t, h) }, 10*time.Second, 20*time.Millisecond)
			var pending, receipts, zero int
			require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_billing_pending WHERE parent_authorization_id=$1`, h.ID).Scan(&pending))
			require.Zero(t, pending, "legacy one-token counts cannot be repriced")
			require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_billing_charge_receipt c JOIN wallet_billing_pending p ON p.id=c.pending_id WHERE p.parent_authorization_id=$1`, h.ID).Scan(&receipts))
			require.Zero(t, receipts)
			require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND zero_ack_at IS NOT NULL`, h.ID).Scan(&zero))
			require.Zero(t, zero)
			providerUsageAssertLedger(t, x, h, 0)
		})
	}
}
