//go:build unit

package service

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type walletNativeHTTPTestClient struct {
	HTTPUpstream
	client *http.Client
}

func (c *walletNativeHTTPTestClient) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	return c.client.Do(req)
}

func TestWalletReaderActualHTTPHandlerMalformedPositiveCannotCharge(t *testing.T) {
	for _, count := range []string{`"10"`, `10.5`, `9007199254740993.5`} {
		t.Run(count, func(t *testing.T) {
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"id":"malformed-http","model":"gpt-5.1","usage":{"input_tokens":`+count+`,"output_tokens":1},"output":[]}`)
			}))
			defer provider.Close()
			var evidence WalletReaderEvidence
			h := &AuthorizationHandle{ID: "auth-http", AttemptKind: "llm"}
			h.beforeWrite = func(context.Context, string) error { return nil }
			h.readerStarted = func() error { return nil }
			h.readerObserved = func(e WalletReaderEvidence) error { evidence = e; return nil }
			h.consumeHTTP = func(_ int, e WalletReaderEvidence, _ error) { evidence = e }
			cfg := &config.Config{CanonicalWallet: config.CanonicalWalletConfig{Mode: "enforce"}}
			upstream := NewAuthorizingHTTPUpstream(&walletNativeHTTPTestClient{client: provider.Client()}, cfg)
			request, err := http.NewRequestWithContext(WithAuthorizationHandle(context.Background(), h), "POST", provider.URL+"/v1/responses", strings.NewReader(`{"model":"gpt-5.1"}`))
			require.NoError(t, err)
			response, err := upstream.Do(request, "", 3, 1)
			require.NoError(t, err)
			logs, billing := &openAIRecordUsageLogRepoStub{}, &openAIRecordUsageBillingRepoStub{}
			users := &openAIRecordUsageUserRepoStub{}
			svc := newOpenAIRecordUsageServiceWithBillingRepoForTest(logs, billing, users, &openAIRecordUsageSubRepoStub{}, nil)
			ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
			ginCtx.Request = request
			account := &Account{ID: 3, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
			parsed, err := svc.handleNonStreamingResponse(request.Context(), response, ginCtx, account, "gpt-5.1", "gpt-5.1")
			require.NoError(t, err, "exercise the real permissive provider handler before the strict financial gate")
			require.NoError(t, response.Body.Close())
			require.Greater(t, parsed.usage.InputTokens, 0)
			require.True(t, evidence.Present && evidence.Malformed && !evidence.Valid)
			raw, err := json.Marshal(evidence)
			require.NoError(t, err)
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			svc.canonicalWallet = &CanonicalWalletBridge{outboxDB: db, cfg: config.CanonicalWalletConfig{LLMImmediateReleaseMode: "enabled"}}
			mock.ExpectQuery("SELECT reader_evidence").WithArgs(h.ID, h.LastWriteToken()).WillReturnRows(sqlmock.NewRows([]string{"reader_evidence"}).AddRow(raw))
			mock.ExpectBegin()
			mock.ExpectQuery("SELECT authorization_id").WithArgs(h.ID).WillReturnRows(sqlmock.NewRows([]string{"authorization_id"}).AddRow(h.ID))
			mock.ExpectQuery("SELECT bool_and").WithArgs(h.ID, h.LastWriteToken(), "", "").WillReturnRows(sqlmock.NewRows([]string{"valid", "zero"}).AddRow(true, false))
			mock.ExpectRollback()
			mock.ExpectExec("UPDATE wallet_authorization_segment SET fee_pending=true,evidence_pending=true").WithArgs(h.ID, h.LastWriteToken()).WillReturnResult(sqlmock.NewResult(0, 1))
			quota := &openAIRecordUsageAPIKeyQuotaStub{}
			err = svc.RecordUsage(context.Background(), &OpenAIRecordUsageInput{Result: &OpenAIForwardResult{RequestID: "malformed-http", Usage: *parsed.usage, Model: "gpt-5.1"}, User: &User{ID: 1}, APIKey: &APIKey{ID: 2, Quota: 100}, Account: account, APIKeyService: quota, AuthorizationID: h.ID, AuthorizationToken: h.LastWriteToken()})
			require.ErrorContains(t, err, "strict selected usage evidence")
			require.Zero(t, billing.calls, "invalid provider values cannot create a charge receipt or quota effect")
			require.Zero(t, logs.calls)
			require.Zero(t, quota.quotaCalls)
			require.Zero(t, users.deductCalls)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestWalletReaderPositiveHandlerFeeMustMatchFrozenNormalization(t *testing.T) {
	pricing, snapshot, _, _, _ := freezeForSettleTest(t, BillingFamilyOpenAI, "gpt-5.1")
	snapshot.ID = "frozen-http-snapshot"
	var evidence WalletReaderEvidence
	observeWalletUsage([]byte(`{"usage":{"input_tokens":1000,"output_tokens":20,"input_tokens_details":{"cached_tokens":250}}}`), &evidence)
	facts := WalletReaderNormalization{Version: 1, Family: BillingFamilyOpenAI, Ready: true, TokenOnly: true, ServiceTier: "priority"}
	input, err := normalizeWalletReaderFee(snapshot, &facts, evidence)
	require.NoError(t, err)
	cost, err := pricing.billing.CalculateCostFromSnapshot(snapshot, input)
	require.NoError(t, err)
	units, err := canonicalWalletUnitsFromUSD(cost.ActualCost)
	require.NoError(t, err)
	snapshotRaw, err := snapshot.MarshalPayload()
	require.NoError(t, err)
	for _, tc := range []struct {
		name      string
		facts     *WalletReaderNormalization
		units     int64
		wantError bool
	}{{"original", &facts, units, false}, {"repriced", &facts, units + 1, true}, {"missing-facts", nil, units, true}, {"unsupported-tool", &WalletReaderNormalization{Version: 1, Family: BillingFamilyOpenAI, Ready: true, TokenOnly: false}, units, true}} {
		t.Run(tc.name, func(t *testing.T) {
			normalizationRaw, err := json.Marshal(tc.facts)
			require.NoError(t, err)
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			bridge := &CanonicalWalletBridge{outboxDB: db}
			mock.ExpectQuery("SELECT s.payload,a.reader_fee_normalization").WithArgs("auth", "auth.1", snapshot.ID).WillReturnRows(sqlmock.NewRows([]string{"payload", "normalization"}).AddRow(snapshotRaw, normalizationRaw))
			err = bridge.validateWalletReaderFee(context.Background(), "auth", "auth.1", snapshot.ID, evidence, tc.units)
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
