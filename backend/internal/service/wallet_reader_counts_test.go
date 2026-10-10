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
	"github.com/stretchr/testify/require"
)

func TestWalletReaderCountsActualHTTPFrozenBillingMatrix(t *testing.T) {
	for _, tc := range []struct {
		name, kind, payload, response string
		family                        BillingFamily
		mode                          BillingMode
		want                          SnapshotSettlementInput
	}{
		{"per-request-without-usage", "request", `{}`, `{"result":"success"}`, BillingFamilyOpenAI, BillingModePerRequest, SnapshotSettlementInput{}},
		{"per-request-image-max", "request", `{"size":"1K"}`, `{"data":[{"url":"private-output-1","size":"2048x2048"},{"url":"private-output-2"}]}`, BillingFamilyOpenAI, BillingModePerRequest, SnapshotSettlementInput{ImageCount: 2, ImageSize: "2K"}},
		{"images-output-size", "openai_images", `{"size":"1K"}`, `{"data":[{"b64_json":"private-bitmap","size":"3840x2160"}]}`, BillingFamilyOpenAI, BillingModeImage, SnapshotSettlementInput{ImageCount: 1, ImageSize: "4K"}},
		{"images-frozen-fallback", "openai_images", `{"size":"4K"}`, `{"output":[{"type":"image_generation_call","id":"output-image","result":"private-bitmap"}]}`, BillingFamilyOpenAI, BillingModeImage, SnapshotSettlementInput{ImageCount: 1, ImageSize: "4K"}},
		{"gemini-one-successful-request", "gemini_image", `{"generationConfig":{"imageConfig":{"imageSize":"4K"}}}`, `{"candidates":[{"content":{"parts":[{"inlineData":{"data":"private-bitmap"}}]}}]}`, BillingFamilyGeneric, BillingModeImage, SnapshotSettlementInput{ImageCount: 1, ImageSize: "4K"}},
		{"alpha-success-without-usage", "alpha_search", `{"model":"gpt-5.1"}`, `{"results":[{"title":"private-search-text"}]}`, BillingFamilyOpenAI, BillingModeToken, SnapshotSettlementInput{WebSearchCalls: 1}},
		{"alpha-responses-fallback", "alpha_search", `{"model":"gpt-5.1","tools":[{"type":"web_search"}]}`, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"fallback\",\"output\":[]}}\n\ndata: [DONE]\n\n", BillingFamilyOpenAI, BillingModeToken, SnapshotSettlementInput{WebSearchCalls: 1}},
		{"grok-video-submit", "grok_video", `{"resolution":"hd","duration":30}`, `{"request_id":"generated-video-id"}`, BillingFamilyOpenAI, BillingModeVideo, SnapshotSettlementInput{VideoCount: 1, GrokVideo: true, VideoResolution: "720p", VideoDurationSeconds: 15}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pricing, snapshot, _, _, _ := freezeForSettleTest(t, tc.family, "gpt-5.1")
			snapshot.Pricing.Mode = tc.mode
			if tc.mode == BillingModePerRequest {
				snapshot.Pricing.Source = PricingSourceChannel
				snapshot.Pricing.DefaultPerRequestPrice = .025
			}
			h := &AuthorizationHandle{ID: "count-http", readerBillingFamily: tc.family, readerPlatform: PlatformOpenAI, readerCountKind: tc.kind, stageUsage: func(_ context.Context, task UsageRecordTask) (UsageRecordTask, error) { return task, nil }}
			journal := testWalletReaderJournal(t)
			h.readerJournal = journal
			h.beforeWrite = func(context.Context, string) error { return nil }
			h.readerStarted = func() error { return nil }
			h.readerObserved = func(WalletReaderEvidence) error { return nil }
			var evidence WalletReaderEvidence
			h.consumeHTTP = func(_ int, e WalletReaderEvidence, _ error) { evidence = e }
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, tc.response) }))
			defer provider.Close()
			request, err := http.NewRequestWithContext(WithAuthorizationHandle(context.Background(), h), "POST", provider.URL+"/selected-test-endpoint", strings.NewReader(tc.payload))
			require.NoError(t, err)
			decorated := NewAuthorizingHTTPUpstream(&walletNativeHTTPTestClient{client: provider.Client()}, &config.Config{CanonicalWallet: config.CanonicalWalletConfig{Mode: "enforce"}})
			response, err := decorated.Do(request, "", 1, 1)
			require.NoError(t, err)
			_, err = io.Copy(io.Discard, response.Body)
			require.NoError(t, err)
			require.NoError(t, response.Body.Close())
			require.True(t, evidence.ObservedPositive)
			got, err := normalizeWalletReaderFee(snapshot, h.readerNormalization, evidence)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			actual, err := pricing.billing.CalculateCostFromSnapshot(snapshot, got)
			require.NoError(t, err)
			original, err := pricing.billing.CalculateCostFromSnapshot(snapshot, tc.want)
			require.NoError(t, err)
			require.Equal(t, original.ActualCost, actual.ActualCost)
			require.Greater(t, actual.ActualCost, float64(0))
			require.NoError(t, journal.acquire(false))
			require.NoError(t, journal.loadLocked())
			encoded, err := json.Marshal(journal.record)
			require.NoError(t, err)
			journal.release()
			require.NotContains(t, string(encoded), "private-")
			require.LessOrEqual(t, len(encoded), walletReaderJournalBound)
		})
	}
}

func TestWalletReaderCountsWSCheckpointDeduplicatesAndRecoversOriginalImageFee(t *testing.T) {
	_, snapshot, _, _, _ := freezeForSettleTest(t, BillingFamilyOpenAI, "gpt-5.1")
	snapshot.Pricing.Mode = BillingModeImage
	facts := &WalletReaderNormalization{Version: 1, Family: BillingFamilyOpenAI, ProviderPlatform: PlatformOpenAI, Ready: true, CountKind: "openai_images", ImageSize: "4K"}
	journal := testWalletReaderJournal(t)
	require.NoError(t, journal.beginRead())
	first := []byte(`{"type":"response.output_item.done","response_id":"selected-response","item":{"type":"image_generation_call","id":"selected-image-id","result":"private-bitmap","size":"1024x1024"}}`)
	require.NoError(t, journal.checkpointLocked(first, "llm_ws_usage"))
	// Recover from checkpoint fsync before normalized evidence/PG handoff.
	require.NoError(t, journal.replayCheckpointLocked())
	second := []byte(`{"type":"response.completed","response":{"id":"selected-response","output":[{"type":"image_generation_call","id":"selected-image-id","result":"private-bitmap","size":"1024x1024"}]}}`)
	require.NoError(t, journal.checkpointLocked(second, "llm_ws_usage"))
	require.NoError(t, journal.replayCheckpointLocked())
	evidence := journal.record.Evidence
	finishWalletReaderCounts(facts, &evidence, 200, true)
	require.Equal(t, 1, evidence.Counts.imageCount())
	input, err := normalizeWalletReaderFee(snapshot, facts, evidence)
	require.NoError(t, err)
	require.Equal(t, SnapshotSettlementInput{ImageCount: 1, ImageSize: "1K"}, input)
	encoded, err := json.Marshal(journal.record)
	require.NoError(t, err)
	journal.release()
	require.NotContains(t, string(encoded), "private-bitmap")
	require.NotContains(t, string(encoded), "selected-image-id")
}

func TestWalletReaderCountsMalformedCannotChooseFeeOrKnownZero(t *testing.T) {
	_, snapshot, _, _, _ := freezeForSettleTest(t, BillingFamilyOpenAI, "gpt-5.1")
	snapshot.Pricing.Mode = BillingModeImage
	facts := &WalletReaderNormalization{Version: 1, Family: BillingFamilyOpenAI, ProviderPlatform: PlatformOpenAI, Ready: true, CountKind: "openai_images", ImageSize: "2K"}
	for _, raw := range []string{`{"data":"2","usage":{"input_tokens":0,"output_tokens":0}}`, `{"data":[{"b64_json":"private-bitmap","size":2048}]}`, `{"output":[{"type":"image_generation_call","id":2,"result":"private-bitmap"}]}`} {
		var evidence WalletReaderEvidence
		observeWalletUsage([]byte(raw), &evidence)
		require.NoError(t, observeWalletReaderCounts([]byte(raw), &evidence, facts))
		finishWalletReaderCounts(facts, &evidence, 200, true)
		require.True(t, evidence.Malformed)
		require.False(t, walletReaderEvidenceTrusted(evidence))
		_, err := normalizeWalletReaderFee(snapshot, facts, evidence)
		require.Error(t, err)
	}
}

func TestWalletReaderCountsChannelTokenStillRequiresRealTokens(t *testing.T) {
	_, snapshot, _, _, _ := freezeForSettleTest(t, BillingFamilyOpenAI, "gpt-5.1")
	snapshot.Pricing.Mode, snapshot.Pricing.Source = BillingModeToken, PricingSourceChannel
	facts := &WalletReaderNormalization{Version: 1, Family: BillingFamilyOpenAI, ProviderPlatform: PlatformOpenAI, Ready: true, CountKind: "openai_images", ImageSize: "4K"}
	var evidence WalletReaderEvidence
	require.NoError(t, observeWalletReaderCounts([]byte(`{"data":[{"b64_json":"private-bitmap"}]}`), &evidence))
	_, err := normalizeWalletReaderFee(snapshot, facts, evidence)
	require.ErrorContains(t, err, "token proof")
}

func TestWalletReaderOrdinarySearchToolUsesOriginalTokenBranch(t *testing.T) {
	_, snapshot, _, _, _ := freezeForSettleTest(t, BillingFamilyOpenAI, "gpt-5.1")
	h := &AuthorizationHandle{readerBillingFamily: BillingFamilyOpenAI, readerPlatform: PlatformOpenAI, readerTokenOnly: true, stageUsage: func(_ context.Context, task UsageRecordTask) (UsageRecordTask, error) { return task, nil }}
	h.captureWalletReaderNormalization(context.Background(), []byte(`{"tools":[{"type":"web_search"}]}`), true)
	var evidence WalletReaderEvidence
	observeWalletUsage([]byte(`{"usage":{"input_tokens":10,"output_tokens":2}}`), &evidence)
	input, err := normalizeWalletReaderFee(snapshot, h.readerNormalization, evidence)
	require.NoError(t, err)
	require.Zero(t, input.WebSearchCalls)
	require.Equal(t, 10, input.Tokens.InputTokens)
}

func TestWalletReaderCountsLargeHTTPImageUsesExistingConfiguredBodyLimit(t *testing.T) {
	const imageBytes = 17 * 1024 * 1024
	bitmap := strings.Repeat("a", imageBytes)
	raw := `{"data":[{"b64_json":"` + bitmap + `","size":"1024x1024"}]}`
	var evidence WalletReaderEvidence
	h := &AuthorizationHandle{readerNormalization: &WalletReaderNormalization{Version: 1, Family: BillingFamilyOpenAI, ProviderPlatform: PlatformOpenAI, Ready: true, CountKind: "openai_images", ImageSize: "4K"}, readerStarted: func() error { return nil }, readerObserved: func(WalletReaderEvidence) error { return nil }}
	journal := testWalletReaderJournal(t)
	h.readerJournal = journal
	h.consumeHTTP = func(_ int, e WalletReaderEvidence, _ error) { evidence = e }
	body := &walletResponseBody{ReadCloser: io.NopCloser(strings.NewReader(raw)), handle: h, status: 200, jsonLimit: 32 * 1024 * 1024}
	_, err := io.Copy(io.Discard, body)
	require.NoError(t, err)
	require.NoError(t, body.Close())
	require.False(t, evidence.Malformed)
	require.Equal(t, 1, evidence.Counts.imageCount())
	require.NoError(t, journal.acquire(false))
	require.NoError(t, journal.loadLocked())
	encoded, err := json.Marshal(journal.record)
	require.NoError(t, err)
	journal.release()
	require.LessOrEqual(t, len(encoded), walletReaderJournalBound)
	require.NotContains(t, string(encoded), strings.Repeat("a", 128))
}

func TestWalletReaderCountsUnboundWSImageCannotEnterNextTurnJournalOrFee(t *testing.T) {
	facts := &WalletReaderNormalization{Version: 1, Family: BillingFamilyOpenAI, ProviderPlatform: PlatformOpenAI, Ready: true, CountKind: "openai_images", ImageSize: "4K"}
	h := &AuthorizationHandle{ID: "next-turn", writes: []AuthorizationWrite{{Token: "next-turn.1"}}, readerNormalization: facts, readerStarted: func() error { return nil }}
	var evidence WalletReaderEvidence
	h.readerObserved = func(e WalletReaderEvidence) error { evidence = e; return nil }
	journal := testWalletReaderJournal(t)
	journal.record.Normalization = facts
	h.readerJournal = journal
	require.NoError(t, journal.beginRead())
	conn := &authorizingOpenAIWSClientConn{}
	conn.armed.Store(h)
	orphan := []byte(`{"type":"response.output_item.done","item":{"type":"image_generation_call","id":"old-unbound-image","result":"old-private-bitmap"}}`)
	require.NoError(t, conn.observeOwnedFrameEvidence(orphan, h))
	require.Nil(t, h.readerEvidence, "an output item identity cannot select the next response owner")
	require.Empty(t, journal.record.PendingUsage)
	owned := []byte(`{"type":"response.completed","response":{"id":"next-owned-response","output":[{"type":"image_generation_call","id":"next-owned-image","result":"next-private-bitmap","size":"1024x1024"}]}}`)
	require.NoError(t, conn.observeOwnedFrameEvidence(owned, h))
	require.Equal(t, 1, evidence.Counts.imageCount())
	require.NotContains(t, evidence.Counts.Images, hashOpenAIImageOutputResult("old-unbound-image"))
	encoded, err := json.Marshal(journal.record)
	require.NoError(t, err)
	journal.release()
	require.NotContains(t, string(encoded), "old-unbound-image")
	require.NotContains(t, string(encoded), hashOpenAIImageOutputResult("old-unbound-image"))
	_, snapshot, _, _, _ := freezeForSettleTest(t, BillingFamilyOpenAI, "gpt-5.1")
	snapshot.Pricing.Mode = BillingModeImage
	input, err := normalizeWalletReaderFee(snapshot, facts, evidence)
	require.NoError(t, err)
	require.Equal(t, SnapshotSettlementInput{ImageCount: 1, ImageSize: "1K"}, input)
}

func TestWalletReaderCountsSSELimitAppliesToEachExistingLine(t *testing.T) {
	frame := "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n"
	var evidence WalletReaderEvidence
	h := &AuthorizationHandle{readerStarted: func() error { return nil }, readerObserved: func(WalletReaderEvidence) error { return nil }, consumeHTTP: func(_ int, e WalletReaderEvidence, _ error) { evidence = e }}
	body := &walletResponseBody{ReadCloser: io.NopCloser(strings.NewReader(strings.Repeat(frame, 20))), handle: h, status: 200, stream: true, streamLimit: int64(len(frame))}
	_, err := io.Copy(io.Discard, body)
	require.NoError(t, err)
	require.NoError(t, body.Close())
	require.False(t, evidence.Malformed, "one read containing many valid lines cannot create a new aggregate API limit")
	require.True(t, evidence.Valid)
	require.Equal(t, 1, evidence.Tokens.InputTokens)
}

func TestWalletReaderCountsZeroMultiplierCannotConcealPositiveSearchAfterSignedZero(t *testing.T) {
	for _, stored := range []string{`{}`, `{"present":true,"valid":true,"input_present":true,"output_present":true}`} {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		bridge := &CanonicalWalletBridge{outboxDB: db}
		billing := &openAIRecordUsageBillingRepoStub{}
		mock.ExpectQuery("SELECT reader_evidence").WithArgs("search-owner", "search-owner.1").WillReturnRows(sqlmock.NewRows([]string{"reader_evidence"}).AddRow(stored))
		mock.ExpectBegin()
		mock.ExpectQuery("SELECT authorization_id").WithArgs("search-owner").WillReturnRows(sqlmock.NewRows([]string{"authorization_id"}).AddRow("search-owner"))
		mock.ExpectQuery("SELECT bool_and").WithArgs("search-owner", "search-owner.1", "search-snapshot", "platform-user").WillReturnRows(sqlmock.NewRows([]string{"valid", "zero"}).AddRow(true, true))
		mock.ExpectExec("INSERT INTO wallet_billing_anomaly").WithArgs("search-owner", "search-owner.1", sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()
		mock.ExpectClose()
		applied, result, err := applyUsageBillingDetailed(context.Background(), "late-free-search", &UsageLog{Model: "gpt-5.1"}, &postUsageBillingParams{
			WalletBridge: bridge, AuthorizationID: "search-owner", AuthorizationToken: "search-owner.1", BillingSnapshotID: "search-snapshot", WalletEvidencePolicyVersion: WalletImmediateReleasePolicyVersion, WalletEvidenceKind: "llm",
			Cost: &CostBreakdown{TotalCost: .02, ActualCost: 0}, User: &User{ID: 1, PlatformUserID: "platform-user", BillingCurrency: "USD"}, APIKey: &APIKey{ID: 2}, Account: &Account{ID: 3, Type: AccountTypeAPIKey},
		}, &billingDeps{}, billing)
		require.ErrorIs(t, err, ErrWalletPositiveAfterZero)
		require.False(t, applied)
		require.Nil(t, result)
		require.Zero(t, billing.calls, "a zero current multiplier cannot turn the changed search into a user charge or trusted zero command")
		require.NoError(t, db.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	}
}
