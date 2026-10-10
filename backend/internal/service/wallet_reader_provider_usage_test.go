//go:build unit

package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

const walletGrokSeparateReasoning = `{"id":"grok-proof","usage":{"prompt_tokens":1247,"completion_tokens":1,"total_tokens":1324,"prompt_tokens_details":{"cached_tokens":1152},"completion_tokens_details":{"reasoning_tokens":76}}}`

func TestWalletReaderFreezesSelectedProviderWithoutMutableAccountLookup(t *testing.T) {
	svc, key, user, account := newSnapshotTestFixture(t)
	account.Platform = PlatformGrok
	snapshot, err := svc.Freeze(context.Background(), FreezeInput{APIKey: key, User: user, Account: account, Family: BillingFamilyOpenAI, RequestedModel: "gpt-5.1", BillingModel: "gpt-5.1"})
	require.NoError(t, err)
	account.Platform = PlatformOpenAI
	raw, err := snapshot.MarshalPayload()
	require.NoError(t, err)
	restored, err := UnmarshalBillingSnapshotPayload(raw)
	require.NoError(t, err)
	require.Equal(t, PlatformGrok, restored.ProviderPlatform)
	h := &AuthorizationHandle{readerBillingFamily: restored.Family, readerPlatform: restored.ProviderPlatform, readerTokenOnly: true, stageUsage: func(_ context.Context, task UsageRecordTask) (UsageRecordTask, error) { return task, nil }}
	h.captureWalletReaderNormalization(context.Background(), []byte(`{"provider_platform":"openai","model":"gpt-5.1"}`), true)
	require.Equal(t, PlatformGrok, h.readerNormalization.ProviderPlatform, "the wire request cannot select its financial provider")
	h.readerPlatform = PlatformOpenAI
	h.captureWalletReaderNormalization(context.Background(), []byte(`{}`), true)
	require.Equal(t, PlatformGrok, h.readerNormalization.ProviderPlatform, "later writes cannot replace the selected provider")

	var evidence WalletReaderEvidence
	observeWalletUsage([]byte(walletGrokSeparateReasoning), &evidence, h.readerNormalization)
	input, err := normalizeWalletReaderFee(restored, h.readerNormalization, evidence)
	require.NoError(t, err)
	require.Equal(t, 95, input.Tokens.InputTokens)
	require.Equal(t, 1152, input.Tokens.CacheReadTokens, "cached prompt tokens are subtracted once")
	require.Equal(t, 77, input.Tokens.OutputTokens)
	parsed, ok := extractOpenAIUsageFromJSONBytes([]byte(walletGrokSeparateReasoning))
	require.True(t, ok)
	normalizeGrokChatCompletionUsage(&Account{Platform: PlatformGrok}, gjson.Get(walletGrokSeparateReasoning, "usage"), &parsed)
	readerFee, err := svc.billing.CalculateCostFromSnapshot(restored, input)
	require.NoError(t, err)
	billerFee, err := svc.billing.CalculateCostFromSnapshot(restored, SnapshotSettlementInput{Tokens: openAIUsageTokens(parsed)})
	require.NoError(t, err)
	require.Equal(t, billerFee.ActualCost, readerFee.ActualCost)
}

func TestWalletReaderGrokReasoningUsesSelectedProviderAndExactConservation(t *testing.T) {
	for _, tc := range []struct {
		name, platform, raw string
		output              int
		valid               bool
	}{
		{"grok-separate", PlatformGrok, walletGrokSeparateReasoning, 77, true},
		{"openai-reasoning-is-inclusive", PlatformOpenAI, walletGrokSeparateReasoning, 1, true},
		{"grok-already-inclusive", PlatformGrok, strings.Replace(walletGrokSeparateReasoning, `"completion_tokens":1`, `"completion_tokens":77`, 1), 77, true},
		{"grok-responses-is-inclusive", PlatformGrok, `{"usage":{"input_tokens":4,"output_tokens":77,"output_tokens_details":{"reasoning_tokens":76}}}`, 77, true},
		{"missing-total", PlatformGrok, strings.Replace(walletGrokSeparateReasoning, `,"total_tokens":1324`, "", 1), 0, false},
		{"mismatched-total", PlatformGrok, strings.Replace(walletGrokSeparateReasoning, `1324`, `1323`, 1), 0, false},
		{"negative-reasoning", PlatformGrok, strings.Replace(walletGrokSeparateReasoning, `"reasoning_tokens":76`, `"reasoning_tokens":-1`, 1), 0, false},
		{"fractional-reasoning", PlatformGrok, strings.Replace(walletGrokSeparateReasoning, `"reasoning_tokens":76`, `"reasoning_tokens":76.5`, 1), 0, false},
		{"string-reasoning", PlatformGrok, strings.Replace(walletGrokSeparateReasoning, `"reasoning_tokens":76`, `"reasoning_tokens":"76"`, 1), 0, false},
		{"null-reasoning", PlatformGrok, strings.Replace(walletGrokSeparateReasoning, `"reasoning_tokens":76`, `"reasoning_tokens":null`, 1), 0, false},
		{"overflow-total", PlatformGrok, strings.Replace(walletGrokSeparateReasoning, `1324`, `9223372036854775808`, 1), 0, false},
		{"impossible-inclusive-reasoning", PlatformGrok, strings.Replace(walletGrokSeparateReasoning, `1324`, `1248`, 1), 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			facts := &WalletReaderNormalization{Version: 1, ProviderPlatform: tc.platform}
			var live, replay WalletReaderEvidence
			observeWalletUsage([]byte(tc.raw), &live, facts)
			frame, err := selectedWalletUsageFrame([]byte(tc.raw), facts)
			require.NoError(t, err)
			observeWalletUsage(frame, &replay, facts)
			require.Equal(t, live, replay, "selected checkpoint must retain all evidence used by live normalization")
			require.Equal(t, tc.valid, walletReaderEvidenceTrusted(live))
			require.Equal(t, tc.output, live.Tokens.OutputTokens)
			require.True(t, live.ObservedPositive, "invalid positive facts cannot become zero")
			observeWalletUsage([]byte(tc.raw), &live, facts)
			require.Equal(t, tc.output, live.Tokens.OutputTokens, "cumulative duplicates merge by maximum")
		})
	}
}

func TestWalletReaderGeminiDefaultOutputRequiresProviderTerminalAndConservation(t *testing.T) {
	for _, tc := range []struct {
		name, platform, raw string
		valid, malformed    bool
		input, output       int
	}{
		{"blocked-default-zero-output", PlatformGemini, `{"promptFeedback":{"blockReason":"SAFETY"},"usageMetadata":{"promptTokenCount":8,"totalTokenCount":8}}`, true, false, 8, 0},
		{"thoughts-are-output", PlatformGemini, `{"candidates":[{"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":8,"thoughtsTokenCount":3,"totalTokenCount":11,"cachedContentTokenCount":4}}`, true, false, 4, 3},
		{"wrapped-antigravity-native", PlatformAntigravity, `{"response":{"responseId":"gemini-proof","candidates":[{"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":8,"thoughtsTokenCount":3,"totalTokenCount":11}}}`, true, false, 8, 3},
		{"intermediate-default-is-unresolved", PlatformGemini, `{"usageMetadata":{"promptTokenCount":8,"totalTokenCount":8}}`, false, false, 8, 0},
		{"unknown-terminal-is-unresolved", PlatformGemini, `{"candidates":[{"finishReason":"FUTURE_UNKNOWN"}],"usageMetadata":{"promptTokenCount":8,"totalTokenCount":8}}`, false, false, 8, 0},
		{"wrong-provider-cannot-default", PlatformOpenAI, `{"promptFeedback":{"blockReason":"SAFETY"},"usageMetadata":{"promptTokenCount":8,"totalTokenCount":8}}`, false, true, 0, 0},
		{"missing-provider-cannot-default", "", `{"promptFeedback":{"blockReason":"SAFETY"},"usageMetadata":{"promptTokenCount":8,"totalTokenCount":8}}`, false, true, 0, 0},
		{"mismatch-total", PlatformGemini, `{"promptFeedback":{"blockReason":"SAFETY"},"usageMetadata":{"promptTokenCount":8,"totalTokenCount":9}}`, false, true, 0, 0},
		{"missing-total", PlatformGemini, `{"promptFeedback":{"blockReason":"SAFETY"},"usageMetadata":{"promptTokenCount":8}}`, false, true, 0, 0},
		{"cache-exceeds-input", PlatformGemini, `{"promptFeedback":{"blockReason":"SAFETY"},"usageMetadata":{"promptTokenCount":8,"totalTokenCount":8,"cachedContentTokenCount":9}}`, false, true, 0, 0},
		{"intermediate-cache-exceeds-input", PlatformGemini, `{"usageMetadata":{"promptTokenCount":8,"totalTokenCount":8,"cachedContentTokenCount":9}}`, false, true, 0, 0},
		{"intermediate-total-mismatch", PlatformGemini, `{"usageMetadata":{"promptTokenCount":8,"totalTokenCount":9}}`, false, true, 0, 0},
		{"thoughts-overflow", PlatformGemini, `{"candidates":[{"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":8,"candidatesTokenCount":1,"thoughtsTokenCount":9223372036854775807}}`, false, true, 0, 0},
		{"null-is-still-invalid", PlatformGemini, `{"promptFeedback":{"blockReason":"SAFETY"},"usageMetadata":{"promptTokenCount":8,"candidatesTokenCount":null,"totalTokenCount":8}}`, false, true, 0, 0},
		{"unpriced-tool-prompt", PlatformGemini, `{"candidates":[{"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":8,"thoughtsTokenCount":3,"totalTokenCount":11,"toolUsePromptTokenCount":2}}`, false, true, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			facts := &WalletReaderNormalization{Version: 1, ProviderPlatform: tc.platform}
			var live, replay WalletReaderEvidence
			observeWalletUsage([]byte(tc.raw), &live, facts)
			frame, err := selectedWalletUsageFrame([]byte(tc.raw), facts)
			require.NoError(t, err)
			observeWalletUsage(frame, &replay, facts)
			require.Equal(t, live, replay)
			require.Equal(t, tc.valid, walletReaderEvidenceTrusted(live))
			require.Equal(t, tc.malformed, live.Malformed)
			require.Equal(t, tc.input, live.Tokens.InputTokens)
			require.Equal(t, tc.output, live.Tokens.OutputTokens)
			require.True(t, live.ObservedPositive, "prompt/thoughts/total positive cannot be known zero")
		})
	}
}

func TestWalletReaderGeminiPartialPositiveCannotBeErasedByTerminalZero(t *testing.T) {
	for _, thoughts := range []int{0, 3} {
		t.Run(fmt.Sprintf("thoughts-%d", thoughts), func(t *testing.T) {
			facts := &WalletReaderNormalization{Version: 1, Family: BillingFamilyGeneric, ProviderPlatform: PlatformGemini, Ready: true, TokenOnly: true}
			partial := []byte(fmt.Sprintf(`{"usageMetadata":{"promptTokenCount":8,"thoughtsTokenCount":%d,"totalTokenCount":%d}}`, thoughts, 8+thoughts))
			zero := []byte(`{"candidates":[{"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":0,"totalTokenCount":0}}`)
			var live, replay WalletReaderEvidence
			observeWalletUsage(partial, &live, facts)
			require.False(t, walletReaderEvidenceTrusted(live), "partial default output cannot supply terminal proof")
			require.False(t, live.OutputPresent)
			h := &AuthorizationHandle{readerNormalization: facts, consumeHTTP: func(_ int, _ WalletReaderEvidence, _ error) {}}
			body := &walletResponseBody{ReadCloser: io.NopCloser(strings.NewReader("data: " + string(partial) + "\n\n")), handle: h, stream: true, status: 200}
			_, err := io.ReadAll(body)
			require.NoError(t, err)
			require.NoError(t, body.Close())
			require.True(t, body.evidence.Complete)
			require.False(t, walletReaderEvidenceTrusted(body.evidence), "partial-only EOF keeps its fee unresolved")
			require.True(t, body.evidence.ObservedPositive)
			for _, frame := range [][]byte{partial, zero} {
				selected, err := selectedWalletUsageFrame(frame, facts)
				require.NoError(t, err)
				observeWalletUsage(selected, &replay, facts)
			}
			observeWalletUsage(zero, &live, facts)
			require.Equal(t, live, replay, "checkpoint replay preserves the exact positive cumulative facts")
			require.True(t, walletReaderEvidenceTrusted(live))
			require.True(t, live.ObservedPositive)
			require.Equal(t, 8, live.RawInputTokens)
			require.Equal(t, UsageTokens{InputTokens: 8, OutputTokens: thoughts}, live.Tokens)
			_, snapshot, _, _, _ := freezeForSettleTest(t, BillingFamilyGeneric, "gpt-5.1")
			snapshot.ProviderPlatform = PlatformGemini
			_, err = normalizeWalletReaderFee(snapshot, facts, body.evidence)
			require.Error(t, err, "completing a partial reader cannot confer terminal fee proof")
			input, err := normalizeWalletReaderFee(snapshot, facts, live)
			require.NoError(t, err)
			fee, err := (&BillingService{}).CalculateCostFromSnapshot(snapshot, input)
			require.NoError(t, err)
			require.Positive(t, fee.ActualCost, "positive prompt/thoughts can never become a signed zero fee")
		})
	}
}

func TestWalletReaderGeminiCheckpointFailureRetainsTerminalProofAndExactCacheFee(t *testing.T) {
	for _, tc := range []struct {
		name                                           string
		explicit, malformed, countsMalformed, complete bool
	}{
		{"omitted-partial-output", false, false, false, true},
		{"explicit-partial-output", true, false, false, true},
		{"live-malformed-remains-untrusted", false, true, false, true},
		{"live-counts-malformed-remains-untrusted", false, false, true, true},
		{"read-error-cannot-manufacture-eof", false, false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			facts := &WalletReaderNormalization{Version: 1, Family: BillingFamilyGeneric, ProviderPlatform: PlatformGemini, Ready: true, TokenOnly: true}
			partial := []byte(`{"usageMetadata":{"promptTokenCount":8,"totalTokenCount":8}}`)
			if tc.explicit {
				partial = []byte(`{"usageMetadata":{"promptTokenCount":8,"candidatesTokenCount":0,"totalTokenCount":8}}`)
			}
			terminal := []byte(`{"responseId":"selected-gemini","candidates":[{"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":8,"cachedContentTokenCount":4,"totalTokenCount":8}}`)
			journal := testWalletReaderJournal(t)
			require.NoError(t, journal.acquire(false))
			journal.record.Normalization = facts
			require.NoError(t, journal.saveLocked())
			journal.release()
			require.NoError(t, journal.beginRead())
			require.NoError(t, journal.checkpointLocked(partial, "llm_http_usage"))
			var live WalletReaderEvidence
			observeWalletUsage(partial, &live, facts)
			live.Source = "llm_http_usage"
			require.NoError(t, journal.observeLocked(live, false))
			original := journal.dir
			journal.dir = filepath.Join(original, "missing-mount")
			require.Error(t, journal.checkpointLocked(terminal, "llm_http_usage"), "a real filesystem failure precedes live terminal parsing")
			require.NotEmpty(t, journal.record.PendingUsage)
			journal.dir = original
			journal.release()
			live.Complete, live.Malformed = tc.complete, tc.malformed
			if tc.countsMalformed {
				live.Counts = &WalletReaderCounts{Version: 1, Malformed: true}
			}
			_, snapshot, _, _, _ := freezeForSettleTest(t, BillingFamilyGeneric, "gpt-5.1")
			snapshot.ProviderPlatform = PlatformGemini
			want := UsageTokens{InputTokens: 4, CacheReadTokens: 4}
			expected, err := (&BillingService{}).CalculateCostFromSnapshot(snapshot, SnapshotSettlementInput{Tokens: want})
			require.NoError(t, err)
			verify := func(e WalletReaderEvidence) {
				require.Equal(t, tc.complete, e.Complete, "finishReason cannot manufacture an EOF or joined reader completion")
				require.True(t, e.ObservedPositive)
				if tc.malformed || tc.countsMalformed {
					require.False(t, walletReaderEvidenceTrusted(e), "checkpoint replay cannot erase live malformed evidence")
					_, err := normalizeWalletReaderFee(snapshot, facts, e)
					require.Error(t, err)
					return
				}
				require.True(t, walletReaderEvidenceTrusted(e))
				require.Equal(t, "selected-gemini", e.ResponseID)
				require.Equal(t, 8, e.RawInputTokens)
				require.Equal(t, want, e.Tokens, "an earlier uncached partial cannot override replayed native cache normalization")
				input, err := normalizeWalletReaderFee(snapshot, facts, e)
				require.NoError(t, err)
				cost, err := (&BillingService{}).CalculateCostFromSnapshot(snapshot, input)
				require.NoError(t, err)
				require.Equal(t, expected.ActualCost, cost.ActualCost)
			}
			pgFault := errors.New("isolated PG handoff fault")
			require.ErrorIs(t, journal.transfer(live, func(e WalletReaderEvidence) error { verify(e); return pgFault }), pgFault)
			reopened := &walletReaderJournal{dir: journal.dir, path: journal.path, record: journal.record}
			require.NoError(t, reopened.acquire(false))
			require.NoError(t, reopened.loadLocked())
			require.True(t, reopened.record.Joined && reopened.record.PersistenceFailed)
			require.Empty(t, reopened.record.PendingUsage)
			verify(reopened.record.Evidence)
			reopened.release()
			require.NoError(t, reopened.transfer(WalletReaderEvidence{}, func(e WalletReaderEvidence) error { verify(e); return nil }))
		})
	}
}

func TestWalletReaderProviderHTTPAndSSEMergeMatchCheckpointRecovery(t *testing.T) {
	for _, platform := range []string{PlatformGrok, PlatformGemini} {
		t.Run(platform, func(t *testing.T) {
			facts := &WalletReaderNormalization{Version: 1, ProviderPlatform: platform}
			final := walletGrokSeparateReasoning
			prelude := `{"usage":null}`
			output := 77
			if platform == PlatformGemini {
				prelude = `{"usageMetadata":{"promptTokenCount":8,"totalTokenCount":8}}`
				final = `{"candidates":[{"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":8,"thoughtsTokenCount":3,"totalTokenCount":11}}`
				output = 3
			}
			for _, status := range []int{200, 401, 500} {
				h := &AuthorizationHandle{readerNormalization: facts, consumeHTTP: func(_ int, _ WalletReaderEvidence, _ error) {}}
				body := &walletResponseBody{ReadCloser: io.NopCloser(strings.NewReader("data: " + prelude + "\n\ndata: " + final + "\n\ndata: " + final + "\n\n")), handle: h, stream: true, status: status}
				_, err := io.ReadAll(body)
				require.NoError(t, err)
				require.NoError(t, body.Close())
				require.True(t, body.evidence.Complete)
				require.True(t, walletReaderEvidenceTrusted(body.evidence))
				require.Equal(t, output, body.evidence.Tokens.OutputTokens)
			}
			journal := testWalletReaderJournal(t)
			require.NoError(t, journal.acquire(false))
			journal.record.Normalization = facts
			require.NoError(t, journal.saveLocked())
			journal.release()
			require.NoError(t, journal.beginRead())
			require.NoError(t, journal.checkpointLocked([]byte(final), "llm_http_usage"))
			journal.release()
			reopened := &walletReaderJournal{dir: journal.dir, path: journal.path, record: journal.record}
			require.NoError(t, reopened.acquire(false))
			defer reopened.release()
			require.NoError(t, reopened.loadLocked())
			require.Equal(t, platform, reopened.record.Normalization.ProviderPlatform)
			require.False(t, reopened.record.Evidence.ObservedPositive, "simulate crash before reader adopts the durable checkpoint")
			require.NoError(t, reopened.replayCheckpointLocked())
			require.True(t, walletReaderEvidenceTrusted(reopened.record.Evidence))
			require.Equal(t, output, reopened.record.Evidence.Tokens.OutputTokens)
			require.Empty(t, reopened.record.PendingUsage)
		})
	}
}

func TestWalletReaderHistoricalProviderFactsCannotSelectOneTokenFee(t *testing.T) {
	_, snapshot, _, _, _ := freezeForSettleTest(t, BillingFamilyOpenAI, "gpt-5.1")
	var oldEvidence WalletReaderEvidence
	observeWalletUsage([]byte(walletGrokSeparateReasoning), &oldEvidence)
	require.Equal(t, 1, oldEvidence.Tokens.OutputTokens, "historical reader evidence has already lost the reasoning reconciliation")
	facts := &WalletReaderNormalization{Version: 1, Family: snapshot.Family, ProviderPlatform: PlatformGrok, Ready: true, TokenOnly: true}
	snapshot.ProviderPlatform = PlatformGrok
	for _, missing := range []string{"snapshot", "normalization", "mismatch"} {
		t.Run(missing, func(t *testing.T) {
			s := *snapshot
			f := *facts
			switch missing {
			case "snapshot":
				s.ProviderPlatform = ""
			case "normalization":
				f.ProviderPlatform = ""
			case "mismatch":
				f.ProviderPlatform = PlatformOpenAI
			}
			_, err := normalizeWalletReaderFee(&s, &f, oldEvidence)
			require.ErrorIs(t, err, errWalletReaderProviderFactsUnavailable)
		})
	}
	raw, err := json.Marshal(snapshot)
	require.NoError(t, err)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(raw, &payload))
	delete(payload, "provider_platform")
	raw, err = json.Marshal(payload)
	require.NoError(t, err)
	legacy, err := UnmarshalBillingSnapshotPayload(raw)
	require.NoError(t, err, "the additive payload remains backward readable without changing deployed migrations")
	require.Empty(t, legacy.ProviderPlatform)
	_, err = normalizeWalletReaderFee(legacy, facts, oldEvidence)
	require.ErrorIs(t, err, errWalletReaderProviderFactsUnavailable)
}
