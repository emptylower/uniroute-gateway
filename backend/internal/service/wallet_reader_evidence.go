package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

// This is normalized parser evidence, not raw output or a caller-selected
// pricing source. Cumulative token snapshots merge by maximum, never by sum.
type WalletReaderEvidence struct {
	Present          bool                `json:"present"`
	Valid            bool                `json:"valid"`
	Malformed        bool                `json:"malformed"`
	ObservedPositive bool                `json:"observed_positive"`
	Complete         bool                `json:"complete"`
	ResponseID       string              `json:"response_id,omitempty"`
	RawInputTokens   int                 `json:"raw_input_tokens"`
	Source           string              `json:"source,omitempty"`
	InputPresent     bool                `json:"input_present"`
	OutputPresent    bool                `json:"output_present"`
	Tokens           UsageTokens         `json:"tokens"`
	Counts           *WalletReaderCounts `json:"counts,omitempty"`
}

func walletUsageInteger(value gjson.Result) (int, bool) {
	if !value.Exists() {
		return 0, true
	}
	if value.Type != gjson.Number {
		return 0, false
	}
	n, err := strconv.ParseInt(value.Raw, 10, 64)
	if err != nil || n < 0 || uint64(n) > uint64(math.MaxInt) {
		return 0, false
	}
	return int(n), true
}

// walletUsageLocations are the places a provider frame can carry its usage, in
// selection order.
var walletUsageLocations = []string{"usage", "response.usage", "message.usage", "usageMetadata", "response.usageMetadata"}

// walletSelectedUsage returns the first location that reports usage. An explicit
// JSON null means "no usage reported", exactly like a missing field: OpenAI
// Responses events that carry the whole response object (created, in_progress)
// have response.usage:null, and Chat Completions chunks have usage:null when
// stream_options.include_usage is on. Treating that null as a present, malformed
// value would latch Malformed for the whole stream and make a complete, strictly
// valid terminal usage untrusted. For HTTP sources the live parser and the
// journal checkpoint must select the same value, so both use this. (The
// WebSocket live parser reads response.usage only; see walletWSUsage.)
func walletSelectedUsage(root gjson.Result) (string, gjson.Result) {
	for _, candidate := range walletUsageLocations {
		if value := root.Get(candidate); value.Exists() && value.Type != gjson.Null {
			return candidate, value
		}
	}
	return "", gjson.Result{}
}

// Native Gemini can omit default-zero candidates only in a recognized terminal
// response. Keep the same small protocol proof in the journal, never its content.
func walletGeminiUsageTerminal(root gjson.Result, path string) bool {
	if path == "response.usageMetadata" {
		root = root.Get("response")
	}
	switch root.Get("promptFeedback.blockReason").String() {
	case "SAFETY", "OTHER", "BLOCKLIST", "PROHIBITED_CONTENT", "IMAGE_SAFETY":
		return true
	}
	candidates := root.Get("candidates")
	if !candidates.IsArray() || len(candidates.Array()) == 0 {
		return false
	}
	for _, candidate := range candidates.Array() {
		switch candidate.Get("finishReason").String() {
		case "STOP", "MAX_TOKENS", "SAFETY", "RECITATION", "LANGUAGE", "OTHER", "BLOCKLIST", "PROHIBITED_CONTENT", "SPII", "MALFORMED_FUNCTION_CALL", "IMAGE_SAFETY", "IMAGE_PROHIBITED_CONTENT", "IMAGE_OTHER", "NO_IMAGE", "IMAGE_RECITATION", "UNEXPECTED_TOOL_CALL", "TOO_MANY_TOOL_CALLS":
		default:
			return false
		}
	}
	return true
}

func observeWalletUsage(raw []byte, evidence *WalletReaderEvidence, facts ...*WalletReaderNormalization) {
	if !gjson.ValidBytes(raw) {
		return
	}
	root := gjson.ParseBytes(raw)
	path, usage := walletSelectedUsage(root)
	if path == "" {
		return
	}
	gemini := path == "usageMetadata" || path == "response.usageMetadata"
	platform := ""
	if len(facts) > 0 && facts[0] != nil {
		platform = facts[0].ProviderPlatform
	}
	selectedGemini := gemini && (platform == PlatformGemini || platform == PlatformAntigravity)
	evidence.Present = true
	if !usage.IsObject() {
		evidence.Malformed = true
		return
	}
	fields := []string{"input_tokens", "output_tokens", "cache_creation_input_tokens", "cache_read_input_tokens", "image_output_tokens"}
	if gemini {
		fields = []string{"promptTokenCount", "candidatesTokenCount", "cachedContentTokenCount", "thoughtsTokenCount"}
		if selectedGemini {
			fields = append(fields, "totalTokenCount", "toolUsePromptTokenCount")
		}
	}
	values := make(map[string]int, len(fields))
	saw := false
	valid := true
	for _, name := range fields {
		v := usage.Get(name)
		if v.Exists() {
			saw = true
		}
		n, ok := walletUsageInteger(v)
		valid = valid && ok
		values[name] = n
		if v.Type == gjson.Number && v.Num > 0 {
			evidence.ObservedPositive = true
		}
	}
	// Chat Completions and Bedrock use different field names, with the same
	// normalized buckets. Reasoning is part of OpenAI output, not an add-on.
	if !gemini && !saw {
		for _, name := range []string{"prompt_tokens", "completion_tokens", "inputTokens", "outputTokens"} {
			v := usage.Get(name)
			if v.Exists() {
				saw = true
			}
			n, ok := walletUsageInteger(v)
			valid = valid && ok
			values[name] = n
			if v.Type == gjson.Number && v.Num > 0 {
				evidence.ObservedPositive = true
			}
		}
		values["input_tokens"] = max(values["prompt_tokens"], values["inputTokens"])
		values["output_tokens"] = max(values["completion_tokens"], values["outputTokens"])
	}
	if !saw || !valid {
		evidence.Malformed = true
		return
	}
	if selectedGemini {
		prompt, candidates, thoughts := values["promptTokenCount"], values["candidatesTokenCount"], values["thoughtsTokenCount"]
		if values["cachedContentTokenCount"] > prompt || thoughts > math.MaxInt-candidates || values["toolUsePromptTokenCount"] != 0 {
			evidence.Malformed = true
			return
		}
		if usage.Get("totalTokenCount").Exists() && (values["totalTokenCount"] < prompt || values["totalTokenCount"]-prompt != candidates+thoughts) {
			evidence.Malformed = true
			return
		}
	}
	inputPresent := usage.Get("input_tokens").Exists() || usage.Get("prompt_tokens").Exists() || usage.Get("inputTokens").Exists() || usage.Get("promptTokenCount").Exists()
	outputPresent := usage.Get("output_tokens").Exists() || usage.Get("completion_tokens").Exists() || usage.Get("outputTokens").Exists() || usage.Get("candidatesTokenCount").Exists()
	partialGemini := false
	if selectedGemini && !outputPresent {
		// Proto3 omits default-zero candidates. Intermediate stream metadata is
		// not a final output count. Preserve its proven positive buckets without
		// poisoning a later terminal frame or allowing it to erase those buckets.
		if !walletGeminiUsageTerminal(root, path) {
			partialGemini = true
		} else {
			outputPresent = usage.Get("totalTokenCount").Exists()
		}
	}
	evidence.InputPresent = evidence.InputPresent || inputPresent
	evidence.OutputPresent = evidence.OutputPresent || outputPresent
	partialClaude := root.Get("type").String() == "message_start" || root.Get("type").String() == "message_delta"
	if (!inputPresent || !outputPresent) && !partialClaude && (!partialGemini || !inputPresent) {
		evidence.Malformed = true
		return
	}
	input, output, cacheWrite, cacheRead, image := values["input_tokens"], values["output_tokens"], values["cache_creation_input_tokens"], values["cache_read_input_tokens"], values["image_output_tokens"]
	if gemini {
		input, output, cacheRead = values["promptTokenCount"], values["candidatesTokenCount"], values["cachedContentTokenCount"]
		if values["thoughtsTokenCount"] > math.MaxInt-output {
			evidence.Malformed = true
			return
		}
		output += values["thoughtsTokenCount"]
	} else if platform == PlatformGrok && usage.Get("prompt_tokens").Exists() && !usage.Get("input_tokens").Exists() && !usage.Get("output_tokens").Exists() {
		// xAI may exclude reasoning from completion_tokens. The selected provider
		// and exact total are required before either inclusive or separate output
		// can become a fee; subtracting avoids completion+reasoning overflow.
		total, totalOK := walletUsageInteger(usage.Get("total_tokens"))
		reasoning, reasoningOK := walletUsageInteger(usage.Get("completion_tokens_details.reasoning_tokens"))
		if reasoning > 0 || usage.Get("total_tokens").Num > 0 {
			evidence.ObservedPositive = true
		}
		if !usage.Get("total_tokens").Exists() || !totalOK || !reasoningOK || total < input {
			evidence.Malformed = true
			return
		}
		inclusive := total - input
		if reasoning > inclusive || inclusive < output || inclusive != output && (reasoning <= 0 || inclusive-output != reasoning) {
			evidence.Malformed = true
			return
		}
		output = inclusive
	}
	evidence.RawInputTokens = max(evidence.RawInputTokens, input)
	for _, path := range []string{"input_tokens_details.cache_write_tokens", "prompt_tokens_details.cache_write_tokens", "input_tokens_details.cache_creation_tokens", "prompt_tokens_details.cache_creation_tokens", "cache_write_tokens", "cache_creation_input_tokens", "cache_write_input_tokens", "cache_creation_tokens"} {
		if v := usage.Get(path); v.Exists() {
			n, ok := walletUsageInteger(v)
			if v.Type == gjson.Number && v.Num > 0 {
				evidence.ObservedPositive = true
			}
			if !ok {
				evidence.Malformed = true
				return
			}
			cacheWrite = n
			break
		}
	}
	for _, path := range []string{"input_tokens_details.cached_tokens", "prompt_tokens_details.cached_tokens"} {
		if v := usage.Get(path); v.Exists() {
			n, ok := walletUsageInteger(v)
			if !ok {
				evidence.Malformed = true
				return
			}
			cacheRead = max(cacheRead, n)
		}
	}
	extra := map[string]int{}
	for _, path := range []string{"cache_creation.ephemeral_5m_input_tokens", "cache_creation.ephemeral_1h_input_tokens", "input_tokens_details.image_tokens", "output_tokens_details.image_tokens", "image_input_tokens"} {
		v := usage.Get(path)
		if !v.Exists() {
			continue
		}
		n, ok := walletUsageInteger(v)
		if v.Type == gjson.Number && v.Num > 0 {
			evidence.ObservedPositive = true
		}
		if !ok {
			evidence.Malformed = true
			return
		}
		extra[path] = n
	}
	image = max(image, extra["output_tokens_details.image_tokens"])
	imageInput := max(extra["image_input_tokens"], extra["input_tokens_details.image_tokens"])
	// OpenAI/Gemini total input includes cache buckets. Anthropic reports the
	// buckets separately; do not subtract its explicit cache_read field.
	if gemini || usage.Get("prompt_tokens").Exists() || usage.Get("input_tokens_details").Exists() {
		if cacheRead > input || cacheWrite > input-cacheRead || imageInput > input-cacheRead-cacheWrite {
			evidence.Malformed = true
			return
		}
		input -= cacheRead + cacheWrite
	}
	evidence.Valid = evidence.InputPresent && evidence.OutputPresent
	evidence.Tokens.InputTokens = max(evidence.Tokens.InputTokens, input)
	evidence.Tokens.OutputTokens = max(evidence.Tokens.OutputTokens, output)
	evidence.Tokens.CacheCreationTokens = max(evidence.Tokens.CacheCreationTokens, cacheWrite)
	evidence.Tokens.CacheReadTokens = max(evidence.Tokens.CacheReadTokens, cacheRead)
	if selectedGemini {
		// A later terminal frame may first report cached prompt tokens. Subtract
		// the cumulative cache once instead of retaining an earlier uncached input.
		if evidence.Tokens.CacheReadTokens > evidence.RawInputTokens || evidence.Tokens.CacheCreationTokens > evidence.RawInputTokens-evidence.Tokens.CacheReadTokens {
			evidence.Malformed = true
			return
		}
		evidence.Tokens.InputTokens = evidence.RawInputTokens - evidence.Tokens.CacheReadTokens - evidence.Tokens.CacheCreationTokens
	}
	evidence.Tokens.ImageOutputTokens = max(evidence.Tokens.ImageOutputTokens, image)
	evidence.Tokens.ImageInputTokens = max(evidence.Tokens.ImageInputTokens, imageInput)
	evidence.Tokens.CacheCreation5mTokens = max(evidence.Tokens.CacheCreation5mTokens, extra["cache_creation.ephemeral_5m_input_tokens"])
	evidence.Tokens.CacheCreation1hTokens = max(evidence.Tokens.CacheCreation1hTokens, extra["cache_creation.ephemeral_1h_input_tokens"])
	for _, path := range []string{"response.id", "response_id", "id", "response.responseId", "responseId"} {
		if id := root.Get(path).String(); id != "" {
			evidence.ResponseID = id
			break
		}
	}
}

func (h *AuthorizationHandle) recordReaderEvidence(evidence WalletReaderEvidence, err error) {
	if h == nil {
		return
	}
	h.mu.Lock()
	copy := evidence
	h.readerEvidence = &copy
	if err != nil {
		h.readerEvidenceErr = err
	}
	h.mu.Unlock()
}

// A completed strict usage record still needs its frozen command persisted
// when its fee is zero. Sealing that reader cannot turn it into unknown cost.
func walletReaderEvidenceNeedsFeeRecovery(evidence WalletReaderEvidence) bool {
	return evidence.ObservedPositive || evidence.Complete && evidence.Present && evidence.Valid && evidence.InputPresent && evidence.OutputPresent && walletReaderEvidenceTrusted(evidence)
}

func (b *CanonicalWalletBridge) persistReaderEvidence(ctx context.Context, h *AuthorizationHandle, evidence WalletReaderEvidence) error {
	raw, err := json.Marshal(evidence)
	if err != nil {
		return err
	}
	result, err := b.outboxDB.ExecContext(ctx, `UPDATE wallet_authorization_segment SET reader_evidence=$3::jsonb,fee_pending=fee_pending OR $4 WHERE parent_authorization_id=$1 AND authorization_token=$2 AND zero_intent_at IS NULL AND terminal_sealed_at IS NULL`, h.ID, h.LastWriteToken(), string(raw), walletReaderEvidenceNeedsFeeRecovery(evidence))
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return errors.New("wallet reader handoff lost its owner")
	}
	return nil
}

// The final transfer is an explicit reader-owner operation. It does not use
// write_ended_at (which also records context cancellation) as a reader join.
func (b *CanonicalWalletBridge) handoffReaderEvidence(ctx context.Context, h *AuthorizationHandle, evidence WalletReaderEvidence) error {
	raw, err := json.Marshal(evidence)
	if err != nil {
		return err
	}
	result, err := b.outboxDB.ExecContext(ctx, `UPDATE wallet_authorization_segment SET reader_evidence=$3::jsonb,fee_pending=fee_pending OR $4,reader_handoff_at=COALESCE(reader_handoff_at,now()) WHERE parent_authorization_id=$1 AND authorization_token=$2 AND zero_intent_at IS NULL AND terminal_sealed_at IS NULL`, h.ID, h.LastWriteToken(), string(raw), walletReaderEvidenceNeedsFeeRecovery(evidence))
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return errors.New("wallet reader transfer is already fenced")
	}
	h.mu.Lock()
	copy := evidence
	h.readerEvidence = &copy
	h.readerEvidenceErr = nil
	h.mu.Unlock()
	return nil
}

func (b *CanonicalWalletBridge) transferReaderEvidence(ctx context.Context, h *AuthorizationHandle, evidence WalletReaderEvidence) error {
	if h.readerJournal != nil {
		return h.readerJournal.transfer(evidence, func(frozen WalletReaderEvidence) error { return b.handoffReaderEvidence(ctx, h, frozen) })
	}
	return b.handoffReaderEvidence(ctx, h, evidence)
}

func (b *CanonicalWalletBridge) stageReaderFee(ctx context.Context, h *AuthorizationHandle, user string, evidence WalletReaderEvidence) (string, error) {
	if !walletReaderEvidenceTrusted(evidence) {
		return "", errors.New("wallet reader fee evidence incomplete")
	}
	var raw []byte
	if err := b.outboxDB.QueryRowContext(ctx, `SELECT payload FROM wallet_billing_snapshot WHERE id=$1`, h.SnapshotID).Scan(&raw); err != nil {
		return "", err
	}
	snapshot, err := UnmarshalBillingSnapshotPayload(raw)
	if err != nil {
		return "", err
	}
	normalization := h.readerNormalization
	if normalization == nil {
		var facts []byte
		if err = b.outboxDB.QueryRowContext(ctx, `SELECT reader_fee_normalization FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND authorization_token=$2 AND ordinal=0`, h.ID, h.LastWriteToken()).Scan(&facts); err != nil {
			return "", err
		}
		if len(facts) == 0 || json.Unmarshal(facts, &normalization) != nil {
			return "", errWalletReaderProviderFactsUnavailable
		}
	}
	settlementInput, err := normalizeWalletReaderFee(snapshot, normalization, evidence)
	if err != nil {
		return "", err
	}
	billing := &BillingService{}
	cost, err := billing.CalculateCostFromSnapshot(snapshot, settlementInput)
	if err != nil {
		return "", err
	}
	evidence.Tokens = settlementInput.Tokens
	units, err := canonicalWalletUnitsFromUSD(cost.ActualCost)
	if err != nil {
		return "", err
	}
	token := h.LastWriteToken()
	cmd := &UsageBillingCommand{RequestID: "wallet:" + h.ID, APIKeyID: snapshot.APIKeyID, UserID: snapshot.UserID, AccountID: snapshot.AccountID, SettlementCurrency: "USD", Model: snapshot.BillingModel, ServiceTier: settlementInput.ServiceTier, WalletCostUSD: cost.ActualCost, InputTokens: evidence.Tokens.InputTokens, OutputTokens: evidence.Tokens.OutputTokens, CacheCreationTokens: evidence.Tokens.CacheCreationTokens, CacheReadTokens: evidence.Tokens.CacheReadTokens, ImageCount: settlementInput.ImageCount}
	cmd.AccountType = snapshot.Flags.AccountType
	if snapshot.Flags.APIKeyQuotaEnabled {
		cmd.APIKeyQuotaCost = cost.ActualCost
	}
	if snapshot.Flags.APIKeyRateLimitEnabled {
		cmd.APIKeyRateLimitCost = cost.ActualCost
	}
	if snapshot.Flags.AccountQuotaEnabled {
		cmd.AccountQuotaCost = cost.TotalCost * snapshot.Multipliers.Account
	}
	cmd.WalletUsageLog = &UsageLog{RequestID: cmd.RequestID, UserID: cmd.UserID, APIKeyID: cmd.APIKeyID, AccountID: cmd.AccountID, Model: cmd.Model, RequestedModel: snapshot.RequestedModel, GroupID: snapshot.GroupID, BillingSnapshotID: &snapshot.ID, InputTokens: cmd.InputTokens, OutputTokens: cmd.OutputTokens, CacheCreationTokens: cmd.CacheCreationTokens, CacheReadTokens: cmd.CacheReadTokens, CacheCreation5mTokens: evidence.Tokens.CacheCreation5mTokens, CacheCreation1hTokens: evidence.Tokens.CacheCreation1hTokens, ImageInputTokens: evidence.Tokens.ImageInputTokens, ImageOutputTokens: evidence.Tokens.ImageOutputTokens, InputCost: cost.InputCost, OutputCost: cost.OutputCost, ImageInputCost: cost.ImageInputCost, ImageOutputCost: cost.ImageOutputCost, CacheCreationCost: cost.CacheCreationCost, CacheReadCost: cost.CacheReadCost, TotalCost: cost.TotalCost, ActualCost: cost.ActualCost, SettlementCurrency: "USD", SourceCurrency: "USD", SourceCost: cost.ActualCost, BaseCost: cost.ActualCost, RateMultiplier: snapshot.Multipliers.Text, OpenAIWSMode: evidence.Source == "llm_ws_usage"}
	settlement := fixedUSDSettlement(cost, CanonicalWalletUnitVersion)
	settlement.ExchangeRateAsOf = snapshot.FrozenAt
	applySettlementSnapshot(cmd.WalletUsageLog, settlement)
	cmd.WalletUsageLog.ServiceTier = optionalTrimmedStringPtr(settlementInput.ServiceTier)
	cmd.WalletUsageLog.ImageCount = settlementInput.ImageCount
	cmd.WalletUsageLog.ImageSize = optionalTrimmedStringPtr(settlementInput.ImageSize)
	cmd.WalletUsageLog.VideoCount = settlementInput.VideoCount
	cmd.WalletUsageLog.VideoResolution = optionalTrimmedStringPtr(settlementInput.VideoResolution)
	if settlementInput.VideoCount > 0 {
		cmd.WalletUsageLog.VideoDurationSeconds = &settlementInput.VideoDurationSeconds
	}
	cmd.WalletUsageLog.BillingMode = optionalTrimmedStringPtr(cost.BillingMode)
	if cost.BillingMode != string(BillingModeToken) {
		if settlementInput.VideoCount > 0 {
			cmd.WalletUsageLog.RateMultiplier = snapshot.Multipliers.Video
		} else if settlementInput.ImageCount > 0 {
			cmd.WalletUsageLog.RateMultiplier = snapshot.Multipliers.Image
		}
	}
	source := evidence.Source
	if source != "llm_ws_usage" {
		source = "llm_http_usage"
	}
	cmd.WalletBinding = &WalletBillingBinding{PendingID: h.ID + ":" + token, ParentAuthorizationID: h.ID, AuthorizationToken: token, PlatformUserID: user, BillingSnapshotID: h.SnapshotID, EventID: CanonicalWalletSettlementEventID(h.ID, user, CurrencyUSD), ProviderAccountID: snapshot.AccountID, Source: source, PolicyVersion: WalletImmediateReleasePolicyVersion, FeeUnits: units}
	return b.stageWalletBilling(ctx, cmd)
}

func (b *CanonicalWalletBridge) installImmediateEvidence(h *AuthorizationHandle, user string) {
	if h.AttemptKind != "llm" || b.ImmediateWalletReleaseMode("llm") == "off" {
		return
	}
	h.requiresReaderJournal = b.ImmediateWalletReleaseMode("llm") == "enabled"
	h.stageUsage = func(ctx context.Context, task UsageRecordTask) (UsageRecordTask, error) {
		return b.prepareDurableUsage(ctx, h, task)
	}
	h.sealEvidence = func(ctx context.Context, reason string) error {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		h.mu.Lock()
		var evidence WalletReaderEvidence
		if h.readerEvidence != nil {
			evidence = *h.readerEvidence
		}
		h.mu.Unlock()
		if err := b.transferReaderEvidence(ctx, h, evidence); err != nil {
			var sealed bool
			if b.outboxDB.QueryRowContext(ctx, `SELECT terminal_sealed_at IS NOT NULL FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND authorization_token=$2 AND ordinal=0`, h.ID, h.LastWriteToken()).Scan(&sealed) != nil || !sealed {
				return err
			}
		}
		return b.sealPoolEvidence(ctx, h, reason)
	}
	h.dispatchEvidence = func(ctx context.Context) error { return b.ApplyPendingWalletBilling(ctx, h.ID+":"+h.LastWriteToken()) }
	h.readerStarted = func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		result, err := b.outboxDB.ExecContext(ctx, `UPDATE wallet_authorization_segment SET reader_started=true,evidence_pending=true WHERE parent_authorization_id=$1 AND authorization_token=$2 AND terminal_sealed_at IS NULL AND zero_intent_at IS NULL AND reader_handoff_at IS NULL`, h.ID, h.LastWriteToken())
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return errors.New("wallet reader start is fenced")
		}
		return nil
	}
	h.readerObserved = func(evidence WalletReaderEvidence) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return b.persistReaderEvidence(ctx, h, evidence)
	}
	h.headerObserved = func(status int) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if !walletLegacyZeroStatus(status) {
			return nil
		}
		if h.readerJournal != nil {
			if err := h.readerJournal.updateHeader(status); err != nil {
				return err
			}
		}
		_, err := b.outboxDB.ExecContext(ctx, `UPDATE wallet_authorization_segment SET legacy_zero_candidate=$3 WHERE parent_authorization_id=$1 AND authorization_token=$2 AND terminal_sealed_at IS NULL`, h.ID, h.LastWriteToken(), status)
		h.mu.Lock()
		h.pendingZero = true
		h.mu.Unlock()
		return err
	}
	h.consumeHTTP = func(status int, evidence WalletReaderEvidence, readErr error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		evidence.Source = "llm_http_usage"
		if err := b.transferReaderEvidence(ctx, h, evidence); err != nil {
			h.recordReaderEvidence(evidence, err)
			return
		}
		h.mu.Lock()
		if h.readerEvidence != nil {
			evidence = *h.readerEvidence
		}
		h.mu.Unlock()
		if evidence.ObservedPositive {
			if status >= 400 {
				if id, err := b.stageReaderFee(ctx, h, user, evidence); err == nil {
					if b.sealPoolEvidence(ctx, h, "http_error_usage") == nil {
						_ = b.ApplyPendingWalletBilling(ctx, id)
					}
				}
			}
			return
		}
		if status > 0 && status < 400 {
			return
		} // successful parsers transfer their frozen fee next
		if err := b.sealPoolEvidence(ctx, h, "http_error_reader_joined"); err != nil {
			return
		}
		if walletLegacyZeroStatus(status) {
			if _, err := b.outboxDB.ExecContext(ctx, `UPDATE wallet_authorization_segment SET known_fee_units=0 WHERE parent_authorization_id=$1 AND authorization_token=$2 AND NOT fee_pending`, h.ID, h.LastWriteToken()); err != nil {
				return
			}
			if b.releasePoolAttempt(ctx, h.ID, user, h.Segments, h.LastWriteToken()) == nil {
				h.mu.Lock()
				h.zeroAcknowledged = true
				h.mu.Unlock()
			}
		}
	}
}

func walletLegacyZeroStatus(status int) bool {
	switch status {
	case 400, 401, 403, 404, 413, 415, 422:
		return true
	}
	return false
}

func walletTrimSSEData(line []byte) []byte {
	text := strings.TrimSpace(string(line))
	if strings.HasPrefix(text, "data:") {
		text = strings.TrimSpace(strings.TrimPrefix(text, "data:"))
	}
	return []byte(text)
}

// Recover from the persisted normalized handoff, including a crash between the
// final reader transfer and staging its frozen command. Do not price mutable
// catalog data, incomplete readers, or a caller's raw token/credit estimates.
func (b *CanonicalWalletBridge) recoverReaderBillingEvidence(ctx context.Context) {
	rows, err := b.outboxDB.QueryContext(ctx, `SELECT a.parent_authorization_id,a.platform_user_id,a.billing_snapshot_id,a.authorization_token,a.reader_evidence,COALESCE(a.legacy_zero_candidate,0),a.reader_started,a.reader_owner_id FROM wallet_authorization_segment a WHERE a.kind='llm' AND a.ordinal=0 AND a.authorization_token IS NOT NULL AND a.zero_intent_at IS NULL AND (a.terminal_sealed_at IS NULL OR a.fee_pending) AND a.write_ended_at IS NOT NULL AND (a.reader_handoff_at IS NOT NULL OR (a.legacy_zero_candidate IS NOT NULL AND NOT a.reader_started)) AND NOT EXISTS(SELECT 1 FROM wallet_billing_pending p WHERE p.parent_authorization_id=a.parent_authorization_id) ORDER BY a.updated_at,a.parent_authorization_id LIMIT 32`)
	if err != nil {
		return
	}
	type candidate struct {
		parent, user, snapshot, token string
		raw                           []byte
		status                        int
		started                       bool
		owner                         sql.NullString
	}
	items := []candidate{}
	for rows.Next() {
		var item candidate
		if rows.Scan(&item.parent, &item.user, &item.snapshot, &item.token, &item.raw, &item.status, &item.started, &item.owner) != nil {
			break
		}
		items = append(items, item)
	}
	_ = rows.Close()
	for _, item := range items {
		_, _ = b.outboxDB.ExecContext(ctx, `UPDATE wallet_authorization_segment SET updated_at=now() WHERE parent_authorization_id=$1 AND ordinal=0`, item.parent)
		var evidence WalletReaderEvidence
		if len(item.raw) > 0 && json.Unmarshal(item.raw, &evidence) != nil {
			continue
		}
		segments, readErr := b.authorizationSegments(ctx, item.parent)
		if readErr != nil {
			continue
		}
		h := &AuthorizationHandle{ID: item.parent, SnapshotID: item.snapshot, AttemptKind: "llm", Segments: segments, writes: []AuthorizationWrite{{Token: item.token}}}
		if !item.started {
			// CAS freezes the unread body before a concurrent reader can start.
			// The reader's own start transition checks this durable fence.
			tx, beginErr := b.outboxDB.BeginTx(ctx, nil)
			if beginErr != nil {
				continue
			}
			if _, lockErr := lockWalletAttempt(ctx, tx, item.parent); lockErr != nil {
				_ = tx.Rollback()
				continue
			}
			result, updateErr := tx.ExecContext(ctx, `UPDATE wallet_authorization_segment SET reader_handoff_at=COALESCE(reader_handoff_at,now()),reader_evidence=COALESCE(reader_evidence,'{}'::jsonb),known_fee_units=0,evidence_pending=false WHERE parent_authorization_id=$1 AND authorization_token=$2 AND NOT reader_started AND write_ended_at IS NOT NULL AND terminal_sealed_at IS NULL AND NOT fee_pending AND known_fee_units IS NULL`, item.parent, item.token)
			if updateErr != nil {
				_ = tx.Rollback()
				continue
			}
			n, _ := result.RowsAffected()
			if n != int64(len(segments)) {
				_ = tx.Rollback()
				continue
			}
			if tx.Commit() != nil {
				continue
			}
		}
		// A completed strict zero uses the same frozen parser proof as a positive
		// fee. Missing/malformed usage cannot acquire a zero from its estimate.
		// A WS actor may already have sealed its joined reader after staging failed;
		// that immutable seal does not discard its durable pending fee evidence.
		if walletReaderEvidenceNeedsFeeRecovery(evidence) {
			// A live reader owner normally keeps staging its own fee, so recovery
			// waits. Its process holds the owner lock until it exits, so a barrier left
			// behind by a LIVE process would never be revisited; once the attempt is
			// past its deadline plus grace no staging can still be in flight, so
			// recovery proceeds regardless of the owner.
			if item.owner.Valid && !b.walletReaderOwnerGone(ctx, item.owner.String) && !b.walletBarrierDue(ctx, item.parent, item.token) {
				continue
			}
			if !walletReaderEvidenceTrusted(evidence) {
				// The reader's own evidence can never be priced. Past the deadline plus
				// grace no trusted fee will arrive: release as an unknown cost that the
				// platform bears, all-or-nothing, instead of freezing the hold forever.
				_ = b.releaseExpiredFeeBarrier(ctx, h, item.user, item.raw, evidence)
				continue
			}
			id, stageErr := b.stageReaderFee(ctx, h, item.user, evidence)
			if stageErr != nil {
				if errors.Is(stageErr, errWalletReaderProviderFactsUnavailable) {
					// Counts parsed before provider facts were frozen cannot prove a
					// billable fee. Use the existing guarded unknown disposition once
					// overdue; never reprice them from today's selected account.
					_ = b.releaseExpiredFeeBarrier(ctx, h, item.user, item.raw, evidence)
					continue
				}
				// Provable usage is never released as unknown: keep the barrier and
				// tell an operator once the attempt is overdue.
				if b.walletBarrierDue(ctx, item.parent, item.token) {
					b.alertFeeBarrier("fee_barrier_trusted_fee_unstaged", "wallet trusted reader fee could not be staged; the hold stays until it can", item.parent, "platform_user_id", item.user, "error", stageErr.Error())
				}
				continue
			}
			if b.sealPoolEvidence(ctx, h, "recovered_reader_fee") == nil {
				_ = b.ApplyPendingWalletBilling(ctx, id)
			}
			continue
		}
		if walletLegacyZeroStatus(item.status) {
			// The selected safe-4xx compatibility policy defaults only after the
			// durable owner fence and preserves every positive/pending barrier.
			_, err = b.outboxDB.ExecContext(ctx, `UPDATE wallet_authorization_segment SET known_fee_units=0 WHERE parent_authorization_id=$1 AND authorization_token=$2 AND NOT fee_pending AND reader_handoff_at IS NOT NULL`, item.parent, item.token)
			if err == nil && b.sealPoolEvidence(ctx, h, "recovered_legacy_zero") == nil {
				_ = b.releasePoolAttempt(ctx, item.parent, item.user, segments, item.token)
			}
			continue
		}
		// A barrier set from the gateway's own positive number while the reader saw
		// nothing positive is just as unresolvable once overdue.
		if b.releaseExpiredFeeBarrier(ctx, h, item.user, item.raw, evidence) {
			continue
		}
		_ = b.sealPoolEvidence(ctx, h, "recovered_reader_unknown")
	}
}
