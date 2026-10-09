package service

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strings"

	openaiwsv2 "github.com/Wei-Shaw/sub2api/internal/service/openai_ws_v2"
	"github.com/tidwall/gjson"
)

func (b *CanonicalWalletBridge) validateWalletReaderFee(ctx context.Context, parent, token, snapshotID string, evidence WalletReaderEvidence, proposedUnits int64) error {
	var snapshotRaw, normalizationRaw []byte
	if err := b.outboxDB.QueryRowContext(ctx, `SELECT s.payload,a.reader_fee_normalization FROM wallet_authorization_segment a JOIN wallet_billing_snapshot s ON s.id=a.billing_snapshot_id WHERE a.parent_authorization_id=$1 AND a.authorization_token=$2 AND a.ordinal=0 AND a.billing_snapshot_id=$3`, parent, token, snapshotID).Scan(&snapshotRaw, &normalizationRaw); err != nil {
		return err
	}
	snapshot, err := UnmarshalBillingSnapshotPayload(snapshotRaw)
	if err != nil {
		return err
	}
	var facts WalletReaderNormalization
	if err = json.Unmarshal(normalizationRaw, &facts); err != nil {
		return errors.New("wallet selected fee normalization unavailable")
	}
	input, err := normalizeWalletReaderFee(snapshot, &facts, evidence)
	if err != nil {
		return err
	}
	cost, err := (&BillingService{}).CalculateCostFromSnapshot(snapshot, input)
	if err != nil {
		return err
	}
	units, err := canonicalWalletUnitsFromUSD(cost.ActualCost)
	if err != nil {
		return err
	}
	if units != proposedUnits {
		return errors.New("wallet fee differs from strict frozen reader normalization")
	}
	return nil
}

// Only selected pre-wire policy facts are frozen; request bodies and credentials
// are never retained. Ready=false is durable unresolved positive evidence.
type WalletReaderNormalization struct {
	Version              int           `json:"version"`
	Family               BillingFamily `json:"family"`
	Ready                bool          `json:"ready"`
	TokenOnly            bool          `json:"token_only"`
	ForceCacheBilling    bool          `json:"force_cache_billing"`
	ServiceTier          string        `json:"service_tier,omitempty"`
	CountKind            string        `json:"count_kind,omitempty"`
	ImageSize            string        `json:"image_size,omitempty"`
	VideoResolution      string        `json:"video_resolution,omitempty"`
	VideoDurationSeconds int           `json:"video_duration_seconds,omitempty"`
}

func (h *AuthorizationHandle) captureWalletReaderHTTPNormalization(req *http.Request, payload []byte, known bool) {
	if known && strings.HasPrefix(strings.ToLower(req.Header.Get("Content-Type")), "multipart/") && h.readerCountKind != "" {
		// The existing multipart parser selects only request sizing here; the
		// original uploads, prompt and credentials are never frozen or journaled.
		info := ParseGrokMediaRequest(req.Header.Get("Content-Type"), payload)
		payload, _ = json.Marshal(map[string]any{"size": info.Size, "resolution": info.Resolution, "duration": info.DurationSeconds})
	}
	h.captureWalletReaderNormalization(req.Context(), payload, known)
}

func (h *AuthorizationHandle) captureWalletReaderNormalization(ctx context.Context, payload []byte, payloadKnown bool) {
	if h == nil || h.stageUsage == nil {
		return
	}
	facts := &WalletReaderNormalization{Version: 1, Family: h.readerBillingFamily, TokenOnly: h.readerTokenOnly, CountKind: h.readerCountKind, ForceCacheBilling: IsForceCacheBilling(ctx)}
	if payloadKnown && gjson.ValidBytes(payload) {
		facts.Ready = facts.Family == BillingFamilyGeneric || facts.Family == BillingFamilyOpenAI
		tier := gjson.GetBytes(payload, "service_tier")
		if tier.Exists() && tier.Type != gjson.String {
			facts.Ready = false
		}
		if selected := extractOpenAIServiceTierFromBody(payload); selected != nil {
			facts.ServiceTier = *selected
		}
		for _, tool := range gjson.GetBytes(payload, "tools").Array() {
			switch strings.ToLower(tool.Get("type").String()) {
			case "image_generation":
				facts.TokenOnly = false
				if facts.CountKind == "" {
					facts.CountKind = "openai_images"
				}
			case "web_search", "web_search_preview":
				// Ordinary Responses search tools are billed by their usage;
				// only the internally selected alpha branch charges per call.
			}
		}
		if facts.CountKind != "" {
			var sizeValid bool
			facts.ImageSize, sizeValid = selectedWalletRequestImageSize(payload)
			facts.Ready = facts.Ready && sizeValid
		}
		if facts.CountKind == "grok_video" {
			resolution := gjson.GetBytes(payload, "resolution")
			if resolution.Exists() && resolution.Type != gjson.String {
				facts.Ready = false
			}
			facts.VideoResolution = NormalizeVideoBillingResolutionOrDefault(resolution.String())
			duration := gjson.GetBytes(payload, "duration")
			value, valid := walletUsageInteger(duration)
			facts.Ready = facts.Ready && valid
			facts.VideoDurationSeconds = NormalizeVideoBillingDurationSecondsOrDefault(value)
		}
	}
	h.mu.Lock()
	if h.readerNormalization == nil {
		h.readerNormalization = facts
	}
	h.mu.Unlock()
}

func normalizeWalletReaderFee(snapshot *BillingSnapshot, facts *WalletReaderNormalization, evidence WalletReaderEvidence) (SnapshotSettlementInput, error) {
	if snapshot == nil || facts == nil || facts.Version != 1 || !facts.Ready || facts.Family != snapshot.Family || !walletReaderEvidenceTrusted(evidence) {
		return SnapshotSettlementInput{}, errors.New("wallet reader fee normalization is unresolved")
	}
	input := SnapshotSettlementInput{ServiceTier: facts.ServiceTier}
	if facts.CountKind != "" {
		counts := evidence.Counts
		if counts == nil || counts.Version != 1 || counts.Malformed {
			return SnapshotSettlementInput{}, errors.New("wallet selected count proof is unavailable")
		}
		switch facts.CountKind {
		case "openai_images":
			input.ImageCount = counts.imageCount()
			if input.ImageCount > 0 {
				input.ImageSize = ResolveImageBillingSize(facts.ImageSize, counts.imageSizes()).BillingSize
			}
		case "request", "gemini_image", "alpha_search", "grok_video":
			if !counts.Success || !counts.Complete {
				return SnapshotSettlementInput{}, errors.New("wallet successful request count is unresolved")
			}
			switch facts.CountKind {
			case "request":
				input.ImageCount = counts.imageCount()
				if input.ImageCount > 0 {
					input.ImageSize = ResolveImageBillingSize(facts.ImageSize, counts.imageSizes()).BillingSize
				}
			case "gemini_image":
				input.ImageCount, input.ImageSize = 1, facts.ImageSize
			case "alpha_search":
				input.WebSearchCalls = 1
			case "grok_video":
				input.VideoCount, input.GrokVideo, input.VideoResolution, input.VideoDurationSeconds = 1, true, facts.VideoResolution, facts.VideoDurationSeconds
			}
		default:
			return SnapshotSettlementInput{}, errors.New("wallet selected count branch is unsupported")
		}
	} else if !facts.TokenOnly || snapshot.Pricing.Mode != BillingModeToken {
		return SnapshotSettlementInput{}, errors.New("wallet reader count normalization is unresolved")
	}
	channelToken := snapshot.Pricing.Source == PricingSourceChannel && snapshot.Pricing.Mode == BillingModeToken
	needsTokens := input.WebSearchCalls == 0 && (input.VideoCount <= 0 || !input.GrokVideo || channelToken) && (input.ImageCount <= 0 || channelToken) && snapshot.Pricing.Mode == BillingModeToken
	if !evidence.Present || !evidence.Valid {
		if needsTokens {
			return SnapshotSettlementInput{}, errors.New("wallet reader token proof is unavailable")
		}
		return input, nil
	}
	tokens := evidence.Tokens
	if facts.Family == BillingFamilyOpenAI {
		if evidence.RawInputTokens < tokens.CacheReadTokens || evidence.RawInputTokens-tokens.CacheReadTokens < tokens.CacheCreationTokens {
			return SnapshotSettlementInput{}, errors.New("wallet reader cache buckets exceed frozen input")
		}
		tokens.InputTokens = evidence.RawInputTokens - tokens.CacheReadTokens - tokens.CacheCreationTokens
		if tokens.ImageInputTokens > tokens.InputTokens {
			return SnapshotSettlementInput{}, errors.New("wallet reader image tokens exceed frozen input")
		}
		input.Tokens = tokens
		return input, nil
	}
	if facts.ForceCacheBilling {
		if tokens.InputTokens > math.MaxInt-tokens.CacheReadTokens {
			return SnapshotSettlementInput{}, errors.New("wallet reader cache normalization overflow")
		}
		tokens.CacheReadTokens += tokens.InputTokens
		tokens.InputTokens = 0
	}
	usage := ClaudeUsage{InputTokens: tokens.InputTokens, OutputTokens: tokens.OutputTokens, CacheCreationInputTokens: tokens.CacheCreationTokens, CacheReadInputTokens: tokens.CacheReadTokens, CacheCreation5mTokens: tokens.CacheCreation5mTokens, CacheCreation1hTokens: tokens.CacheCreation1hTokens, ImageOutputTokens: tokens.ImageOutputTokens}
	input.Tokens = snapshotSettlementInputFromClaudeUsage(usage, snapshotCacheTTLOverride(snapshot), 0, "").Tokens
	return input, nil
}

func observeWalletWSUsage(payload []byte, evidence *WalletReaderEvidence) {
	parsed := openaiwsv2.ParseUsage(payload)
	if !parsed.Present {
		return
	}
	evidence.Present = true
	evidence.Valid = parsed.Valid
	evidence.Malformed = evidence.Malformed || parsed.Malformed
	evidence.ObservedPositive = evidence.ObservedPositive || parsed.ObservedPositive
	if !parsed.Valid {
		return
	}
	for _, path := range []string{"response.id", "response_id", "id"} {
		if id := gjson.GetBytes(payload, path).String(); id != "" {
			evidence.ResponseID = id
			break
		}
	}
	if parsed.CacheReadInputTokens > parsed.InputTokens || parsed.CacheCreationInputTokens > parsed.InputTokens-parsed.CacheReadInputTokens {
		evidence.Malformed = true
		evidence.Valid = false
		return
	}
	evidence.RawInputTokens = max(evidence.RawInputTokens, parsed.InputTokens)
	evidence.InputPresent = true
	evidence.OutputPresent = true
	evidence.Tokens.InputTokens = max(evidence.Tokens.InputTokens, parsed.InputTokens-parsed.CacheReadInputTokens-parsed.CacheCreationInputTokens)
	evidence.Tokens.OutputTokens = max(evidence.Tokens.OutputTokens, parsed.OutputTokens)
	evidence.Tokens.CacheReadTokens = max(evidence.Tokens.CacheReadTokens, parsed.CacheReadInputTokens)
	evidence.Tokens.CacheCreationTokens = max(evidence.Tokens.CacheCreationTokens, parsed.CacheCreationInputTokens)
	evidence.Tokens.ImageOutputTokens = max(evidence.Tokens.ImageOutputTokens, parsed.ImageOutputTokens)
}

func (b *CanonicalWalletBridge) walletReaderOwnerGone(ctx context.Context, owner string) bool {
	tx, err := b.outboxDB.BeginTx(ctx, nil)
	if err != nil {
		return false
	}
	defer func() { _ = tx.Rollback() }()
	var gone bool
	if tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, "wallet-reader-owner:"+owner).Scan(&gone) != nil || !gone {
		return false
	}
	return tx.Commit() == nil
}
