package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/timezone"
)

// Phase 3.2 — the immutable billing snapshot.
//
// Everything the two settlement paths (recordUsageCore in
// gateway_usage_billing.go and OpenAIGatewayService.RecordUsage in
// openai_gateway_usage.go) read at record time is captured here at the
// freeze point instead: resolved pricing, user×group multiplier and its
// peak/image/video/web-search derivatives, account multiplier and flags,
// group media prices, the pinned FX snapshot, the billing model and its
// candidates, and the model's context window. Settlement in "settle" mode
// reads this and nothing else (see billing_snapshot_settle.go).
//
// The snapshot rides settlement inputs as an explicit field — never
// context.Context (spec §3.5) — and is persisted best-effort from the
// settlement path, not the request path.

const BillingSnapshotVersion = 1

type BillingSnapshotMode string

const (
	BillingSnapshotModeOff    BillingSnapshotMode = "off"
	BillingSnapshotModeRecord BillingSnapshotMode = "record"
	BillingSnapshotModeSettle BillingSnapshotMode = "settle"
)

// BillingFamily selects the long-context policy the settlement path applies.
// Generic (Anthropic/Gemini/Bedrock) applies long-context pricing whenever
// the resolved pricing has no intervals; OpenAI additionally requires the
// account's IsOpenAILongContextBillingEnabled flag.
type BillingFamily string

const (
	BillingFamilyGeneric BillingFamily = "generic"
	BillingFamilyOpenAI  BillingFamily = "openai"
	BillingFamilyLive    BillingFamily = "live"
)

type BillingSnapshotPricing struct {
	Mode                   BillingMode          `json:"mode"`
	Source                 string               `json:"source"`
	Base                   *ModelPricing        `json:"base,omitempty"`
	Intervals              []PricingInterval    `json:"intervals,omitempty"`
	RequestTiers           []PricingInterval    `json:"request_tiers,omitempty"`
	DefaultPerRequestPrice float64              `json:"default_per_request_price"`
	SupportsCacheBreakdown bool                 `json:"supports_cache_breakdown"`
	Channel                *ChannelModelPricing `json:"channel,omitempty"`
	MaxInputTokens         int                  `json:"max_input_tokens"`
	MaxOutputTokens        int                  `json:"max_output_tokens"`
}

type BillingSnapshotMultipliers struct {
	Base      float64   `json:"base"`       // user×group rate multiplier in the multiplier currency, before peak
	Text      float64   `json:"text"`       // Base × peak-hour multiplier at FrozenAt
	Image     float64   `json:"image"`      // resolveImageRateMultiplier(apiKey, Base)
	Video     float64   `json:"video"`      // resolveVideoRateMultiplier(apiKey, Base)
	WebSearch float64   `json:"web_search"` // = Base (openai_gateway_usage.go passes baseMultiplier)
	Account   float64   `json:"account"`    // account.BillingRateMultiplier(); recorded on the usage log, not applied to user cost
	PeakAt    time.Time `json:"peak_at"`    // the instant the peak multiplier was evaluated at
}

type BillingSnapshotMedia struct {
	ImagePrice            *ImagePriceConfig `json:"image_price,omitempty"`
	VideoPrice            *VideoPriceConfig `json:"video_price,omitempty"`
	WebSearchPricePerCall *float64          `json:"web_search_price_per_call,omitempty"`
}

type BillingSnapshotFlags struct {
	SubscriptionBilling       bool   `json:"subscription_billing"`
	BillingCurrency           string `json:"billing_currency"`
	MultiplierCurrency        string `json:"multiplier_currency"`
	LongContextBillingEnabled bool   `json:"long_context_billing_enabled"` // OpenAI account flag; generic families ignore it
	// resolveCacheTTLUsageOverrideTarget returns (target, ok) and ok can be true
	// with an EMPTY target (gateway_upstream_response.go:1325-1336), which the live
	// path applies as the 5m default — so both halves are frozen, never a bare string.
	CacheTTLOverrideEnabled bool    `json:"cache_ttl_override_enabled"`
	CacheTTLOverrideTarget  string  `json:"cache_ttl_override_target,omitempty"`
	LongContextThreshold    int     `json:"long_context_threshold,omitempty"` // Gemini path opts (RecordUsageWithLongContext)
	LongContextMultiplier   float64 `json:"long_context_multiplier,omitempty"`
}

type BillingSnapshot struct {
	ID             string                     `json:"id"`
	Version        int                        `json:"version"`
	FrozenAt       time.Time                  `json:"frozen_at"`
	Family         BillingFamily              `json:"family"`
	UserID         int64                      `json:"user_id"`
	APIKeyID       int64                      `json:"api_key_id"`
	GroupID        *int64                     `json:"group_id,omitempty"`
	AccountID      int64                      `json:"account_id"`
	RequestedModel string                     `json:"requested_model"`
	BillingModel   string                     `json:"billing_model"`
	Candidates     []string                   `json:"candidates"`
	Pricing        BillingSnapshotPricing     `json:"pricing"`
	Multipliers    BillingSnapshotMultipliers `json:"multipliers"`
	Media          BillingSnapshotMedia       `json:"media"`
	FX             ExchangeRateSnapshot       `json:"fx"`
	Flags          BillingSnapshotFlags       `json:"flags"`
}

func (s *BillingSnapshot) MarshalPayload() ([]byte, error) {
	if s == nil {
		return nil, errors.New("nil billing snapshot")
	}
	return json.Marshal(s)
}

func UnmarshalBillingSnapshotPayload(raw []byte) (*BillingSnapshot, error) {
	var s BillingSnapshot
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, err
	}
	if s.Version != BillingSnapshotVersion {
		return nil, fmt.Errorf("unsupported billing snapshot version %d", s.Version)
	}
	return &s, nil
}

// resolvedPricing rebuilds the ResolvedPricing the resolver produced at
// freeze time, so CalculateCostUnified can run with Resolved pre-set and
// never touch the resolver again.
func (s *BillingSnapshot) resolvedPricing() *ResolvedPricing {
	return &ResolvedPricing{
		Mode:                   s.Pricing.Mode,
		BasePricing:            s.Pricing.Base,
		Intervals:              s.Pricing.Intervals,
		RequestTiers:           s.Pricing.RequestTiers,
		DefaultPerRequestPrice: s.Pricing.DefaultPerRequestPrice,
		Source:                 s.Pricing.Source,
		SupportsCacheBreakdown: s.Pricing.SupportsCacheBreakdown,
		channelPricing:         s.Pricing.Channel,
	}
}

// BillingSnapshotStore is implemented by repository.BillingSnapshotStore.
type BillingSnapshotStore interface {
	InsertBillingSnapshot(ctx context.Context, snapshot *BillingSnapshot) error
	GetBillingSnapshot(ctx context.Context, id string) (*BillingSnapshot, error)
}

var ErrBillingSnapshotNotFound = errors.New("billing snapshot not found")

// billingSnapshotMetrics is process-wide. Tests read it through
// BillingSnapshotMetricsSnapshot to prove record-mode drift stays at exactly
// the deliberate one (Task 8) and that an FX outage is counted, not fatal.
var billingSnapshotMetrics struct {
	drift, settled, settleError, persistError, fxUnavailable, modelMismatch, freezeError atomic.Int64
}

type BillingSnapshotMetrics struct {
	Drift, Settled, SettleError, PersistError, FXUnavailable, ModelMismatch, FreezeError int64
}

func BillingSnapshotMetricsSnapshot() BillingSnapshotMetrics {
	m := &billingSnapshotMetrics
	return BillingSnapshotMetrics{
		Drift: m.drift.Load(), Settled: m.settled.Load(), SettleError: m.settleError.Load(),
		PersistError: m.persistError.Load(), FXUnavailable: m.fxUnavailable.Load(), ModelMismatch: m.modelMismatch.Load(), FreezeError: m.freezeError.Load(),
	}
}

type FreezeInput struct {
	APIKey  *APIKey
	User    *User
	Account *Account // required: the freeze point is after account selection
	// BillingAccount is the account whose FLAGS settlement reads — for a shadow
	// account that is the credential account behind it (openai_gateway_usage.go:205-212,
	// resolveCredentialAccount). nil = Account. AccountID always records Account.
	BillingAccount *Account
	Subscription   *UserSubscription
	RequestedModel string
	BillingModel   string // the mapped model the settlement will price (account.GetMappedModel(...) already applied)
	Family         BillingFamily
	// ResolveUserGroupRate is the gateway service's cached resolver
	// (GatewayService/OpenAIGatewayService.ResolveUserGroupRateMultiplier).
	ResolveUserGroupRate func(ctx context.Context, userID, groupID int64, groupDefault float64) float64
	// RefreshGroupMediaPricing mirrors OpenAIGatewayService.apiKeyWithFreshGroupMediaPricing
	// (nil for families that do not refresh).
	RefreshGroupMediaPricing func(ctx context.Context, apiKey *APIKey) *APIKey
	CacheTTLOverride         cacheTTLOverride // (enabled, target) as resolveCacheTTLUsageOverrideTarget returned it
	LongContextThreshold     int              // Gemini only
	LongContextMultiplier    float64          // Gemini only
}

// cacheTTLOverride is the (target, ok) pair of resolveCacheTTLUsageOverrideTarget.
type cacheTTLOverride struct {
	Enabled bool
	Target  string
}

type BillingSnapshotService struct {
	cfg           *config.Config
	resolver      *ModelPricingResolver
	billing       *BillingService
	exchangeRates *ExchangeRateService
	store         BillingSnapshotStore
	now           func() time.Time
}

func NewBillingSnapshotService(cfg *config.Config, resolver *ModelPricingResolver, billing *BillingService, exchangeRates *ExchangeRateService, store BillingSnapshotStore) *BillingSnapshotService {
	return &BillingSnapshotService{cfg: cfg, resolver: resolver, billing: billing, exchangeRates: exchangeRates, store: store, now: func() time.Time { return timezone.Now() }}
}

func (s *BillingSnapshotService) Mode() BillingSnapshotMode {
	if s == nil || s.cfg == nil {
		return BillingSnapshotModeOff
	}
	switch BillingSnapshotMode(strings.TrimSpace(s.cfg.CanonicalWallet.BillingSnapshotMode)) {
	case BillingSnapshotModeRecord:
		return BillingSnapshotModeRecord
	case BillingSnapshotModeSettle:
		return BillingSnapshotModeSettle
	default:
		return BillingSnapshotModeOff
	}
}

// billingSnapshotRandRead is an injection seam so the unit suite can exercise
// the id-generation failure branch (crypto/rand never fails in practice).
var billingSnapshotRandRead = rand.Read

func newBillingSnapshotID() (string, error) {
	var b [16]byte
	if _, err := billingSnapshotRandRead(b[:]); err != nil {
		return "", err
	}
	return "bsnap_" + hex.EncodeToString(b[:]), nil
}

// Freeze validates billability exactly as ensureResolvedModelPricing does
// (same ErrModelPricingUnavailable) and captures every settlement input.
func (s *BillingSnapshotService) Freeze(ctx context.Context, in FreezeInput) (*BillingSnapshot, error) {
	if s == nil || s.resolver == nil || s.billing == nil {
		return nil, errors.New("billing snapshot service is unavailable")
	}
	if in.APIKey == nil || in.User == nil || in.Account == nil {
		return nil, errors.New("billing snapshot freeze requires api key, user and account")
	}
	billingModel := strings.TrimSpace(in.BillingModel)
	if billingModel == "" {
		return nil, fmt.Errorf("%w: model is empty", ErrModelPricingUnavailable)
	}
	apiKey := in.APIKey
	if in.RefreshGroupMediaPricing != nil {
		apiKey = in.RefreshGroupMediaPricing(ctx, apiKey)
	}
	resolved := s.resolver.Resolve(ctx, PricingInput{Model: billingModel, GroupID: apiKey.GroupID})
	if !resolvedPricingIsBillable(resolved) {
		return nil, fmt.Errorf("%w for model: %s", ErrModelPricingUnavailable, billingModel)
	}

	isSubscription := in.Subscription != nil && apiKey.Group != nil && apiKey.Group.IsSubscriptionType()
	multiplierCurrency := CurrencyUSD
	if !isSubscription {
		multiplierCurrency = NormalizeUserBillingCurrency(in.User.BillingCurrency)
	}
	base := 1.0
	if s.cfg != nil {
		base = s.cfg.Default.RateMultiplier
	}
	if apiKey.GroupID != nil && apiKey.Group != nil {
		groupDefault := apiKey.Group.RateMultiplierForCurrency(multiplierCurrency)
		if in.ResolveUserGroupRate != nil {
			base = in.ResolveUserGroupRate(ctx, in.User.ID, *apiKey.GroupID, groupDefault)
		} else {
			base = groupDefault
		}
	}
	now := s.now()
	text, image := computePeakAwareMultipliers(apiKey, base, now)
	video := resolveVideoRateMultiplier(apiKey, base)

	var fx ExchangeRateSnapshot
	if !isSubscription && strings.TrimSpace(in.User.BillingCurrency) != "" {
		currency := NormalizeUserBillingCurrency(in.User.BillingCurrency)
		if pinned, ok := pinnedBillingSettlementSnapshot(ctx, CurrencyUSD, currency); ok {
			fx = pinned
		} else if s.exchangeRates != nil {
			snapshot, err := s.exchangeRates.Snapshot(ctx, CurrencyUSD, currency)
			if err != nil {
				// NON-FATAL. Today FX is resolved only at settlement, after the
				// response is served (gateway_usage_billing.go:796); making it
				// fatal here would turn an FX outage into a request-path 503 /
				// failover loop in record mode. A zero-rate FX pins nothing
				// (SettlementContextFromSnapshot) and settlement uses the live rate.
				billingSnapshotMetrics.fxUnavailable.Add(1)
			} else {
				fx = snapshot
			}
		}
	}

	id, err := newBillingSnapshotID()
	if err != nil {
		return nil, err
	}
	var groupID *int64
	if apiKey.GroupID != nil {
		v := *apiKey.GroupID
		groupID = &v
	}
	var maxIn, maxOut int
	if resolved.BasePricing != nil {
		maxIn, maxOut = resolved.BasePricing.MaxInputTokens, resolved.BasePricing.MaxOutputTokens
	}
	flagsAccount := in.Account
	if in.BillingAccount != nil {
		flagsAccount = in.BillingAccount
	}
	snap := &BillingSnapshot{
		ID: id, Version: BillingSnapshotVersion, FrozenAt: now, Family: in.Family,
		UserID: in.User.ID, APIKeyID: apiKey.ID, GroupID: groupID, AccountID: in.Account.ID,
		RequestedModel: strings.TrimSpace(in.RequestedModel), BillingModel: billingModel,
		Candidates: usageBillingModelCandidates(billingModel, strings.TrimSpace(in.RequestedModel)),
		Pricing: BillingSnapshotPricing{
			Mode: resolved.Mode, Source: resolved.Source, Base: clonePricing(resolved.BasePricing),
			Intervals:              clonePricingIntervals(resolved.Intervals),
			RequestTiers:           clonePricingIntervals(resolved.RequestTiers),
			DefaultPerRequestPrice: resolved.DefaultPerRequestPrice, SupportsCacheBreakdown: resolved.SupportsCacheBreakdown,
			Channel: cloneChannelPricing(resolved.channelPricing), MaxInputTokens: maxIn, MaxOutputTokens: maxOut,
		},
		Multipliers: BillingSnapshotMultipliers{Base: base, Text: text, Image: image, Video: video, WebSearch: base, Account: in.Account.BillingRateMultiplier(), PeakAt: now},
		Media:       mediaPricingFromGroup(apiKey.Group),
		FX:          fx,
		Flags: BillingSnapshotFlags{
			SubscriptionBilling: isSubscription, BillingCurrency: NormalizeUserBillingCurrency(in.User.BillingCurrency), MultiplierCurrency: multiplierCurrency,
			LongContextBillingEnabled: flagsAccount.IsOpenAILongContextBillingEnabled(), CacheTTLOverrideEnabled: in.CacheTTLOverride.Enabled, CacheTTLOverrideTarget: in.CacheTTLOverride.Target,
			LongContextThreshold: in.LongContextThreshold, LongContextMultiplier: in.LongContextMultiplier,
		},
	}
	if snap.Pricing.Mode == "" {
		snap.Pricing.Mode = BillingModeToken
	}
	return snap, nil
}

func clonePricing(p *ModelPricing) *ModelPricing {
	if p == nil {
		return nil
	}
	c := *p
	return &c
}

func cloneFloatPtr(p *float64) *float64 {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

func cloneIntPtr(p *int) *int {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// clonePricingIntervals deep-copies every pointer field of PricingInterval
// (channel.go:104-119: MaxTokens *int and the five *float64 prices — a struct
// copy would share them with the resolver's 10-minute channel cache).
func clonePricingIntervals(in []PricingInterval) []PricingInterval {
	if len(in) == 0 {
		return nil
	}
	out := make([]PricingInterval, len(in))
	for i, iv := range in {
		c := iv
		c.MaxTokens = cloneIntPtr(iv.MaxTokens)
		c.InputPrice = cloneFloatPtr(iv.InputPrice)
		c.OutputPrice = cloneFloatPtr(iv.OutputPrice)
		c.CacheWritePrice = cloneFloatPtr(iv.CacheWritePrice)
		c.CacheReadPrice = cloneFloatPtr(iv.CacheReadPrice)
		c.PerRequestPrice = cloneFloatPtr(iv.PerRequestPrice)
		out[i] = c
	}
	return out
}

// cloneChannelPricing deep-copies ChannelModelPricing (channel.go:85-101):
// the Models slice, every *float64 price, and the intervals.
func cloneChannelPricing(p *ChannelModelPricing) *ChannelModelPricing {
	if p == nil {
		return nil
	}
	c := *p
	c.Models = append([]string(nil), p.Models...)
	c.InputPrice = cloneFloatPtr(p.InputPrice)
	c.OutputPrice = cloneFloatPtr(p.OutputPrice)
	c.CacheWritePrice = cloneFloatPtr(p.CacheWritePrice)
	c.CacheReadPrice = cloneFloatPtr(p.CacheReadPrice)
	c.ImageInputPrice = cloneFloatPtr(p.ImageInputPrice)
	c.ImageOutputPrice = cloneFloatPtr(p.ImageOutputPrice)
	c.PerRequestPrice = cloneFloatPtr(p.PerRequestPrice)
	c.Intervals = clonePricingIntervals(p.Intervals)
	return &c
}

// mediaPricingFromGroup freezes the same group fields the settlement paths
// read through ImagePriceConfig/VideoPriceConfig/webSearchPricePerCallFromAPIKey.
func mediaPricingFromGroup(g *Group) BillingSnapshotMedia {
	if g == nil {
		return BillingSnapshotMedia{}
	}
	return BillingSnapshotMedia{
		ImagePrice:            &ImagePriceConfig{Price1K: cloneFloatPtr(g.ImagePrice1K), Price2K: cloneFloatPtr(g.ImagePrice2K), Price4K: cloneFloatPtr(g.ImagePrice4K)},
		VideoPrice:            &VideoPriceConfig{Price480P: cloneFloatPtr(g.VideoPrice480P), Price720P: cloneFloatPtr(g.VideoPrice720P), Price1080P: cloneFloatPtr(g.VideoPrice1080P)},
		WebSearchPricePerCall: cloneFloatPtr(g.WebSearchPricePerCall),
	}
}

// Persist is best-effort and idempotent (ON CONFLICT DO NOTHING on id).
func (s *BillingSnapshotService) Persist(ctx context.Context, snap *BillingSnapshot) error {
	if s == nil || s.store == nil || snap == nil {
		return nil
	}
	return s.store.InsertBillingSnapshot(ctx, snap)
}

func (s *BillingSnapshotService) Load(ctx context.Context, id string) (*BillingSnapshot, error) {
	if s == nil || s.store == nil {
		return nil, ErrBillingSnapshotNotFound
	}
	return s.store.GetBillingSnapshot(ctx, strings.TrimSpace(id))
}
