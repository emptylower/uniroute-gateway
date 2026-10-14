package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// PublicCatalogGroupSource is deliberately separate from user access APIs.
// Optional implementations never need a synthetic user or negotiated rates.
type PublicCatalogGroupSource interface {
	GetPublicCatalogGroups(context.Context) ([]Group, error)
}

type PublicModelPrice struct {
	ID                             string  `json:"id"`
	Channel                        string  `json:"channel"`
	Currency                       string  `json:"currency"`
	InputPricePerMillionUSD        string  `json:"input_price_per_million_usd"`
	OutputPricePerMillionUSD       string  `json:"output_price_per_million_usd"`
	CacheReadPricePerMillionUSD    *string `json:"cache_read_price_per_million_usd"`
	CacheWrite5mPricePerMillionUSD *string `json:"cache_write_5m_price_per_million_usd"`
	CacheWrite1hPricePerMillionUSD *string `json:"cache_write_1h_price_per_million_usd"`
	ContextScope                   string  `json:"context_scope"`
	MaxContextTokens               *int    `json:"max_context_tokens"`
	ImageInputPricePerMillionUSD   *string `json:"image_input_price_per_million_usd,omitempty"`
	ImageOutputPricePerMillionUSD  *string `json:"image_output_price_per_million_usd,omitempty"`
}

type PublicModelPriceSnapshot struct {
	Items                   []PublicModelPrice `json:"items"`
	GeneratedAt             time.Time          `json:"generated_at"`
	EffectiveAt             *time.Time         `json:"effective_at"`
	SnapshotVersion         string             `json:"snapshot_version"`
	PriceScope              string             `json:"price_scope"`
	OfficialSource          string             `json:"official_source"`
	OfficialVerifiedAt      time.Time          `json:"official_verified_at"`
	OfficialSnapshotVersion string             `json:"official_snapshot_version"`
}

// NewPublicModelPriceSnapshot hashes only the sorted public prices and their
// scope. Request timestamps and internal routing identity cannot alter it.
func NewPublicModelPriceSnapshot(items []PublicModelPrice, now time.Time) PublicModelPriceSnapshot {
	if items == nil {
		items = []PublicModelPrice{}
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].ID != items[j].ID {
			return items[i].ID < items[j].ID
		}
		return items[i].Channel < items[j].Channel
	})
	snapshot := PublicModelPriceSnapshot{Items: items, GeneratedAt: now.UTC(), PriceScope: "default_retail_standard_tokens"}
	if anthropicOfficialBaselineEnabled() {
		snapshot.OfficialSource = anthropicOfficialPrices.Source
		snapshot.OfficialVerifiedAt = anthropicOfficialPrices.VerifiedAt
		snapshot.OfficialSnapshotVersion = anthropicOfficialPrices.version
	}
	content, _ := json.Marshal(struct {
		Items      []PublicModelPrice `json:"items"`
		PriceScope string             `json:"price_scope"`
	}{snapshot.Items, snapshot.PriceScope})
	hash := sha256.Sum256(content)
	snapshot.SnapshotVersion = "sha256:" + hex.EncodeToString(hash[:])
	return snapshot
}

// PublicPrices projects one coherent lowest-price vector per Claude model and
// generic price band, using only currently schedulable default retail routes.
func (s *ModelCatalogService) PublicPrices(ctx context.Context, now time.Time) (PublicModelPriceSnapshot, error) {
	return s.publicPricesForScope(ctx, now, "claude")
}

// PublicTextPrices uses the same default-retail projection for all text model
// families. It retains the legacy Claude-only endpoint's default behavior.
func (s *ModelCatalogService) PublicTextPrices(ctx context.Context, now time.Time) (PublicModelPriceSnapshot, error) {
	snapshot, err := s.publicPricesForScope(ctx, now, "all_text")
	// The pinned Anthropic source describes Claude only, not other manufacturers.
	snapshot.OfficialSource = ""
	snapshot.OfficialVerifiedAt = time.Time{}
	snapshot.OfficialSnapshotVersion = ""
	return snapshot, err
}

func (s *ModelCatalogService) publicPricesForScope(ctx context.Context, now time.Time, scope string) (PublicModelPriceSnapshot, error) {
	empty := NewPublicModelPriceSnapshot(nil, now)
	if s == nil || s.selector == nil || s.accounts == nil || s.pricing == nil {
		return empty, nil
	}
	source, ok := s.selector.apiKeys.(PublicCatalogGroupSource)
	if !ok {
		return empty, nil
	}
	groups, err := source.GetPublicCatalogGroups(ctx)
	if err != nil {
		return empty, err
	}
	quote, err := s.quoteChannelCostsForGroups(ctx, groups, nil, now, ChannelCostQuote{BaseCurrency: CurrencyUSD, QuoteCurrency: CurrencyUSD, Rate: 1, ExchangeRate: 1}, true, scope == "image_tokens")
	if err != nil {
		return empty, err
	}
	type candidate struct {
		item  PublicModelPrice
		total float64
		tie   string
	}
	selected := make(map[string]candidate)
	for _, group := range quote.Groups {
		multiplier := group.EffectiveMultiplier
		if math.IsNaN(multiplier) || math.IsInf(multiplier, 0) || multiplier < 0 {
			continue
		}
		band := "standard"
		if multiplier < 1 {
			band = "discounted"
		}
		for _, model := range group.Models {
			if (scope == "claude" && !strings.HasPrefix(model.ID, "claude-")) || (scope != "image_tokens" && !isCatalogTextModel(model.ID)) || (scope == "image_tokens" && !isPublicImageTokenModel(model.ID)) || model.publicPricing == nil {
				continue
			}
			pricing := model.publicPricing
			// Zero can be an intentional free token rate. Missing/unbillable resolver
			// data and nonfinite rates are excluded rather than serialized as prices.
			values := []float64{pricing.InputPricePerToken, pricing.OutputPricePerToken, pricing.CacheReadPricePerToken, pricing.CacheCreationPricePerToken, pricing.CacheCreation5mPrice, pricing.CacheCreation1hPrice}
			valid := true
			for _, v := range values {
				if v < 0 || math.IsNaN(v) || math.IsInf(v, 0) || math.IsInf(v*multiplier*modelCatalogPriceScale, 0) {
					valid = false
				}
			}
			if !valid {
				continue
			}
			scaled := func(v float64) string { return publicPriceDecimal(v * multiplier * modelCatalogPriceScale) }
			optional := func(v float64, explicit bool) *string {
				if v == 0 && !explicit {
					return nil
				}
				value := scaled(v)
				return &value
			}
			write5m, write1h := pricing.CacheCreationPricePerToken, pricing.CacheCreationPricePerToken
			writeKnown := pricing.CacheCreationPriceExplicit
			if pricing.SupportsCacheBreakdown && (pricing.CacheCreation5mPrice > 0 || pricing.CacheCreation1hPrice > 0) {
				write5m, write1h = pricing.CacheCreation5mPrice, pricing.CacheCreation1hPrice
				writeKnown = true
			}
			item := PublicModelPrice{
				ID: model.ID, Channel: band, Currency: CurrencyUSD,
				InputPricePerMillionUSD:        scaled(pricing.InputPricePerToken),
				OutputPricePerMillionUSD:       scaled(pricing.OutputPricePerToken),
				CacheReadPricePerMillionUSD:    optional(pricing.CacheReadPricePerToken, model.publicCacheReadKnown),
				CacheWrite5mPricePerMillionUSD: optional(write5m, writeKnown),
				CacheWrite1hPricePerMillionUSD: optional(write1h, writeKnown),
				ContextScope:                   "standard", MaxContextTokens: model.publicMaxContext,
			}
			imageTotal := 0.0
			if scope == "image_tokens" {
				input, output := pricing.ImageInputPricePerToken, pricing.ImageOutputPricePerToken
				if input == 0 {
					input = pricing.InputPricePerToken
				}
				if output == 0 && !pricing.ImageOutputPriceExplicit {
					output = pricing.OutputPricePerToken
				}
				if input < 0 || output < 0 || math.IsNaN(input) || math.IsNaN(output) || math.IsInf((input+output)*multiplier, 0) {
					continue
				}
				imageInput, imageOutput := scaled(input), scaled(output)
				item.ImageInputPricePerMillionUSD = &imageInput
				item.ImageOutputPricePerMillionUSD = &imageOutput
				imageTotal = input + output
			}
			total := (pricing.InputPricePerToken + pricing.OutputPricePerToken + pricing.CacheReadPricePerToken + write5m + write1h + imageTotal) * multiplier
			encoded, _ := json.Marshal(item)
			key := model.ID + "\x00" + band
			next := candidate{item, total, string(encoded)}
			previous, exists := selected[key]
			if !exists || total < previous.total || (total == previous.total && next.tie < previous.tie) {
				selected[key] = next
			}
		}
	}
	items := make([]PublicModelPrice, 0, len(selected))
	for _, item := range selected {
		items = append(items, item.item)
	}
	return NewPublicModelPriceSnapshot(items, now), nil
}

func publicPriceDecimal(value float64) string {
	// Keep decimal strings independent of binary floating-point rendering noise.
	return strings.TrimRight(strings.TrimRight(strconv.FormatFloat(value, 'f', 12, 64), "0"), ".")
}

// Standard context uses the same first positive-token interval as billing.
// The upper bound refers to this standard price scope, not an invented window.
func publicStandardPricing(resolved *ResolvedPricing) (*ModelPricing, *int) {
	if resolved == nil || resolved.Mode != BillingModeToken {
		return nil, nil
	}
	pricing := (&ModelPricingResolver{}).GetIntervalPricing(resolved, 1)
	if pricing == nil {
		return nil, nil
	}
	limit := pricing.MaxInputTokens
	threshold := pricing.LongContextInputThreshold
	if resolved.BasePricing != nil && resolved.BasePricing.LongContextInputThreshold > 0 {
		threshold = resolved.BasePricing.LongContextInputThreshold
	}
	if threshold > 0 && (limit == 0 || threshold < limit) {
		limit = threshold
	}
	if interval := FindMatchingInterval(resolved.Intervals, 1); interval != nil && interval.MaxTokens != nil && *interval.MaxTokens > 0 && (limit == 0 || *interval.MaxTokens < limit) {
		limit = *interval.MaxTokens
	}
	if limit > 0 {
		return pricing, &limit
	}
	return pricing, nil
}

var _ PublicCatalogGroupSource = (*APIKeyService)(nil)

// Resolver overrides can intentionally make a supported cache read free.
func publicCacheReadKnown(resolved *ResolvedPricing) bool {
	if resolved == nil {
		return false
	}
	if interval := FindMatchingInterval(resolved.Intervals, 1); interval != nil {
		return interval.CacheReadPrice != nil
	}
	if len(resolved.Intervals) == 0 && resolved.channelPricing != nil && resolved.channelPricing.CacheReadPrice != nil {
		return true
	}
	return false
}

// Only the four separately supplied token-image catalog IDs are projected.
func isPublicImageTokenModel(model string) bool {
	switch modelCatalogKey(model) {
	case "gpt-image-2", "gpt-image-2-beta", "gpt-image-2.5-flare", "gpt-image-2.5-sunburst":
		return true
	default:
		return false
	}
}
func catalogAccountModelsForPublicScope(account *Account, includeImages bool) []catalogModelSeed {
	if !includeImages {
		return catalogAccountModels(account)
	}
	if account == nil || len(rawDiscoveredModelMapping(account)) == 0 {
		return nil
	}
	seeds := catalogAccountModels(account)
	for _, model := range []string{"gpt-image-2", "gpt-image-2-beta", "gpt-image-2.5-flare", "gpt-image-2.5-sunburst"} {
		if account.IsModelSupported(model) && checkMappedModelAvailability(account, model) == nil {
			seeds = append(seeds, catalogModelSeed{name: model, platform: account.Platform})
		}
	}
	return seeds
}
func (s *ModelCatalogService) PublicImageTokenPrices(ctx context.Context, now time.Time) (PublicModelPriceSnapshot, error) {
	snapshot, err := s.publicPricesForScope(ctx, now, "image_tokens")
	snapshot.OfficialSource = ""
	snapshot.OfficialVerifiedAt = time.Time{}
	snapshot.OfficialSnapshotVersion = ""
	snapshot.PriceScope = "default_retail_image_tokens"
	content, _ := json.Marshal(struct {
		Items []PublicModelPrice `json:"items"`
		Scope string             `json:"price_scope"`
	}{snapshot.Items, snapshot.PriceScope})
	hash := sha256.Sum256(content)
	snapshot.SnapshotVersion = "sha256:" + hex.EncodeToString(hash[:])
	return snapshot, err
}
