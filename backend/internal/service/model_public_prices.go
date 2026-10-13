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
}

type PublicModelPriceSnapshot struct {
	Items           []PublicModelPrice `json:"items"`
	GeneratedAt     time.Time          `json:"generated_at"`
	EffectiveAt     *time.Time         `json:"effective_at"`
	SnapshotVersion string             `json:"snapshot_version"`
	PriceScope      string             `json:"price_scope"`
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
	quote, err := s.quoteChannelCostsForGroups(ctx, groups, nil, now, ChannelCostQuote{BaseCurrency: CurrencyUSD, QuoteCurrency: CurrencyUSD, Rate: 1, ExchangeRate: 1}, true)
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
			if !strings.HasPrefix(model.ID, "claude-") || model.publicPricing == nil {
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
			total := (pricing.InputPricePerToken + pricing.OutputPricePerToken + pricing.CacheReadPricePerToken + write5m + write1h) * multiplier
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
