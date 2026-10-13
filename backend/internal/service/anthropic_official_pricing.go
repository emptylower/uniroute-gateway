package service

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	modelpricing "github.com/Wei-Shaw/sub2api/resources/model-pricing"
)

type anthropicOfficialModelPrice struct {
	ID              string   `json:"id"`
	Aliases         []string `json:"aliases"`
	Input           float64  `json:"input_per_million_usd"`
	Output          float64  `json:"output_per_million_usd"`
	CacheRead       float64  `json:"cache_read_per_million_usd"`
	CacheWrite5m    float64  `json:"cache_write_5m_per_million_usd"`
	CacheWrite1h    float64  `json:"cache_write_1h_per_million_usd"`
	ContextPolicy   string   `json:"context_policy"`
	MaxInputTokens  int      `json:"max_input_tokens"`
	MaxOutputTokens int      `json:"max_output_tokens"`
}

type anthropicOfficialPriceSnapshot struct {
	Source     string                        `json:"source"`
	VerifiedAt time.Time                     `json:"verified_at"`
	Models     []anthropicOfficialModelPrice `json:"models"`
	version    string
	byID       map[string]anthropicOfficialModelPrice
}

var anthropicOfficialPrices = loadAnthropicOfficialPrices()

func loadAnthropicOfficialPrices() anthropicOfficialPriceSnapshot {
	raw := modelpricing.AnthropicOfficialSnapshot()
	var snapshot anthropicOfficialPriceSnapshot
	if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
		panic("invalid embedded Anthropic official price snapshot: " + err.Error())
	}
	hash := sha256.Sum256([]byte(raw))
	snapshot.version = "sha256:" + hex.EncodeToString(hash[:])
	snapshot.byID = make(map[string]anthropicOfficialModelPrice)
	for _, model := range snapshot.Models {
		snapshot.byID[model.ID] = model
		for _, alias := range model.Aliases {
			snapshot.byID[alias] = model
		}
	}
	return snapshot
}

// officialAnthropicBasePricing updates the shared standard baseline before
// any channel/interval overrides. Exact verified IDs avoid guessing prices for
// future or vendor-specific model variants. Loaded rows and frozen snapshots
// remain immutable; priority/image rates are outside this evidence's scope.
func officialAnthropicBasePricing(model string, loaded *ModelPricing) (*ModelPricing, bool) {
	official, ok := anthropicOfficialPrices.byID[strings.ToLower(strings.TrimSpace(model))]
	if !ok {
		return loaded, false
	}
	var pricing ModelPricing
	if loaded != nil {
		pricing = *loaded
	}
	pricing.PreservedPriorityPricing = clonePriorityPricingPolicy(pricing.PreservedPriorityPricing)
	if loaded != nil && usePriorityServiceTierPricing("priority", loaded) && pricing.PreservedPriorityPricing == nil {
		effective := func(priority, standard float64) float64 {
			if priority > 0 {
				return priority
			}
			return standard
		}
		pricing.PreservedPriorityPricing = &PriorityPricingPolicy{
			InputPricePerToken:         effective(loaded.InputPricePerTokenPriority, loaded.InputPricePerToken),
			OutputPricePerToken:        effective(loaded.OutputPricePerTokenPriority, loaded.OutputPricePerToken),
			CacheReadPricePerToken:     effective(loaded.CacheReadPricePerTokenPriority, loaded.CacheReadPricePerToken),
			CacheCreationPricePerToken: effective(loaded.CacheCreationPricePerTokenPriority, loaded.CacheCreationPricePerToken),
			SupportsCacheBreakdown:     loaded.SupportsCacheBreakdown,
			CacheCreation5mPrice:       loaded.CacheCreation5mPrice, CacheCreation1hPrice: loaded.CacheCreation1hPrice,
			LongContextInputThreshold:  loaded.LongContextInputThreshold,
			LongContextInputMultiplier: loaded.LongContextInputMultiplier, LongContextOutputMultiplier: loaded.LongContextOutputMultiplier,
		}
	}

	pricing.InputPricePerToken = official.Input / modelCatalogPriceScale
	pricing.OutputPricePerToken = official.Output / modelCatalogPriceScale
	pricing.CacheReadPricePerToken = official.CacheRead / modelCatalogPriceScale
	pricing.CacheCreationPricePerToken = official.CacheWrite5m / modelCatalogPriceScale
	pricing.CacheCreation5mPrice = official.CacheWrite5m / modelCatalogPriceScale
	pricing.CacheCreation1hPrice = official.CacheWrite1h / modelCatalogPriceScale
	pricing.SupportsCacheBreakdown = true
	pricing.CacheCreationPriceExplicit = false
	switch official.ContextPolicy {
	case "no_premium":
		pricing.LongContextInputThreshold = 0
		pricing.LongContextInputMultiplier = 0
		pricing.LongContextOutputMultiplier = 0
	case "prompt_over_100k_5x":
		pricing.LongContextInputThreshold = 100000
		pricing.LongContextInputMultiplier = 5
		pricing.LongContextOutputMultiplier = 5
	}
	if official.MaxInputTokens > 0 {
		pricing.MaxInputTokens = official.MaxInputTokens
	}
	if official.MaxOutputTokens > 0 {
		pricing.MaxOutputTokens = official.MaxOutputTokens
	}
	return &pricing, true
}
