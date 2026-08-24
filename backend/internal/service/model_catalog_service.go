package service

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

const (
	modelCatalogPriceScale = 1_000_000
)

type modelCatalogPricingResolver interface {
	Resolve(ctx context.Context, input PricingInput) *ResolvedPricing
}

// TextModelCatalogItem is a channel-neutral model view for end users. Prices
// are effective USD prices per one million tokens for the first route the
// runtime selector would attempt at the supplied time.
type TextModelCatalogItem struct {
	ID                        string    `json:"id"`
	Name                      string    `json:"name"`
	Provider                  string    `json:"provider"`
	Category                  string    `json:"category"`
	Protocols                 []string  `json:"protocols"`
	Available                 bool      `json:"available"`
	AvailableRouteCount       int       `json:"available_route_count"`
	Currency                  string    `json:"currency"`
	InputPricePerMillion      *float64  `json:"input_price_per_million"`
	OutputPricePerMillion     *float64  `json:"output_price_per_million"`
	CacheReadPricePerMillion  *float64  `json:"cache_read_price_per_million"`
	CacheWritePricePerMillion *float64  `json:"cache_write_price_per_million"`
	EffectiveAt               time.Time `json:"effective_at"`
}

// ChannelModelCostItem describes the exact standard token price a user pays
// through one routing group, alongside the supplier reference price. Every
// numeric price is already converted to Currency.
type ChannelModelCostItem struct {
	ID                                string   `json:"id"`
	Name                              string   `json:"name"`
	Provider                          string   `json:"provider"`
	Currency                          string   `json:"currency"`
	InputPricePerMillion              *float64 `json:"input_price_per_million"`
	OutputPricePerMillion             *float64 `json:"output_price_per_million"`
	CacheReadPricePerMillion          *float64 `json:"cache_read_price_per_million"`
	CacheWritePricePerMillion         *float64 `json:"cache_write_price_per_million"`
	OfficialInputPricePerMillion      *float64 `json:"official_input_price_per_million"`
	OfficialOutputPricePerMillion     *float64 `json:"official_output_price_per_million"`
	OfficialCacheReadPricePerMillion  *float64 `json:"official_cache_read_price_per_million"`
	OfficialCacheWritePricePerMillion *float64 `json:"official_cache_write_price_per_million"`
}

// RoutingGroupModelCosts keeps model availability and pricing scoped to the
// actual group used by the gateway. The same account attached to two groups
// therefore produces two independent channel price lists.
type RoutingGroupModelCosts struct {
	GroupID             int64                  `json:"group_id"`
	GroupName           string                 `json:"group_name"`
	Platform            string                 `json:"platform"`
	EffectiveMultiplier float64                `json:"effective_multiplier"`
	Models              []ChannelModelCostItem `json:"models"`
	EffectiveAt         time.Time              `json:"effective_at"`
}

type ChannelCostQuote struct {
	BaseCurrency  string                   `json:"base_currency"`
	QuoteCurrency string                   `json:"quote_currency"`
	ExchangeRate  float64                  `json:"exchange_rate"`
	Rate          float64                  `json:"rate"`
	RateSource    string                   `json:"rate_source"`
	RateAsOf      time.Time                `json:"rate_as_of"`
	RateFetchedAt time.Time                `json:"rate_fetched_at"`
	RateExpiresAt time.Time                `json:"rate_expires_at"`
	RateFallback  bool                     `json:"rate_fallback"`
	Groups        []RoutingGroupModelCosts `json:"groups"`
}

type ModelCatalogService struct {
	channels         ChannelRoutingCatalog
	selector         *ChannelRoutingSelector
	pricing          modelCatalogPricingResolver
	accounts         modelCatalogAccountSource
	fx               *ExchangeRateService
	publicationStore ModelAuthorizationStore
	cfg              *config.Config
}

type modelCatalogAccountSource interface {
	ListSchedulableByGroupID(ctx context.Context, groupID int64) ([]Account, error)
}

func NewModelCatalogService(
	channels *ChannelService,
	selector *ChannelRoutingSelector,
	pricing *ModelPricingResolver,
	accounts AccountRepository,
	fx *ExchangeRateService,
) *ModelCatalogService {
	return &ModelCatalogService{
		channels: channels,
		selector: selector,
		pricing:  pricing,
		accounts: accounts,
		fx:       fx,
	}
}

func (s *ModelCatalogService) SetPublicationStore(store ModelAuthorizationStore) {
	if s != nil {
		s.publicationStore = store
	}
}

func (s *ModelCatalogService) SetConfig(cfg *config.Config) {
	if s != nil {
		s.cfg = cfg
	}
}

func (s *ModelCatalogService) isEnforceMode() bool {
	return s != nil && s.cfg != nil && s.cfg.ModelGovernance.AuthorizationMode == "enforce"
}

func (s *ModelCatalogService) isModelEligibleForChannel(ctx context.Context, channelID int64, canonicalID string) bool {
	if !s.isEnforceMode() || s.publicationStore == nil || channelID == 0 || canonicalID == "" {
		return true
	}
	eligible, err := s.publicationStore.IsChannelModelEligible(ctx, channelID, strings.ToLower(strings.TrimSpace(canonicalID)))
	if err != nil {
		return false
	}
	return eligible
}

func (s *ModelCatalogService) isModelEligibleForAccount(ctx context.Context, accountID, channelID int64, canonicalID string) bool {
	if !s.isEnforceMode() || s.publicationStore == nil || accountID == 0 || channelID == 0 || canonicalID == "" {
		return true
	}
	key := ModelAuthorizationKey{AccountID: accountID, ChannelID: channelID, CanonicalModelID: strings.ToLower(strings.TrimSpace(canonicalID))}
	dec, err := s.publicationStore.Decision(ctx, key)
	if err != nil {
		return false
	}
	return dec.Eligibility == PublicationEligibilityEligible
}

type catalogModelSeed struct {
	name     string
	platform string
}

func modelCatalogKey(model string) string {
	return strings.ToLower(strings.TrimSpace(model))
}

// modelVendorFamily classifies a model id into its vendor family.
// NOTE: the governance catalog's provider_hint is the RETAILER
// (aihubmix/llmgateway/abacus...), not the vendor — vendor truth is derived
// from the model family itself. Returns "" for unknown/custom models.
func modelVendorFamily(model string) string {
	name := modelCatalogKey(model)
	if i := strings.LastIndex(name, "/"); i >= 0 {
		name = name[i+1:] // openrouter-style "vendor/model" prefixes
	}
	switch {
	case strings.HasPrefix(name, "claude-"):
		return "anthropic"
	case strings.HasPrefix(name, "gpt-"), strings.HasPrefix(name, "o1"),
		strings.HasPrefix(name, "o3"), strings.HasPrefix(name, "o4"),
		strings.HasPrefix(name, "codex-"), strings.HasPrefix(name, "chatgpt-"):
		return "openai"
	case strings.HasPrefix(name, "gemini-"):
		return "google"
	case strings.HasPrefix(name, "grok-"):
		return "xai"
	case strings.HasPrefix(name, "deepseek-"):
		return "deepseek"
	case strings.HasPrefix(name, "glm-"):
		return "glm"
	case strings.HasPrefix(name, "kimi-"):
		return "kimi"
	case strings.HasPrefix(name, "qwen"):
		return "qwen"
	case strings.HasPrefix(name, "longcat-"):
		return "longcat"
	case strings.HasPrefix(name, "seed-"):
		return "bytedance"
	case strings.HasPrefix(name, "minimax-"), strings.HasPrefix(name, "mimo-"):
		return "minimax"
	default:
		return ""
	}
}

// vendorPlatform maps a vendor family to the routing platform that serves it.
// Protocol is decoupled from platform: grok models ride the openai wire format
// yet are served exclusively by grok-platform groups. Vendor families without
// a registered platform (deepseek/glm/kimi/...) map to themselves — no groups
// exist for them until an admin registers one.
func vendorPlatform(vendor string) string {
	switch vendor {
	case "anthropic":
		return PlatformAnthropic
	case "openai":
		return PlatformOpenAI
	case "google":
		return PlatformGemini
	case "xai":
		return PlatformGrok
	default:
		return vendor
	}
}

// SchedulableModelsForPlatform filters a fetched upstream model list to the
// families the platform can actually serve. Shared by sync persistence
// (stored mapping) and the admin sync response (what was actually stored).
func SchedulableModelsForPlatform(platform string, models []string) []string {
	out := make([]string, 0, len(models))
	for _, model := range models {
		if groupServesModel(platform, model) {
			out = append(out, model)
		}
	}
	return out
}

// groupServesModel gates a group's catalog/scheduling surface to the model
// families that platform can actually serve. Unknown/custom models (vendor "")
// keep legacy passthrough so admin-curated mappings still work.
func groupServesModel(groupPlatform, model string) bool {
	if groupPlatform == "" || groupPlatform == PlatformComposite {
		return true
	}
	vendor := modelVendorFamily(model)
	if vendor == "" {
		return true
	}
	switch groupPlatform {
	case PlatformAntigravity:
		p := vendorPlatform(vendor)
		return p == PlatformAnthropic || p == PlatformGemini
	case PlatformAnthropic, PlatformOpenAI, PlatformGemini, PlatformGrok,
		PlatformDeepseek, PlatformGLM, PlatformKimi, PlatformQwen,
		PlatformLongcat, PlatformBytedance, PlatformMinimax:
		return groupPlatform == vendorPlatform(vendor)
	default:
		return true // bespoke/future platforms: allow-all until registered as governed
	}
}

func modelCatalogProvider(platform, model string) string {
	if vendor := modelVendorFamily(model); vendor != "" {
		return vendor
	}
	if platform == PlatformGrok {
		// grok platform IS xAI — never emit "grok" as a separate vendor.
		return "xai"
	}
	return platform
}

func isCatalogTextModel(model string) bool {
	name := modelCatalogKey(model)
	if name == "" || strings.Contains(name, "*") {
		return false
	}
	for _, marker := range []string{
		"image", "imagine", "video", "audio", "whisper", "tts", "dall-e", "embedding", "rerank",
	} {
		if strings.Contains(name, marker) {
			return false
		}
	}
	return true
}

func effectiveMillionPrice(perToken, multiplier float64) *float64 {
	if perToken <= 0 {
		return nil
	}
	value := perToken * multiplier * modelCatalogPriceScale
	return &value
}

func catalogPrices(resolved *ResolvedPricing, multiplier float64) (*float64, *float64, *float64, *float64) {
	if resolved == nil || resolved.Mode != BillingModeToken || resolved.BasePricing == nil {
		return nil, nil, nil, nil
	}
	pricing := resolved.BasePricing
	return effectiveMillionPrice(pricing.InputPricePerToken, multiplier),
		effectiveMillionPrice(pricing.OutputPricePerToken, multiplier),
		effectiveMillionPrice(pricing.CacheReadPricePerToken, multiplier),
		effectiveMillionPrice(pricing.CacheCreationPricePerToken, multiplier)
}

func resolvedPricingIsBillable(resolved *ResolvedPricing) bool {
	if resolved == nil {
		return false
	}
	positive := func(value *float64) bool { return value != nil && *value > 0 }
	switch resolved.Mode {
	case BillingModePerRequest, BillingModeImage:
		if resolved.DefaultPerRequestPrice > 0 {
			return true
		}
		for i := range resolved.RequestTiers {
			if positive(resolved.RequestTiers[i].PerRequestPrice) {
				return true
			}
		}
		return false
	default:
		if resolved.BasePricing != nil {
			pricing := resolved.BasePricing
			if pricing.InputPricePerToken > 0 || pricing.OutputPricePerToken > 0 ||
				pricing.CacheCreationPricePerToken > 0 || pricing.CacheReadPricePerToken > 0 ||
				pricing.ImageInputPricePerToken > 0 || pricing.ImageOutputPricePerToken > 0 {
				return true
			}
		}
		for i := range resolved.Intervals {
			interval := resolved.Intervals[i]
			if positive(interval.InputPrice) || positive(interval.OutputPrice) ||
				positive(interval.CacheWritePrice) || positive(interval.CacheReadPrice) {
				return true
			}
		}
		return false
	}
}

func addCatalogModelSeed(seeds map[string]catalogModelSeed, model, platform string) {
	key := modelCatalogKey(model)
	if !isCatalogTextModel(key) {
		return
	}
	if _, exists := seeds[key]; !exists {
		seeds[key] = catalogModelSeed{name: model, platform: platform}
	}
}

// rawDiscoveredModelMapping returns the mapping actually stored by upstream
// model discovery (credentials.model_mapping). GetModelMapping() is NOT safe
// for catalog truth: it silently substitutes hardcoded platform default
// mappings (grok aliases, antigravity passthroughs) when nothing was synced.
func rawDiscoveredModelMapping(account *Account) map[string]any {
	if account == nil {
		return nil
	}
	raw, _ := account.Credentials["model_mapping"].(map[string]any)
	if len(raw) == 0 {
		return nil
	}
	return raw
}

func catalogAccountModels(account *Account) []catalogModelSeed {
	if account == nil {
		return nil
	}
	mapping := rawDiscoveredModelMapping(account)
	if len(mapping) == 0 {
		// Strict truth: an account whose upstream model list was never
		// discovered publishes NOTHING to the catalog. Previously non-openai
		// platforms fell back to hardcoded default lists, surfacing models the
		// upstream does not actually carry (fake catalog entries).
		return nil
	}
	seeds := make(map[string]catalogModelSeed)
	for model := range mapping {
		addCatalogModelSeed(seeds, model, account.Platform)
	}
	// Platform defaults resolve concrete models covered by wildcard mappings
	// (e.g. {"*": ...}); they never apply to unsynced accounts (handled above).
	for _, model := range defaultModelsListCandidateIDs(account.Platform) {
		if account.IsModelSupported(model) {
			addCatalogModelSeed(seeds, model, account.Platform)
		}
	}
	items := make([]catalogModelSeed, 0, len(seeds))
	for _, seed := range seeds {
		items = append(items, seed)
	}
	return items
}

func catalogAccountSupportsModel(account *Account, model string) bool {
	if account == nil {
		return false
	}
	if rawDiscoveredModelMapping(account) == nil {
		// Unsynced account: nothing is published (was: openai-only rule).
		return false
	}
	return account.IsModelSupported(model)
}

// ListChannelCosts returns every currently accessible routing group with the
// models schedulable by that group and the standard per-million-token prices
// used for balance deductions. It intentionally does not apply the user's
// enabled/disabled preference because a disabled channel still needs a visible
// price before the user decides to enable it.
func (s *ModelCatalogService) ListChannelCosts(ctx context.Context, userID int64, now time.Time) ([]RoutingGroupModelCosts, error) {
	quote, err := s.QuoteChannelCosts(ctx, userID, now, CurrencyCNY)
	return quote.Groups, err
}

func (s *ModelCatalogService) QuoteChannelCosts(ctx context.Context, userID int64, now time.Time, currency string) (ChannelCostQuote, error) {
	quoteCurrency, err := NormalizeBillingCurrency(currency)
	if err != nil {
		return ChannelCostQuote{}, err
	}
	if s == nil || s.fx == nil {
		return ChannelCostQuote{}, fmt.Errorf("exchange-rate service is unavailable")
	}
	snapshot, err := s.fx.Snapshot(ctx, CurrencyUSD, quoteCurrency)
	if err != nil {
		return ChannelCostQuote{}, err
	}
	quote := ChannelCostQuote{
		BaseCurrency: CurrencyUSD, QuoteCurrency: quoteCurrency,
		ExchangeRate: snapshot.Rate, Rate: snapshot.Rate, RateSource: snapshot.Source,
		RateAsOf: snapshot.AsOf, RateFetchedAt: snapshot.FetchedAt,
		RateExpiresAt: snapshot.ExpiresAt, RateFallback: snapshot.Fallback,
		Groups: []RoutingGroupModelCosts{},
	}
	if s.selector == nil || s.selector.apiKeys == nil || s.accounts == nil {
		return quote, nil
	}
	groups, err := s.selector.apiKeys.GetAvailableGroups(ctx, userID)
	if err != nil {
		return ChannelCostQuote{}, err
	}
	rates, err := s.selector.apiKeys.GetUserGroupRates(ctx, userID)
	if err != nil {
		return ChannelCostQuote{}, err
	}

	groupPlatform := make(map[int64]string, len(groups))
	for i := range groups {
		groupPlatform[groups[i].ID] = groups[i].Platform
	}
	channelSeeds := make(map[int64]map[string]catalogModelSeed)
	groupChannels := make(map[int64][]int64)
	if s.channels != nil {
		channels, channelErr := s.channels.ListAvailable(ctx)
		if channelErr != nil {
			return ChannelCostQuote{}, channelErr
		}
		for _, channel := range channels {
			if channel.Status != StatusActive {
				continue
			}
			for _, ref := range channel.Groups {
				seeds := channelSeeds[ref.ID]
				if seeds == nil {
					seeds = make(map[string]catalogModelSeed)
					channelSeeds[ref.ID] = seeds
				}
				groupChannels[ref.ID] = append(groupChannels[ref.ID], channel.ID)
				for _, model := range channel.SupportedModels {
					if model.Pricing != nil && model.Pricing.BillingMode != "" && model.Pricing.BillingMode != BillingModeToken {
						continue
					}
					platform := groupPlatform[ref.ID]
					if model.Platform != "" && platform != "" && ChannelRoutingFamilyForPlatform(model.Platform) != ChannelRoutingFamilyForPlatform(platform) {
						continue
					}
					addCatalogModelSeed(seeds, model.Name, model.Platform)
				}
			}
		}
	}

	items := make([]RoutingGroupModelCosts, 0, len(groups))
	for i := range groups {
		group := groups[i]
		if group.Status != StatusActive || group.ClaudeCodeOnly {
			continue
		}
		accounts, accountErr := s.accounts.ListSchedulableByGroupID(ctx, group.ID)
		if accountErr != nil {
			return ChannelCostQuote{}, accountErr
		}
		seeds := make(map[string]catalogModelSeed)
		for key, seed := range channelSeeds[group.ID] {
			seeds[key] = seed
		}
		for j := range accounts {
			for _, seed := range catalogAccountModels(&accounts[j]) {
				addCatalogModelSeed(seeds, seed.name, seed.platform)
			}
		}
		// Family isolation: a group publishes only models its platform can
		// actually serve (the anthropic surface 400s foreign families; grok
		// models belong to grok-platform groups despite the openai wire format).
		for key := range seeds {
			if !groupServesModel(group.Platform, key) {
				delete(seeds, key)
			}
		}

		effectiveMultiplier := group.RateMultiplierForCurrency(quoteCurrency)
		if override, ok := rates[group.ID]; ok {
			effectiveMultiplier = override
		}
		effectiveMultiplier *= group.PeakMultiplierAt(now)
		groupModels := make([]ChannelModelCostItem, 0, len(seeds))
		for key, seed := range seeds {
			if s.channels != nil && s.channels.IsModelRestricted(ctx, group.ID, seed.name) {
				continue
			}
			supported := false
			for j := range accounts {
				if catalogAccountSupportsModel(&accounts[j], seed.name) {
					supported = true
					break
				}
			}
			if !supported {
				continue
			}
			if s.isEnforceMode() && s.publicationStore != nil {
				chs := groupChannels[group.ID]
				if len(chs) > 0 {
					eligible := false
					for _, chID := range chs {
						if s.isModelEligibleForChannel(ctx, chID, seed.name) {
							eligible = true
							break
						}
					}
					if !eligible {
						continue
					}
				}
			}

			routePricing := s.pricing.Resolve(ctx, PricingInput{Model: seed.name, GroupID: &group.ID})
			if routePricing == nil || routePricing.Mode != BillingModeToken || !resolvedPricingIsBillable(routePricing) {
				continue
			}
			officialPricing := s.pricing.Resolve(ctx, PricingInput{Model: seed.name})
			model := ChannelModelCostItem{
				ID:       key,
				Name:     key,
				Provider: modelCatalogProvider(group.Platform, seed.name),
				Currency: quoteCurrency,
			}
			model.InputPricePerMillion,
				model.OutputPricePerMillion,
				model.CacheReadPricePerMillion,
				model.CacheWritePricePerMillion = catalogPrices(routePricing, snapshot.Rate*effectiveMultiplier)
			model.OfficialInputPricePerMillion,
				model.OfficialOutputPricePerMillion,
				model.OfficialCacheReadPricePerMillion,
				model.OfficialCacheWritePricePerMillion = catalogPrices(officialPricing, snapshot.Rate)
			groupModels = append(groupModels, model)
		}
		sort.SliceStable(groupModels, func(i, j int) bool {
			return groupModels[i].ID < groupModels[j].ID
		})
		items = append(items, RoutingGroupModelCosts{
			GroupID:             group.ID,
			GroupName:           group.Name,
			Platform:            group.Platform,
			EffectiveMultiplier: effectiveMultiplier,
			Models:              groupModels,
			EffectiveAt:         now.UTC(),
		})
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].Platform != items[j].Platform {
			return items[i].Platform < items[j].Platform
		}
		if items[i].EffectiveMultiplier != items[j].EffectiveMultiplier {
			return items[i].EffectiveMultiplier < items[j].EffectiveMultiplier
		}
		return items[i].GroupID < items[j].GroupID
	})
	quote.Groups = items
	return quote, nil
}

// ListText returns only concrete text models. Candidate selection is delegated
// to ChannelRoutingSelector so access checks, model restrictions, peak pricing,
// ordering, and the fallback cap stay aligned with the gateway path.
func (s *ModelCatalogService) ListText(ctx context.Context, userID int64, now time.Time) ([]TextModelCatalogItem, error) {
	channels, err := s.channels.ListAvailable(ctx)
	if err != nil {
		return nil, err
	}

	seeds := make(map[string]catalogModelSeed)
	accountsByGroup := make(map[int64][]Account)
	accountsLoaded := make(map[int64]bool)
	loadAccounts := func(groupID int64) ([]Account, error) {
		if s.accounts == nil {
			return nil, nil
		}
		if accountsLoaded[groupID] {
			return accountsByGroup[groupID], nil
		}
		accounts, accountErr := s.accounts.ListSchedulableByGroupID(ctx, groupID)
		if accountErr != nil {
			return nil, accountErr
		}
		accountsLoaded[groupID] = true
		accountsByGroup[groupID] = accounts
		return accounts, nil
	}
	addSeed := func(model, platform string) { addCatalogModelSeed(seeds, model, platform) }
	for _, channel := range channels {
		if channel.Status != StatusActive || len(channel.Groups) == 0 {
			continue
		}
		for _, model := range channel.SupportedModels {
			if model.Pricing != nil && model.Pricing.BillingMode != "" && model.Pricing.BillingMode != BillingModeToken {
				continue
			}
			addSeed(model.Name, model.Platform)
		}

		// Always merge account capabilities for unrestricted routes: explicit
		// pricing for one provider must not hide a newly attached provider from the
		// user catalog. OpenAI contributes only its discovered account snapshot.
		if !channel.RestrictModels && s.accounts != nil {
			for _, group := range channel.Groups {
				accounts, accountErr := loadAccounts(group.ID)
				if accountErr != nil {
					return nil, accountErr
				}
				for i := range accounts {
					for _, seed := range catalogAccountModels(&accounts[i]) {
						addSeed(seed.name, seed.platform)
					}
				}
			}
		}
	}

	items := make([]TextModelCatalogItem, 0, len(seeds))
	for key, seed := range seeds {
		family := ChannelRoutingFamilyForPlatform(seed.platform)
		apiKey := &APIKey{UserID: userID, RoutingMode: APIKeyRoutingModeAutoChannels}
		candidates, candidateErr := s.selector.Candidates(ctx, apiKey, seed.name, family, now)
		if candidateErr != nil && candidateErr != ErrNoChannelRoutingCandidate {
			return nil, candidateErr
		}
		if s.accounts != nil {
			filtered := candidates[:0]
			for _, candidate := range candidates {
				if !groupServesModel(candidate.Group.Platform, seed.name) {
					continue // family isolation: same rule as QuoteChannelCosts
				}
				accounts, accountErr := loadAccounts(candidate.Group.ID)
				if accountErr != nil {
					return nil, accountErr
				}
				for i := range accounts {
					if catalogAccountSupportsModel(&accounts[i], seed.name) {
						filtered = append(filtered, candidate)
						break
					}
				}
			}
			candidates = filtered
		}
		pricedCandidates := candidates[:0]
		for _, candidate := range candidates {
			groupID := candidate.Group.ID
			if resolvedPricingIsBillable(s.pricing.Resolve(ctx, PricingInput{Model: seed.name, GroupID: &groupID})) {
				pricedCandidates = append(pricedCandidates, candidate)
			}
		}
		candidates = pricedCandidates
		if s.isEnforceMode() && s.publicationStore != nil {
			filteredGovernance := candidates[:0]
			for _, c := range candidates {
				if c.ChannelID == 0 {
					filteredGovernance = append(filteredGovernance, c)
					continue
				}
				if s.isModelEligibleForChannel(ctx, c.ChannelID, seed.name) {
					filteredGovernance = append(filteredGovernance, c)
				}
			}
			candidates = filteredGovernance
		}
		item := TextModelCatalogItem{
			ID:                  key,
			Name:                seed.name,
			Provider:            modelCatalogProvider(seed.platform, seed.name),
			Category:            "text",
			Protocols:           []string{"chat_completions", "responses"},
			Available:           len(candidates) > 0,
			AvailableRouteCount: len(candidates),
			Currency:            "USD",
			EffectiveAt:         now.UTC(),
		}
		if len(candidates) > 0 {
			groupID := candidates[0].Group.ID
			resolved := s.pricing.Resolve(ctx, PricingInput{Model: seed.name, GroupID: &groupID})
			item.InputPricePerMillion,
				item.OutputPricePerMillion,
				item.CacheReadPricePerMillion,
				item.CacheWritePricePerMillion = catalogPrices(resolved, candidates[0].EffectiveMultiplier)
		}
		items = append(items, item)
	}

	sort.SliceStable(items, func(i, j int) bool {
		if items[i].Available != items[j].Available {
			return items[i].Available
		}
		if items[i].Provider != items[j].Provider {
			return items[i].Provider < items[j].Provider
		}
		return items[i].ID < items[j].ID
	})
	return items, nil
}

func ensureResolvedModelPricing(resolver *ModelPricingResolver, billing *BillingService, ctx context.Context, apiKey *APIKey, model string) error {
	model = strings.TrimSpace(model)
	if model == "" {
		return fmt.Errorf("%w: model is empty", ErrModelPricingUnavailable)
	}
	if resolver != nil {
		var groupID *int64
		if apiKey != nil {
			groupID = apiKey.GroupID
		}
		if resolvedPricingIsBillable(resolver.Resolve(ctx, PricingInput{Model: model, GroupID: groupID})) {
			return nil
		}
		return fmt.Errorf("%w for model: %s", ErrModelPricingUnavailable, model)
	}
	if billing == nil {
		return fmt.Errorf("%w for model: %s", ErrModelPricingUnavailable, model)
	}
	_, err := billing.GetModelPricing(model)
	return err
}
