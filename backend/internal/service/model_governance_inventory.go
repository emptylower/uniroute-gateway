package service

import (
	"context"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/domain"
)

type InventoryItem struct {
	AccountID               int64  `json:"account_id"`
	GroupID                 *int64 `json:"group_id"`
	ChannelID               *int64 `json:"channel_id"`
	TargetPlatform          string `json:"target_platform"`
	UpstreamModelID         string `json:"upstream_model_id"`
	Classification          string `json:"classification"`
	Requests7d              int64  `json:"requests_7d"`
	Requests30d             int64  `json:"requests_30d"`
	Revenue7dBillingMicros  int64  `json:"revenue_7d_billing_micros"`
	Revenue30dBillingMicros int64  `json:"revenue_30d_billing_micros"`
	BillingCurrency         string `json:"billing_currency"`
	AffectedAPIKeys7d       int64  `json:"affected_api_keys_7d"`
	AffectedAPIKeys30d      int64  `json:"affected_api_keys_30d"`
}

type ModelGovernanceInventoryRepository interface {
	List(ctx context.Context, projector InventoryAccountProjector, cutoff7d, cutoff30d, windowEnd time.Time) ([]InventoryItem, error)
}

type InventoryProjectionInput struct {
	Accounts     []Account
	Candidates   []InventoryRuntimeDimensionCandidate
	Observations []InventoryModelObservation
}

type InventoryModelObservation struct {
	AccountID       int64
	UpstreamModelID string
}

type InventoryRuntimeDimensionCandidate struct {
	AccountID               int64
	GroupID                 *int64
	ChannelID               *int64
	GroupPlatform           string
	TargetPlatform          string
	ForcePlatform           bool
	Ungrouped               bool
	AccountHasGroupBindings bool
	CompositeRoute          *CompositeModelRoute
	Channel                 *InventoryChannelCandidate
}

type InventoryChannelCandidate struct {
	ID                 int64
	BillingModelSource string
	RestrictModels     bool
	ModelMapping       map[string]map[string]string
	Pricing            []InventoryChannelPricingCandidate
}

type InventoryChannelPricingCandidate struct {
	Platform string
	Models   []string
}

type InventoryRuntimeDimensionProjection struct {
	AccountID        int64    `json:"account_id"`
	GroupID          *int64   `json:"group_id"`
	ChannelID        *int64   `json:"channel_id"`
	TargetPlatform   string   `json:"target_platform"`
	UpstreamModelIDs []string `json:"upstream_model_ids"`
}

// InventoryAccountProjector keeps runtime mapping semantics in the service layer
// while allowing the repository to own the consistent database snapshot.
type InventoryAccountProjector func(input InventoryProjectionInput) []InventoryRuntimeDimensionProjection

type ModelGovernanceInventoryService struct {
	repo    ModelGovernanceInventoryRepository
	runMode string
	now     func() time.Time
}

func NewModelGovernanceInventoryService(repo ModelGovernanceInventoryRepository, cfg *config.Config) *ModelGovernanceInventoryService {
	runMode := config.RunModeStandard
	if cfg != nil {
		runMode = config.NormalizeRunMode(cfg.RunMode)
	}
	return &ModelGovernanceInventoryService{repo: repo, runMode: runMode, now: time.Now}
}

func (s *ModelGovernanceInventoryService) List(ctx context.Context) ([]InventoryItem, error) {
	windowEnd := s.now().UTC()
	projector := func(input InventoryProjectionInput) []InventoryRuntimeDimensionProjection {
		return ProjectInventoryRuntimeDimensions(input, s.runMode)
	}
	return s.repo.List(ctx, projector, windowEnd.Add(-7*24*time.Hour), windowEnd.Add(-30*24*time.Hour), windowEnd)
}

// ProjectInventoryRuntimeDimensions approves candidate dimensions using the
// same stable platform eligibility as runtime scheduling.
func ProjectInventoryRuntimeDimensions(input InventoryProjectionInput, runMode string) []InventoryRuntimeDimensionProjection {
	accounts := make(map[int64]*Account, len(input.Accounts))
	observations := make(map[int64][]string)
	for i := range input.Accounts {
		account := &input.Accounts[i]
		accounts[account.ID] = account
	}
	for _, observation := range input.Observations {
		if observation.UpstreamModelID != "" {
			observations[observation.AccountID] = append(observations[observation.AccountID], observation.UpstreamModelID)
		}
	}
	type dimensionKey struct {
		accountID      int64
		groupID        int64
		channelID      int64
		hasGroup       bool
		hasChannel     bool
		targetPlatform string
	}
	routesByGroup := make(map[int64][]CompositeModelRoute)
	for _, candidate := range input.Candidates {
		if candidate.GroupID != nil && candidate.CompositeRoute != nil {
			routesByGroup[*candidate.GroupID] = append(routesByGroup[*candidate.GroupID], *candidate.CompositeRoute)
		}
	}
	projectionIndex := make(map[dimensionKey]int, len(input.Candidates))
	projections := make([]InventoryRuntimeDimensionProjection, 0, len(input.Candidates))
	for _, candidate := range input.Candidates {
		account := accounts[candidate.AccountID]
		if candidate.Ungrouped && candidate.AccountHasGroupBindings && runMode != config.RunModeSimple {
			continue
		}
		if !IsStableRuntimePlatformEligible(account, candidate.TargetPlatform, candidate.ForcePlatform) {
			continue
		}
		key := dimensionKey{accountID: candidate.AccountID, targetPlatform: candidate.TargetPlatform}
		if candidate.GroupID != nil {
			key.groupID, key.hasGroup = *candidate.GroupID, true
		}
		if candidate.ChannelID != nil {
			key.channelID, key.hasChannel = *candidate.ChannelID, true
		}
		candidateModels := projectInventoryCandidateModels(account, candidate, routesByGroup, observations[candidate.AccountID])
		if index, exists := projectionIndex[key]; exists {
			projections[index].UpstreamModelIDs = inventoryUnionModels(projections[index].UpstreamModelIDs, candidateModels)
			continue
		}
		projectionIndex[key] = len(projections)
		projections = append(projections, InventoryRuntimeDimensionProjection{
			AccountID: candidate.AccountID, GroupID: candidate.GroupID, ChannelID: candidate.ChannelID,
			TargetPlatform: candidate.TargetPlatform, UpstreamModelIDs: candidateModels,
		})
	}
	return projections
}

func projectInventoryCandidateModels(account *Account, candidate InventoryRuntimeDimensionCandidate, routesByGroup map[int64][]CompositeModelRoute, observations []string) []string {
	if account == nil {
		return nil
	}
	routes := []CompositeModelRoute(nil)
	if candidate.GroupID != nil {
		routes = routesByGroup[*candidate.GroupID]
	}
	witnesses := inventoryPublicWitnesses(account, candidate, routes, observations)
	seen := make(map[string]struct{})
	for _, witness := range witnesses {
		for _, routed := range inventoryRouteWitnesses(candidate, routes, witness.model) {
			if !IsCompositeEndpointTargetCompatible(routed.endpoint, candidate.TargetPlatform) {
				continue
			}
			if (account.IsBedrock() || account.Platform == PlatformAntigravity) && normalizeCompositeRouteEndpoint(routed.endpoint) == CompositeRouteEndpointCountTokens {
				continue
			}
			for _, endpointModel := range inventoryEndpointModelVariants(account, candidate, routed) {
				for _, channelMapped := range inventoryResolveChannelMappings(candidate.Channel, candidate.TargetPlatform, endpointModel) {
					schedulerModel := endpointModel
					if normalizeCompositeRouteEndpoint(routed.endpoint) == CompositeRouteEndpointGemini {
						schedulerModel = channelMapped
					}
					if !inventoryAccountSupportsRoutedModel(account, schedulerModel) {
						continue
					}
					for _, upstream := range inventoryResolveAccountForwardModels(account, channelMapped, routed.endpoint, candidate.TargetPlatform) {
						if upstream == "" || strings.Contains(upstream, "*") || inventoryChannelRestricts(candidate.Channel, candidate.TargetPlatform, witness.model, channelMapped, upstream) {
							continue
						}
						if account.Platform == PlatformAntigravity && candidate.TargetPlatform == PlatformGemini && strings.HasSuffix(upstream, "-thinking") {
							continue
						}
						if witness.pricingOnly && !inventoryPricingSourceMatches(candidate.Channel, candidate.TargetPlatform, witness.model, channelMapped, upstream) {
							continue
						}
						if witness.observed && !witness.configured && upstream != witness.model {
							continue
						}
						seen[upstream] = struct{}{}
					}
				}
			}
		}
	}
	return inventorySortedModels(seen)
}

func inventoryEndpointModelVariants(account *Account, candidate InventoryRuntimeDimensionCandidate, routed inventoryRoutedWitness) []string {
	endpoint := normalizeCompositeRouteEndpoint(routed.endpoint)
	model := routed.model
	if account.IsGrok() {
		if inventoryHasUnspecifiedGrokVideoEndpoint(candidate, model) {
			if eligible, _ := account.GrokMediaGenerationEligibility(); !eligible {
				return nil
			}
			return inventoryUnionModels(
				[]string{NormalizeGrokMediaModelForEndpoint(GrokMediaEndpointVideosGenerations, model, false)},
				[]string{NormalizeGrokMediaModelForEndpoint(GrokMediaEndpointVideosGenerations, model, true)},
			)
		}
		if endpoint == CompositeRouteEndpointImages {
			if eligible, _ := account.GrokMediaGenerationEligibility(); !eligible {
				return nil
			}
			return inventoryUnionModels(
				[]string{NormalizeGrokMediaModelForEndpoint(GrokMediaEndpointImagesGenerations, model, false)},
				[]string{NormalizeGrokMediaModelForEndpoint(GrokMediaEndpointImagesEdits, model, true)},
			)
		}
		if !accountSupportsOpenAICapabilities(account, OpenAIEndpointCapabilityChatCompletions, "") {
			return nil
		}
		return []string{model}
	}
	if !account.IsOpenAI() {
		return []string{model}
	}
	switch endpoint {
	case CompositeRouteEndpointEmbeddings:
		if !accountSupportsOpenAICapabilities(account, OpenAIEndpointCapabilityEmbeddings, "") {
			return nil
		}
	case CompositeRouteEndpointResponses:
		if !accountSupportsOpenAICapabilities(account, OpenAIEndpointCapabilityResponses, "") {
			return nil
		}
	case CompositeRouteEndpointImages:
		basic := accountSupportsOpenAICapabilities(account, "", OpenAIImagesCapabilityBasic)
		native := accountSupportsOpenAICapabilities(account, "", OpenAIImagesCapabilityNative)
		if !basic && !native {
			return nil
		}
	case CompositeRouteEndpointChatCompletions:
		if !accountSupportsOpenAICapabilities(account, OpenAIEndpointCapabilityChatCompletions, "") {
			return nil
		}
	}
	return []string{model}
}

func inventoryHasUnspecifiedGrokVideoEndpoint(candidate InventoryRuntimeDimensionCandidate, model string) bool {
	if !isGrokVideoGenerationModel(model) {
		return false
	}
	if candidate.CompositeRoute != nil {
		return normalizeCompositeRouteEndpoint(candidate.CompositeRoute.Endpoint) == CompositeRouteEndpointAny
	}
	return candidate.Channel != nil
}

func isGrokVideoGenerationModel(model string) bool {
	model = strings.TrimSpace(model)
	return model == "grok-imagine-video" || model == "grok-imagine-video-1.5"
}

type inventoryPublicWitness struct {
	model       string
	configured  bool
	observed    bool
	pricingOnly bool
}

func inventoryPublicWitnesses(account *Account, candidate InventoryRuntimeDimensionCandidate, routes []CompositeModelRoute, observations []string) []inventoryPublicWitness {
	seen := make(map[string]inventoryPublicWitness)
	add := func(model string, configured, observed bool) {
		model = strings.TrimSpace(model)
		if model != "" && !strings.Contains(model, "*") && openAIProjectionPrintable(model) {
			witness := seen[model]
			witness.model = model
			witness.configured = witness.configured || configured
			witness.observed = witness.observed || observed
			seen[model] = witness
		}
	}
	for requested := range account.GetModelMapping() {
		add(requested, true, false)
	}
	if candidate.GroupID == nil && candidate.Channel == nil && candidate.CompositeRoute == nil {
		for _, requested := range inventoryAccountOnlyRequestWitnesses(account) {
			add(requested, true, false)
		}
	}
	if candidate.Channel != nil {
		mapping := candidate.Channel.ModelMapping[candidate.TargetPlatform]
		for requested := range mapping {
			if strings.HasSuffix(requested, "*") {
				prefix := strings.TrimSuffix(requested, "*")
				if prefix != "" {
					add(inventoryChannelPatternWitness(mapping, requested, prefix), true, false)
				}
				continue
			}
			add(requested, true, false)
		}
	}
	for _, observation := range observations {
		add(observation, false, true)
	}
	if candidate.CompositeRoute != nil {
		route := *candidate.CompositeRoute
		if normalizeCompositeRouteMatchType(route.MatchType) == CompositeRouteMatchExact || strings.TrimSpace(route.UpstreamModel) != "" {
			add(route.PublicModel, true, false)
		}
		for _, witness := range compositeRouteRequestWitnessesFromFacts(account, candidate.Channel, candidate.TargetPlatform, route, routes) {
			add(witness, true, false)
		}
	}
	for _, witness := range inventoryPricingRequestWitnesses(candidate.Channel, candidate.TargetPlatform) {
		witness = strings.TrimSpace(witness)
		if witness == "" || strings.Contains(witness, "*") || !openAIProjectionPrintable(witness) {
			continue
		}
		current := seen[witness]
		current.pricingOnly = current.model == ""
		current.model = witness
		current.configured = true
		seen[witness] = current
	}
	witnesses := make([]inventoryPublicWitness, 0, len(seen))
	for _, witness := range seen {
		witnesses = append(witnesses, witness)
	}
	sort.Slice(witnesses, func(i, j int) bool { return witnesses[i].model < witnesses[j].model })
	return witnesses
}

func inventoryPricingRequestWitnesses(channel *InventoryChannelCandidate, platform string) []string {
	if channel == nil {
		return nil
	}
	concrete := inventoryConcretePricingModels(channel, platform)
	if len(concrete) == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	add := func(model string) {
		model = strings.TrimSpace(model)
		if model != "" && !strings.Contains(model, "*") && openAIProjectionPrintable(model) {
			seen[model] = struct{}{}
		}
	}
	addMappedWitnesses := func(pricingModel string) {
		for _, mapped := range inventoryResolveChannelMappings(channel, platform, pricingModel) {
			if inventoryPricingModelsMatch(pricingModel, mapped) {
				add(pricingModel)
			}
		}
		for requested := range channel.ModelMapping[platform] {
			if strings.Contains(requested, "*") {
				continue
			}
			for _, mapped := range inventoryResolveChannelMappings(channel, platform, requested) {
				if inventoryPricingModelsMatch(pricingModel, mapped) {
					add(requested)
				}
			}
		}
	}
	for _, pricingModel := range concrete {
		switch channel.BillingModelSource {
		case BillingModelSourceRequested:
			add(pricingModel)
		case BillingModelSourceUpstream:
			add(pricingModel)
		case "", BillingModelSourceChannelMapped:
			addMappedWitnesses(pricingModel)
		default:
			addMappedWitnesses(pricingModel)
			add(pricingModel)
		}
	}
	return inventorySortedModels(seen)
}

func inventoryConcretePricingModels(channel *InventoryChannelCandidate, platform string) []string {
	seen := make(map[string]struct{})
	for _, pricing := range channel.Pricing {
		if pricing.Platform != platform {
			continue
		}
		for _, model := range pricing.Models {
			model = strings.TrimSpace(model)
			if model != "" && !strings.Contains(model, "*") && openAIProjectionPrintable(model) {
				seen[model] = struct{}{}
			}
		}
	}
	return inventorySortedModels(seen)
}

func inventoryAccountOnlyRequestWitnesses(account *Account) []string {
	if account == nil {
		return nil
	}
	seen := make(map[string]struct{})
	if account.Platform == PlatformOpenAI && account.AllowsOpenAICompact() && len(account.GetModelMapping()) == 0 {
		rules := compileOpenAIPassthroughProjectionRules(nil, account.GetCompactModelMapping())
		witnesses, _ := openAIPassthroughCompactWitnessesForRules(rules)
		for _, requested := range witnesses {
			seen[requested] = struct{}{}
		}
	}
	if account.IsBedrock() {
		for requested := range domain.DefaultBedrockModelMapping {
			seen[requested] = struct{}{}
		}
	}
	return inventorySortedModels(seen)
}

type inventoryRoutedWitness struct {
	model    string
	endpoint string
}

func inventoryRouteWitnesses(candidate InventoryRuntimeDimensionCandidate, routes []CompositeModelRoute, witness string) []inventoryRoutedWitness {
	if candidate.GroupPlatform != PlatformComposite && candidate.CompositeRoute == nil {
		result := make([]inventoryRoutedWitness, 0, len(compositeRouteWitnessEndpoints(CompositeRouteEndpointAny)))
		for _, endpoint := range compositeRouteWitnessEndpoints(CompositeRouteEndpointAny) {
			result = append(result, inventoryRoutedWitness{model: witness, endpoint: endpoint})
		}
		return result
	}
	endpoints := compositeRouteWitnessEndpoints(CompositeRouteEndpointAny)
	if candidate.CompositeRoute != nil {
		endpoints = compositeRouteWitnessEndpoints(candidate.CompositeRoute.Endpoint)
	}
	result := make([]inventoryRoutedWitness, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if winner, ok := matchCompositeRoute(routes, witness, endpoint); ok {
			if candidate.CompositeRoute == nil || winner.ID != candidate.CompositeRoute.ID {
				continue
			}
			routed := strings.TrimSpace(winner.UpstreamModel)
			if routed == "" {
				routed = witness
			}
			result = append(result, inventoryRoutedWitness{model: routed, endpoint: endpoint})
			continue
		}
		if candidate.CompositeRoute == nil {
			if platform, ok := DetectModelPlatform(witness); ok && platform == candidate.TargetPlatform {
				result = append(result, inventoryRoutedWitness{model: witness, endpoint: endpoint})
			}
		}
	}
	return result
}

// IsCompositeEndpointTargetCompatible mirrors endpoint dispatch gates without
// participating in runtime routing. Inventory uses it to reject impossible
// route witnesses before scheduler/account projection.
func IsCompositeEndpointTargetCompatible(endpoint, platform string) bool {
	switch normalizeCompositeRouteEndpoint(endpoint) {
	case CompositeRouteEndpointCountTokens:
		return platform != PlatformAntigravity
	case CompositeRouteEndpointEmbeddings:
		return platform == PlatformOpenAI
	case CompositeRouteEndpointImages:
		return platform == PlatformOpenAI || platform == PlatformGrok
	case CompositeRouteEndpointGemini:
		return platform == PlatformGemini
	default:
		return true
	}
}

func inventoryResolveChannelMappings(channel *InventoryChannelCandidate, platform, model string) []string {
	if channel == nil {
		return []string{model}
	}
	mapping := channel.ModelMapping[platform]
	modelLower := strings.ToLower(model)
	for pattern, mapped := range mapping {
		if !strings.Contains(pattern, "*") && strings.ToLower(pattern) == modelLower && strings.TrimSpace(mapped) != "" {
			return []string{mapped}
		}
	}
	seen := make(map[string]struct{})
	for pattern, target := range mapping {
		if !strings.HasSuffix(pattern, "*") || strings.TrimSpace(target) == "" {
			continue
		}
		prefix := strings.ToLower(strings.TrimSuffix(pattern, "*"))
		if strings.HasPrefix(modelLower, prefix) {
			seen[target] = struct{}{}
		}
	}
	if len(seen) == 0 {
		return []string{model}
	}
	return inventorySortedModels(seen)
}

func inventoryChannelRestricts(channel *InventoryChannelCandidate, platform, requested, mapped, upstream string) bool {
	if channel == nil || !channel.RestrictModels {
		return false
	}
	models := []string{billingModelForRestriction(channel.BillingModelSource, requested, mapped)}
	if channel.BillingModelSource == BillingModelSourceUpstream {
		models = []string{upstream}
	}
	for _, model := range models {
		if inventoryChannelPricingMatches(channel, platform, model) {
			return false
		}
	}
	return true
}

func inventoryPricingSourceMatches(channel *InventoryChannelCandidate, platform, requested, mapped, upstream string) bool {
	if channel == nil {
		return false
	}
	switch channel.BillingModelSource {
	case BillingModelSourceRequested:
		return inventoryChannelPricingMatches(channel, platform, requested)
	case BillingModelSourceUpstream:
		return inventoryChannelPricingMatches(channel, platform, upstream)
	case "", BillingModelSourceChannelMapped:
		return inventoryChannelPricingMatches(channel, platform, mapped)
	default:
		if channel.RestrictModels {
			return inventoryChannelPricingMatches(channel, platform, mapped)
		}
		return inventoryChannelPricingMatches(channel, platform, mapped) || inventoryChannelPricingMatches(channel, platform, upstream)
	}
}

func inventoryChannelPricingMatches(channel *InventoryChannelCandidate, platform, model string) bool {
	candidate := normalizeChannelPricingModelName(model)
	for _, pricing := range channel.Pricing {
		if pricing.Platform != platform {
			continue
		}
		for _, pattern := range pricing.Models {
			pattern = normalizeChannelPricingModelName(pattern)
			if strings.HasSuffix(pattern, "*") {
				if strings.HasPrefix(candidate, strings.TrimSuffix(pattern, "*")) {
					return true
				}
			} else if candidate == pattern {
				return true
			}
		}
	}
	return false
}

func inventoryPricingModelsMatch(left, right string) bool {
	return normalizeChannelPricingModelName(left) == normalizeChannelPricingModelName(right)
}

func projectCompositeRouteAccountMappings(account *Account, route CompositeModelRoute, routes []CompositeModelRoute, observations []string) []string {
	if account == nil {
		return nil
	}
	requestWitnesses := compositeRouteRequestWitnesses(account, route, routes)
	requestWitnesses = inventoryUnionModels(requestWitnesses, observations)
	seen := make(map[string]struct{})
	for _, requestModel := range requestWitnesses {
		for _, endpoint := range compositeRouteWitnessEndpoints(route.Endpoint) {
			winner, ok := matchCompositeRoute(routes, requestModel, endpoint)
			if !ok || winner.ID != route.ID {
				continue
			}
			routedModel := strings.TrimSpace(route.UpstreamModel)
			if routedModel == "" {
				routedModel = requestModel
			}
			if !inventoryAccountSupportsRoutedModel(account, routedModel) {
				continue
			}
			upstream := inventoryResolveAccountForwardModel(account, routedModel)
			if upstream != "" && !strings.Contains(upstream, "*") {
				seen[upstream] = struct{}{}
			}
		}
	}
	return inventorySortedModels(seen)
}

func compositeRouteRequestWitnesses(account *Account, route CompositeModelRoute, routes []CompositeModelRoute) []string {
	return compositeRouteRequestWitnessesFromFacts(account, nil, route.TargetPlatform, route, routes)
}

func compositeRouteRequestWitnessesFromFacts(account *Account, channel *InventoryChannelCandidate, platform string, route CompositeModelRoute, routes []CompositeModelRoute) []string {
	publicModel := strings.TrimSpace(route.PublicModel)
	if publicModel == "" {
		return nil
	}
	seen := make(map[string]struct{})
	if normalizeCompositeRouteMatchType(route.MatchType) == CompositeRouteMatchExact || strings.TrimSpace(route.UpstreamModel) != "" {
		seen[publicModel] = struct{}{}
	}
	if normalizeCompositeRouteMatchType(route.MatchType) == CompositeRouteMatchPrefix {
		configured := make(map[string]struct{})
		for requested := range account.GetModelMapping() {
			configured[requested] = struct{}{}
		}
		if channel != nil {
			for requested := range channel.ModelMapping[platform] {
				configured[requested] = struct{}{}
			}
		}
		for requested := range configured {
			if requested == "" || requested != strings.TrimSpace(requested) || !openAIProjectionPrintable(requested) {
				continue
			}
			switch strings.Count(requested, "*") {
			case 0:
				if strings.HasPrefix(requested, publicModel) {
					seen[requested] = struct{}{}
				}
			case 1:
				if !strings.HasSuffix(requested, "*") {
					continue
				}
				mappingPrefix := strings.TrimSuffix(requested, "*")
				base := ""
				switch {
				case strings.HasPrefix(mappingPrefix, publicModel):
					base = mappingPrefix
				case strings.HasPrefix(publicModel, mappingPrefix):
					base = publicModel
				}
				for _, endpoint := range compositeRouteWitnessEndpoints(route.Endpoint) {
					if witness := compositeRoutePrefixWitness(route, routes, base, endpoint); witness != "" {
						if channel != nil {
							witness = inventoryChannelPatternWitness(channel.ModelMapping[platform], requested, witness)
						}
						seen[witness] = struct{}{}
					}
				}
			}
		}
	}
	return inventorySortedModels(seen)
}

func inventoryChannelPatternWitness(mapping map[string]string, pattern, base string) string {
	if !strings.HasSuffix(pattern, "*") || base == "" {
		return base
	}
	prefix := strings.ToLower(strings.TrimSuffix(pattern, "*"))
	if !strings.HasPrefix(strings.ToLower(base), prefix) {
		return base
	}
	witness := base
	for {
		if _, exact := mapping[witness]; !exact {
			return witness
		}
		witness += "!"
	}
}

func compositeRoutePrefixWitness(route CompositeModelRoute, routes []CompositeModelRoute, base, endpoint string) string {
	if base == "" || normalizeCompositeRouteMatchType(route.MatchType) != CompositeRouteMatchPrefix {
		return ""
	}
	blockers := make([]openAIProjectionRule, 0, len(routes))
	for _, other := range routes {
		if other.ID == route.ID || !compositeRoutesShareEndpoint(other, endpoint) {
			continue
		}
		otherModel := strings.TrimSpace(other.PublicModel)
		if otherModel == "" || !strings.HasPrefix(otherModel, strings.TrimSpace(route.PublicModel)) {
			continue
		}
		if !compositeRouteOutranks(other, route, endpoint) {
			continue
		}
		blocker := openAIProjectionRule{key: otherModel, prefix: otherModel}
		blocker.exact = normalizeCompositeRouteMatchType(other.MatchType) == CompositeRouteMatchExact
		blockers = append(blockers, blocker)
	}
	comparisons := 0
	witness := openAIProjectionWitnessInPrefixDomain(blockers, base, &comparisons)
	if winner, ok := matchCompositeRoute(routes, witness, endpoint); ok && winner.ID == route.ID {
		return witness
	}
	return ""
}

func compositeRoutesShareEndpoint(route CompositeModelRoute, endpoint string) bool {
	routeEndpoint := normalizeCompositeRouteEndpoint(route.Endpoint)
	return routeEndpoint == CompositeRouteEndpointAny || routeEndpoint == endpoint
}

func compositeRouteOutranks(left, right CompositeModelRoute, endpoint string) bool {
	leftExact := normalizeCompositeRouteMatchType(left.MatchType) == CompositeRouteMatchExact
	rightExact := normalizeCompositeRouteMatchType(right.MatchType) == CompositeRouteMatchExact
	if leftExact != rightExact {
		return leftExact
	}
	leftSpecific := normalizeCompositeRouteEndpoint(left.Endpoint) == endpoint
	rightSpecific := normalizeCompositeRouteEndpoint(right.Endpoint) == endpoint
	if leftSpecific != rightSpecific {
		return leftSpecific
	}
	leftLen := len(strings.TrimSpace(left.PublicModel))
	rightLen := len(strings.TrimSpace(right.PublicModel))
	if leftLen != rightLen {
		return leftLen > rightLen
	}
	if left.Priority != right.Priority {
		return left.Priority < right.Priority
	}
	return left.ID < right.ID
}

func compositeRouteWitnessEndpoints(endpoint string) []string {
	endpoint = normalizeCompositeRouteEndpoint(endpoint)
	if endpoint != CompositeRouteEndpointAny {
		return []string{endpoint}
	}
	return []string{
		CompositeRouteEndpointMessages, CompositeRouteEndpointCountTokens, CompositeRouteEndpointResponses,
		CompositeRouteEndpointChatCompletions, CompositeRouteEndpointEmbeddings, CompositeRouteEndpointImages,
		CompositeRouteEndpointGemini,
	}
}

func inventoryAccountSupportsRoutedModel(account *Account, model string) bool {
	if account.Platform == PlatformAntigravity {
		return mapAntigravityModel(account, model) != ""
	}
	if account.IsBedrock() {
		_, ok := ResolveBedrockModelID(account, model)
		return ok
	}
	if account.Platform == PlatformOpenAI && account.IsOpenAIPassthroughEnabled() {
		return true
	}
	return account.IsModelSupported(model)
}

func inventoryResolveAccountForwardModel(account *Account, model string) string {
	if account.IsBedrock() {
		resolved, _ := ResolveBedrockModelID(account, model)
		return resolved
	}
	if account.Platform == PlatformOpenAI {
		return resolveOpenAIForwardModelForEndpoint(account, model, "", false).UpstreamModel
	}
	if account.Platform == PlatformAnthropic {
		return ResolveAnthropicFinalModel(account, model)
	}
	return resolveAccountUpstreamModel(account, model)
}

func inventoryResolveAccountForwardModels(account *Account, model, endpoint, targetPlatform string) []string {
	normal := inventoryResolveAccountForwardModelForEndpoint(account, model, endpoint)
	if account.Platform == PlatformAntigravity && inventoryEndpointCarriesAntigravityThinking(endpoint, targetPlatform) {
		thinking := applyThinkingModelSuffix(normal, true)
		if thinking != normal && account.IsModelSupported(thinking) {
			return inventoryUnionModels([]string{normal}, []string{thinking})
		}
	}
	if account.Platform != PlatformOpenAI || normalizeCompositeRouteEndpoint(endpoint) != CompositeRouteEndpointResponses || !account.AllowsOpenAICompact() {
		return []string{normal}
	}
	compact := resolveOpenAIForwardModelForEndpoint(account, model, "", true).UpstreamModel
	return inventoryUnionModels([]string{normal}, []string{compact})
}

func inventoryResolveAccountForwardModelForEndpoint(account *Account, model, endpoint string) string {
	if account != nil && account.Platform == PlatformAnthropic && normalizeCompositeRouteEndpoint(endpoint) == CompositeRouteEndpointCountTokens {
		return ResolveAnthropicCountTokensFinalModel(account, model)
	}
	return inventoryResolveAccountForwardModel(account, model)
}

func inventoryEndpointCarriesAntigravityThinking(endpoint, targetPlatform string) bool {
	if targetPlatform == PlatformGemini {
		return false
	}
	switch normalizeCompositeRouteEndpoint(endpoint) {
	case CompositeRouteEndpointMessages, CompositeRouteEndpointResponses, CompositeRouteEndpointChatCompletions:
		return true
	default:
		return false
	}
}

func inventoryUnionModels(left, right []string) []string {
	seen := make(map[string]struct{}, len(left)+len(right))
	for _, model := range left {
		seen[model] = struct{}{}
	}
	for _, model := range right {
		seen[model] = struct{}{}
	}
	return inventorySortedModels(seen)
}

// ProjectInventoryAccountMappings enumerates finite effective upstream IDs only.
// Generic allow-all behavior and wildcard namespaces are intentionally absent;
// wildcard requested keys may still contribute concrete, wildcard-free targets.
func ProjectInventoryAccountMappings(account *Account) InventoryRuntimeDimensionProjection {
	if account == nil {
		return InventoryRuntimeDimensionProjection{}
	}
	seen := make(map[string]struct{})
	add := func(model string) {
		if model == "" || strings.Contains(model, "*") {
			return
		}
		seen[model] = struct{}{}
	}

	mapping := account.GetModelMapping()
	if account.Platform == PlatformOpenAI && account.IsOpenAIPassthroughEnabled() {
		projectOpenAIPassthroughModels(account, mapping, add)
		models := inventorySortedModels(seen)
		return InventoryRuntimeDimensionProjection{AccountID: account.ID, UpstreamModelIDs: models}
	}
	for requested, upstream := range mapping {
		if account.IsBedrock() {
			if resolved, ok := ResolveBedrockModelID(account, requested); ok {
				add(resolved)
				continue
			}
			if resolved, ok := ResolveBedrockModelID(account, upstream); ok {
				add(resolved)
			}
			continue
		}
		if account.Platform == PlatformOpenAI {
			upstream = resolveOpenAIForwardModelForEndpoint(account, requested, "", false).UpstreamModel
		}
		if account.Platform == PlatformAnthropic {
			upstream = ResolveAnthropicFinalModel(account, requested)
		}
		if account.Platform == PlatformAntigravity {
			nonThinking := applyThinkingModelSuffix(upstream, false)
			add(nonThinking)
			thinking := applyThinkingModelSuffix(upstream, true)
			if thinking != nonThinking && account.IsModelSupported(thinking) {
				add(thinking)
			}
			continue
		}
		add(upstream)
	}
	if compactMapping := account.GetCompactModelMapping(); account.AllowsOpenAICompact() && len(compactMapping) > 0 {
		if len(mapping) == 0 {
			for requested := range compactMapping {
				add(resolveOpenAIForwardModelForEndpoint(account, requested, "", true).UpstreamModel)
			}
		} else {
			for requested := range mapping {
				add(resolveOpenAIForwardModelForEndpoint(account, requested, "", true).UpstreamModel)
			}
		}
	}
	if account.IsBedrock() {
		for requested := range domain.DefaultBedrockModelMapping {
			if resolved, ok := ResolveBedrockModelID(account, requested); ok {
				add(resolved)
			}
		}
	}

	models := inventorySortedModels(seen)
	return InventoryRuntimeDimensionProjection{AccountID: account.ID, UpstreamModelIDs: models}
}

func projectOpenAIPassthroughModels(account *Account, mapping map[string]string, add func(string)) {
	rules := compileOpenAIPassthroughProjectionRules(mapping, account.GetCompactModelMapping())
	for _, requested := range rules.normalExact {
		add(resolveOpenAIForwardModelForEndpoint(account, requested, "", false).UpstreamModel)
	}

	if !account.AllowsOpenAICompact() {
		return
	}
	witnesses, _ := openAIPassthroughCompactWitnessesForRules(rules)
	for _, requested := range witnesses {
		add(resolveOpenAIForwardModelForEndpoint(account, requested, "", true).UpstreamModel)
	}
}

type openAIProjectionRule struct {
	key    string
	prefix string
	target string
	exact  bool
}

type openAIPassthroughProjectionRules struct {
	allowAll       bool
	normalExact    []string
	normalPrefixes []string
	compact        []openAIProjectionRule
}

func compileOpenAIPassthroughProjectionRules(normal, compact map[string]string) openAIPassthroughProjectionRules {
	rules := openAIPassthroughProjectionRules{allowAll: len(normal) == 0}
	for key := range normal {
		if key != strings.TrimSpace(key) || !openAIProjectionPrintable(key) {
			continue
		}
		switch strings.Count(key, "*") {
		case 0:
			rules.normalExact = append(rules.normalExact, key)
		case 1:
			if strings.HasSuffix(key, "*") {
				rules.normalPrefixes = append(rules.normalPrefixes, strings.TrimSuffix(key, "*"))
			}
		}
	}
	for key, target := range compact {
		canonicalKey := key == strings.TrimSpace(key) && openAIProjectionPrintable(key)
		target = strings.TrimSpace(target)
		if !canonicalKey || !openAIProjectionPrintable(target) || strings.Contains(target, "*") {
			continue
		}
		rule := openAIProjectionRule{key: key, target: target}
		switch strings.Count(key, "*") {
		case 0:
			rule.exact = true
		case 1:
			if !strings.HasSuffix(key, "*") {
				continue
			}
			rule.prefix = strings.TrimSuffix(key, "*")
		default:
			continue
		}
		rules.compact = append(rules.compact, rule)
	}
	sort.Strings(rules.normalExact)
	rules.normalExact = slices.Compact(rules.normalExact)
	sort.Slice(rules.normalPrefixes, func(i, j int) bool {
		if len(rules.normalPrefixes[i]) != len(rules.normalPrefixes[j]) {
			return len(rules.normalPrefixes[i]) > len(rules.normalPrefixes[j])
		}
		return rules.normalPrefixes[i] < rules.normalPrefixes[j]
	})
	sort.Slice(rules.compact, func(i, j int) bool {
		if rules.compact[i].exact != rules.compact[j].exact {
			return rules.compact[i].exact
		}
		if len(rules.compact[i].prefix) != len(rules.compact[j].prefix) {
			return len(rules.compact[i].prefix) > len(rules.compact[j].prefix)
		}
		return rules.compact[i].key < rules.compact[j].key
	})
	return rules
}

func openAIProjectionPrintable(value string) bool {
	if value == "" || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) || !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}

func openAIPassthroughCompactWitnesses(account *Account) []string {
	witnesses, _ := openAIPassthroughCompactWitnessesWithConstructionComparisons(account)
	return witnesses
}

func openAIPassthroughCompactWitnessesWithConstructionComparisons(account *Account) ([]string, int) {
	if account == nil || !account.AllowsOpenAICompact() {
		return nil, 0
	}
	rules := compileOpenAIPassthroughProjectionRules(account.GetModelMapping(), account.GetCompactModelMapping())
	return openAIPassthroughCompactWitnessesForRules(rules)
}

func openAIPassthroughCompactWitnessesForRules(rules openAIPassthroughProjectionRules) ([]string, int) {
	witnesses := make([]string, 0, len(rules.compact))
	seen := make(map[string]struct{}, len(rules.compact))
	witnessedRules := make(map[int]struct{}, len(rules.compact))
	comparisons := 0
	addWitness := func(ruleIndex int, witness string) {
		if witness == "" {
			return
		}
		if _, exists := witnessedRules[ruleIndex]; exists {
			return
		}
		if _, exists := seen[witness]; exists {
			return
		}
		witnessedRules[ruleIndex] = struct{}{}
		seen[witness] = struct{}{}
		witnesses = append(witnesses, witness)
	}

	for _, requested := range rules.normalExact {
		if winner := openAIProjectionWinningRule(rules.compact, requested, &comparisons); winner >= 0 {
			addWitness(winner, requested)
		}
	}
	for index, rule := range rules.compact {
		if rule.exact {
			if openAIProjectionNormalDomainContains(rules, rule.key) {
				addWitness(index, rule.key)
			}
			continue
		}
		if _, exists := witnessedRules[index]; exists {
			continue
		}
		if rules.allowAll {
			addWitness(index, openAIProjectionWitnessInPrefixDomain(rules.compact[:index], rule.prefix, &comparisons))
			continue
		}
		higherWildcardPrefixes := make(map[string]struct{}, index)
		for _, higher := range rules.compact[:index] {
			comparisons++
			if !higher.exact {
				higherWildcardPrefixes[higher.prefix] = struct{}{}
			}
		}
		base := ""
		for _, normalPrefix := range rules.normalPrefixes {
			comparisons++
			candidateBase := rule.prefix
			switch {
			case strings.HasPrefix(normalPrefix, rule.prefix):
				candidateBase = normalPrefix
			case !strings.HasPrefix(rule.prefix, normalPrefix):
				continue
			}
			if !openAIProjectionHasWildcardAncestor(higherWildcardPrefixes, candidateBase, &comparisons) {
				base = candidateBase
				break
			}
		}
		if base != "" {
			addWitness(index, openAIProjectionWitnessInPrefixDomain(rules.compact[:index], base, &comparisons))
		}
	}
	return witnesses, comparisons
}

func openAIProjectionHasWildcardAncestor(prefixes map[string]struct{}, value string, comparisons *int) bool {
	for index := range value {
		(*comparisons)++
		if _, exists := prefixes[value[:index]]; exists {
			return true
		}
	}
	(*comparisons)++
	_, exists := prefixes[value]
	return exists
}

func openAIProjectionNormalDomainContains(rules openAIPassthroughProjectionRules, requested string) bool {
	if rules.allowAll {
		return true
	}
	index := sort.SearchStrings(rules.normalExact, requested)
	if index < len(rules.normalExact) && rules.normalExact[index] == requested {
		return true
	}
	for _, prefix := range rules.normalPrefixes {
		if strings.HasPrefix(requested, prefix) {
			return true
		}
	}
	return false
}

func openAIProjectionWinningRule(rules []openAIProjectionRule, requested string, comparisons *int) int {
	for index, rule := range rules {
		(*comparisons)++
		if rule.exact {
			if rule.key == requested {
				return index
			}
			continue
		}
		if strings.HasPrefix(requested, rule.prefix) {
			return index
		}
	}
	return -1
}

func openAIProjectionRuleWins(higher []openAIProjectionRule, requested string, comparisons *int) bool {
	for _, blocker := range higher {
		(*comparisons)++
		if blocker.exact {
			if blocker.key == requested {
				return false
			}
			continue
		}
		if strings.HasPrefix(requested, blocker.prefix) {
			return false
		}
	}
	return true
}

func openAIProjectionWitnessInPrefixDomain(higher []openAIProjectionRule, base string, comparisons *int) string {
	if base != "" && openAIProjectionRuleWins(higher, base, comparisons) {
		return base
	}
	blockedNext := make(map[rune]struct{}, len(higher))
	exactBlockers := make(map[string]struct{}, len(higher))
	for _, blocker := range higher {
		(*comparisons)++
		if blocker.exact {
			exactBlockers[blocker.key] = struct{}{}
			continue
		}
		blockerKey := blocker.prefix
		if strings.HasPrefix(base, blockerKey) {
			return ""
		}
		if strings.HasPrefix(blockerKey, base) {
			suffix := strings.TrimPrefix(blockerKey, base)
			if next, _ := utf8.DecodeRuneInString(suffix); next != utf8.RuneError {
				blockedNext[next] = struct{}{}
			}
		}
	}
	for candidateRune := rune('!'); ; candidateRune++ {
		if candidateRune == '*' || candidateRune > unicode.MaxRune || !unicode.IsPrint(candidateRune) || unicode.IsControl(candidateRune) {
			if candidateRune > unicode.MaxRune {
				return ""
			}
			continue
		}
		if _, blocked := blockedNext[candidateRune]; blocked {
			continue
		}
		candidate := base + string(candidateRune)
		for {
			(*comparisons)++
			if _, blocked := exactBlockers[candidate]; !blocked {
				break
			}
			candidate += string(candidateRune)
		}
		if openAIProjectionRuleWins(higher, candidate, comparisons) {
			return candidate
		}
	}
}

func inventorySortedModels(seen map[string]struct{}) []string {
	models := make([]string, 0, len(seen))
	for model := range seen {
		models = append(models, model)
	}
	sort.Strings(models)
	if len(models) == 0 {
		return nil
	}
	return models
}
