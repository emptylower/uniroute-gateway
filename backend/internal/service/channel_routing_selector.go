package service

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
)

var ErrNoChannelRoutingCandidate = infraerrors.ServiceUnavailable(
	"NO_CHANNEL_ROUTING_CANDIDATE",
	"no enabled channel can serve this request",
)

const (
	ChannelRoutingFamilyAnthropic = "anthropic_compatible"
	ChannelRoutingFamilyOpenAI    = "openai_compatible"
)

type channelRoutingFamilyContextKey struct{}

func WithChannelRoutingFamily(ctx context.Context, family string) context.Context {
	if ctx == nil || (family != ChannelRoutingFamilyAnthropic && family != ChannelRoutingFamilyOpenAI) {
		return ctx
	}
	return context.WithValue(ctx, channelRoutingFamilyContextKey{}, family)
}

func ChannelRoutingFamilyFromContext(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	family, ok := ctx.Value(channelRoutingFamilyContextKey{}).(string)
	return family, ok && (family == ChannelRoutingFamilyAnthropic || family == ChannelRoutingFamilyOpenAI)
}

type ChannelRoutingCandidate struct {
	ChannelID           int64
	Group               Group
	EffectiveMultiplier float64
}

func (c ChannelRoutingCandidate) Apply(apiKey *APIKey) *APIKey {
	if apiKey == nil {
		return nil
	}
	copyKey := *apiKey
	groupID := c.Group.ID
	group := c.Group
	copyKey.GroupID = &groupID
	copyKey.Group = &group
	if apiKey.User != nil {
		user := *apiKey.User
		user.UserGroupRPMOverride = nil
		copyKey.User = &user
	}
	return &copyKey
}

type ChannelRoutingCatalog interface {
	ListAvailable(ctx context.Context) ([]AvailableChannel, error)
	IsModelRestricted(ctx context.Context, groupID int64, model string) bool
}

type ChannelRoutingAccess interface {
	GetAvailableGroups(ctx context.Context, userID int64) ([]Group, error)
	GetUserGroupRates(ctx context.Context, userID int64) (map[int64]float64, error)
}

type GroupRoutingPreferences interface {
	GetUserDisabledGroupIDs(ctx context.Context, userID int64) ([]int64, error)
}

type ChannelRoutingSelector struct {
	channels         ChannelRoutingCatalog
	apiKeys          ChannelRoutingAccess
	groupPreferences GroupRoutingPreferences
	cfg              *config.Config
}

func NewChannelRoutingSelector(channels ChannelRoutingCatalog, apiKeys ChannelRoutingAccess, cfg *config.Config) *ChannelRoutingSelector {
	return &ChannelRoutingSelector{channels: channels, apiKeys: apiKeys, cfg: cfg}
}

func ProvideChannelRoutingSelector(channels ChannelRoutingCatalog, apiKeys ChannelRoutingAccess, groupPreferences *ChannelPreferenceService, cfg *config.Config) *ChannelRoutingSelector {
	selector := NewChannelRoutingSelector(channels, apiKeys, cfg)
	selector.groupPreferences = groupPreferences
	return selector
}

func ChannelRoutingFamilyForPlatform(platform string) string {
	switch platform {
	case PlatformOpenAI, PlatformGrok:
		return ChannelRoutingFamilyOpenAI
	default:
		return ChannelRoutingFamilyAnthropic
	}
}

func channelRoutingFamilyForModel(model string) (string, bool) {
	name := strings.ToLower(strings.TrimSpace(model))
	switch {
	case strings.HasPrefix(name, "gpt-"),
		strings.HasPrefix(name, "o1"),
		strings.HasPrefix(name, "o3"),
		strings.HasPrefix(name, "o4"),
		strings.HasPrefix(name, "grok-"),
		strings.HasPrefix(name, "codex-"):
		return ChannelRoutingFamilyOpenAI, true
	case strings.HasPrefix(name, "claude-"), strings.HasPrefix(name, "gemini-"):
		return ChannelRoutingFamilyAnthropic, true
	default:
		return "", false
	}
}

func IsChannelRoutingEndpoint(path string) bool {
	path = "/" + strings.Trim(strings.TrimSpace(path), "/")
	return path == "/responses" || strings.HasPrefix(path, "/responses/") ||
		path == "/v1/responses" || strings.HasPrefix(path, "/v1/responses/") ||
		path == "/backend-api/codex/responses" || strings.HasPrefix(path, "/backend-api/codex/responses/") ||
		path == "/chat/completions" || path == "/v1/chat/completions"
}

func (s *ChannelRoutingSelector) enabled() bool {
	return s != nil && s.cfg != nil && s.cfg.Gateway.ChannelRoutingEnabled
}

func (s *ChannelRoutingSelector) maxCandidates() int {
	if s == nil || s.cfg == nil || s.cfg.Gateway.ChannelRoutingMaxCandidates <= 0 {
		return 3
	}
	return s.cfg.Gateway.ChannelRoutingMaxCandidates
}

// PreferredFamily resolves which compatibility handler should process a
// channel-routed request. Known provider model names avoid an extra catalog
// lookup; custom aliases fall back to the cheapest currently available family.
func (s *ChannelRoutingSelector) PreferredFamily(ctx context.Context, apiKey *APIKey, model string, now time.Time) (string, bool, error) {
	if !s.enabled() || apiKey == nil || !IsChannelRoutingMode(apiKey.RoutingMode) {
		return "", false, nil
	}
	if family, ok := channelRoutingFamilyForModel(model); ok {
		return family, true, nil
	}

	type familyCandidate struct {
		family    string
		candidate ChannelRoutingCandidate
	}
	var best *familyCandidate
	for _, family := range []string{ChannelRoutingFamilyAnthropic, ChannelRoutingFamilyOpenAI} {
		candidates, err := s.Candidates(ctx, apiKey, model, family, now)
		if err != nil {
			if errors.Is(err, ErrNoChannelRoutingCandidate) {
				continue
			}
			return "", false, err
		}
		if len(candidates) == 0 {
			continue
		}
		current := familyCandidate{family: family, candidate: candidates[0]}
		if best == nil || current.candidate.EffectiveMultiplier < best.candidate.EffectiveMultiplier {
			best = &current
		}
	}
	if best == nil {
		return "", false, nil
	}
	return best.family, true, nil
}

func (s *ChannelRoutingSelector) legacy(apiKey *APIKey) ([]ChannelRoutingCandidate, error) {
	if apiKey == nil || apiKey.Group == nil || apiKey.GroupID == nil {
		return nil, ErrNoChannelRoutingCandidate
	}
	rate := apiKey.Group.RateMultiplierForCurrency(channelRoutingCurrency(apiKey, *apiKey.Group))
	return []ChannelRoutingCandidate{{
		Group:               *apiKey.Group,
		EffectiveMultiplier: rate * apiKey.Group.PeakMultiplierAt(time.Now()),
	}}, nil
}

func channelRoutingCurrency(apiKey *APIKey, group Group) string {
	if group.IsSubscriptionType() {
		return CurrencyUSD
	}
	if apiKey != nil && apiKey.User != nil {
		return NormalizeUserBillingCurrency(apiKey.User.BillingCurrency)
	}
	return CurrencyCNY
}

func channelRoutingRate(apiKey *APIKey, group Group, overrides map[int64]float64) float64 {
	rate := group.RateMultiplierForCurrency(channelRoutingCurrency(apiKey, group))
	if override, ok := overrides[group.ID]; ok {
		return override
	}
	return rate
}

func (s *ChannelRoutingSelector) Candidates(ctx context.Context, apiKey *APIKey, model, handlerFamily string, now time.Time) ([]ChannelRoutingCandidate, error) {
	if !s.enabled() || apiKey == nil || !IsChannelRoutingMode(apiKey.RoutingMode) {
		return s.legacy(apiKey)
	}
	automatic := apiKey.RoutingMode == APIKeyRoutingModeAutoChannels
	selected := normalizeChannelIDs(apiKey.ChannelIDs)
	if !automatic && len(selected) == 0 {
		return nil, ErrNoChannelRoutingCandidate
	}
	selectedSet := make(map[int64]struct{}, len(selected))
	for _, id := range selected {
		selectedSet[id] = struct{}{}
	}
	disabledGroupIDs := make(map[int64]struct{})
	if s.groupPreferences != nil {
		disabled, err := s.groupPreferences.GetUserDisabledGroupIDs(ctx, apiKey.UserID)
		if err != nil {
			return nil, err
		}
		for _, groupID := range disabled {
			disabledGroupIDs[groupID] = struct{}{}
		}
	}

	groups, err := s.apiKeys.GetAvailableGroups(ctx, apiKey.UserID)
	if err != nil {
		return nil, err
	}
	groupByID := make(map[int64]Group, len(groups))
	for i := range groups {
		groupByID[groups[i].ID] = groups[i]
	}
	rates, err := s.apiKeys.GetUserGroupRates(ctx, apiKey.UserID)
	if err != nil {
		return nil, err
	}
	if automatic {
		return s.automaticCandidates(ctx, apiKey, groups, rates, disabledGroupIDs, model, handlerFamily, now)
	}
	channels, err := s.channels.ListAvailable(ctx)
	if err != nil {
		return nil, err
	}

	candidates := make([]ChannelRoutingCandidate, 0, len(selected))
	for _, channel := range channels {
		if channel.Status != StatusActive {
			continue
		}
		if _, ok := selectedSet[channel.ID]; !automatic && !ok {
			continue
		}
		for _, ref := range channel.Groups {
			group, ok := groupByID[ref.ID]
			if !ok || ChannelRoutingFamilyForPlatform(group.Platform) != handlerFamily {
				continue
			}
			if _, disabled := disabledGroupIDs[group.ID]; disabled {
				continue
			}
			if group.ClaudeCodeOnly || s.channels.IsModelRestricted(ctx, group.ID, model) {
				continue
			}
			rate := channelRoutingRate(apiKey, group, rates)
			candidates = append(candidates, ChannelRoutingCandidate{
				ChannelID:           channel.ID,
				Group:               group,
				EffectiveMultiplier: rate * group.PeakMultiplierAt(now),
			})
		}
	}
	if len(candidates) == 0 {
		return nil, ErrNoChannelRoutingCandidate
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].EffectiveMultiplier != candidates[j].EffectiveMultiplier {
			return candidates[i].EffectiveMultiplier < candidates[j].EffectiveMultiplier
		}
		if candidates[i].Group.SortOrder != candidates[j].Group.SortOrder {
			return candidates[i].Group.SortOrder < candidates[j].Group.SortOrder
		}
		if candidates[i].ChannelID != candidates[j].ChannelID {
			return candidates[i].ChannelID < candidates[j].ChannelID
		}
		return candidates[i].Group.ID < candidates[j].Group.ID
	})
	if max := s.maxCandidates(); len(candidates) > max {
		candidates = candidates[:max]
	}
	return candidates, nil
}

// automaticCandidates treats every currently accessible routing group as a
// user-facing channel. Channel catalog associations remain optional metadata;
// adding a new active group must not require a second manual attachment before
// auto-routing can discover it.
func (s *ChannelRoutingSelector) automaticCandidates(
	ctx context.Context,
	apiKey *APIKey,
	groups []Group,
	rates map[int64]float64,
	disabledGroupIDs map[int64]struct{},
	model string,
	handlerFamily string,
	now time.Time,
) ([]ChannelRoutingCandidate, error) {
	channelByGroup := make(map[int64]int64)
	if s.channels != nil {
		channels, err := s.channels.ListAvailable(ctx)
		if err != nil {
			return nil, err
		}
		for _, channel := range channels {
			if channel.Status != StatusActive {
				continue
			}
			for _, ref := range channel.Groups {
				if current, exists := channelByGroup[ref.ID]; !exists || channel.ID < current {
					channelByGroup[ref.ID] = channel.ID
				}
			}
		}
	}

	candidates := make([]ChannelRoutingCandidate, 0, len(groups))
	for i := range groups {
		group := groups[i]
		if ChannelRoutingFamilyForPlatform(group.Platform) != handlerFamily {
			continue
		}
		if _, disabled := disabledGroupIDs[group.ID]; disabled {
			continue
		}
		if group.ClaudeCodeOnly || (s.channels != nil && s.channels.IsModelRestricted(ctx, group.ID, model)) {
			continue
		}
		rate := channelRoutingRate(apiKey, group, rates)
		candidates = append(candidates, ChannelRoutingCandidate{
			ChannelID:           channelByGroup[group.ID],
			Group:               group,
			EffectiveMultiplier: rate * group.PeakMultiplierAt(now),
		})
	}
	if len(candidates) == 0 {
		return nil, ErrNoChannelRoutingCandidate
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].EffectiveMultiplier != candidates[j].EffectiveMultiplier {
			return candidates[i].EffectiveMultiplier < candidates[j].EffectiveMultiplier
		}
		if candidates[i].Group.SortOrder != candidates[j].Group.SortOrder {
			return candidates[i].Group.SortOrder < candidates[j].Group.SortOrder
		}
		return candidates[i].Group.ID < candidates[j].Group.ID
	})
	if max := s.maxCandidates(); len(candidates) > max {
		candidates = candidates[:max]
	}
	return candidates, nil
}
