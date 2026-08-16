package service

import (
	"context"
	"fmt"
	"sort"
	"time"
)

type ChannelPreferenceRepository interface {
	GetAPIKeyChannelIDs(ctx context.Context, apiKeyID, userID int64) ([]int64, error)
	ReplaceAPIKeyChannels(ctx context.Context, apiKeyID, userID, anchorGroupID int64, channelIDs []int64) (string, error)
	GetUserDefaultChannelIDs(ctx context.Context, userID int64) ([]int64, error)
	ReplaceUserDefaultChannels(ctx context.Context, userID int64, channelIDs []int64) error
	GetUserDisabledGroupIDs(ctx context.Context, userID int64) ([]int64, error)
	ReplaceUserDisabledGroups(ctx context.Context, userID int64, groupIDs []int64) error
}

type ChannelPreferences struct {
	RoutingMode string  `json:"routing_mode"`
	ChannelIDs  []int64 `json:"channel_ids"`
	GroupID     *int64  `json:"group_id,omitempty"`
}

type ChannelPreferenceAPIKeys interface {
	GetAvailableGroups(ctx context.Context, userID int64) ([]Group, error)
	GetUserGroupRates(ctx context.Context, userID int64) (map[int64]float64, error)
	Create(ctx context.Context, userID int64, req CreateAPIKeyRequest) (*APIKey, error)
	Delete(ctx context.Context, id, userID int64) error
	GetByID(ctx context.Context, id int64) (*APIKey, error)
}

type ChannelPreferenceCatalog interface {
	ListAvailable(ctx context.Context) ([]AvailableChannel, error)
}

type ChannelPreferenceService struct {
	repo        ChannelPreferenceRepository
	apiKeys     ChannelPreferenceAPIKeys
	channels    ChannelPreferenceCatalog
	invalidator APIKeyAuthCacheInvalidator
}

func NewChannelPreferenceService(
	repo ChannelPreferenceRepository,
	apiKeys ChannelPreferenceAPIKeys,
	channels ChannelPreferenceCatalog,
	invalidator APIKeyAuthCacheInvalidator,
) *ChannelPreferenceService {
	return &ChannelPreferenceService{
		repo: repo, apiKeys: apiKeys, channels: channels, invalidator: invalidator,
	}
}

func normalizeChannelIDs(ids []int64) []int64 {
	seen := make(map[int64]struct{}, len(ids))
	out := make([]int64, 0, len(ids))
	for _, id := range ids {
		if id <= 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func normalizeGroupIDs(ids []int64) []int64 {
	return normalizeChannelIDs(ids)
}

type channelAnchorCandidate struct {
	channelID int64
	group     Group
	rate      float64
}

func (s *ChannelPreferenceService) collectAnchorCandidates(ctx context.Context, userID int64, requested map[int64]struct{}, now time.Time) ([]channelAnchorCandidate, map[int64]struct{}, error) {
	groups, err := s.apiKeys.GetAvailableGroups(ctx, userID)
	if err != nil {
		return nil, nil, err
	}
	groupByID := make(map[int64]Group, len(groups))
	for i := range groups {
		groupByID[groups[i].ID] = groups[i]
	}
	userRates, err := s.apiKeys.GetUserGroupRates(ctx, userID)
	if err != nil {
		return nil, nil, err
	}

	available, err := s.channels.ListAvailable(ctx)
	if err != nil {
		return nil, nil, err
	}
	visible := make(map[int64]struct{})
	candidates := make([]channelAnchorCandidate, 0)
	for _, ch := range available {
		if ch.Status != StatusActive {
			continue
		}
		if _, ok := requested[ch.ID]; requested != nil && !ok {
			continue
		}
		for _, ref := range ch.Groups {
			group, ok := groupByID[ref.ID]
			if !ok {
				continue
			}
			visible[ch.ID] = struct{}{}
			rate := group.RateMultiplier
			if override, ok := userRates[group.ID]; ok {
				rate = override
			}
			candidates = append(candidates, channelAnchorCandidate{
				channelID: ch.ID,
				group:     group,
				rate:      rate * group.PeakMultiplierAt(now),
			})
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].rate != candidates[j].rate {
			return candidates[i].rate < candidates[j].rate
		}
		if candidates[i].group.SortOrder != candidates[j].group.SortOrder {
			return candidates[i].group.SortOrder < candidates[j].group.SortOrder
		}
		if candidates[i].channelID != candidates[j].channelID {
			return candidates[i].channelID < candidates[j].channelID
		}
		return candidates[i].group.ID < candidates[j].group.ID
	})
	return candidates, visible, nil
}

func (s *ChannelPreferenceService) resolveAnchor(ctx context.Context, userID int64, channelIDs []int64, now time.Time) (*Group, []int64, error) {
	channelIDs = normalizeChannelIDs(channelIDs)
	if len(channelIDs) == 0 {
		return nil, nil, ErrChannelsRequired
	}
	requested := make(map[int64]struct{}, len(channelIDs))
	for _, id := range channelIDs {
		requested[id] = struct{}{}
	}
	candidates, visible, err := s.collectAnchorCandidates(ctx, userID, requested, now)
	if err != nil {
		return nil, nil, err
	}
	if len(visible) != len(channelIDs) || len(candidates) == 0 {
		return nil, nil, ErrChannelNotAllowed
	}
	anchor := candidates[0].group
	return &anchor, channelIDs, nil
}

func (s *ChannelPreferenceService) resolveAutomaticAnchor(ctx context.Context, userID int64, now time.Time) (*Group, error) {
	candidates, _, err := s.collectAnchorCandidates(ctx, userID, nil, now)
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, ErrChannelNotAllowed
	}
	anchor := candidates[0].group
	return &anchor, nil
}

func (s *ChannelPreferenceService) CreateAPIKey(ctx context.Context, userID int64, req CreateAPIKeyRequest) (*APIKey, error) {
	mode := req.RoutingMode
	if mode == "" && len(req.ChannelIDs) == 0 {
		defaults, err := s.repo.GetUserDefaultChannelIDs(ctx, userID)
		if err != nil {
			return nil, err
		}
		if len(defaults) > 0 {
			mode = APIKeyRoutingModeChannels
			req.ChannelIDs = defaults
		}
	}
	normalizedMode, err := NormalizeAPIKeyRoutingMode(mode)
	if err != nil {
		return nil, err
	}
	req.RoutingMode = normalizedMode
	if normalizedMode == APIKeyRoutingModeAutoChannels {
		anchor, err := s.resolveAutomaticAnchor(ctx, userID, time.Now())
		if err != nil {
			return nil, err
		}
		req.GroupID = &anchor.ID
		req.ChannelIDs = nil
		return s.apiKeys.Create(ctx, userID, req)
	}
	if normalizedMode != APIKeyRoutingModeChannels {
		return s.apiKeys.Create(ctx, userID, req)
	}

	anchor, channelIDs, err := s.resolveAnchor(ctx, userID, req.ChannelIDs, time.Now())
	if err != nil {
		return nil, err
	}
	req.GroupID = &anchor.ID
	req.ChannelIDs = channelIDs
	key, err := s.apiKeys.Create(ctx, userID, req)
	if err != nil {
		return nil, err
	}
	keyValue, err := s.repo.ReplaceAPIKeyChannels(ctx, key.ID, userID, anchor.ID, channelIDs)
	if err != nil {
		_ = s.apiKeys.Delete(ctx, key.ID, userID)
		return nil, fmt.Errorf("save api key channel preferences: %w", err)
	}
	key.RoutingMode = APIKeyRoutingModeChannels
	key.ChannelIDs = channelIDs
	s.invalidator.InvalidateAuthCacheByKey(ctx, keyValue)
	return key, nil
}

func (s *ChannelPreferenceService) GetAPIKey(ctx context.Context, apiKeyID, userID int64) (*ChannelPreferences, error) {
	key, err := s.apiKeys.GetByID(ctx, apiKeyID)
	if err != nil || key == nil || key.UserID != userID {
		return nil, ErrAPIKeyNotFound
	}
	mode := key.RoutingMode
	if mode == "" {
		mode = APIKeyRoutingModeLegacyGroup
	}
	var ids []int64
	if mode == APIKeyRoutingModeChannels {
		ids, err = s.repo.GetAPIKeyChannelIDs(ctx, apiKeyID, userID)
		if err != nil {
			return nil, err
		}
	}
	return &ChannelPreferences{RoutingMode: mode, ChannelIDs: ids, GroupID: key.GroupID}, nil
}

func (s *ChannelPreferenceService) SetAPIKey(ctx context.Context, apiKeyID, userID int64, channelIDs []int64) (*ChannelPreferences, error) {
	key, err := s.apiKeys.GetByID(ctx, apiKeyID)
	if err != nil || key == nil || key.UserID != userID {
		return nil, ErrAPIKeyNotFound
	}
	anchor, channelIDs, err := s.resolveAnchor(ctx, userID, channelIDs, time.Now())
	if err != nil {
		return nil, err
	}
	keyValue, err := s.repo.ReplaceAPIKeyChannels(ctx, apiKeyID, userID, anchor.ID, channelIDs)
	if err != nil {
		return nil, err
	}
	s.invalidator.InvalidateAuthCacheByKey(ctx, keyValue)
	return &ChannelPreferences{RoutingMode: APIKeyRoutingModeChannels, ChannelIDs: channelIDs, GroupID: &anchor.ID}, nil
}

func (s *ChannelPreferenceService) GetDefaults(ctx context.Context, userID int64) ([]int64, error) {
	return s.repo.GetUserDefaultChannelIDs(ctx, userID)
}

func (s *ChannelPreferenceService) SetDefaults(ctx context.Context, userID int64, channelIDs []int64) ([]int64, error) {
	_, channelIDs, err := s.resolveAnchor(ctx, userID, channelIDs, time.Now())
	if err != nil {
		return nil, err
	}
	if err := s.repo.ReplaceUserDefaultChannels(ctx, userID, channelIDs); err != nil {
		return nil, err
	}
	return channelIDs, nil
}

func (s *ChannelPreferenceService) GetUserDisabledGroupIDs(ctx context.Context, userID int64) ([]int64, error) {
	return s.repo.GetUserDisabledGroupIDs(ctx, userID)
}

func (s *ChannelPreferenceService) SetUserDisabledGroupIDs(ctx context.Context, userID int64, groupIDs []int64) ([]int64, error) {
	groupIDs = normalizeGroupIDs(groupIDs)
	available, err := s.apiKeys.GetAvailableGroups(ctx, userID)
	if err != nil {
		return nil, err
	}
	allowed := make(map[int64]struct{}, len(available))
	for i := range available {
		allowed[available[i].ID] = struct{}{}
	}
	for _, groupID := range groupIDs {
		if _, ok := allowed[groupID]; !ok {
			return nil, ErrGroupNotAllowed
		}
	}
	if err := s.repo.ReplaceUserDisabledGroups(ctx, userID, groupIDs); err != nil {
		return nil, err
	}
	s.invalidator.InvalidateAuthCacheByUserID(ctx, userID)
	return groupIDs, nil
}

func (s *ChannelPreferenceService) Attach(ctx context.Context, userID int64, keys []APIKey) error {
	for i := range keys {
		if keys[i].RoutingMode == "" {
			keys[i].RoutingMode = APIKeyRoutingModeLegacyGroup
		}
		if keys[i].RoutingMode != APIKeyRoutingModeChannels {
			keys[i].ChannelIDs = nil
			continue
		}
		ids, err := s.repo.GetAPIKeyChannelIDs(ctx, keys[i].ID, userID)
		if err != nil {
			return err
		}
		keys[i].ChannelIDs = ids
	}
	return nil
}
