package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type channelPreferenceRepoFake struct {
	defaults         []int64
	disabledGroups   []int64
	replacedKeyID    int64
	replacedUserID   int64
	replacedAnchorID int64
	replacedChannels []int64
	replacedDefaults []int64
	replacedDisabled []int64
	apiKeyChannelIDs []int64
	getKeyChannels   int
}

func (f *channelPreferenceRepoFake) GetAPIKeyChannelIDs(context.Context, int64, int64) ([]int64, error) {
	f.getKeyChannels++
	return append([]int64(nil), f.apiKeyChannelIDs...), nil
}

func (f *channelPreferenceRepoFake) ReplaceAPIKeyChannels(_ context.Context, apiKeyID, userID, anchorGroupID int64, channelIDs []int64) (string, error) {
	f.replacedKeyID = apiKeyID
	f.replacedUserID = userID
	f.replacedAnchorID = anchorGroupID
	f.replacedChannels = append([]int64(nil), channelIDs...)
	return "sk-created", nil
}

func (f *channelPreferenceRepoFake) GetUserDefaultChannelIDs(context.Context, int64) ([]int64, error) {
	return append([]int64(nil), f.defaults...), nil
}

func (f *channelPreferenceRepoFake) ReplaceUserDefaultChannels(_ context.Context, _ int64, channelIDs []int64) error {
	f.replacedDefaults = append([]int64(nil), channelIDs...)
	return nil
}

func (f *channelPreferenceRepoFake) GetUserDisabledGroupIDs(context.Context, int64) ([]int64, error) {
	return append([]int64(nil), f.disabledGroups...), nil
}

func (f *channelPreferenceRepoFake) ReplaceUserDisabledGroups(_ context.Context, _ int64, groupIDs []int64) error {
	f.replacedDisabled = append([]int64(nil), groupIDs...)
	return nil
}

type channelPreferenceAPIKeysFake struct {
	groups     []Group
	rates      map[int64]float64
	created    []CreateAPIKeyRequest
	deletedIDs []int64
	key        *APIKey
}

func (f *channelPreferenceAPIKeysFake) GetAvailableGroups(context.Context, int64) ([]Group, error) {
	return f.groups, nil
}

func (f *channelPreferenceAPIKeysFake) GetUserGroupRates(context.Context, int64) (map[int64]float64, error) {
	return f.rates, nil
}

func (f *channelPreferenceAPIKeysFake) Create(_ context.Context, userID int64, req CreateAPIKeyRequest) (*APIKey, error) {
	f.created = append(f.created, req)
	return &APIKey{
		ID: 55, UserID: userID, Key: "sk-created", Name: req.Name,
		GroupID: req.GroupID, RoutingMode: req.RoutingMode, ChannelIDs: append([]int64(nil), req.ChannelIDs...),
	}, nil
}

func (f *channelPreferenceAPIKeysFake) Delete(_ context.Context, id, _ int64) error {
	f.deletedIDs = append(f.deletedIDs, id)
	return nil
}

func (f *channelPreferenceAPIKeysFake) GetByID(context.Context, int64) (*APIKey, error) {
	return f.key, nil
}

type channelPreferenceCatalogFake struct {
	channels []AvailableChannel
}

func (f channelPreferenceCatalogFake) ListAvailable(context.Context) ([]AvailableChannel, error) {
	return f.channels, nil
}

type channelPreferenceInvalidatorFake struct {
	keys []string
}

func (f *channelPreferenceInvalidatorFake) InvalidateAuthCacheByKey(_ context.Context, key string) {
	f.keys = append(f.keys, key)
}

func (*channelPreferenceInvalidatorFake) InvalidateAuthCacheByUserID(context.Context, int64)  {}
func (*channelPreferenceInvalidatorFake) InvalidateAuthCacheByGroupID(context.Context, int64) {}

func TestChannelPreferenceService_DefaultsCopyOnlyWhenCreatingNewKey(t *testing.T) {
	cheap := Group{ID: 1, RateMultiplier: 0.2, SortOrder: 2, Status: StatusActive}
	stable := Group{ID: 2, RateMultiplier: 0.8, SortOrder: 1, Status: StatusActive}
	repo := &channelPreferenceRepoFake{defaults: []int64{20, 10, 20}}
	apiKeys := &channelPreferenceAPIKeysFake{groups: []Group{cheap, stable}}
	invalidator := &channelPreferenceInvalidatorFake{}
	svc := NewChannelPreferenceService(
		repo,
		apiKeys,
		channelPreferenceCatalogFake{channels: []AvailableChannel{
			{ID: 10, Status: StatusActive, Groups: []AvailableGroupRef{{ID: cheap.ID}}},
			{ID: 20, Status: StatusActive, Groups: []AvailableGroupRef{{ID: stable.ID}}},
		}},
		invalidator,
	)

	key, err := svc.CreateAPIKey(context.Background(), 42, CreateAPIKeyRequest{Name: "new key"})

	require.NoError(t, err)
	require.Equal(t, APIKeyRoutingModeChannels, key.RoutingMode)
	require.Equal(t, []int64{10, 20}, key.ChannelIDs)
	require.Equal(t, int64(1), repo.replacedAnchorID, "cheapest selected group is the rollback anchor")
	require.Equal(t, []int64{10, 20}, repo.replacedChannels)
	require.Equal(t, []string{"sk-created"}, invalidator.keys)
	require.Len(t, apiKeys.created, 1)
	require.Equal(t, int64(1), *apiKeys.created[0].GroupID)

	// Reading or changing defaults never mutates an already-created key. Only
	// CreateAPIKey consumes defaults and writes api_key_channels.
	_, err = svc.SetDefaults(context.Background(), 42, []int64{20})
	require.NoError(t, err)
	require.Equal(t, []int64{20}, repo.replacedDefaults)
	require.Equal(t, []int64{10, 20}, repo.replacedChannels)
}

func TestChannelPreferenceService_LegacyCreatePreservesGroupContract(t *testing.T) {
	groupID := int64(7)
	repo := &channelPreferenceRepoFake{}
	apiKeys := &channelPreferenceAPIKeysFake{}
	svc := NewChannelPreferenceService(repo, apiKeys, channelPreferenceCatalogFake{}, &channelPreferenceInvalidatorFake{})

	key, err := svc.CreateAPIKey(context.Background(), 42, CreateAPIKeyRequest{Name: "legacy", GroupID: &groupID})

	require.NoError(t, err)
	require.Equal(t, APIKeyRoutingModeLegacyGroup, key.RoutingMode)
	require.Equal(t, groupID, *key.GroupID)
	require.Zero(t, repo.replacedKeyID)
}

func TestChannelPreferenceService_AutomaticCreateStoresOnlyCompatibilityAnchor(t *testing.T) {
	cheap := Group{ID: 1, RateMultiplier: 0.2, SortOrder: 2, Status: StatusActive}
	stable := Group{ID: 2, RateMultiplier: 0.8, SortOrder: 1, Status: StatusActive}
	repo := &channelPreferenceRepoFake{}
	apiKeys := &channelPreferenceAPIKeysFake{groups: []Group{cheap, stable}}
	svc := NewChannelPreferenceService(
		repo,
		apiKeys,
		channelPreferenceCatalogFake{channels: []AvailableChannel{
			{ID: 10, Status: StatusActive, Groups: []AvailableGroupRef{{ID: cheap.ID}}},
			{ID: 20, Status: StatusActive, Groups: []AvailableGroupRef{{ID: stable.ID}}},
		}},
		&channelPreferenceInvalidatorFake{},
	)

	key, err := svc.CreateAPIKey(context.Background(), 42, CreateAPIKeyRequest{
		Name:        "automatic",
		RoutingMode: APIKeyRoutingModeAutoChannels,
	})

	require.NoError(t, err)
	require.Equal(t, APIKeyRoutingModeAutoChannels, key.RoutingMode)
	require.Equal(t, int64(1), *key.GroupID)
	require.Empty(t, key.ChannelIDs)
	require.Zero(t, repo.replacedKeyID, "automatic keys must not snapshot channel associations")
}

func TestChannelPreferenceService_GroupPreferencesValidateAndPersistOptOuts(t *testing.T) {
	repo := &channelPreferenceRepoFake{disabledGroups: []int64{2}}
	invalidator := &channelPreferenceInvalidatorFake{}
	svc := NewChannelPreferenceService(
		repo,
		&channelPreferenceAPIKeysFake{groups: []Group{{ID: 1}, {ID: 2}}},
		channelPreferenceCatalogFake{},
		invalidator,
	)

	ids, err := svc.GetUserDisabledGroupIDs(context.Background(), 42)
	require.NoError(t, err)
	require.Equal(t, []int64{2}, ids)

	ids, err = svc.SetUserDisabledGroupIDs(context.Background(), 42, []int64{2, 1, 2})
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, ids)
	require.Equal(t, []int64{1, 2}, repo.replacedDisabled)

	_, err = svc.SetUserDisabledGroupIDs(context.Background(), 42, []int64{9})
	require.ErrorIs(t, err, ErrGroupNotAllowed)
}

func TestChannelPreferenceService_RejectsEmptyAndUnavailableChannels(t *testing.T) {
	group := Group{ID: 1, RateMultiplier: 1, Status: StatusActive}
	svc := NewChannelPreferenceService(
		&channelPreferenceRepoFake{},
		&channelPreferenceAPIKeysFake{groups: []Group{group}},
		channelPreferenceCatalogFake{channels: []AvailableChannel{{ID: 10, Status: StatusActive, Groups: []AvailableGroupRef{{ID: 1}}}}},
		&channelPreferenceInvalidatorFake{},
	)

	_, _, err := svc.resolveAnchor(context.Background(), 42, nil, time.Now())
	require.ErrorIs(t, err, ErrChannelsRequired)
	_, _, err = svc.resolveAnchor(context.Background(), 42, []int64{10, 99}, time.Now())
	require.ErrorIs(t, err, ErrChannelNotAllowed)
}

func TestChannelPreferenceService_KeyOwnershipIsCheckedBeforeChannelResolution(t *testing.T) {
	apiKeys := &channelPreferenceAPIKeysFake{
		key:    &APIKey{ID: 9, UserID: 99},
		groups: []Group{{ID: 1, RateMultiplier: 1, Status: StatusActive}},
	}
	svc := NewChannelPreferenceService(
		&channelPreferenceRepoFake{},
		apiKeys,
		channelPreferenceCatalogFake{channels: []AvailableChannel{{ID: 10, Status: StatusActive, Groups: []AvailableGroupRef{{ID: 1}}}}},
		&channelPreferenceInvalidatorFake{},
	)

	_, err := svc.SetAPIKey(context.Background(), 9, 42, []int64{10})

	require.ErrorIs(t, err, ErrAPIKeyNotFound)
}

func TestChannelPreferenceService_LegacyKeyDoesNotReadChannelAssociations(t *testing.T) {
	groupID := int64(3)
	repo := &channelPreferenceRepoFake{apiKeyChannelIDs: []int64{10}}
	svc := NewChannelPreferenceService(
		repo,
		&channelPreferenceAPIKeysFake{key: &APIKey{ID: 9, UserID: 42, GroupID: &groupID}},
		channelPreferenceCatalogFake{},
		&channelPreferenceInvalidatorFake{},
	)

	prefs, err := svc.GetAPIKey(context.Background(), 9, 42)

	require.NoError(t, err)
	require.Equal(t, APIKeyRoutingModeLegacyGroup, prefs.RoutingMode)
	require.Empty(t, prefs.ChannelIDs)
	require.Zero(t, repo.getKeyChannels)
}

func TestChannelPreferenceService_AttachReadsAssociationsOnlyForChannelKeys(t *testing.T) {
	repo := &channelPreferenceRepoFake{apiKeyChannelIDs: []int64{10, 20}}
	svc := NewChannelPreferenceService(
		repo,
		&channelPreferenceAPIKeysFake{},
		channelPreferenceCatalogFake{},
		&channelPreferenceInvalidatorFake{},
	)
	keys := []APIKey{
		{ID: 1, RoutingMode: APIKeyRoutingModeLegacyGroup},
		{ID: 2, RoutingMode: APIKeyRoutingModeChannels},
	}

	err := svc.Attach(context.Background(), 42, keys)

	require.NoError(t, err)
	require.Empty(t, keys[0].ChannelIDs)
	require.Equal(t, []int64{10, 20}, keys[1].ChannelIDs)
	require.Equal(t, 1, repo.getKeyChannels)
}
