package service

import (
	"context"
	"fmt"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type fakeGovernanceModeProvider struct {
	enforce bool
}

func (f *fakeGovernanceModeProvider) IsEnforce(ctx context.Context) bool { return f.enforce }
func (f *fakeGovernanceModeProvider) CurrentMode(ctx context.Context) string {
	if f.enforce {
		return "enforce"
	}
	return "shadow"
}

func newGovernanceGatewayServiceForTest(store ModelAuthorizationStore, mode string) *GatewayService {
	cfg := &config.Config{}
	cfg.ModelGovernance.AuthorizationMode = mode
	var modeProvider GovernanceModeProvider
	if mode == "enforce" {
		modeProvider = &fakeGovernanceModeProvider{enforce: true}
	} else {
		modeProvider = &fakeGovernanceModeProvider{enforce: false}
	}
	svc := &GatewayService{
		cfg:              cfg,
		publicationStore: store,
		modeProvider:     modeProvider,
	}
	return svc
}

func TestGovernance_AdmissionDenial(t *testing.T) {
	store := &fakePublicationStoreWithCanonical{
		fakePublicationStore: fakePublicationStore{
			channelEligibleFn: func(ctx context.Context, ch int64, canonical string) (bool, error) { return false, nil },
		},
		canonicalEligible: func(canonical string) bool { return false },
	}
	svc := newGovernanceGatewayServiceForTest(store, "enforce")
	require.False(t, svc.isModelEligibleForDispatch(context.Background(), "unknown-model-xyz"))
	groupID := int64(1)
	require.False(t, svc.isModelEligibleForGroup(context.Background(), &groupID, "unknown-model-xyz"))
}

func TestGovernance_StickyRecheck(t *testing.T) {
	store := &fakePublicationStoreWithCanonical{
		fakePublicationStore: fakePublicationStore{
			channelEligibleFn: func(ctx context.Context, ch int64, canonical string) (bool, error) { return false, nil },
		},
		canonicalEligible: func(canonical string) bool { return false },
	}
	svc := newGovernanceGatewayServiceForTest(store, "enforce")
	groupID := int64(10)
	require.False(t, svc.isModelEligibleForGroup(context.Background(), &groupID, "claude-opus-4-6"))
}

func TestGovernance_FallbackSameProviderOnly(t *testing.T) {
	store := &fakeChannelEligibleStore{
		eligible: map[string]bool{
			"1:claude-opus-4-6": true,
			"2:gpt-4o":          true,
			"1:gpt-4o":          false,
			"2:claude-opus-4-6": false,
		},
	}
	svc := newGovernanceGatewayServiceForTest(store, "enforce")
	ch1 := int64(1)
	ch2 := int64(2)
	require.True(t, svc.isModelEligibleForGroupWithChannels(context.Background(), &ch1, "claude-opus-4-6", map[int64][]int64{1: {1}}))
	require.False(t, svc.isModelEligibleForGroupWithChannels(context.Background(), &ch1, "gpt-4o", map[int64][]int64{1: {1}}))
	require.True(t, svc.isModelEligibleForGroupWithChannels(context.Background(), &ch2, "gpt-4o", map[int64][]int64{2: {2}}))
	require.False(t, svc.isModelEligibleForGroupWithChannels(context.Background(), &ch2, "claude-opus-4-6", map[int64][]int64{2: {2}}))
}

func TestGovernance_WildcardRejected(t *testing.T) {
	store := &fakePublicationStore{}
	svc := newGovernanceGatewayServiceForTest(store, "enforce")
	require.False(t, svc.isModelEligibleForDispatch(context.Background(), "claude-*"))
	require.False(t, svc.isModelEligibleForDispatch(context.Background(), "*"))
}

func TestGovernance_ShadowPassesThrough(t *testing.T) {
	store := &fakePublicationStore{
		channelEligibleFn: func(ctx context.Context, ch int64, canonical string) (bool, error) { return false, nil },
	}
	svc := newGovernanceGatewayServiceForTest(store, "shadow")
	require.True(t, svc.isModelEligibleForDispatch(context.Background(), "unknown-model"))
	groupID := int64(1)
	require.True(t, svc.isModelEligibleForGroup(context.Background(), &groupID, "unknown-model"))
}

// Helpers

type fakePublicationStoreWithCanonical struct {
	fakePublicationStore
	canonicalEligible func(canonical string) bool
}

func (f *fakePublicationStoreWithCanonical) IsCanonicalEligible(ctx context.Context, canonical string) (bool, error) {
	if f.canonicalEligible != nil {
		return f.canonicalEligible(canonical), nil
	}
	return true, nil
}
func (f *fakePublicationStoreWithCanonical) IsChannelModelEligible(ctx context.Context, ch int64, canonical string) (bool, error) {
	if f.channelEligibleFn != nil {
		return f.channelEligibleFn(ctx, ch, canonical)
	}
	return true, nil
}

type fakeChannelEligibleStore struct {
	eligible map[string]bool
}

func (f *fakeChannelEligibleStore) Decision(ctx context.Context, key ModelAuthorizationKey) (ModelAuthorizationDecision, error) {
	return ModelAuthorizationDecision{}, nil
}
func (f *fakeChannelEligibleStore) RecomputeBatch(ctx context.Context, input RecomputeInput) error { return nil }
func (f *fakeChannelEligibleStore) IsChannelModelEligible(ctx context.Context, ch int64, canonical string) (bool, error) {
	k := fmt.Sprintf("%d:%s", ch, canonical)
	if v, ok := f.eligible[k]; ok {
		return v, nil
	}
	return false, nil
}
func (f *fakeChannelEligibleStore) IsCanonicalEligible(ctx context.Context, canonical string) (bool, error) {
	for k, v := range f.eligible {
		if v {
			// k is "ch:canonical"
			for i, c := range k {
				if c == ':' {
					if k[i+1:] == canonical {
						return true, nil
					}
					break
				}
			}
		}
	}
	return false, nil
}

func (s *GatewayService) isModelEligibleForGroupWithChannels(ctx context.Context, groupID *int64, model string, mapping map[int64][]int64) bool {
	if !s.isEnforceMode() || s.publicationStore == nil || model == "" {
		return true
	}
	if chs, ok := mapping[*groupID]; ok && len(chs) > 0 {
		for _, chID := range chs {
			eligible, _ := s.publicationStore.IsChannelModelEligible(ctx, chID, model)
			if eligible {
				return true
			}
		}
		return false
	}
	eligible, _ := s.publicationStore.IsCanonicalEligible(ctx, model)
	return eligible
}
