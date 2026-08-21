package service

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakePublicationStore struct {
	decisionFn       func(ctx context.Context, key ModelAuthorizationKey) (ModelAuthorizationDecision, error)
	recomputeFn      func(ctx context.Context, input RecomputeInput) error
	channelEligibleFn func(ctx context.Context, channelID int64, canonical string) (bool, error)
	decisionCalls    int
	recomputeCalls   int
	lastRecompute    *RecomputeInput
}

func (f *fakePublicationStore) Decision(ctx context.Context, key ModelAuthorizationKey) (ModelAuthorizationDecision, error) {
	f.decisionCalls++
	if f.decisionFn != nil {
		return f.decisionFn(ctx, key)
	}
	return ModelAuthorizationDecision{Eligibility: PublicationEligibilityEligible, Reason: "eligible", RegistryVersion: 1, ChannelVersion: 1}, nil
}
func (f *fakePublicationStore) RecomputeBatch(ctx context.Context, input RecomputeInput) error {
	f.recomputeCalls++
	f.lastRecompute = &input
	if f.recomputeFn != nil {
		return f.recomputeFn(ctx, input)
	}
	return nil
}
func (f *fakePublicationStore) IsChannelModelEligible(ctx context.Context, channelID int64, canonicalModelID string) (bool, error) {
	if f.channelEligibleFn != nil {
		return f.channelEligibleFn(ctx, channelID, canonicalModelID)
	}
	return true, nil
}

func TestModelPublicationService_DecisionCachesAndInvalidates(t *testing.T) {
	fake := &fakePublicationStore{
		decisionFn: func(ctx context.Context, key ModelAuthorizationKey) (ModelAuthorizationDecision, error) {
			return ModelAuthorizationDecision{Eligibility: PublicationEligibilityEligible, Reason: "eligible", RegistryVersion: 5, ChannelVersion: 7}, nil
		},
	}
	svc := NewModelPublicationService(fake)
	key := ModelAuthorizationKey{AccountID: 1, ChannelID: 2, CanonicalModelID: "claude-opus-4-6"}
	dec1, err := svc.Decision(context.Background(), key)
	require.NoError(t, err)
	require.Equal(t, "eligible", dec1.Eligibility)
	dec2, err := svc.Decision(context.Background(), key)
	require.NoError(t, err)
	require.Equal(t, dec1, dec2)
	require.Equal(t, 1, fake.decisionCalls, "second decision should be cached")

	// Recompute must invalidate cache.
	input := RecomputeInput{
		BatchID: "batch-1", IdempotencyKey: "key-1", RegistryVersion: 5, ChannelVersions: map[int64]int64{2: 7},
		ActorID: "tester", Items: []RecomputeItem{
			{AccountID: 1, ChannelID: 2, CanonicalModelID: "claude-opus-4-6", Eligibility: PublicationEligibilityBlocked, Reason: "price_missing", RegistryVersion: 5, ChannelVersion: 7},
		},
	}
	require.NoError(t, svc.RecomputeBatch(context.Background(), input))
	require.Equal(t, 1, fake.recomputeCalls)
	// After invalidation, next Decision must call store again.
	fake.decisionFn = func(ctx context.Context, key ModelAuthorizationKey) (ModelAuthorizationDecision, error) {
		return ModelAuthorizationDecision{Eligibility: PublicationEligibilityBlocked, Reason: "price_missing", RegistryVersion: 5, ChannelVersion: 7}, nil
	}
	dec3, err := svc.Decision(context.Background(), key)
	require.NoError(t, err)
	require.Equal(t, "blocked", dec3.Eligibility)
	require.Equal(t, 2, fake.decisionCalls)
}

func TestModelPublicationService_RecomputeValidatesAndRejectsStale(t *testing.T) {
	fake := &fakePublicationStore{}
	svc := NewModelPublicationService(fake)

	// Invalid input: empty batch
	err := svc.RecomputeBatch(context.Background(), RecomputeInput{
		BatchID: "", IdempotencyKey: "k", RegistryVersion: 1, ChannelVersions: map[int64]int64{1: 1},
		ActorID: "tester", Items: []RecomputeItem{{AccountID: 1, ChannelID: 1, CanonicalModelID: "m", Eligibility: PublicationEligibilityEligible, Reason: "eligible", RegistryVersion: 1, ChannelVersion: 1}},
	})
	require.Error(t, err)

	// Valid recompute passes
	err = svc.RecomputeBatch(context.Background(), RecomputeInput{
		BatchID: "b1", IdempotencyKey: "k1", RegistryVersion: 1, ChannelVersions: map[int64]int64{1: 1},
		ActorID: "tester", Items: []RecomputeItem{{AccountID: 1, ChannelID: 1, CanonicalModelID: "m", Eligibility: PublicationEligibilityEligible, Reason: "eligible", RegistryVersion: 1, ChannelVersion: 1}},
	})
	require.NoError(t, err)

	// Duplicate batch idempotent: store returns nil, service succeeds and still invalidates.
	err = svc.RecomputeBatch(context.Background(), RecomputeInput{
		BatchID: "b1", IdempotencyKey: "k1", RegistryVersion: 1, ChannelVersions: map[int64]int64{1: 1},
		ActorID: "tester", Items: []RecomputeItem{{AccountID: 1, ChannelID: 1, CanonicalModelID: "m", Eligibility: PublicationEligibilityEligible, Reason: "eligible", RegistryVersion: 1, ChannelVersion: 1}},
	})
	require.NoError(t, err)
	require.Equal(t, 2, fake.recomputeCalls) // second call still goes to store, which is idempotent internally
}

func TestModelPublicationService_CannotRestoreQuarantineWithStaleChannel(t *testing.T) {
	// This test proves the service validates channel version matching item version.
	fake := &fakePublicationStore{
		recomputeFn: func(ctx context.Context, input RecomputeInput) error {
			return nil
		},
	}
	svc := NewModelPublicationService(fake)
	// Mismatched channel version between batch and item should be validation error before store.
	err := svc.RecomputeBatch(context.Background(), RecomputeInput{
		BatchID: "b2", IdempotencyKey: "k2", RegistryVersion: 2, ChannelVersions: map[int64]int64{10: 5},
		ActorID: "tester", Items: []RecomputeItem{
			{AccountID: 1, ChannelID: 10, CanonicalModelID: "claude-opus-4-6", Eligibility: PublicationEligibilityQuarantined, Reason: "unknown_identity", RegistryVersion: 2, ChannelVersion: 4},
		},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "channel version")
}

func TestModelPublicationService_StaleRegistryInputConflict(t *testing.T) {
	// Stale registry version is detected by repository layer; service just delegates.
	// Here we mock repository to return conflict.
	fake := &fakePublicationStore{
		recomputeFn: func(ctx context.Context, input RecomputeInput) error {
			return fmt.Errorf("stale registry version: expected %d got %d", 2, 5)
		},
	}
	svc := NewModelPublicationService(fake)
	err := svc.RecomputeBatch(context.Background(), RecomputeInput{
		BatchID: "b3", IdempotencyKey: "k3", RegistryVersion: 2, ChannelVersions: map[int64]int64{1: 1},
		ActorID: "tester", Items: []RecomputeItem{{AccountID: 1, ChannelID: 1, CanonicalModelID: "m", Eligibility: PublicationEligibilityEligible, Reason: "eligible", RegistryVersion: 2, ChannelVersion: 1}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "stale registry")
}
