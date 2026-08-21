package service

import (
	"context"
	"testing"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestModelQuarantineService_QuarantinePreservesAndConflicts(t *testing.T) {
	fakeStore := &fakePublicationStore{
		recomputeFn: func(ctx context.Context, input RecomputeInput) error {
			// Verify quarantine sets quarantined eligibility
			require.Equal(t, PublicationEligibilityQuarantined, input.Items[0].Eligibility)
			return nil
		},
	}
	svc := NewModelQuarantineService(fakeStore, nil, nil)
	input := QuarantineInput{
		BatchID: "batch-q1", IdempotencyKey: "idem-q1", ExpectedRegistryVersion: 5,
		ChannelVersions: map[int64]int64{10: 3}, ActorID: "tester",
		Items: []QuarantineItem{{AccountID: 1, ChannelID: 10, CanonicalModelID: "claude-opus-4-6"}},
	}
	require.NoError(t, svc.Quarantine(context.Background(), input))

	// Stale channel version should conflict
	input.ChannelVersions = map[int64]int64{10: 2}
	// Need to make store return conflict for stale
	fakeStore.recomputeFn = func(ctx context.Context, input RecomputeInput) error {
		return Conflict("STALE_CHANNEL_VERSION", "stale")
	}
	err := svc.Quarantine(context.Background(), input)
	require.Error(t, err)
}

func TestModelQuarantineService_RestoreFailsUntilGatesPass(t *testing.T) {
	// Loader that returns blocked gate
	// Manually set loader to return blocked input via fake store? Instead we test via evaluator directly.
	// For this test we use real loader with nil deps: it will return eligible by default (true), but we need blocked.
	// Instead create a fake evaluator that returns blocked.
	fakeEval := &fakeEvaluator{result: PublicationEvaluation{Eligibility: PublicationEligibilityBlocked, Reason: "price_missing"}}
	fakeStore := &fakePublicationStore{}
	// Need to inject loader that returns input with RegistryEntry active text etc but ModelAllowed false -> blocked
	// Simplify: create a loader that returns blocked via evaluator
	loader := NewModelPublicationInputLoader(nil, nil)
	svc := NewModelQuarantineService(fakeStore, loader, fakeEval)
	_ = loader
	input := RestoreInput{IdempotencyKey: "idem-r1", AccountID: 1, ChannelID: 10, CanonicalModelID: "claude-opus-4-6", ActorID: "tester"}
	err := svc.Restore(context.Background(), input)
	require.Error(t, err)
	require.Contains(t, err.Error(), "price_missing")

	// Now make eligible
	fakeEval.result = PublicationEvaluation{Eligibility: PublicationEligibilityEligible, Reason: "eligible"}
	fakeStore.recomputeFn = func(ctx context.Context, input RecomputeInput) error {
		require.Equal(t, PublicationEligibilityEligible, input.Items[0].Eligibility)
		return nil
	}
	require.NoError(t, svc.Restore(context.Background(), input))
}

type fakeEvaluator struct {
	result PublicationEvaluation
}

func (f *fakeEvaluator) Evaluate(input PublicationInput) PublicationEvaluation {
	return f.result
}

func Conflict(reason, msg string) error {
	// Use infraerrors
	return infraerrors.Conflict(reason, msg)
}
