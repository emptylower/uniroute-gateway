package service

import (
	"context"
	"testing"
)

type fakeRegistryService struct {
	snapshot *ModelRegistrySnapshot
}

func (f *fakeRegistryService) GetSnapshot(ctx context.Context) (*ModelRegistrySnapshot, error) {
	return f.snapshot, nil
}
func (f *fakeRegistryService) List(ctx context.Context) ([]ModelRegistryEntry, error) { return nil, nil }
func (f *fakeRegistryService) Get(ctx context.Context, canonicalID string) (*ModelRegistryEntry, error) {
	return nil, nil
}
func (f *fakeRegistryService) CreateDecision(ctx context.Context, input RegistryDecisionInput) (*ModelRegistryEntry, error) {
	return nil, nil
}
func (f *fakeRegistryService) Rebuild(ctx context.Context) error { return nil }

type fakeObservationRepo struct {
	calls int
	batchID string
}

func (f *fakeObservationRepo) RecordDiscovery(ctx context.Context, input DiscoveryBatchInput) (string, error) {
	f.calls++
	if f.batchID == "" {
		f.batchID = "batch-test-123"
	}
	return f.batchID, nil
}

type fakeShadowRepo struct {
	calls     int
	decisions []ShadowDecision
}

func (f *fakeShadowRepo) RecordShadowDecisions(ctx context.Context, batchID string, decisions []ShadowDecision) error {
	f.calls++
	f.decisions = decisions
	return nil
}

type countingEvaluator struct {
	calls int
	inner PublicationEvaluator
}

func (c *countingEvaluator) Evaluate(input PublicationInput) PublicationEvaluation {
	c.calls++
	if c.inner != nil {
		return c.inner.Evaluate(input)
	}
	return PublicationEvaluation{Eligibility: PublicationEligibilityBlocked, Reason: "test"}
}

func TestModelGovernanceService_MixedAnthropicDiscovery(t *testing.T) {
	// Mixed Anthropic discovery fixture: Claude, GPT, Gemini, unknown, wildcard, non-text
	anthropic := GovernanceProviderAnthropic
	snapshot := &ModelRegistrySnapshot{
		Version: 7,
		Entries: map[string]ModelRegistryEntry{
			"claude-3-5-sonnet": {
				CanonicalID: "claude-3-5-sonnet",
				Provider:    anthropic,
				Modality:    ModelModalityText,
				Status:      ModelLifecycleActive,
			},
			"gpt-4o": {
				CanonicalID: "gpt-4o",
				Provider:    GovernanceProviderOpenAI,
				Modality:    ModelModalityText,
				Status:      ModelLifecycleActive,
			},
			"gemini-2.0-flash": {
				CanonicalID: "gemini-2.0-flash",
				Provider:    GovernanceProviderGemini,
				Modality:    ModelModalityText,
				Status:      ModelLifecycleActive,
			},
			"dall-e-3": {
				CanonicalID: "dall-e-3",
				Provider:    GovernanceProviderOpenAI,
				Modality:    ModelModalityImage,
				Status:      ModelLifecycleActive,
			},
		},
		Aliases: map[string]string{
			"claude-sonnet-alias": "claude-3-5-sonnet",
		},
	}

	registryService := &fakeRegistryService{snapshot: snapshot}
	observationRepo := &fakeObservationRepo{}
	shadowRepo := &fakeShadowRepo{}
	countingEval := &countingEvaluator{inner: NewPublicationEvaluator()}
	classifier := NewModelClassifier()

	svc := NewModelGovernanceService(ModelGovernanceServiceConfig{
		RegistryService: registryService,
		ObservationRepo: observationRepo,
		Classifier:      classifier,
		Evaluator:       countingEval,
		ShadowDecisionRepo: shadowRepo,
	})

	input := DiscoveryBatchInput{
		IdempotencyKey:  "test-idempotency",
		AccountID:       1001,
		AccountProvider: &anthropic,
		RoutingPlatform: PlatformAnthropic,
		ModelIDs: []string{
			"claude-3-5-sonnet", // approved
			"gpt-4o",            // cross_provider
			"gemini-2.0-flash",  // cross_provider
			"unknown-model-xyz", // unknown
			"gpt-*",             // wildcard -> ignored
			"dall-e-3",          // non-text -> ignored
		},
		RawSnapshot: []byte(`{"evidence": {"models": ["claude-3-5-sonnet", "gpt-4o"]}}`),
	}

	summary, err := svc.ClassifyAndPersistShadow(context.Background(), input)
	if err != nil {
		t.Fatalf("ClassifyAndPersistShadow failed: %v", err)
	}

	// Assert one registry snapshot, one batch, exact counts
	if summary.RegistryVersion != 7 {
		t.Errorf("registry version = %d, want 7", summary.RegistryVersion)
	}
	if summary.Discovered != 6 {
		t.Errorf("discovered = %d, want 6", summary.Discovered)
	}
	if summary.CrossProvider != 2 {
		t.Errorf("cross_provider = %d, want 2", summary.CrossProvider)
	}
	if summary.Unknown != 1 {
		t.Errorf("unknown = %d, want 1", summary.Unknown)
	}
	if summary.Ignored != 2 {
		t.Errorf("ignored = %d, want 2", summary.Ignored)
	}
	// Publishable is eligible count – only approved with all gates passing should be eligible
	// In this fixture, only claude-3-5-sonnet is approved and price/billing/probe default to present
	if summary.Publishable != 1 {
		t.Errorf("publishable = %d, want 1", summary.Publishable)
	}

	if observationRepo.calls != 1 {
		t.Errorf("observation repo calls = %d, want 1", observationRepo.calls)
	}
	if shadowRepo.calls != 1 {
		t.Errorf("shadow repo calls = %d, want 1", shadowRepo.calls)
	}
	// One evaluator call per discovered model
	if countingEval.calls != 6 {
		t.Errorf("evaluator calls = %d, want 6", countingEval.calls)
	}

	// Verify no routing/pricing/mapping/cache mutations – shadow mode only persists decisions
	// This is enforced by the service not calling any pricing/mapping mutators; we assert shadow decisions were recorded
	if len(shadowRepo.decisions) != 6 {
		t.Errorf("shadow decisions len = %d, want 6", len(shadowRepo.decisions))
	}

	// Verify summary fields exactly
	if summary.BatchID == "" {
		t.Error("batch_id should not be empty")
	}
	// Ensure never describes every discovery as published
	if summary.Publishable == summary.Discovered {
		t.Error("publishable should not equal discovered when cross_provider/unknown exist")
	}
}

func TestModelGovernanceService_ShadowDoesNotMutateRouting(t *testing.T) {
	anthropic := GovernanceProviderAnthropic
	snapshot := &ModelRegistrySnapshot{
		Version: 1,
		Entries: map[string]ModelRegistryEntry{
			"claude-3-5-sonnet": {CanonicalID: "claude-3-5-sonnet", Provider: anthropic, Modality: ModelModalityText, Status: ModelLifecycleActive},
		},
		Aliases: map[string]string{},
	}
	registryService := &fakeRegistryService{snapshot: snapshot}
	observationRepo := &fakeObservationRepo{batchID: "batch-shadow-1"}
	shadowRepo := &fakeShadowRepo{}

	svc := NewModelGovernanceService(ModelGovernanceServiceConfig{
		RegistryService:    registryService,
		ObservationRepo:    observationRepo,
		Classifier:         NewModelClassifier(),
		Evaluator:          NewPublicationEvaluator(),
		ShadowDecisionRepo: shadowRepo,
	})

	input := DiscoveryBatchInput{
		IdempotencyKey:  "idempotent-key-2",
		AccountID:       2002,
		AccountProvider: &anthropic,
		RoutingPlatform: PlatformAnthropic,
		ModelIDs:        []string{"claude-3-5-sonnet"},
		RawSnapshot:     []byte(`{"evidence": {}}`),
	}

	summary, err := svc.ClassifyAndPersistShadow(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if summary.Publishable != 1 {
		t.Errorf("publishable = %d, want 1", summary.Publishable)
	}
	// Ensure shadow decisions persist eligibility but don't mutate other state – we check that only observation and shadow repos were touched
	if observationRepo.calls != 1 || shadowRepo.calls != 1 {
		t.Errorf("expected exactly 1 batch and 1 shadow call, got %d and %d", observationRepo.calls, shadowRepo.calls)
	}
}
