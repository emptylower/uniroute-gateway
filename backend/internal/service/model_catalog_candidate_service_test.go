package service

import (
	"context"
	"crypto/sha256"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ---------- fakes ----------

type fakeEvidenceReader struct {
	settings  []CatalogSourceSetting
	snapshots map[string]CatalogSnapshotRef
	evidence  map[int64][]CatalogCandidateEvidenceRow
	missing   []CatalogMissingSummaryRow

	updated struct {
		source    string
		enabled   bool
		threshold int
	}
}

func (f *fakeEvidenceReader) SourceSettings(ctx context.Context) ([]CatalogSourceSetting, error) {
	return f.settings, nil
}
func (f *fakeEvidenceReader) LatestAcceptedSnapshotPerSource(ctx context.Context) (map[string]CatalogSnapshotRef, error) {
	return f.snapshots, nil
}
func (f *fakeEvidenceReader) ListEvidence(ctx context.Context, snapshotID int64) ([]CatalogCandidateEvidenceRow, error) {
	return f.evidence[snapshotID], nil
}
func (f *fakeEvidenceReader) MissingEvidenceSummary(ctx context.Context) ([]CatalogMissingSummaryRow, error) {
	return f.missing, nil
}
func (f *fakeEvidenceReader) UpdateSourceSetting(ctx context.Context, source string, enabled bool, thresholdPercent int) error {
	for i := range f.settings {
		if f.settings[i].Source == source {
			f.settings[i].Enabled = enabled
			f.settings[i].CountDropThresholdPercent = thresholdPercent
		}
	}
	f.updated = struct {
		source    string
		enabled   bool
		threshold int
	}{source, enabled, thresholdPercent}
	return nil
}

type fakeSnapshotWriter struct {
	mu       sync.Mutex
	started  int
	finished []struct {
		runID   int64
		status  string
		items   int
		errMsg  string
		commit  string
	}
	snapshots []CatalogSnapshotInput
	evidence  [][]CatalogCandidateEvidenceInput
	missing   [][]CatalogMissingEvidenceInput
}

func (f *fakeSnapshotWriter) StartSyncRun(ctx context.Context, input StartCatalogSyncRunInput) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.started++
	return int64(f.started), nil
}
func (f *fakeSnapshotWriter) FinishSyncRun(ctx context.Context, runID int64, status string, itemCount int, resolvedCommit string, errorMessage string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.finished = append(f.finished, struct {
		runID  int64
		status string
		items  int
		errMsg string
		commit string
	}{runID, status, itemCount, errorMessage, resolvedCommit})
	return nil
}
func (f *fakeSnapshotWriter) FailStaleRunningSyncRuns(ctx context.Context, reason string) (int64, error) {
	return 0, nil
}

func (f *fakeSnapshotWriter) InsertSnapshotWithEvidence(ctx context.Context, input CatalogSnapshotInput, evidence []CatalogCandidateEvidenceInput, missing []CatalogMissingEvidenceInput) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snapshots = append(f.snapshots, input)
	f.evidence = append(f.evidence, evidence)
	f.missing = append(f.missing, missing)
	return int64(len(f.snapshots)), nil
}

type countingAlertSink struct {
	calls []string
	mu    sync.Mutex
}

func (s *countingAlertSink) AlertGrouped(ctx context.Context, source, kind, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, source+"|"+kind)
}

// ---------- classification ----------

func TestClassifyCatalogConfidenceRules(t *testing.T) {
	cases := []struct {
		name           string
		registryActive bool
		hints          map[string]string
		want           string
		wantOrder      int
	}{
		{name: "internal active match wins even single source", registryActive: true, hints: map[string]string{"openrouter": "anthropic"}, want: CatalogConfidenceCertain, wantOrder: 1},
		{name: "two source agreement is likely", hints: map[string]string{"openrouter": "anthropic", "modelsdev": "anthropic"}, want: CatalogConfidenceLikely, wantOrder: 2},
		{name: "single source is unclear", hints: map[string]string{"litellm": "mistral"}, want: CatalogConfidenceUnclear, wantOrder: 3},
		{name: "conflicting hints are unclear", hints: map[string]string{"openrouter": "anthropic", "modelsdev": "openai"}, want: CatalogConfidenceUnclear, wantOrder: 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			confidence, order := ClassifyCatalogConfidence(tc.registryActive, tc.hints)
			require.Equal(t, tc.want, confidence)
			require.Equal(t, tc.wantOrder, order)
		})
	}
}

// ---------- views over fake reader ----------

func newCandidateTestService(reader *fakeEvidenceReader, writer *fakeSnapshotWriter, sink *countingAlertSink, clock func() time.Time) *ModelCatalogCandidateService {
	return NewModelCatalogCandidateService(ModelCatalogCandidateServiceConfig{
		Reader:   reader,
		Writer:   writer,
		Registry: &registryReadSpy{},
		Audit:    &auditRepoSpy{},
		Alerts:   sink,
		Fetcher:  &fakeCatalogFetcher{responses: map[string][]byte{}},
		Clock:    clock,
	})
}

type registryReadSpy struct{}

func (r *registryReadSpy) GetSnapshot(ctx context.Context) (*ModelRegistrySnapshot, error) {
	return &ModelRegistrySnapshot{
		Version: 1,
		Entries: map[string]ModelRegistryEntry{
			"claude-active": {CanonicalID: "claude-active", Provider: GovernanceProviderAnthropic, Status: ModelLifecycleActive},
			"gone-model":    {CanonicalID: "gone-model", Provider: GovernanceProviderOpenAI, Status: ModelLifecycleActive},
		},
		Aliases: map[string]string{},
	}, nil
}

type auditRepoSpy struct {
	inserts int
	last    *AuditLog
}

func (a *auditRepoSpy) Insert(ctx context.Context, log *AuditLog) error {
	a.inserts++
	a.last = log
	return nil
}

func (a *auditRepoSpy) BatchInsert(ctx context.Context, logs []*AuditLog) (int64, error) {
	return 0, nil
}

func TestCatalogCandidateListConfidenceAndOrdering(t *testing.T) {
	now := time.Now().UTC()
	reader := &fakeEvidenceReader{
		settings: defaultCatalogSettings(),
		snapshots: map[string]CatalogSnapshotRef{
			CatalogSourceOpenRouter: {SnapshotID: 1, ExternalVersion: "sha256:a", AcceptedAt: now, ItemCount: 4},
			CatalogSourceModelsDev:  {SnapshotID: 2, ExternalVersion: "c0ffee", AcceptedAt: now, ItemCount: 3},
		},
		evidence: map[int64][]CatalogCandidateEvidenceRow{
			1: {
				{Source: CatalogSourceOpenRouter, CanonicalModelID: "gpt-agreed", ProviderHint: "openai"},
				{Source: CatalogSourceOpenRouter, CanonicalModelID: "claude-active", ProviderHint: "anthropic"},
				{Source: CatalogSourceOpenRouter, CanonicalModelID: "conflict-model", ProviderHint: "anthropic"},
				{Source: CatalogSourceOpenRouter, CanonicalModelID: "mistral-single", ProviderHint: "mistral"},
			},
			2: {
				{Source: CatalogSourceModelsDev, CanonicalModelID: "gpt-agreed", ProviderHint: "openai"},
				{Source: CatalogSourceModelsDev, CanonicalModelID: "conflict-model", ProviderHint: "openai"},
			},
		},
		missing: nil,
	}
	svc := newCandidateTestService(reader, &fakeSnapshotWriter{}, &countingAlertSink{}, func() time.Time { return now })

	candidates, err := svc.CandidateList(context.Background())
	require.NoError(t, err)
	require.Len(t, candidates, 4)

	byID := map[string]CatalogCandidateView{}
	for _, c := range candidates {
		byID[c.CanonicalModelID] = c
	}
	require.Equal(t, CatalogConfidenceCertain, byID["claude-active"].Confidence)
	require.NotNil(t, byID["claude-active"].RegistryProvider)
	require.Equal(t, "anthropic", *byID["claude-active"].RegistryProvider)
	require.Equal(t, CatalogConfidenceLikely, byID["gpt-agreed"].Confidence)
	require.Equal(t, CatalogConfidenceUnclear, byID["conflict-model"].Confidence)
	require.Equal(t, CatalogConfidenceUnclear, byID["mistral-single"].Confidence)

	// Confidence affects queue order only: certain < likely < unclear.
	require.True(t, candidates[0].QueueOrder <= candidates[1].QueueOrder)
	require.Equal(t, 1, candidates[0].QueueOrder)
	require.Equal(t, "claude-active", candidates[0].CanonicalModelID)
}

func TestCatalogDisappearanceYieldsMissingOnly(t *testing.T) {
	now := time.Now().UTC()
	reader := &fakeEvidenceReader{
		settings: defaultCatalogSettings(),
		snapshots: map[string]CatalogSnapshotRef{
			CatalogSourceOpenRouter: {SnapshotID: 1, ExternalVersion: "sha256:a", AcceptedAt: now, ItemCount: 1},
		},
		evidence: map[int64][]CatalogCandidateEvidenceRow{
			1: {
				{Source: CatalogSourceOpenRouter, CanonicalModelID: "still-here", ProviderHint: "openai"},
				{Source: CatalogSourceOpenRouter, CanonicalModelID: "claude-active", ProviderHint: "anthropic"},
			},
		},
		missing: nil,
	}
	svc := newCandidateTestService(reader, &fakeSnapshotWriter{}, &countingAlertSink{}, func() time.Time { return now })

	candidates, err := svc.CandidateList(context.Background())
	require.NoError(t, err)
	for _, c := range candidates {
		require.NotEqual(t, "gone-model", c.CanonicalModelID, "disappeared registry models must not become candidates")
	}

	missingViews, err := svc.MissingModels(context.Background())
	require.NoError(t, err)
	require.Len(t, missingViews, 1)
	require.Equal(t, "gone-model", missingViews[0].CanonicalModelID)
	require.Equal(t, "openai", missingViews[0].RegistryProvider)
}

func TestCatalogRetirementSuggestionsRule(t *testing.T) {
	now := time.Now().UTC()
	reader := &fakeEvidenceReader{
		settings: defaultCatalogSettings(),
		snapshots: map[string]CatalogSnapshotRef{
			CatalogSourceOpenRouter: {SnapshotID: 1, ExternalVersion: "x", AcceptedAt: now, ItemCount: 0},
		},
		evidence: map[int64][]CatalogCandidateEvidenceRow{},
		missing: []CatalogMissingSummaryRow{
			{CanonicalModelID: "span-ok", AcceptedRunCount: 3, FirstMissingAt: now.Add(-8 * 24 * time.Hour), LastMissingAt: now},
			{CanonicalModelID: "too-few-runs", AcceptedRunCount: 2, FirstMissingAt: now.Add(-9 * 24 * time.Hour), LastMissingAt: now},
			{CanonicalModelID: "too-recent-span", AcceptedRunCount: 3, FirstMissingAt: now.Add(-2 * 24 * time.Hour), LastMissingAt: now},
		},
	}
	svc := newCandidateTestService(reader, &fakeSnapshotWriter{}, &countingAlertSink{}, func() time.Time { return now })

	suggestions, err := svc.RetirementSuggestions(context.Background())
	require.NoError(t, err)
	require.Len(t, suggestions, 1)
	require.Equal(t, "span-ok", suggestions[0].CanonicalModelID)
}

func TestCatalogPriceAnomaliesFromCrossSourceDivergence(t *testing.T) {
	now := time.Now().UTC()
	reader := &fakeEvidenceReader{
		settings: defaultCatalogSettings(),
		snapshots: map[string]CatalogSnapshotRef{
			CatalogSourceOpenRouter: {SnapshotID: 1, ExternalVersion: "x", AcceptedAt: now, ItemCount: 3},
			CatalogSourceLiteLLM:    {SnapshotID: 2, ExternalVersion: "y", AcceptedAt: now, ItemCount: 3},
		},
		evidence: map[int64][]CatalogCandidateEvidenceRow{
			1: {
				{Source: CatalogSourceOpenRouter, CanonicalModelID: "divergent", PriceJSON: `{"input":0.000002}`},
				{Source: CatalogSourceOpenRouter, CanonicalModelID: "stable", PriceJSON: `{"input":0.000003}`},
				{Source: CatalogSourceOpenRouter, CanonicalModelID: "single-priced", PriceJSON: `{"input":0.000001}`},
			},
			2: {
				{Source: CatalogSourceLiteLLM, CanonicalModelID: "divergent", PriceJSON: `{"input":0.000006}`},
				{Source: CatalogSourceLiteLLM, CanonicalModelID: "stable", PriceJSON: `{"input":0.0000031}`},
			},
		},
		missing: nil,
	}
	svc := newCandidateTestService(reader, &fakeSnapshotWriter{}, &countingAlertSink{}, func() time.Time { return now })

	anomalies, err := svc.PriceAnomalies(context.Background())
	require.NoError(t, err)
	require.Len(t, anomalies, 1)
	require.Equal(t, "divergent", anomalies[0].CanonicalModelID)
	require.InDelta(t, 200.0, anomalies[0].MaxSpreadPercent, 0.01)
	require.Len(t, anomalies[0].PerSourcePriceJSON, 2)
}

func TestCatalogSourceStatusStaleness(t *testing.T) {
	now := time.Now().UTC()
	reader := &fakeEvidenceReader{
		settings: defaultCatalogSettings(), // openrouter 24h, others 72h
		snapshots: map[string]CatalogSnapshotRef{
			CatalogSourceOpenRouter: {SnapshotID: 1, ExternalVersion: "sha256:a", AcceptedAt: now.Add(-48 * time.Hour), ItemCount: 5, ResolvedCommit: ""},
			CatalogSourceModelsDev:  {SnapshotID: 2, ExternalVersion: "abc", AcceptedAt: now.Add(-30 * time.Hour), ItemCount: 4, ResolvedCommit: "abc"},
		},
		evidence: map[int64][]CatalogCandidateEvidenceRow{},
		missing:  nil,
	}
	svc := newCandidateTestService(reader, &fakeSnapshotWriter{}, &countingAlertSink{}, func() time.Time { return now })

	statuses, err := svc.SourceStatus(context.Background())
	require.NoError(t, err)
	require.Len(t, statuses, 3)
	bySource := map[string]CatalogSourceStatusView{}
	for _, s := range statuses {
		bySource[s.Source] = s
	}
	require.True(t, bySource["openrouter"].Stale, "48h since last accept exceeds 24h staleness")
	require.False(t, bySource["modelsdev"].Stale, "30h is within 72h staleness")
	require.Equal(t, "abc", bySource["modelsdev"].LastResolvedCommit)
}

func defaultCatalogSettings() []CatalogSourceSetting {
	return []CatalogSourceSetting{
		{Source: CatalogSourceOpenRouter, Enabled: true, CountDropThresholdPercent: 20, StaleAfterHours: 24},
		{Source: CatalogSourceModelsDev, Enabled: true, CountDropThresholdPercent: 20, StaleAfterHours: 72},
		{Source: CatalogSourceLiteLLM, Enabled: true, CountDropThresholdPercent: 20, StaleAfterHours: 72},
	}
}

// ---------- ingestion ----------

func TestIngestModelsDevUsesHostedAPIWithDigestPinning(t *testing.T) {
	// models.dev no longer publishes api.json via GitHub; the hosted endpoint
	// is canonical. Version pinning is by content digest (OpenRouter pattern).
	payload := []byte(`{"openai":{"models":{"gpt-5.5":{"id":"gpt-5.5","name":"GPT-5.5","attachment":false,"reasoning":true,"tool_call":true,"release_date":"2026-01-01","last_updated":"2026-01-02","modalities":{"input":["text"],"output":["text"]},"cost":{"input":1.0,"output":2.0},"limit":{"context":128000,"output":4096}}}}}`)
	reader := &fakeEvidenceReader{settings: defaultCatalogSettings()}
	writer := &fakeSnapshotWriter{}
	svc := NewModelCatalogCandidateService(ModelCatalogCandidateServiceConfig{
		Reader:   reader,
		Writer:   writer,
		Registry: &registryReadSpy{},
		Audit:    &auditRepoSpy{},
		Alerts:   &countingAlertSink{},
		Fetcher: &fakeCatalogFetcher{responses: map[string][]byte{
			"https://models.dev/api.json": payload,
		}},
		Clock: func() time.Time { return time.Now().UTC() },
	})

	require.NoError(t, svc.IngestSource(context.Background(), CatalogSourceModelsDev))
	require.Len(t, writer.snapshots, 1)
	sum := sha256.Sum256(payload)
	require.Equal(t, fmt.Sprintf("sha256:%x", sum), writer.snapshots[0].ExternalVersion)
}

func TestIngestOpenRouterWritesSnapshotEvidenceAndMissing(t *testing.T) {
	payload := []byte(`{"data":[{"id":"anthropic/new-candidate","name":"New Candidate","context_length":100000,"pricing":{"prompt":"0.000001","completion":"0.000002"}}]}`)
	reader := &fakeEvidenceReader{settings: defaultCatalogSettings()}
	writer := &fakeSnapshotWriter{}
	sink := &countingAlertSink{}
	clock := func() time.Time { return time.Now().UTC() }
	svc := NewModelCatalogCandidateService(ModelCatalogCandidateServiceConfig{
		Reader:   reader,
		Writer:   writer,
		Registry: &registryReadSpy{},
		Audit:    &auditRepoSpy{},
		Alerts:   sink,
		Fetcher: &fakeCatalogFetcher{responses: map[string][]byte{
			"https://openrouter.ai/api/v1/models": payload,
		}},
		Clock: clock,
	})

	require.NoError(t, svc.IngestSource(context.Background(), CatalogSourceOpenRouter))

	require.Len(t, writer.snapshots, 1)
	sum := sha256.Sum256(payload)
	require.Equal(t, fmt.Sprintf("sha256:%x", sum), writer.snapshots[0].ExternalVersion, "content-addressed immutable version")
	require.Len(t, writer.evidence[0], 1)
	require.Equal(t, "anthropic/new-candidate", writer.evidence[0][0].CanonicalModelID)
	// Registry active ids absent from the payload become missing evidence rows.
	var missingIDs []string
	for _, m := range writer.missing[0] {
		missingIDs = append(missingIDs, m.CanonicalModelID)
	}
	require.ElementsMatch(t, []string{"claude-active", "gone-model"}, missingIDs)
	require.Len(t, writer.finished, 1)
	require.Equal(t, CatalogSyncStatusSucceeded, writer.finished[0].status)
	require.Empty(t, sink.calls)
}

func TestIngestAllGroupsAlertsPerSourceOnFailures(t *testing.T) {
	reader := &fakeEvidenceReader{settings: defaultCatalogSettings()}
	writer := &fakeSnapshotWriter{}
	sink := &countingAlertSink{}
	svc := NewModelCatalogCandidateService(ModelCatalogCandidateServiceConfig{
		Reader:   reader,
		Writer:   writer,
		Registry: &registryReadSpy{},
		Audit:    &auditRepoSpy{},
		Alerts:   sink,
		Fetcher:  &fakeCatalogFetcher{err: fmt.Errorf("network down")},
		Clock:    func() time.Time { return time.Now().UTC() },
	})

	svc.IngestAll(context.Background())

	require.Len(t, sink.calls, 3, "one grouped alert per source, not per failure detail")
	seen := map[string]bool{}
	for _, call := range sink.calls {
		parts := splitN(call, "|")
		require.False(t, seen[parts[0]], "source %s must be alerted once", parts[0])
		seen[parts[0]] = true
	}
	require.Len(t, writer.finished, 3)
	for _, fin := range writer.finished {
		require.Equal(t, CatalogSyncStatusFailed, fin.status)
	}
}

func splitN(s, sep string) []string {
	out := []string{}
	start := 0
	for i := 0; i+len(sep) <= len(s); i++ {
		if s[i:i+len(sep)] == sep {
			out = append(out, s[start:i])
			start = i + len(sep)
			i += len(sep) - 1
		}
	}
	out = append(out, s[start:])
	return out
}

// TestCatalogServiceCannotWriteRegistry proves at runtime that the candidate
// service's registry dependency exposes no mutation methods: external catalog
// evidence can never create an active registry entry or eligible publication.
func TestCatalogServiceCannotWriteRegistry(t *testing.T) {
	reader := &fakeEvidenceReader{settings: defaultCatalogSettings()}
	svc := newCandidateTestService(reader, &fakeSnapshotWriter{}, &countingAlertSink{}, time.Now)

	registryType := reflect.TypeOf(svc.registry)
	require.NotNil(t, registryType)
	for _, forbidden := range []string{"CreateDecision", "Rebuild", "ApplyDecision"} {
		_, ok := registryType.MethodByName(forbidden)
		require.False(t, ok, "registry dependency must not expose write method %s", forbidden)
	}
}

func TestUpdateSourceSettingAuditsAndValidates(t *testing.T) {
	now := time.Now().UTC()
	reader := &fakeEvidenceReader{settings: defaultCatalogSettings()}
	audit := &auditRepoSpy{}
	writer := &fakeSnapshotWriter{}
	svc := newCandidateTestService(reader, writer, &countingAlertSink{}, func() time.Time { return now })
	svc.audit = audit

	newThreshold := 30
	enabled := true
	require.NoError(t, svc.UpdateSourceSetting(context.Background(), "admin-1", CatalogSourceLiteLLM, &enabled, &newThreshold))
	require.Equal(t, 1, audit.inserts)
	require.Contains(t, audit.last.Action, "model_catalog")
	require.Contains(t, audit.last.RequestBody, "litellm")
	require.Equal(t, CatalogSourceLiteLLM, reader.updated.source)
	require.True(t, reader.updated.enabled)
	require.Equal(t, 30, reader.updated.threshold)

	// Partial update: nil fields keep current settings instead of clobbering.
	partial := false
	require.NoError(t, svc.UpdateSourceSetting(context.Background(), "admin-1", CatalogSourceLiteLLM, &partial, nil))
	require.Equal(t, CatalogSourceLiteLLM, reader.updated.source)
	require.False(t, reader.updated.enabled)
	require.Equal(t, 30, reader.updated.threshold, "omitted threshold must keep the current value")

	// Unknown source rejected without audit.
	err := svc.UpdateSourceSetting(context.Background(), "admin-1", "huggingface", nil, nil)
	require.Error(t, err)
	require.Equal(t, 2, audit.inserts)

	// Threshold bounds enforced.
	tooBig := 101
	require.Error(t, svc.UpdateSourceSetting(context.Background(), "admin-1", CatalogSourceOpenRouter, nil, &tooBig))
}

func TestModelCatalogCandidateServiceStartStopLoop(t *testing.T) {
	reader := &fakeEvidenceReader{settings: defaultCatalogSettings()}
	sink := &countingAlertSink{}
	var cycles int32
	svc := NewModelCatalogCandidateService(ModelCatalogCandidateServiceConfig{
		Reader:   reader,
		Writer:   &fakeSnapshotWriter{},
		Registry: &registryReadSpy{},
		Audit:    &auditRepoSpy{},
		Alerts:   sink,
		Fetcher:  &fakeCatalogFetcher{responses: map[string][]byte{}},
		Clock:    time.Now,
		Interval: 15 * time.Millisecond,
	})
	svc.afterCycle = func() { atomic.AddInt32(&cycles, 1) }

	svc.Start()
	svc.Start() // idempotent
	time.Sleep(60 * time.Millisecond)
	svc.Stop()
	svc.Stop() // idempotent
	require.Positive(t, atomic.LoadInt32(&cycles))
}
