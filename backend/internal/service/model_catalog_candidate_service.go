package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Candidate confidence levels. Confidence affects queue order only — it never
// grants routing, publication, or billing authority.
const (
	CatalogConfidenceCertain = "certain"
	CatalogConfidenceLikely  = "likely"
	CatalogConfidenceUnclear = "unclear"

	certainQueueOrder = 1
	likelyQueueOrder  = 2
	unclearQueueOrder = 3

	catalogIngestionIntervalDefault = 6 * time.Hour

	retirementMinAcceptedRuns = 3
	retirementMinSpanDays     = 7
	priceAnomalySpreadPercent = 25.0
	openRouterModelsURL       = "https://openrouter.ai/api/v1/models"
)

// ClassifyCatalogConfidence applies the phase 6 confidence rules:
// internal active match => certain; two-source agreement => likely;
// single source or conflicting hints => unclear.
func ClassifyCatalogConfidence(registryActive bool, hintsBySource map[string]string) (string, int) {
	if registryActive {
		return CatalogConfidenceCertain, certainQueueOrder
	}
	hints := make(map[string]struct{}, len(hintsBySource))
	for _, hint := range hintsBySource {
		hints[strings.ToLower(strings.TrimSpace(hint))] = struct{}{}
	}
	if len(hintsBySource) >= 2 && len(hints) == 1 {
		return CatalogConfidenceLikely, likelyQueueOrder
	}
	return CatalogConfidenceUnclear, unclearQueueOrder
}

// ---------- read model ----------

// CatalogSourceSetting is one row of model_catalog_source_settings.
type CatalogSourceSetting struct {
	Source                    string
	Enabled                   bool
	CountDropThresholdPercent int
	StaleAfterHours           int
}

// CatalogSnapshotRef points at the latest accepted snapshot of one source.
type CatalogSnapshotRef struct {
	SnapshotID      int64
	ExternalVersion string
	AcceptedAt      time.Time
	ItemCount       int
	ResolvedCommit  string
}

// CatalogCandidateEvidenceRow is one normalized evidence line read back from storage.
type CatalogCandidateEvidenceRow struct {
	Source           string
	CanonicalModelID string
	ProviderHint     string
	DisplayName      string
	ContextWindow    int64
	Aliases          []string
	Capabilities     []string
	PriceJSON        string
}

// CatalogMissingSummaryRow aggregates missing evidence across accepted runs.
type CatalogMissingSummaryRow struct {
	CanonicalModelID string
	AcceptedRunCount int
	FirstMissingAt   time.Time
	LastMissingAt    time.Time
}

// ModelCatalogEvidenceReader is the read side of the immutable catalog store.
type ModelCatalogEvidenceReader interface {
	SourceSettings(ctx context.Context) ([]CatalogSourceSetting, error)
	LatestAcceptedSnapshotPerSource(ctx context.Context) (map[string]CatalogSnapshotRef, error)
	ListEvidence(ctx context.Context, snapshotID int64) ([]CatalogCandidateEvidenceRow, error)
	MissingEvidenceSummary(ctx context.Context) ([]CatalogMissingSummaryRow, error)
	UpdateSourceSetting(ctx context.Context, source string, enabled bool, thresholdPercent int) error
}

// CatalogRegistryReader is deliberately read-only: external catalog aggregation
// can never mutate the reviewed registry or publication projections.
type CatalogRegistryReader interface {
	GetSnapshot(ctx context.Context) (*ModelRegistrySnapshot, error)
}

// CatalogAlertSink receives grouped alerts: one call per source per cycle.
type CatalogAlertSink interface {
	AlertGrouped(ctx context.Context, source, kind, message string)
}

// ---------- views ----------

type CatalogSourceStatusView struct {
	Source                    string     `json:"source"`
	Enabled                   bool       `json:"enabled"`
	CountDropThresholdPercent int        `json:"count_drop_threshold_percent"`
	StaleAfterHours           int        `json:"stale_after_hours"`
	LastAcceptedAt            *time.Time `json:"last_accepted_at"`
	LastResolvedCommit        string     `json:"last_resolved_commit"`
	LastItemCount             int        `json:"last_item_count"`
	Stale                     bool       `json:"stale"`
}

type CatalogCandidateView struct {
	CanonicalModelID  string            `json:"canonical_model_id"`
	Confidence        string            `json:"confidence"`
	QueueOrder        int               `json:"queue_order"`
	ProviderHints     map[string]string `json:"provider_hints"`
	Sources           []string          `json:"sources"`
	RegistryProvider  *string           `json:"registry_provider,omitempty"`
	RegistryStatus    string            `json:"registry_status,omitempty"`
	DisplayName       string            `json:"display_name"`
	ContextWindow     int64             `json:"context_window"`
	PriceJSONBySource map[string]string `json:"price_json_by_source"`
}

type CatalogMissingView struct {
	CanonicalModelID string `json:"canonical_model_id"`
	RegistryProvider string `json:"registry_provider"`
}

type CatalogRetirementSuggestionView struct {
	CanonicalModelID string    `json:"canonical_model_id"`
	MissingRunCount  int       `json:"missing_run_count"`
	FirstMissingAt   time.Time `json:"first_missing_at"`
	LastMissingAt    time.Time `json:"last_missing_at"`
	SpanDays         float64   `json:"span_days"`
}

type CatalogPriceAnomalyView struct {
	CanonicalModelID   string            `json:"canonical_model_id"`
	PerSourcePriceJSON map[string]string `json:"per_source_price_json"`
	MaxSpreadPercent   float64           `json:"max_spread_percent"`
}

// CatalogAuditWriter appends audit records for audited threshold changes.
type CatalogAuditWriter interface {
	Insert(ctx context.Context, log *AuditLog) error
}

// ModelCatalogCandidateService aggregates immutable external catalog evidence
// into review candidates. It is read-only with respect to registry and
// publication: its registry dependency exposes no mutation methods and its only
// writes go to the append-only catalog evidence store plus settings/audit.
type ModelCatalogCandidateService struct {
	reader   ModelCatalogEvidenceReader
	writer   ModelCatalogSnapshotStore
	registry CatalogRegistryReader
	audit    CatalogAuditWriter
	alerts   CatalogAlertSink
	fetcher  CatalogFetcher
	clock    func() time.Time
	interval time.Duration
	maxItems int

	afterCycle func() // test hook

	startOnce sync.Once
	stopOnce  sync.Once
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

// ModelCatalogCandidateServiceConfig wires the service dependencies.
type ModelCatalogCandidateServiceConfig struct {
	Reader   ModelCatalogEvidenceReader
	Writer   ModelCatalogSnapshotStore
	Registry CatalogRegistryReader
	Audit    CatalogAuditWriter
	Alerts   CatalogAlertSink
	Fetcher  CatalogFetcher
	Clock    func() time.Time
	Interval time.Duration
	// MaxItems caps normalized items per payload; <=0 falls back to
	// CatalogMaxItemsDefault.
	MaxItems int
}

// NewModelCatalogCandidateService builds the aggregation service.
func NewModelCatalogCandidateService(cfg ModelCatalogCandidateServiceConfig) *ModelCatalogCandidateService {
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	interval := cfg.Interval
	if interval <= 0 {
		interval = catalogIngestionIntervalDefault
	}
	var alerts CatalogAlertSink = noopCatalogAlertSink{}
	if cfg.Alerts != nil {
		alerts = cfg.Alerts
	}
	maxItems := cfg.MaxItems
	if maxItems <= 0 {
		maxItems = CatalogMaxItemsDefault
	}
	return &ModelCatalogCandidateService{
		reader:   cfg.Reader,
		writer:   cfg.Writer,
		registry: cfg.Registry,
		audit:    cfg.Audit,
		alerts:   alerts,
		fetcher:  cfg.Fetcher,
		clock:    clock,
		interval: interval,
		maxItems: maxItems,
	}
}

type noopCatalogAlertSink struct{}

func (noopCatalogAlertSink) AlertGrouped(context.Context, string, string, string) {}

// Start launches the scheduled ingestion loop; idempotent.
func (s *ModelCatalogCandidateService) Start() {
	if s == nil {
		return
	}
	s.startOnce.Do(func() {
		s.stopCh = make(chan struct{})
		s.wg.Add(1)
		go s.run()
	})
}

// Stop terminates the loop and waits for the current cycle; idempotent.
func (s *ModelCatalogCandidateService) Stop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		if s.stopCh != nil {
			close(s.stopCh)
		}
	})
	s.wg.Wait()
}

func (s *ModelCatalogCandidateService) run() {
	defer s.wg.Done()
	timer := time.NewTimer(s.interval)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			s.IngestAll(ctx)
			cancel()
			s.notifyCycle()
			timer.Reset(s.interval)
		case <-s.stopCh:
			return
		}
	}
}

func (s *ModelCatalogCandidateService) notifyCycle() {
	if s.afterCycle != nil {
		s.afterCycle()
	}
}

// ---------- read views ----------

func (s *ModelCatalogCandidateService) SourceStatus(ctx context.Context) ([]CatalogSourceStatusView, error) {
	settings, err := s.reader.SourceSettings(ctx)
	if err != nil {
		return nil, err
	}
	snaps, err := s.reader.LatestAcceptedSnapshotPerSource(ctx)
	if err != nil {
		return nil, err
	}
	now := s.clock()
	views := make([]CatalogSourceStatusView, 0, len(settings))
	for _, setting := range settings {
		view := CatalogSourceStatusView{
			Source:                    setting.Source,
			Enabled:                   setting.Enabled,
			CountDropThresholdPercent: setting.CountDropThresholdPercent,
			StaleAfterHours:           setting.StaleAfterHours,
		}
		if ref, ok := snaps[setting.Source]; ok {
			accepted := ref.AcceptedAt
			view.LastAcceptedAt = &accepted
			view.LastItemCount = ref.ItemCount
			view.LastResolvedCommit = ref.ResolvedCommit
			view.Stale = now.Sub(ref.AcceptedAt) > time.Duration(setting.StaleAfterHours)*time.Hour
		} else {
			view.Stale = true
		}
		views = append(views, view)
	}
	sort.Slice(views, func(i, j int) bool { return views[i].Source < views[j].Source })
	return views, nil
}

// latestEvidence loads the merged latest-accepted evidence per source.
func (s *ModelCatalogCandidateService) latestEvidence(ctx context.Context) (map[string]CatalogSnapshotRef, map[string]map[string]CatalogCandidateEvidenceRow, error) {
	snaps, err := s.reader.LatestAcceptedSnapshotPerSource(ctx)
	if err != nil {
		return nil, nil, err
	}
	byModel := make(map[string]map[string]CatalogCandidateEvidenceRow)
	for source, ref := range snaps {
		rows, err := s.reader.ListEvidence(ctx, ref.SnapshotID)
		if err != nil {
			return nil, nil, err
		}
		for _, row := range rows {
			if byModel[row.CanonicalModelID] == nil {
				byModel[row.CanonicalModelID] = make(map[string]CatalogCandidateEvidenceRow)
			}
			byModel[row.CanonicalModelID][source] = row
		}
	}
	return snaps, byModel, nil
}

// CandidateList merges latest accepted snapshots with the reviewed registry and
// classifies each canonical id. Ordering is queue order (confidence) then id.
func (s *ModelCatalogCandidateService) CandidateList(ctx context.Context) ([]CatalogCandidateView, error) {
	_, byModel, err := s.latestEvidence(ctx)
	if err != nil {
		return nil, err
	}
	regSnapshot, err := s.registry.GetSnapshot(ctx)
	if err != nil {
		return nil, err
	}

	ids := make([]string, 0, len(byModel))
	for id := range byModel {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	views := make([]CatalogCandidateView, 0, len(ids))
	for _, id := range ids {
		perSource := byModel[id]
		entry, inRegistry := regSnapshot.Entries[id]
		registryActive := inRegistry && entry.Status == ModelLifecycleActive

		hints := make(map[string]string, len(perSource))
		sources := make([]string, 0, len(perSource))
		prices := make(map[string]string, len(perSource))
		var windowMax int64
		for source, row := range perSource {
			hints[source] = row.ProviderHint
			sources = append(sources, source)
			if row.PriceJSON != "" {
				prices[source] = row.PriceJSON
			}
			if row.ContextWindow > windowMax {
				windowMax = row.ContextWindow
			}
		}
		sort.Strings(sources)
		// Deterministic display name: first non-empty in sorted source order.
		display := ""
		for _, source := range sources {
			if perSource[source].DisplayName != "" {
				display = perSource[source].DisplayName
				break
			}
		}

		confidence, order := ClassifyCatalogConfidence(registryActive, hints)
		view := CatalogCandidateView{
			CanonicalModelID:  id,
			Confidence:        confidence,
			QueueOrder:        order,
			ProviderHints:     hints,
			Sources:           sources,
			DisplayName:       display,
			ContextWindow:     windowMax,
			PriceJSONBySource: prices,
		}
		if inRegistry {
			provider := string(entry.Provider)
			view.RegistryProvider = &provider
			view.RegistryStatus = entry.Status
		}
		views = append(views, view)
	}
	sort.SliceStable(views, func(i, j int) bool {
		if views[i].QueueOrder != views[j].QueueOrder {
			return views[i].QueueOrder < views[j].QueueOrder
		}
		return views[i].CanonicalModelID < views[j].CanonicalModelID
	})
	return views, nil
}

// MissingModels reports registry-active models absent from every latest
// accepted external snapshot. Disappearance yields catalog_missing records only
// — never a candidate, never a registry change.
func (s *ModelCatalogCandidateService) MissingModels(ctx context.Context) ([]CatalogMissingView, error) {
	_, byModel, err := s.latestEvidence(ctx)
	if err != nil {
		return nil, err
	}
	regSnapshot, err := s.registry.GetSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	views := []CatalogMissingView{}
	for id, entry := range regSnapshot.Entries {
		if entry.Status != ModelLifecycleActive {
			continue
		}
		if _, present := byModel[id]; present {
			continue
		}
		views = append(views, CatalogMissingView{CanonicalModelID: id, RegistryProvider: string(entry.Provider)})
	}
	sort.Slice(views, func(i, j int) bool { return views[i].CanonicalModelID < views[j].CanonicalModelID })
	return views, nil
}

// RetirementSuggestions flags registry models absent from at least three
// accepted snapshots spanning at least seven days. Suggestions are advisory
// views only.
func (s *ModelCatalogCandidateService) RetirementSuggestions(ctx context.Context) ([]CatalogRetirementSuggestionView, error) {
	rows, err := s.reader.MissingEvidenceSummary(ctx)
	if err != nil {
		return nil, err
	}
	views := []CatalogRetirementSuggestionView{}
	for _, row := range rows {
		if row.AcceptedRunCount < retirementMinAcceptedRuns {
			continue
		}
		spanDays := row.LastMissingAt.Sub(row.FirstMissingAt).Hours() / 24
		if spanDays < float64(retirementMinSpanDays) {
			continue
		}
		views = append(views, CatalogRetirementSuggestionView{
			CanonicalModelID: row.CanonicalModelID,
			MissingRunCount:  row.AcceptedRunCount,
			FirstMissingAt:   row.FirstMissingAt,
			LastMissingAt:    row.LastMissingAt,
			SpanDays:         spanDays,
		})
	}
	sort.Slice(views, func(i, j int) bool { return views[i].CanonicalModelID < views[j].CanonicalModelID })
	return views, nil
}

// PriceAnomalies reports cross-source price divergence above the anomaly
// threshold. External price is anomaly evidence only — never a production price.
func (s *ModelCatalogCandidateService) PriceAnomalies(ctx context.Context) ([]CatalogPriceAnomalyView, error) {
	_, byModel, err := s.latestEvidence(ctx)
	if err != nil {
		return nil, err
	}
	views := []CatalogPriceAnomalyView{}
	ids := make([]string, 0, len(byModel))
	for id := range byModel {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		perSource := byModel[id]
		inputs := make(map[string]float64, len(perSource))
		priceJSONs := make(map[string]string, len(perSource))
		for source, row := range perSource {
			if row.PriceJSON == "" {
				continue
			}
			value, err := extractInputPrice(row.PriceJSON)
			if err != nil {
				continue
			}
			inputs[source] = value
			priceJSONs[source] = row.PriceJSON
		}
		if len(inputs) < 2 {
			continue
		}
		min, max := 0.0, 0.0
		first := true
		for _, v := range inputs {
			if first {
				min, max = v, v
				first = false
				continue
			}
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
		if min <= 0 {
			continue
		}
		spread := (max - min) / min * 100
		if spread <= priceAnomalySpreadPercent {
			continue
		}
		views = append(views, CatalogPriceAnomalyView{
			CanonicalModelID:   id,
			PerSourcePriceJSON: priceJSONs,
			MaxSpreadPercent:   spread,
		})
	}
	return views, nil
}

func extractInputPrice(priceJSON string) (float64, error) {
	var parsed map[string]any
	if err := json.Unmarshal([]byte(priceJSON), &parsed); err != nil {
		return 0, err
	}
	raw, ok := parsed["input"]
	if !ok {
		raw, ok = parsed["prompt"]
	}
	if !ok {
		return 0, fmt.Errorf("no input price key")
	}
	switch value := raw.(type) {
	case float64:
		return value, nil
	case string:
		return strconv.ParseFloat(strings.TrimSpace(value), 64)
	default:
		return 0, fmt.Errorf("unsupported price type %T", raw)
	}
}

// UpdateSourceSetting changes enablement/threshold for one source and appends an
// audit record. Threshold changes are audited per plan.
// UpdateSourceSetting changes enablement and/or the drop threshold for one
// source and appends an audit record. Nil fields mean "leave unchanged": a
// partial update never clobbers the other setting. Threshold changes are
// audited per plan.
func (s *ModelCatalogCandidateService) UpdateSourceSetting(ctx context.Context, actorID, source string, enabled *bool, thresholdPercent *int) error {
	if !ValidCatalogSource(source) {
		return fmt.Errorf("invalid catalog source: %q", source)
	}
	if thresholdPercent != nil && (*thresholdPercent < 1 || *thresholdPercent > 100) {
		return fmt.Errorf("count drop threshold must be within 1..100: %d", *thresholdPercent)
	}
	current, err := s.settingFor(ctx, source)
	if err != nil {
		return err
	}
	newEnabled := current.Enabled
	if enabled != nil {
		newEnabled = *enabled
	}
	threshold := current.CountDropThresholdPercent
	if thresholdPercent != nil {
		threshold = *thresholdPercent
	}
	if s.audit == nil {
		return fmt.Errorf("audit repository is not configured")
	}
	if err := s.reader.UpdateSourceSetting(ctx, source, newEnabled, threshold); err != nil {
		return err
	}
	payload, _ := json.Marshal(map[string]any{
		"source":            source,
		"enabled":           newEnabled,
		"threshold_percent": threshold,
	})
	return s.audit.Insert(ctx, &AuditLog{
		ActorEmail:  "system:" + actorID,
		ActorRole:   "admin",
		AuthMethod:  "gateway-admin",
		Action:      "model_catalog.source_settings.update",
		Method:      "PUT",
		Path:        "/api/internal/v1/gateway-admin/model-catalog/sources/" + source + "/settings",
		RequestBody: string(payload),
	})
}

// ---------- scheduled ingestion ----------

type catalogSourcePlan struct {
	source string
	url    string
	// versionOf derives the immutable external version for fetched bytes.
	fetch func(ctx context.Context) (raw []byte, version string, url string, err error)
	parse func(raw []byte) (*CatalogParseSummary, error)
}

func (s *ModelCatalogCandidateService) planFor(source string) (*catalogSourcePlan, error) {
	switch source {
	case CatalogSourceOpenRouter:
		adapter := NewOpenRouterCatalogAdapter(s.maxItems)
		return &catalogSourcePlan{
			source: source,
			url:    openRouterModelsURL,
			fetch: func(ctx context.Context) ([]byte, string, string, error) {
				raw, err := s.fetcher.Get(ctx, openRouterModelsURL)
				if err != nil {
					return nil, "", "", err
				}
				sum := sha256.Sum256(raw)
				return raw, fmt.Sprintf("sha256:%x", sum), openRouterModelsURL, nil
			},
			parse: adapter.Parse,
		}, nil
	case CatalogSourceModelsDev:
		adapter := NewModelsDevCatalogAdapter(s.maxItems)
		return &catalogSourcePlan{
			source: source,
			fetch: func(ctx context.Context) ([]byte, string, string, error) {
				resolver := NewGitHubCatalogCommitResolver(s.fetcher)
				commit, err := resolver.ResolveCommit(ctx, ModelsDevOwner, ModelsDevRepo, ModelsDevBranch)
				if err != nil {
					return nil, "", "", err
				}
				rawURL, err := RawGitHubURL(ModelsDevOwner, ModelsDevRepo, commit, ModelsDevDataPath)
				if err != nil {
					return nil, "", "", err
				}
				raw, err := s.fetcher.Get(ctx, rawURL)
				if err != nil {
					return nil, "", "", err
				}
				return raw, commit, rawURL, nil
			},
			parse: adapter.Parse,
		}, nil
	case CatalogSourceLiteLLM:
		adapter := NewLiteLLMCatalogAdapter(s.maxItems)
		return &catalogSourcePlan{
			source: source,
			fetch: func(ctx context.Context) ([]byte, string, string, error) {
				resolver := NewGitHubCatalogCommitResolver(s.fetcher)
				commit, err := resolver.ResolveCommit(ctx, LiteLLMOwner, LiteLLMRepo, LiteLLMBranch)
				if err != nil {
					return nil, "", "", err
				}
				rawURL, err := RawGitHubURL(LiteLLMOwner, LiteLLMRepo, commit, LiteLLMDataPath)
				if err != nil {
					return nil, "", "", err
				}
				raw, err := s.fetcher.Get(ctx, rawURL)
				if err != nil {
					return nil, "", "", err
				}
				return raw, commit, rawURL, nil
			},
			parse: adapter.Parse,
		}, nil
	default:
		return nil, fmt.Errorf("invalid catalog source: %q", source)
	}
}

// ingestOne runs one bounded ingestion cycle for one source. It returns the
// error (if any) so callers can group alerts per source.
func (s *ModelCatalogCandidateService) IngestSource(ctx context.Context, source string) error {
	if s.writer == nil || s.fetcher == nil || s.reader == nil || s.registry == nil {
		return fmt.Errorf("model catalog ingestion dependencies are not configured")
	}
	setting, err := s.settingFor(ctx, source)
	if err != nil {
		return err
	}
	if !setting.Enabled {
		return nil
	}
	plan, err := s.planFor(source)
	if err != nil {
		return err
	}

	runID, err := s.writer.StartSyncRun(ctx, StartCatalogSyncRunInput{
		Source:      source,
		TriggeredBy: CatalogSyncTriggerScheduled,
		RequestURL:  plan.url,
	})
	if err != nil {
		return err
	}

	raw, version, url, err := plan.fetch(ctx)
	if err != nil {
		return s.failRun(ctx, runID, source, err)
	}

	summary, err := plan.parse(raw)
	if err != nil {
		return s.failRun(ctx, runID, source, err)
	}

	// Disappearance evidence: registry-active ids absent from this payload.
	missing, err := s.missingForPayload(ctx, summary.Candidates)
	if err != nil {
		return s.failRun(ctx, runID, source, err)
	}

	_, err = s.writer.InsertSnapshotWithEvidence(ctx, CatalogSnapshotInput{
		SyncRunID:       runID,
		Source:          source,
		ExternalVersion: version,
		RawPayload:      raw,
	}, summary.Candidates, missing)
	if err != nil {
		return s.failRun(ctx, runID, source, err)
	}

	if err := s.writer.FinishSyncRun(ctx, runID, CatalogSyncStatusSucceeded, len(summary.Candidates), version, ""); err != nil {
		return err
	}
	_ = url
	return nil
}

func (s *ModelCatalogCandidateService) failRun(ctx context.Context, runID int64, source string, cause error) error {
	const truncatedLen = 800
	runes := []rune(cause.Error())
	message := string(runes)
	if len(runes) > truncatedLen {
		message = string(runes[:truncatedLen])
	}
	if err := s.writer.FinishSyncRun(ctx, runID, CatalogSyncStatusFailed, 0, "", message); err != nil {
		log.Printf("[model-catalog] source %s: finish failed run %d: %v (cause: %s)", source, runID, err, message)
	}
	return cause
}

func (s *ModelCatalogCandidateService) settingFor(ctx context.Context, source string) (CatalogSourceSetting, error) {
	settings, err := s.reader.SourceSettings(ctx)
	if err != nil {
		return CatalogSourceSetting{}, err
	}
	for _, setting := range settings {
		if setting.Source == source {
			return setting, nil
		}
	}
	return CatalogSourceSetting{}, fmt.Errorf("no settings for catalog source %q", source)
}

// missingForPayload diffs registry-active canonical ids against the payload ids.
func (s *ModelCatalogCandidateService) missingForPayload(ctx context.Context, candidates []CatalogCandidateEvidenceInput) ([]CatalogMissingEvidenceInput, error) {
	regSnapshot, err := s.registry.GetSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	payloadIDs := make(map[string]struct{}, len(candidates))
	for _, c := range candidates {
		payloadIDs[c.CanonicalModelID] = struct{}{}
		for _, alias := range c.Aliases {
			payloadIDs[alias] = struct{}{}
		}
	}
	missing := make([]CatalogMissingEvidenceInput, 0)
	for id, entry := range regSnapshot.Entries {
		if entry.Status != ModelLifecycleActive {
			continue
		}
		if _, ok := payloadIDs[id]; ok {
			continue
		}
		detail, _ := json.Marshal(map[string]any{
			"registry_provider": string(entry.Provider),
		})
		missing = append(missing, CatalogMissingEvidenceInput{CanonicalModelID: id, DetailJSON: string(detail)})
	}
	sort.Slice(missing, func(i, j int) bool { return missing[i].CanonicalModelID < missing[j].CanonicalModelID })
	return missing, nil
}

// IngestAll runs one full cycle over all enabled sources and emits grouped
// alerts — at most one per source regardless of how many failures occurred.
func (s *ModelCatalogCandidateService) IngestAll(ctx context.Context) {
	sources := []string{CatalogSourceOpenRouter, CatalogSourceModelsDev, CatalogSourceLiteLLM}
	failures := make(map[string]string, len(sources))
	for _, source := range sources {
		if err := s.IngestSource(ctx, source); err != nil {
			failures[source] = err.Error()
		}
	}
	for _, source := range sources {
		if message, ok := failures[source]; ok {
			s.alerts.AlertGrouped(ctx, source, "ingestion_failed", message)
		}
	}
}

// ModelCatalogCandidateViews is the read-only view contract consumed by the
// admin handler. The concrete service satisfies it.
type ModelCatalogCandidateViews interface {
	SourceStatus(ctx context.Context) ([]CatalogSourceStatusView, error)
	CandidateList(ctx context.Context) ([]CatalogCandidateView, error)
	RetirementSuggestions(ctx context.Context) ([]CatalogRetirementSuggestionView, error)
	PriceAnomalies(ctx context.Context) ([]CatalogPriceAnomalyView, error)
	UpdateSourceSetting(ctx context.Context, actorID, source string, enabled *bool, thresholdPercent *int) error
}

var _ ModelCatalogCandidateViews = (*ModelCatalogCandidateService)(nil)
