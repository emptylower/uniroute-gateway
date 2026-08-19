package service

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/domain"
)

type InventoryItem struct {
	AccountID               int64  `json:"account_id"`
	GroupID                 *int64 `json:"group_id"`
	ChannelID               *int64 `json:"channel_id"`
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

type InventoryAccountProjection struct {
	AccountID        int64    `json:"account_id"`
	UpstreamModelIDs []string `json:"upstream_model_ids"`
}

// InventoryAccountProjector keeps runtime mapping semantics in the service layer
// while allowing the repository to own the consistent database snapshot.
type InventoryAccountProjector func(accounts []Account) []InventoryAccountProjection

type ModelGovernanceInventoryService struct {
	repo ModelGovernanceInventoryRepository
	now  func() time.Time
}

func NewModelGovernanceInventoryService(repo ModelGovernanceInventoryRepository) *ModelGovernanceInventoryService {
	return &ModelGovernanceInventoryService{repo: repo, now: time.Now}
}

func (s *ModelGovernanceInventoryService) List(ctx context.Context) ([]InventoryItem, error) {
	windowEnd := s.now().UTC()
	return s.repo.List(ctx, ProjectInventoryAccountMappingsForAccounts, windowEnd.Add(-7*24*time.Hour), windowEnd.Add(-30*24*time.Hour), windowEnd)
}

// ProjectInventoryAccountMappingsForAccounts derives finite runtime mappings for a repository snapshot.
func ProjectInventoryAccountMappingsForAccounts(accounts []Account) []InventoryAccountProjection {
	projections := make([]InventoryAccountProjection, 0, len(accounts))
	for i := range accounts {
		projections = append(projections, ProjectInventoryAccountMappings(&accounts[i]))
	}
	return projections
}

// ProjectInventoryAccountMappings enumerates finite effective upstream IDs only.
// Generic allow-all behavior and wildcard namespaces are intentionally absent;
// wildcard requested keys may still contribute concrete, wildcard-free targets.
func ProjectInventoryAccountMappings(account *Account) InventoryAccountProjection {
	if account == nil {
		return InventoryAccountProjection{}
	}
	seen := make(map[string]struct{})
	add := func(model string) {
		if model == "" || strings.Contains(model, "*") {
			return
		}
		seen[model] = struct{}{}
	}

	mapping := account.GetModelMapping()
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
			upstream = normalizeOpenAIModelForUpstream(account, upstream)
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
	if account.AllowsOpenAICompact() {
		for _, upstream := range account.GetCompactModelMapping() {
			add(strings.TrimSpace(upstream))
		}
	}
	if account.IsBedrock() {
		for requested := range domain.DefaultBedrockModelMapping {
			if resolved, ok := ResolveBedrockModelID(account, requested); ok {
				add(resolved)
			}
		}
	}

	models := make([]string, 0, len(seen))
	for model := range seen {
		models = append(models, model)
	}
	sort.Strings(models)
	if len(models) == 0 {
		models = nil
	}
	return InventoryAccountProjection{AccountID: account.ID, UpstreamModelIDs: models}
}
