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
	List(ctx context.Context, projections []InventoryAccountProjection, cutoff7d, cutoff30d, windowEnd time.Time) ([]InventoryItem, error)
}

type ModelGovernanceInventoryAccountSource interface {
	ListInventoryAccounts(ctx context.Context) ([]Account, error)
}

type InventoryAccountProjection struct {
	AccountID        int64    `json:"account_id"`
	UpstreamModelIDs []string `json:"upstream_model_ids"`
}

type ModelGovernanceInventoryService struct {
	repo     ModelGovernanceInventoryRepository
	accounts ModelGovernanceInventoryAccountSource
	now      func() time.Time
}

func NewModelGovernanceInventoryService(repo ModelGovernanceInventoryRepository, accounts ModelGovernanceInventoryAccountSource) *ModelGovernanceInventoryService {
	return &ModelGovernanceInventoryService{repo: repo, accounts: accounts, now: time.Now}
}

func (s *ModelGovernanceInventoryService) List(ctx context.Context) ([]InventoryItem, error) {
	windowEnd := s.now().UTC()
	accounts, err := s.accounts.ListInventoryAccounts(ctx)
	if err != nil {
		return nil, err
	}
	projections := make([]InventoryAccountProjection, 0, len(accounts))
	for i := range accounts {
		projections = append(projections, ProjectInventoryAccountMappings(&accounts[i]))
	}
	return s.repo.List(ctx, projections, windowEnd.Add(-7*24*time.Hour), windowEnd.Add(-30*24*time.Hour), windowEnd)
}

// ProjectInventoryAccountMappings enumerates finite effective upstream IDs only.
// Generic allow-all behavior and wildcard namespaces are intentionally absent;
// concrete wildcard targets remain finite values and are included.
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
		add(upstream)
	}
	for _, upstream := range account.GetCompactModelMapping() {
		add(upstream)
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
