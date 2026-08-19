package service

import (
	"context"
	"time"
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
	List(ctx context.Context, cutoff7d, cutoff30d time.Time) ([]InventoryItem, error)
}

type ModelGovernanceInventoryService struct {
	repo ModelGovernanceInventoryRepository
	now  func() time.Time
}

func NewModelGovernanceInventoryService(repo ModelGovernanceInventoryRepository) *ModelGovernanceInventoryService {
	return &ModelGovernanceInventoryService{repo: repo, now: time.Now}
}

func (s *ModelGovernanceInventoryService) List(ctx context.Context) ([]InventoryItem, error) {
	now := s.now().UTC()
	return s.repo.List(ctx, now.Add(-7*24*time.Hour), now.Add(-30*24*time.Hour))
}
