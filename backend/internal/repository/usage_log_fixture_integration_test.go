//go:build integration

package repository

import (
	"context"
	"time"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

type usageLogTestRepository struct {
	*usageLogRepository
}

func newUsageLogTestRepository(client *dbent.Client, sqlq sqlExecutor) *usageLogTestRepository {
	return &usageLogTestRepository{usageLogRepository: newUsageLogRepositoryWithSQL(client, sqlq)}
}

func (r *usageLogTestRepository) Create(ctx context.Context, log *service.UsageLog) (bool, error) {
	completeUsageLogTestCurrencySnapshot(log)
	return r.usageLogRepository.Create(ctx, log)
}

func (r *usageLogTestRepository) CreateBestEffort(ctx context.Context, log *service.UsageLog) error {
	completeUsageLogTestCurrencySnapshot(log)
	return r.usageLogRepository.CreateBestEffort(ctx, log)
}

func completeUsageLogTestCurrencySnapshot(log *service.UsageLog) {
	if log == nil || log.SourceCurrency != "" || log.SettlementCurrency != "" ||
		log.ExchangeRate != 0 || log.ExchangeRateSource != "" || log.ExchangeRateAsOf != nil {
		return
	}
	asOf := log.CreatedAt
	if asOf.IsZero() {
		asOf = time.Now().UTC()
	}
	log.SourceCurrency = service.CurrencyUSD
	log.SettlementCurrency = service.CurrencyUSD
	log.ExchangeRate = 1
	log.ExchangeRateSource = "identity"
	log.ExchangeRateAsOf = &asOf
}
