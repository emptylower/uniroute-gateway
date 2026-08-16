package service

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type billingSettlementContextKey struct{}

type billingSettlementSnapshots struct {
	mu        sync.RWMutex
	snapshots map[string]ExchangeRateSnapshot
}

// WithBillingSettlementContext installs a request-scoped holder. Eligibility
// checks pin FX here before forwarding; async usage recording inherits it.
func WithBillingSettlementContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Value(billingSettlementContextKey{}).(*billingSettlementSnapshots); ok {
		return ctx
	}
	return context.WithValue(ctx, billingSettlementContextKey{}, &billingSettlementSnapshots{snapshots: make(map[string]ExchangeRateSnapshot)})
}

// InheritBillingSettlementContext copies only the immutable request-scoped FX
// holder into a worker context. It deliberately does not retain the request's
// cancellation deadline or other large values.
func InheritBillingSettlementContext(parent, worker context.Context) context.Context {
	if worker == nil {
		worker = context.Background()
	}
	if parent == nil {
		return worker
	}
	holder, _ := parent.Value(billingSettlementContextKey{}).(*billingSettlementSnapshots)
	if holder == nil {
		return worker
	}
	return context.WithValue(worker, billingSettlementContextKey{}, holder)
}

func storeBillingSettlementSnapshot(ctx context.Context, snapshot ExchangeRateSnapshot) {
	holder, _ := ctx.Value(billingSettlementContextKey{}).(*billingSettlementSnapshots)
	if holder == nil {
		return
	}
	holder.mu.Lock()
	holder.snapshots[snapshot.BaseCurrency+"/"+snapshot.QuoteCurrency] = snapshot
	holder.mu.Unlock()
}

func pinnedBillingSettlementSnapshot(ctx context.Context, base, quote string) (ExchangeRateSnapshot, bool) {
	holder, _ := ctx.Value(billingSettlementContextKey{}).(*billingSettlementSnapshots)
	if holder == nil {
		return ExchangeRateSnapshot{}, false
	}
	holder.mu.RLock()
	snapshot, ok := holder.snapshots[base+"/"+quote]
	holder.mu.RUnlock()
	return snapshot, ok
}

type CostSettlementSnapshot struct {
	SourceCurrency     string
	SettlementCurrency string
	ExchangeRate       float64
	ExchangeRateSource string
	ExchangeRateAsOf   time.Time
	SourceCost         float64
	BaseCost           float64
}

func settleUsageCost(ctx context.Context, cost *CostBreakdown, user *User, subscriptionBilling bool, fx *ExchangeRateService) (CostSettlementSnapshot, error) {
	currency := CurrencyUSD
	if !subscriptionBilling && user != nil {
		currency = NormalizeUserBillingCurrency(user.BillingCurrency)
	}
	if fx == nil {
		return CostSettlementSnapshot{}, fmt.Errorf("exchange-rate service is unavailable")
	}
	snapshot, ok := pinnedBillingSettlementSnapshot(ctx, CurrencyUSD, currency)
	if !ok {
		var err error
		snapshot, err = fx.Snapshot(ctx, CurrencyUSD, currency)
		if err != nil {
			return CostSettlementSnapshot{}, fmt.Errorf("resolve %s/%s settlement rate: %w", CurrencyUSD, currency, err)
		}
	}
	settlement := CostSettlementSnapshot{
		SourceCurrency: CurrencyUSD, SettlementCurrency: currency,
		ExchangeRate: snapshot.Rate, ExchangeRateSource: snapshot.Source,
		ExchangeRateAsOf: snapshot.AsOf,
	}
	if cost == nil {
		return settlement, nil
	}
	settlement.SourceCost = cost.TotalCost
	settlement.BaseCost = cost.TotalCost * snapshot.Rate
	if cost.TotalCost > 0 {
		effectiveMultiplier := cost.ActualCost / cost.TotalCost
		cost.ActualCost = settlement.BaseCost * effectiveMultiplier
	} else {
		cost.ActualCost *= snapshot.Rate
	}
	return settlement, nil
}

func applySettlementSnapshot(log *UsageLog, settlement CostSettlementSnapshot) {
	if log == nil {
		return
	}
	log.SourceCurrency = settlement.SourceCurrency
	log.SettlementCurrency = settlement.SettlementCurrency
	log.ExchangeRate = settlement.ExchangeRate
	log.ExchangeRateSource = settlement.ExchangeRateSource
	log.ExchangeRateAsOf = &settlement.ExchangeRateAsOf
	log.SourceCost = settlement.SourceCost
	log.BaseCost = settlement.BaseCost
}
