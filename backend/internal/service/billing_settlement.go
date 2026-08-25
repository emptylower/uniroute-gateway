package service

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
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

const (
	// SettlementCurrencyModeFixedUSD pins settlement to USD at rate 1.0,
	// regardless of the user's billing currency.
	SettlementCurrencyModeFixedUSD = "fixed_usd"
	// SettlementCurrencyModeUser settles in the user's billing currency via
	// the exchange-rate service.
	SettlementCurrencyModeUser = "user"
)

const (
	exchangeRateSourceSimpleMode = "simple_mode"
	exchangeRateSourceFixedUSD   = "fixed_usd"
)

// NormalizeSettlementCurrencyMode trims/lowercases the configured mode and
// maps unknown values to "" (follow RunMode, the legacy behavior).
func NormalizeSettlementCurrencyMode(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case SettlementCurrencyModeFixedUSD:
		return SettlementCurrencyModeFixedUSD
	case SettlementCurrencyModeUser:
		return SettlementCurrencyModeUser
	default:
		return ""
	}
}

// ResolveCostSettlement decides the settlement currency strategy for a usage
// record. Historically this was hard-wired to RunMode (simple => pinned USD);
// billing.settlement.currency_mode decouples the two concerns: unset follows
// RunMode (legacy behavior, zero change for existing deployments), fixed_usd
// pins USD 1:1, user settles in the user's billing currency.
func ResolveCostSettlement(ctx context.Context, cost *CostBreakdown, user *User, subscriptionBilling bool, fx *ExchangeRateService, cfg *config.Config) (CostSettlementSnapshot, error) {
	mode := ""
	runMode := ""
	if cfg != nil {
		mode = NormalizeSettlementCurrencyMode(cfg.Billing.Settlement.CurrencyMode)
		runMode = cfg.RunMode
	}
	if mode == "" {
		if runMode == config.RunModeSimple {
			return fixedUSDSettlement(cost, exchangeRateSourceSimpleMode), nil
		}
		mode = SettlementCurrencyModeUser
	}
	if mode == SettlementCurrencyModeFixedUSD {
		return fixedUSDSettlement(cost, exchangeRateSourceFixedUSD), nil
	}
	return settleUsageCost(ctx, cost, user, subscriptionBilling, fx)
}

func fixedUSDSettlement(cost *CostBreakdown, source string) CostSettlementSnapshot {
	settlement := CostSettlementSnapshot{
		SourceCurrency: CurrencyUSD, SettlementCurrency: CurrencyUSD,
		ExchangeRate: 1, ExchangeRateSource: source, ExchangeRateAsOf: time.Now().UTC(),
	}
	if cost != nil {
		settlement.SourceCost = cost.TotalCost
		settlement.BaseCost = cost.TotalCost
	}
	return settlement
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
