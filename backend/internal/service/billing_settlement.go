package service

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

type billingSettlementContextKey struct{}

type billingSettlementSnapshots struct {
	mu                  sync.RWMutex
	snapshots           map[string]ExchangeRateSnapshot
	frozen              bool
	walletPolicyVersion string
	frozenCurrency      string
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

var ErrCanonicalUSDWalletPolicy = errors.New("canonical USD wallet policy is missing or incompatible")

func canonicalUSDWalletSnapshot(user *User, cfg *config.Config) (ExchangeRateSnapshot, bool, error) {
	if cfg == nil || !cfg.CanonicalWallet.USDWalletEnabled || user == nil || strings.TrimSpace(user.PlatformUserID) == "" {
		return ExchangeRateSnapshot{}, false, nil
	}
	if strings.TrimSpace(cfg.CanonicalWallet.USDPolicyVersion) != config.CanonicalUSDWalletPolicyVersion {
		return ExchangeRateSnapshot{}, true, ErrCanonicalUSDWalletPolicy
	}
	return ExchangeRateSnapshot{BaseCurrency: CurrencyUSD, QuoteCurrency: CurrencyUSD, Rate: 1,
		Source: config.CanonicalUSDWalletPolicyVersion, AsOf: time.Now().UTC()}, true, nil
}
func validateCanonicalUSDWalletSnapshot(snapshot ExchangeRateSnapshot, version string) error {
	if version != config.CanonicalUSDWalletPolicyVersion || snapshot.Source != version || snapshot.BaseCurrency != CurrencyUSD || snapshot.QuoteCurrency != CurrencyUSD || snapshot.Rate != 1 {
		return ErrCanonicalUSDWalletPolicy
	}
	return nil
}
func resolveBillingExchangeRate(ctx context.Context, user *User, _ *USDPriceService, cfg *config.Config) (ExchangeRateSnapshot, error) {
	fixed, enabled, err := canonicalUSDWalletSnapshot(user, cfg)
	if err != nil {
		return ExchangeRateSnapshot{}, err
	}
	if pinned, ok := pinnedBillingSettlementSnapshot(ctx, CurrencyUSD, CurrencyUSD); ok {
		if enabled && validateCanonicalUSDWalletSnapshot(pinned, fixed.Source) != nil {
			return ExchangeRateSnapshot{}, ErrCanonicalUSDWalletPolicy
		}
		return pinned, nil
	}
	if enabled {
		return fixed, nil
	}
	return ExchangeRateSnapshot{BaseCurrency: CurrencyUSD, QuoteCurrency: CurrencyUSD, Rate: 1, Source: "identity", AsOf: time.Now().UTC()}, nil
}

// Costs and all quota counters stay in their native USD units. Historical FX
// snapshots are audit facts and must never influence new settlement.
func ResolveCostSettlement(_ context.Context, cost *CostBreakdown, _ *User, _ bool, _ *USDPriceService, _ *config.Config) (CostSettlementSnapshot, error) {
	return fixedUSDSettlement(cost, CanonicalWalletUnitVersion), nil
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
