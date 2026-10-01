package service

import (
	"context"
	"errors"
	"fmt"
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

const canonicalUSDWalletNominalRate = 7.2

var ErrCanonicalUSDWalletPolicy = errors.New("canonical USD wallet policy is missing or incompatible")

// Canonical wallets retain CNY storage and cny-e8-v1. Only explicitly enabled,
// ShipAny-linked CNY users have a USD face value; native USD users are unchanged.
func canonicalUSDWalletSnapshot(user *User, cfg *config.Config) (ExchangeRateSnapshot, bool, error) {
	if cfg == nil || !cfg.CanonicalWallet.USDWalletEnabled || user == nil ||
		strings.TrimSpace(user.PlatformUserID) == "" || strings.ToUpper(strings.TrimSpace(user.BillingCurrency)) != CurrencyCNY {
		return ExchangeRateSnapshot{}, false, nil
	}
	if strings.TrimSpace(cfg.CanonicalWallet.USDPolicyVersion) != config.CanonicalUSDWalletPolicyVersion {
		return ExchangeRateSnapshot{}, true, ErrCanonicalUSDWalletPolicy
	}
	return ExchangeRateSnapshot{
		BaseCurrency: CurrencyUSD, QuoteCurrency: CurrencyCNY, Rate: canonicalUSDWalletNominalRate,
		Source: config.CanonicalUSDWalletPolicyVersion, AsOf: time.Now().UTC(),
	}, true, nil
}

func validateCanonicalUSDWalletSnapshot(snapshot ExchangeRateSnapshot, version string) error {
	if version != config.CanonicalUSDWalletPolicyVersion || snapshot.Source != version ||
		snapshot.BaseCurrency != CurrencyUSD || snapshot.QuoteCurrency != CurrencyCNY || snapshot.Rate != canonicalUSDWalletNominalRate {
		return ErrCanonicalUSDWalletPolicy
	}
	return nil
}

// Used by preflight, Freeze and Live admission. A fixed policy never falls
// back to market FX, even when the rate service is unavailable.
func resolveBillingExchangeRate(ctx context.Context, user *User, fx *ExchangeRateService, cfg *config.Config) (ExchangeRateSnapshot, error) {
	fixed, enabled, err := canonicalUSDWalletSnapshot(user, cfg)
	if err != nil {
		return ExchangeRateSnapshot{}, err
	}
	currency := NormalizeUserBillingCurrency(user.BillingCurrency)
	if pinned, ok := pinnedBillingSettlementSnapshot(ctx, CurrencyUSD, currency); ok {
		if enabled {
			if err := validateCanonicalUSDWalletSnapshot(pinned, fixed.Source); err != nil {
				return ExchangeRateSnapshot{}, err
			}
		}
		return pinned, nil
	}
	if enabled {
		return fixed, nil
	}
	if fx == nil {
		return ExchangeRateSnapshot{}, fmt.Errorf("exchange-rate service is unavailable")
	}
	return fx.Snapshot(ctx, CurrencyUSD, currency)
}

// ResolveCostSettlement decides the settlement currency strategy for a usage
// record. Historically this was hard-wired to RunMode (simple => pinned USD);
// billing.settlement.currency_mode decouples the two concerns: unset follows
// RunMode (legacy behavior, zero change for existing deployments), fixed_usd
// pins USD 1:1, user settles in the user's billing currency.
func ResolveCostSettlement(ctx context.Context, cost *CostBreakdown, user *User, subscriptionBilling bool, fx *ExchangeRateService, cfg *config.Config) (CostSettlementSnapshot, error) {
	if !subscriptionBilling {
		if holder, _ := ctx.Value(billingSettlementContextKey{}).(*billingSettlementSnapshots); holder != nil && holder.frozen {
			if holder.walletPolicyVersion != "" {
				snapshot, ok := pinnedBillingSettlementSnapshot(ctx, CurrencyUSD, CurrencyCNY)
				if !ok || validateCanonicalUSDWalletSnapshot(snapshot, holder.walletPolicyVersion) != nil {
					return CostSettlementSnapshot{}, ErrCanonicalUSDWalletPolicy
				}
			}
			// A frozen historical call retains its old FX after policy activation.
			// Without a USD rollout, retain the legacy run-mode selection below.
			if holder.walletPolicyVersion != "" || (cfg != nil && cfg.CanonicalWallet.USDWalletEnabled && user != nil && strings.TrimSpace(user.PlatformUserID) != "" && holder.frozenCurrency == CurrencyCNY) {
				return settleUsageCost(ctx, cost, &User{BillingCurrency: holder.frozenCurrency}, false, fx)
			}
		}
		_, enabled, err := canonicalUSDWalletSnapshot(user, cfg)
		if err != nil {
			return CostSettlementSnapshot{}, err
		}
		if enabled {
			snapshot, err := resolveBillingExchangeRate(ctx, user, fx, cfg)
			if err != nil {
				return CostSettlementSnapshot{}, err
			}
			ctx = WithBillingSettlementContext(ctx)
			storeBillingSettlementSnapshot(ctx, snapshot)
			return settleUsageCost(ctx, cost, user, false, fx)
		}
	}
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
	snapshot, ok := pinnedBillingSettlementSnapshot(ctx, CurrencyUSD, currency)
	if !ok {
		if fx == nil {
			return CostSettlementSnapshot{}, fmt.Errorf("exchange-rate service is unavailable")
		}
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
