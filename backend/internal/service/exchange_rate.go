package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

type ExchangeRateSnapshot struct {
	BaseCurrency  string    `json:"base_currency"`
	QuoteCurrency string    `json:"quote_currency"`
	Rate          float64   `json:"rate"`
	Source        string    `json:"source"`
	AsOf          time.Time `json:"as_of"`
	FetchedAt     time.Time `json:"fetched_at"`
	ExpiresAt     time.Time `json:"expires_at"`
	Fallback      bool      `json:"fallback"`
}

type ExchangeRateProvider interface {
	Fetch(ctx context.Context, baseCurrency, quoteCurrency string) (ExchangeRateSnapshot, error)
}

type httpExchangeRateProvider struct {
	provider string
	endpoint string
	apiKey   string
	client   *http.Client
}

type exchangeRateHTTPResponse struct {
	Base  string             `json:"base"`
	Date  string             `json:"date"`
	Rates map[string]float64 `json:"rates"`
	Meta  struct {
		LastUpdatedAt string `json:"last_updated_at"`
	} `json:"meta"`
	Data map[string]struct {
		Code  string  `json:"code"`
		Value float64 `json:"value"`
	} `json:"data"`
}

func (p *httpExchangeRateProvider) Fetch(ctx context.Context, baseCurrency, quoteCurrency string) (ExchangeRateSnapshot, error) {
	if p == nil || strings.TrimSpace(p.endpoint) == "" {
		return ExchangeRateSnapshot{}, errors.New("exchange-rate provider URL is not configured")
	}
	endpoint, err := url.Parse(p.endpoint)
	if err != nil {
		return ExchangeRateSnapshot{}, fmt.Errorf("parse exchange-rate provider URL: %w", err)
	}
	query := endpoint.Query()
	if p.provider == "currencyapi" {
		if strings.TrimSpace(p.apiKey) == "" {
			return ExchangeRateSnapshot{}, errors.New("currencyapi API key is not configured")
		}
		query.Set("base_currency", baseCurrency)
		query.Set("currencies", quoteCurrency)
	} else {
		query.Set("from", baseCurrency)
		query.Set("to", quoteCurrency)
	}
	endpoint.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return ExchangeRateSnapshot{}, err
	}
	if p.provider == "currencyapi" {
		req.Header.Set("apikey", p.apiKey)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return ExchangeRateSnapshot{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return ExchangeRateSnapshot{}, fmt.Errorf("exchange-rate provider returned HTTP %d", resp.StatusCode)
	}
	var payload exchangeRateHTTPResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return ExchangeRateSnapshot{}, fmt.Errorf("decode exchange-rate response: %w", err)
	}
	rate := payload.Rates[quoteCurrency]
	if item, ok := payload.Data[quoteCurrency]; ok {
		rate = item.Value
	}
	if !validExchangeRate(rate) {
		return ExchangeRateSnapshot{}, fmt.Errorf("exchange-rate provider returned invalid %s/%s rate", baseCurrency, quoteCurrency)
	}
	asOf := time.Now().UTC()
	if parsed, parseErr := time.Parse(time.RFC3339, payload.Meta.LastUpdatedAt); parseErr == nil {
		asOf = parsed.UTC()
	} else if parsed, parseErr := time.Parse("2006-01-02", payload.Date); parseErr == nil {
		asOf = parsed.UTC()
	}
	source := strings.TrimSpace(p.provider)
	if source == "" {
		source = "http_provider"
	}
	return ExchangeRateSnapshot{
		BaseCurrency: baseCurrency, QuoteCurrency: quoteCurrency, Rate: rate,
		Source: source, AsOf: asOf,
	}, nil
}

type ExchangeRateService struct {
	provider      ExchangeRateProvider
	bootstrapRate float64
	ttl           time.Duration
	staleTTL      time.Duration
	minUSDToCNY   float64
	maxUSDToCNY   float64
	maxAge        time.Duration
	maxFuture     time.Duration

	mu    sync.Mutex
	cache map[string]ExchangeRateSnapshot
}

func NewExchangeRateService(cfg *config.Config) *ExchangeRateService {
	providerURL := ""
	providerName := ""
	providerAPIKey := ""
	bootstrapRate := 0.0
	ttl := 15 * time.Minute
	staleTTL := 24 * time.Hour
	timeout := 2 * time.Second
	minUSDToCNY := 4.0
	maxUSDToCNY := 12.0
	maxAge := 48 * time.Hour
	maxFuture := 5 * time.Minute
	if cfg != nil {
		providerName = strings.ToLower(strings.TrimSpace(cfg.Billing.ExchangeRate.Provider))
		providerURL = strings.TrimSpace(cfg.Billing.ExchangeRate.ProviderURL)
		providerAPIKey = strings.TrimSpace(cfg.Billing.ExchangeRate.APIKey)
		if validExchangeRate(cfg.Billing.ExchangeRate.BootstrapUSDToCNY) {
			bootstrapRate = cfg.Billing.ExchangeRate.BootstrapUSDToCNY
		}
		if cfg.Billing.ExchangeRate.CacheTTLSeconds > 0 {
			ttl = time.Duration(cfg.Billing.ExchangeRate.CacheTTLSeconds) * time.Second
		}
		if cfg.Billing.ExchangeRate.StaleTTLSeconds > 0 {
			staleTTL = time.Duration(cfg.Billing.ExchangeRate.StaleTTLSeconds) * time.Second
		}
		if cfg.Billing.ExchangeRate.TimeoutSeconds > 0 {
			timeout = time.Duration(cfg.Billing.ExchangeRate.TimeoutSeconds) * time.Second
		}
		if cfg.Billing.ExchangeRate.MinUSDToCNY > 0 {
			minUSDToCNY = cfg.Billing.ExchangeRate.MinUSDToCNY
		}
		if cfg.Billing.ExchangeRate.MaxUSDToCNY > minUSDToCNY {
			maxUSDToCNY = cfg.Billing.ExchangeRate.MaxUSDToCNY
		}
		if cfg.Billing.ExchangeRate.MaxAgeSeconds > 0 {
			maxAge = time.Duration(cfg.Billing.ExchangeRate.MaxAgeSeconds) * time.Second
		}
		if cfg.Billing.ExchangeRate.MaxFutureSeconds >= 0 {
			maxFuture = time.Duration(cfg.Billing.ExchangeRate.MaxFutureSeconds) * time.Second
		}
	}
	return &ExchangeRateService{
		provider: &httpExchangeRateProvider{
			provider: providerName, endpoint: providerURL, apiKey: providerAPIKey,
			client: &http.Client{Timeout: timeout},
		},
		bootstrapRate: bootstrapRate, ttl: ttl, staleTTL: staleTTL,
		minUSDToCNY: minUSDToCNY, maxUSDToCNY: maxUSDToCNY,
		maxAge: maxAge, maxFuture: maxFuture,
		cache: make(map[string]ExchangeRateSnapshot),
	}
}

func validExchangeRate(rate float64) bool {
	return rate > 0 && !math.IsNaN(rate) && !math.IsInf(rate, 0)
}

func (s *ExchangeRateService) validateLiveSnapshot(snapshot ExchangeRateSnapshot, base, quote string, now time.Time) error {
	if !validExchangeRate(snapshot.Rate) {
		return errors.New("exchange-rate provider returned a non-positive or non-finite rate")
	}
	usdToCNY := snapshot.Rate
	if base == CurrencyCNY && quote == CurrencyUSD {
		usdToCNY = 1 / snapshot.Rate
	}
	minUSDToCNY, maxUSDToCNY := s.minUSDToCNY, s.maxUSDToCNY
	if minUSDToCNY <= 0 {
		minUSDToCNY = 4
	}
	if maxUSDToCNY <= minUSDToCNY {
		maxUSDToCNY = 12
	}
	if usdToCNY < minUSDToCNY || usdToCNY > maxUSDToCNY {
		return fmt.Errorf("exchange-rate provider returned implausible USD/CNY rate %.8f", usdToCNY)
	}
	if snapshot.AsOf.IsZero() {
		return errors.New("exchange-rate provider did not supply an as-of timestamp")
	}
	maxAge, maxFuture := s.maxAge, s.maxFuture
	if maxAge <= 0 {
		maxAge = 48 * time.Hour
	}
	if maxFuture <= 0 {
		maxFuture = 5 * time.Minute
	}
	if snapshot.AsOf.Before(now.Add(-maxAge)) {
		return fmt.Errorf("exchange-rate provider timestamp is older than %s", maxAge)
	}
	if snapshot.AsOf.After(now.Add(maxFuture)) {
		return fmt.Errorf("exchange-rate provider timestamp is too far in the future")
	}
	return nil
}

func (s *ExchangeRateService) Snapshot(ctx context.Context, baseCurrency, quoteCurrency string) (ExchangeRateSnapshot, error) {
	if s == nil {
		s = NewExchangeRateService(nil)
	}
	now := time.Now().UTC()
	base := strings.ToUpper(strings.TrimSpace(baseCurrency))
	quote := normalizeBillingCurrencyOrDefault(quoteCurrency)
	if base != CurrencyUSD && base != CurrencyCNY {
		return ExchangeRateSnapshot{}, fmt.Errorf("unsupported base currency %q", baseCurrency)
	}
	if base == quote {
		return ExchangeRateSnapshot{BaseCurrency: base, QuoteCurrency: quote, Rate: 1, Source: "identity", AsOf: now, FetchedAt: now, ExpiresAt: now.Add(s.ttl)}, nil
	}
	key := base + "/" + quote
	s.mu.Lock()
	cached, found := s.cache[key]
	s.mu.Unlock()
	if found && now.Before(cached.ExpiresAt) {
		return cached, nil
	}
	if s.provider != nil {
		live, err := s.provider.Fetch(ctx, base, quote)
		if err == nil && s.validateLiveSnapshot(live, base, quote, now) == nil {
			live.BaseCurrency = base
			live.QuoteCurrency = quote
			live.FetchedAt = now
			live.ExpiresAt = now.Add(s.ttl)
			live.Fallback = false
			s.mu.Lock()
			s.cache[key] = live
			s.mu.Unlock()
			return live, nil
		}
	}
	if found && now.Sub(cached.FetchedAt) <= s.staleTTL && s.validateLiveSnapshot(cached, base, quote, now) == nil {
		cached.Source = "stale_" + strings.TrimPrefix(cached.Source, "stale_")
		cached.Fallback = true
		cached.ExpiresAt = now.Add(s.ttl)
		staleLimit := cached.FetchedAt.Add(s.staleTTL)
		if cached.ExpiresAt.After(staleLimit) {
			cached.ExpiresAt = staleLimit
		}
		s.mu.Lock()
		s.cache[key] = cached
		s.mu.Unlock()
		return cached, nil
	}
	bootstrapProbe := ExchangeRateSnapshot{Rate: s.bootstrapRate, AsOf: now}
	if base == CurrencyCNY && quote == CurrencyUSD && validExchangeRate(s.bootstrapRate) {
		bootstrapProbe.Rate = 1 / s.bootstrapRate
	}
	if s.validateLiveSnapshot(bootstrapProbe, base, quote, now) != nil {
		return ExchangeRateSnapshot{
			BaseCurrency: base, QuoteCurrency: quote, Source: "unavailable",
			AsOf: now, FetchedAt: now, ExpiresAt: now, Fallback: true,
		}, fmt.Errorf("no valid exchange rate available for %s/%s", base, quote)
	}
	rate := bootstrapProbe.Rate
	if base == CurrencyCNY && quote == CurrencyUSD {
		rate = bootstrapProbe.Rate
	}
	fallback := ExchangeRateSnapshot{
		BaseCurrency: base, QuoteCurrency: quote, Rate: rate,
		Source: "bootstrap_config", AsOf: now, FetchedAt: now,
		ExpiresAt: now.Add(s.ttl), Fallback: true,
	}
	s.mu.Lock()
	s.cache[key] = fallback
	s.mu.Unlock()
	return fallback, nil
}

func (s *ExchangeRateService) Convert(amount float64, snapshot ExchangeRateSnapshot) float64 {
	return amount * snapshot.Rate
}
