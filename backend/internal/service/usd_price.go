package service

import (
	"context"
	"fmt"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"strings"
	"time"
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

// USDPriceService supplies unit identity snapshots to existing pricing paths.
// It performs no network requests and has no configurable conversion rate.
type USDPriceService struct{}

func NewUSDPriceService(_ *config.Config) *USDPriceService { return &USDPriceService{} }
func (*USDPriceService) Snapshot(_ context.Context, base, quote string) (ExchangeRateSnapshot, error) {
	if !strings.EqualFold(strings.TrimSpace(base), CurrencyUSD) || !strings.EqualFold(strings.TrimSpace(quote), CurrencyUSD) {
		return ExchangeRateSnapshot{}, fmt.Errorf("prices are available only in USD")
	}
	now := time.Now().UTC()
	return ExchangeRateSnapshot{BaseCurrency: CurrencyUSD, QuoteCurrency: CurrencyUSD, Rate: 1, Source: "identity", AsOf: now, FetchedAt: now}, nil
}
func (*USDPriceService) Convert(amount float64, _ ExchangeRateSnapshot) float64 { return amount }
