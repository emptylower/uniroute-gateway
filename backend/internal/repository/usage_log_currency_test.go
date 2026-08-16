package repository

import (
	"database/sql"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestPrepareUsageLogInsertPersistsCurrencySnapshot(t *testing.T) {
	asOf := time.Date(2026, time.August, 12, 4, 0, 0, 0, time.UTC)
	prepared := prepareUsageLogInsert(&service.UsageLog{
		UserID: 1, APIKeyID: 2, AccountID: 3, RequestID: "fx", Model: "gpt-5",
		SourceCurrency: service.CurrencyUSD, SettlementCurrency: service.CurrencyCNY,
		ExchangeRate: 7.2, ExchangeRateSource: "bootstrap_config", ExchangeRateAsOf: &asOf,
		SourceCost: 1.5, BaseCost: 10.8,
	})

	require.Len(t, prepared.args, len(usageLogInsertArgTypes))
	base := len(prepared.args) - 8
	require.Equal(t, service.CurrencyUSD, prepared.args[base])
	require.Equal(t, service.CurrencyCNY, prepared.args[base+1])
	require.Equal(t, 7.2, prepared.args[base+2])
	require.Equal(t, "bootstrap_config", prepared.args[base+3])
	rateTime, ok := prepared.args[base+4].(sql.NullTime)
	require.True(t, ok)
	require.True(t, rateTime.Valid)
	require.Equal(t, asOf, rateTime.Time)
	require.Equal(t, 1.5, prepared.args[base+5])
	require.Equal(t, 10.8, prepared.args[base+6])
}

func TestValidateUsageLogCurrencySnapshotRejectsPartialNewSnapshot(t *testing.T) {
	require.Error(t, validateUsageLogCurrencySnapshot(&service.UsageLog{
		SourceCurrency: service.CurrencyUSD, SettlementCurrency: service.CurrencyCNY,
		ExchangeRate: 7.2, ExchangeRateSource: "currencyapi",
	}))
}

func TestValidateUsageLogCurrencySnapshotRejectsImplicitLegacyRow(t *testing.T) {
	require.Error(t, validateUsageLogCurrencySnapshot(&service.UsageLog{}))
}
