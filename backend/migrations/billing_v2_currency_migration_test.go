package migrations

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBillingV2CurrencyMigrationIsAdditiveAndAuditable(t *testing.T) {
	content, err := FS.ReadFile("195_billing_v2_currency.sql")
	require.NoError(t, err)

	sql := strings.ToUpper(strings.Join(strings.Fields(string(content)), " "))
	for _, fragment := range []string{
		"ADD COLUMN IF NOT EXISTS BILLING_CURRENCY",
		"ADD COLUMN IF NOT EXISTS RATE_MULTIPLIER_CNY",
		"ADD COLUMN IF NOT EXISTS RATE_MULTIPLIER_USD",
		"ADD COLUMN IF NOT EXISTS SOURCE_CURRENCY",
		"ADD COLUMN IF NOT EXISTS SETTLEMENT_CURRENCY",
		"ADD COLUMN IF NOT EXISTS EXCHANGE_RATE",
		"ADD COLUMN IF NOT EXISTS EXCHANGE_RATE_SOURCE",
		"ADD COLUMN IF NOT EXISTS EXCHANGE_RATE_AS_OF",
		"ADD COLUMN IF NOT EXISTS SOURCE_COST",
		"ADD COLUMN IF NOT EXISTS BASE_COST",
		"ADD COLUMN IF NOT EXISTS CURRENCY VARCHAR(3) NOT NULL DEFAULT 'CNY'",
		"ADD COLUMN IF NOT EXISTS SETTLEMENT_CURRENCY VARCHAR(3)",
		"ALTER TABLE BATCH_IMAGE_JOBS",
		"CHECK (BILLING_CURRENCY IN ('CNY', 'USD'))",
		"CHECK (SOURCE_CURRENCY IN ('CNY', 'USD'))",
		"CHECK (SETTLEMENT_CURRENCY IN ('CNY', 'USD'))",
	} {
		require.Contains(t, sql, fragment)
	}
	require.NotContains(t, sql, "DROP COLUMN")
	require.NotContains(t, sql, "DROP TABLE")
	require.NotContains(t, sql, "ALTER COLUMN")
}
