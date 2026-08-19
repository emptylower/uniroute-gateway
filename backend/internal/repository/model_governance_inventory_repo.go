package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/shopspring/decimal"
)

var (
	errModelGovernanceInventoryMixedCurrency = infraerrors.InternalServer(
		"MODEL_GOVERNANCE_INVENTORY_MIXED_CURRENCY", "inventory dimension has mixed settlement currencies",
	)
	errModelGovernanceInventoryNegativeRevenue = infraerrors.InternalServer(
		"MODEL_GOVERNANCE_INVENTORY_NEGATIVE_REVENUE", "inventory dimension has negative revenue",
	)
	errModelGovernanceInventoryRevenueOverflow = infraerrors.InternalServer(
		"MODEL_GOVERNANCE_INVENTORY_REVENUE_OVERFLOW", "inventory dimension revenue exceeds integer micros range",
	)
)

type modelGovernanceInventoryRepository struct {
	db                  *sql.DB
	afterAccountsLoaded func() error
}

func NewModelGovernanceInventoryRepository(db *sql.DB) service.ModelGovernanceInventoryRepository {
	return &modelGovernanceInventoryRepository{db: db}
}

// List uses one read-only repeatable-read transaction for the account source
// SELECT and the single inventory aggregation statement. It performs no writes.
func (r *modelGovernanceInventoryRepository) List(ctx context.Context, projector service.InventoryAccountProjector, cutoff7d, cutoff30d, windowEnd time.Time) ([]service.InventoryItem, error) {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin model governance inventory snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	accounts, err := listModelGovernanceInventoryAccounts(ctx, tx)
	if err != nil {
		return nil, err
	}
	if r.afterAccountsLoaded != nil {
		if err := r.afterAccountsLoaded(); err != nil {
			return nil, fmt.Errorf("after model governance inventory accounts loaded: %w", err)
		}
	}
	projections := projector(accounts)
	projectionJSON, err := json.Marshal(projections)
	if err != nil {
		return nil, fmt.Errorf("encode model governance inventory projections: %w", err)
	}
	rows, err := tx.QueryContext(ctx, modelGovernanceInventoryQuery, cutoff7d.UTC(), cutoff30d.UTC(), windowEnd.UTC(), projectionJSON)
	if err != nil {
		return nil, fmt.Errorf("list model governance inventory: %w", err)
	}
	defer func() { _ = rows.Close() }()

	items := make([]service.InventoryItem, 0)
	for rows.Next() {
		var item service.InventoryItem
		var currencyCount, negativeRevenueCount int
		var revenue7d, revenue30d decimal.Decimal
		if err := rows.Scan(
			&item.AccountID, &item.GroupID, &item.ChannelID, &item.UpstreamModelID, &item.Classification,
			&item.Requests7d, &item.Requests30d, &revenue7d, &revenue30d, &item.BillingCurrency,
			&item.AffectedAPIKeys7d, &item.AffectedAPIKeys30d, &currencyCount, &negativeRevenueCount,
		); err != nil {
			return nil, fmt.Errorf("scan model governance inventory: %w", err)
		}
		if currencyCount > 1 {
			return nil, inventoryDimensionError(errModelGovernanceInventoryMixedCurrency, item)
		}
		if negativeRevenueCount > 0 {
			return nil, inventoryDimensionError(errModelGovernanceInventoryNegativeRevenue, item)
		}
		item.Revenue7dBillingMicros, err = inventoryRevenueMicros(revenue7d, item)
		if err != nil {
			return nil, err
		}
		item.Revenue30dBillingMicros, err = inventoryRevenueMicros(revenue30d, item)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate model governance inventory: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close model governance inventory rows: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit model governance inventory snapshot: %w", err)
	}
	return items, nil
}

func listModelGovernanceInventoryAccounts(ctx context.Context, tx *sql.Tx) ([]service.Account, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT id, platform, type, credentials, extra
		FROM accounts
		WHERE status = 'active' AND schedulable = TRUE AND deleted_at IS NULL
		ORDER BY id
	`)
	if err != nil {
		return nil, fmt.Errorf("list model governance inventory accounts: %w", err)
	}
	defer func() { _ = rows.Close() }()

	accounts := make([]service.Account, 0)
	for rows.Next() {
		var account service.Account
		var credentials, extra []byte
		if err := rows.Scan(&account.ID, &account.Platform, &account.Type, &credentials, &extra); err != nil {
			return nil, fmt.Errorf("scan model governance inventory account: %w", err)
		}
		if err := json.Unmarshal(credentials, &account.Credentials); err != nil {
			return nil, fmt.Errorf("decode model governance inventory account %d credentials: %w", account.ID, err)
		}
		if err := json.Unmarshal(extra, &account.Extra); err != nil {
			return nil, fmt.Errorf("decode model governance inventory account %d extra: %w", account.ID, err)
		}
		accounts = append(accounts, account)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate model governance inventory accounts: %w", err)
	}
	return accounts, nil
}

func inventoryRevenueMicros(revenue decimal.Decimal, item service.InventoryItem) (int64, error) {
	if revenue.IsNegative() {
		return 0, inventoryDimensionError(errModelGovernanceInventoryNegativeRevenue, item)
	}
	micros := revenue.Mul(decimal.NewFromInt(1_000_000)).Round(0)
	if micros.GreaterThan(decimal.NewFromInt(math.MaxInt64)) {
		return 0, inventoryDimensionError(errModelGovernanceInventoryRevenueOverflow, item)
	}
	return micros.IntPart(), nil
}

func inventoryDimensionError(base *infraerrors.ApplicationError, item service.InventoryItem) error {
	metadata := map[string]string{
		"account_id":        strconv.FormatInt(item.AccountID, 10),
		"group_id":          "",
		"channel_id":        "",
		"upstream_model_id": item.UpstreamModelID,
	}
	if item.GroupID != nil {
		metadata["group_id"] = strconv.FormatInt(*item.GroupID, 10)
	}
	if item.ChannelID != nil {
		metadata["channel_id"] = strconv.FormatInt(*item.ChannelID, 10)
	}
	return base.WithMetadata(metadata)
}

const modelGovernanceInventoryQuery = `
WITH enabled_routes AS (
	SELECT a.id AS account_id, a.platform, g.id AS group_id, c.id AS channel_id,
	       c.model_mapping
    FROM accounts a
    LEFT JOIN account_groups ag ON ag.account_id = a.id
    LEFT JOIN groups g ON g.id = ag.group_id AND g.status = 'active' AND g.deleted_at IS NULL
    LEFT JOIN channel_groups cg ON cg.group_id = g.id
    LEFT JOIN channels c ON c.id = cg.channel_id AND c.status = 'active'
    WHERE a.status = 'active' AND a.schedulable = TRUE AND a.deleted_at IS NULL
      AND (ag.group_id IS NULL OR g.id IS NOT NULL)
),
account_mapping_models AS (
	SELECT er.account_id, er.platform, er.group_id, er.channel_id, model.value AS upstream_model_id
	FROM jsonb_to_recordset($4::jsonb) AS projection(account_id bigint, upstream_model_ids jsonb)
	JOIN enabled_routes er ON er.account_id = projection.account_id
	CROSS JOIN LATERAL jsonb_array_elements_text(projection.upstream_model_ids) model
),
channel_mapping_models AS (
    SELECT er.account_id, er.platform, er.group_id, er.channel_id, mapping.value AS upstream_model_id
    FROM enabled_routes er
    CROSS JOIN LATERAL jsonb_each_text(
        CASE WHEN jsonb_typeof(er.model_mapping->er.platform) = 'object'
             THEN er.model_mapping->er.platform ELSE '{}'::jsonb END
    ) mapping
    WHERE er.channel_id IS NOT NULL
      AND mapping.value <> '' AND strpos(mapping.value, '*') = 0
),
channel_pricing_models AS (
    SELECT er.account_id, er.platform, er.group_id, er.channel_id, model.value AS upstream_model_id
    FROM enabled_routes er
    JOIN channel_model_pricing cmp ON cmp.channel_id = er.channel_id AND cmp.platform = er.platform
    CROSS JOIN LATERAL jsonb_array_elements_text(
        CASE WHEN jsonb_typeof(cmp.models) = 'array' THEN cmp.models ELSE '[]'::jsonb END
    ) model
    WHERE model.value <> '' AND strpos(model.value, '*') = 0
),
composite_route_models AS (
    SELECT er.account_id, er.platform, er.group_id, er.channel_id,
           COALESCE(NULLIF(cmr.upstream_model, ''), cmr.public_model) AS upstream_model_id
    FROM enabled_routes er
    JOIN composite_model_routes cmr
      ON cmr.group_id = er.group_id AND cmr.target_platform = er.platform
     AND cmr.enabled = TRUE AND cmr.deleted_at IS NULL
),
observation_models AS (
    SELECT mo.account_id, er.platform, er.group_id, er.channel_id, mo.upstream_model_id
    FROM model_observations mo
    JOIN enabled_routes er ON er.account_id = mo.account_id
),
usage_rows AS (
    SELECT ul.account_id, a.platform, ul.group_id, ul.channel_id,
           COALESCE(NULLIF(ul.upstream_model, ''), ul.model) AS upstream_model_id,
           ul.api_key_id, ul.actual_cost, ul.settlement_currency, ul.created_at
    FROM usage_logs ul
    JOIN accounts a ON a.id = ul.account_id
    WHERE ul.created_at >= $2 AND ul.created_at < $3
),
inventory_keys AS (
    SELECT account_id, platform, group_id, channel_id, upstream_model_id FROM account_mapping_models
    UNION
    SELECT account_id, platform, group_id, channel_id, upstream_model_id FROM channel_mapping_models
    UNION
    SELECT account_id, platform, group_id, channel_id, upstream_model_id FROM channel_pricing_models
    UNION
    SELECT account_id, platform, group_id, channel_id, upstream_model_id FROM composite_route_models
    UNION
    SELECT account_id, platform, group_id, channel_id, upstream_model_id FROM observation_models
    UNION
    SELECT account_id, platform, group_id, channel_id, upstream_model_id FROM usage_rows
),
usage_summary AS (
    SELECT account_id, group_id, channel_id, upstream_model_id,
           COUNT(*) FILTER (WHERE created_at >= $1)::bigint AS requests_7d,
           COUNT(*)::bigint AS requests_30d,
           COALESCE(SUM(actual_cost) FILTER (WHERE created_at >= $1), 0) AS revenue_7d,
           COALESCE(SUM(actual_cost), 0) AS revenue_30d,
           MIN(settlement_currency) AS billing_currency,
           COUNT(DISTINCT api_key_id) FILTER (WHERE created_at >= $1)::bigint AS api_keys_7d,
           COUNT(DISTINCT api_key_id)::bigint AS api_keys_30d,
           COUNT(DISTINCT settlement_currency)::int AS currency_count,
           COUNT(*) FILTER (WHERE actual_cost < 0)::int AS negative_revenue_count
    FROM usage_rows
    GROUP BY account_id, group_id, channel_id, upstream_model_id
)
SELECT k.account_id, k.group_id, k.channel_id, k.upstream_model_id,
       COALESCE(mo.classification,
                CASE WHEN k.platform IN ('anthropic', 'openai', 'gemini', 'grok') THEN 'unknown' ELSE 'ignored' END) AS classification,
       COALESCE(us.requests_7d, 0), COALESCE(us.requests_30d, 0),
       COALESCE(us.revenue_7d, 0), COALESCE(us.revenue_30d, 0),
       COALESCE(us.billing_currency, 'CNY') AS billing_currency,
       COALESCE(us.api_keys_7d, 0), COALESCE(us.api_keys_30d, 0),
       COALESCE(us.currency_count, 0), COALESCE(us.negative_revenue_count, 0)
FROM inventory_keys k
LEFT JOIN model_observations mo
  ON mo.account_id = k.account_id AND mo.upstream_model_id = k.upstream_model_id
 AND EXISTS (SELECT 1 FROM enabled_routes er WHERE er.account_id = mo.account_id)
LEFT JOIN usage_summary us
  ON us.account_id = k.account_id
 AND us.group_id IS NOT DISTINCT FROM k.group_id
 AND us.channel_id IS NOT DISTINCT FROM k.channel_id
 AND us.upstream_model_id = k.upstream_model_id
WHERE k.upstream_model_id <> ''
ORDER BY k.account_id, k.group_id NULLS FIRST, k.channel_id NULLS FIRST, k.upstream_model_id
`
