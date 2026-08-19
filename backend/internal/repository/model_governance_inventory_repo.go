package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

type modelGovernanceInventoryRepository struct {
	db *sql.DB
}

func NewModelGovernanceInventoryRepository(db *sql.DB) service.ModelGovernanceInventoryRepository {
	return &modelGovernanceInventoryRepository{db: db}
}

func (r *modelGovernanceInventoryRepository) List(ctx context.Context, cutoff7d, cutoff30d time.Time) ([]service.InventoryItem, error) {
	rows, err := r.db.QueryContext(ctx, modelGovernanceInventoryQuery, cutoff7d.UTC(), cutoff30d.UTC())
	if err != nil {
		return nil, fmt.Errorf("list model governance inventory: %w", err)
	}
	defer func() { _ = rows.Close() }()

	items := make([]service.InventoryItem, 0)
	for rows.Next() {
		var item service.InventoryItem
		var currencyCount int
		if err := rows.Scan(
			&item.AccountID, &item.GroupID, &item.ChannelID, &item.UpstreamModelID, &item.Classification,
			&item.Requests7d, &item.Requests30d, &item.Revenue7dBillingMicros, &item.Revenue30dBillingMicros,
			&item.BillingCurrency, &item.AffectedAPIKeys7d, &item.AffectedAPIKeys30d, &currencyCount,
		); err != nil {
			return nil, fmt.Errorf("scan model governance inventory: %w", err)
		}
		if currencyCount > 1 {
			return nil, fmt.Errorf(
				"mixed settlement currencies for account %d group %v channel %v model %q",
				item.AccountID, item.GroupID, item.ChannelID, item.UpstreamModelID,
			)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate model governance inventory: %w", err)
	}
	return items, nil
}

const modelGovernanceInventoryQuery = `
WITH enabled_routes AS (
    SELECT a.id AS account_id, a.platform, g.id AS group_id, c.id AS channel_id,
           a.credentials, c.model_mapping
    FROM accounts a
    LEFT JOIN account_groups ag ON ag.account_id = a.id
    LEFT JOIN groups g ON g.id = ag.group_id AND g.status = 'active' AND g.deleted_at IS NULL
    LEFT JOIN channel_groups cg ON cg.group_id = g.id
    LEFT JOIN channels c ON c.id = cg.channel_id AND c.status = 'active'
    WHERE a.status = 'active' AND a.schedulable = TRUE AND a.deleted_at IS NULL
      AND (ag.group_id IS NULL OR g.id IS NOT NULL)
),
account_mapping_models AS (
    SELECT er.account_id, er.group_id, er.channel_id, mapping.value AS upstream_model_id
    FROM enabled_routes er
    CROSS JOIN LATERAL jsonb_each_text(
        CASE WHEN jsonb_typeof(er.credentials->'model_mapping') = 'object'
             THEN er.credentials->'model_mapping' ELSE '{}'::jsonb END
    ) mapping
),
channel_mapping_models AS (
    SELECT er.account_id, er.group_id, er.channel_id, mapping.value AS upstream_model_id
    FROM enabled_routes er
    CROSS JOIN LATERAL jsonb_each_text(
        CASE WHEN jsonb_typeof(er.model_mapping->er.platform) = 'object'
             THEN er.model_mapping->er.platform ELSE '{}'::jsonb END
    ) mapping
    WHERE er.channel_id IS NOT NULL
),
channel_pricing_models AS (
    SELECT er.account_id, er.group_id, er.channel_id, model.value AS upstream_model_id
    FROM enabled_routes er
    JOIN channel_model_pricing cmp ON cmp.channel_id = er.channel_id AND cmp.platform = er.platform
    CROSS JOIN LATERAL jsonb_array_elements_text(
        CASE WHEN jsonb_typeof(cmp.models) = 'array' THEN cmp.models ELSE '[]'::jsonb END
    ) model
),
composite_route_models AS (
    SELECT er.account_id, er.group_id, er.channel_id,
           COALESCE(NULLIF(cmr.upstream_model, ''), cmr.public_model) AS upstream_model_id
    FROM enabled_routes er
    JOIN composite_model_routes cmr
      ON cmr.group_id = er.group_id AND cmr.target_platform = er.platform
     AND cmr.enabled = TRUE AND cmr.deleted_at IS NULL
),
observation_models AS (
    SELECT mo.account_id, er.group_id, er.channel_id, mo.upstream_model_id
    FROM model_observations mo
    LEFT JOIN enabled_routes er ON er.account_id = mo.account_id
),
usage_rows AS (
    SELECT ul.account_id, ul.group_id, ul.channel_id,
           COALESCE(NULLIF(ul.upstream_model, ''), ul.model) AS upstream_model_id,
           ul.api_key_id, ul.actual_cost, ul.settlement_currency, ul.created_at
    FROM usage_logs ul
    WHERE ul.created_at >= $2
),
inventory_keys AS (
    SELECT account_id, group_id, channel_id, upstream_model_id FROM account_mapping_models
    UNION
    SELECT account_id, group_id, channel_id, upstream_model_id FROM channel_mapping_models
    UNION
    SELECT account_id, group_id, channel_id, upstream_model_id FROM channel_pricing_models
    UNION
    SELECT account_id, group_id, channel_id, upstream_model_id FROM composite_route_models
    UNION
    SELECT account_id, group_id, channel_id, upstream_model_id FROM observation_models
    UNION
    SELECT account_id, group_id, channel_id, upstream_model_id FROM usage_rows
),
usage_summary AS (
    SELECT account_id, group_id, channel_id, upstream_model_id,
           COUNT(*) FILTER (WHERE created_at >= $1)::bigint AS requests_7d,
           COUNT(*)::bigint AS requests_30d,
           ROUND(COALESCE(SUM(actual_cost) FILTER (WHERE created_at >= $1), 0) * 1000000)::bigint AS revenue_7d,
           ROUND(COALESCE(SUM(actual_cost), 0) * 1000000)::bigint AS revenue_30d,
           MIN(settlement_currency) AS billing_currency,
           COUNT(DISTINCT api_key_id) FILTER (WHERE created_at >= $1)::bigint AS api_keys_7d,
           COUNT(DISTINCT api_key_id)::bigint AS api_keys_30d,
           COUNT(DISTINCT settlement_currency)::int AS currency_count
    FROM usage_rows
    GROUP BY account_id, group_id, channel_id, upstream_model_id
)
SELECT k.account_id, k.group_id, k.channel_id, k.upstream_model_id,
       COALESCE(mo.classification, 'unknown') AS classification,
       COALESCE(us.requests_7d, 0), COALESCE(us.requests_30d, 0),
       COALESCE(us.revenue_7d, 0), COALESCE(us.revenue_30d, 0),
       COALESCE(us.billing_currency, 'USD') AS billing_currency,
       COALESCE(us.api_keys_7d, 0), COALESCE(us.api_keys_30d, 0),
       COALESCE(us.currency_count, 0)
FROM inventory_keys k
LEFT JOIN model_observations mo
  ON mo.account_id = k.account_id AND mo.upstream_model_id = k.upstream_model_id
LEFT JOIN usage_summary us
  ON us.account_id = k.account_id
 AND us.group_id IS NOT DISTINCT FROM k.group_id
 AND us.channel_id IS NOT DISTINCT FROM k.channel_id
 AND us.upstream_model_id = k.upstream_model_id
WHERE k.upstream_model_id <> ''
ORDER BY k.account_id, k.group_id NULLS FIRST, k.channel_id NULLS FIRST, k.upstream_model_id
`
