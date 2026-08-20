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

// List uses one read-only repeatable-read transaction for account, dimension,
// and observation sources plus the single inventory aggregation statement. The
// one set-based observation query avoids N+1 reads and shares the same snapshot.
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
	candidates, err := listModelGovernanceInventoryDimensionCandidates(ctx, tx)
	if err != nil {
		return nil, err
	}
	channels, err := listModelGovernanceInventoryChannels(ctx, tx)
	if err != nil {
		return nil, err
	}
	for i := range candidates {
		if candidates[i].ChannelID != nil {
			candidates[i].Channel = channels[*candidates[i].ChannelID]
		}
	}
	observations, err := listModelGovernanceInventoryObservations(ctx, tx)
	if err != nil {
		return nil, err
	}
	projections := projector(service.InventoryProjectionInput{
		Accounts: accounts, Candidates: candidates, Observations: observations,
	})
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
			&item.AccountID, &item.GroupID, &item.ChannelID, &item.TargetPlatform,
			&item.UpstreamModelID, &item.Classification,
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

func listModelGovernanceInventoryDimensionCandidates(ctx context.Context, tx *sql.Tx) ([]service.InventoryRuntimeDimensionCandidate, error) {
	rows, err := tx.QueryContext(ctx, `
		WITH active_accounts AS (
			SELECT id, platform
			FROM accounts
			WHERE status = 'active' AND schedulable = TRUE AND deleted_at IS NULL
		), bound_dimensions AS (
			SELECT a.id AS account_id, a.platform AS account_platform,
			       g.id AS group_id, c.id AS channel_id, g.platform AS group_platform
			FROM active_accounts a
			JOIN account_groups ag ON ag.account_id = a.id
			JOIN groups g ON g.id = ag.group_id AND g.status = 'active' AND g.deleted_at IS NULL
			LEFT JOIN channel_groups cg ON cg.group_id = g.id
			LEFT JOIN channels c ON c.id = cg.channel_id AND c.status = 'active'
		), runtime_targets AS (
			SELECT account_id, group_id, channel_id, group_platform, group_platform AS target_platform, FALSE AS force_platform,
			       FALSE AS ungrouped, TRUE AS account_has_group_bindings,
			       NULL::bigint AS route_id, NULL::text AS route_public_model, NULL::text AS route_match_type,
			       NULL::text AS route_upstream_model, NULL::text AS route_endpoint, NULL::integer AS route_priority
			FROM bound_dimensions
			WHERE group_platform <> 'composite'
			UNION
			SELECT bd.account_id, bd.group_id, bd.channel_id, bd.group_platform, cmr.target_platform, FALSE, FALSE, TRUE,
			       cmr.id, cmr.public_model, cmr.match_type, cmr.upstream_model, cmr.endpoint, cmr.priority
			FROM bound_dimensions bd
			JOIN composite_model_routes cmr ON cmr.group_id = bd.group_id
			WHERE bd.group_platform = 'composite' AND cmr.enabled = TRUE AND cmr.deleted_at IS NULL
			UNION
			SELECT bd.account_id, bd.group_id, bd.channel_id, bd.group_platform, target.platform, FALSE, FALSE, TRUE,
			       NULL, NULL, NULL, NULL, NULL, NULL
			FROM bound_dimensions bd
			CROSS JOIN LATERAL (
				VALUES ('anthropic'::text), ('openai'::text), ('gemini'::text), ('antigravity'::text), ('grok'::text)
			) AS target(platform)
			WHERE bd.group_platform = 'composite'
			UNION
			SELECT account_id, group_id, channel_id, group_platform, account_platform, TRUE, FALSE, TRUE,
			       NULL, NULL, NULL, NULL, NULL, NULL
			FROM bound_dimensions
			WHERE account_platform = 'antigravity'
		), ungrouped_targets AS (
			SELECT a.id AS account_id, NULL::bigint AS group_id, NULL::bigint AS channel_id, ''::text AS group_platform,
			       target.platform AS target_platform, FALSE AS force_platform, TRUE AS ungrouped,
			       EXISTS (SELECT 1 FROM account_groups ag WHERE ag.account_id = a.id) AS account_has_group_bindings,
			       NULL::bigint AS route_id, NULL::text AS route_public_model, NULL::text AS route_match_type,
			       NULL::text AS route_upstream_model, NULL::text AS route_endpoint, NULL::integer AS route_priority
			FROM active_accounts a
			CROSS JOIN LATERAL (
				VALUES (a.platform), ('anthropic'::text), ('gemini'::text)
			) AS target(platform)
		)
		SELECT account_id, group_id, channel_id, group_platform, target_platform, force_platform, ungrouped,
		       account_has_group_bindings, route_id, route_public_model, route_match_type,
		       route_upstream_model, route_endpoint, route_priority
		FROM runtime_targets
		UNION
		SELECT account_id, group_id, channel_id, group_platform, target_platform, force_platform, ungrouped,
		       account_has_group_bindings, route_id, route_public_model, route_match_type,
		       route_upstream_model, route_endpoint, route_priority
		FROM ungrouped_targets
		ORDER BY account_id, group_id NULLS FIRST, channel_id NULLS FIRST, target_platform, force_platform, route_id
	`)
	if err != nil {
		return nil, fmt.Errorf("list model governance inventory dimension candidates: %w", err)
	}
	defer func() { _ = rows.Close() }()

	candidates := make([]service.InventoryRuntimeDimensionCandidate, 0)
	for rows.Next() {
		var candidate service.InventoryRuntimeDimensionCandidate
		var routeID sql.NullInt64
		var routePublicModel, routeMatchType, routeUpstreamModel, routeEndpoint sql.NullString
		var routePriority sql.NullInt64
		if err := rows.Scan(
			&candidate.AccountID, &candidate.GroupID, &candidate.ChannelID, &candidate.GroupPlatform,
			&candidate.TargetPlatform, &candidate.ForcePlatform, &candidate.Ungrouped,
			&candidate.AccountHasGroupBindings, &routeID, &routePublicModel, &routeMatchType,
			&routeUpstreamModel, &routeEndpoint, &routePriority,
		); err != nil {
			return nil, fmt.Errorf("scan model governance inventory dimension candidate: %w", err)
		}
		if routeID.Valid && candidate.GroupID != nil {
			candidate.CompositeRoute = &service.CompositeModelRoute{
				ID: routeID.Int64, GroupID: *candidate.GroupID, PublicModel: routePublicModel.String,
				MatchType: routeMatchType.String, TargetPlatform: candidate.TargetPlatform,
				UpstreamModel: routeUpstreamModel.String, Endpoint: routeEndpoint.String,
				Priority: int(routePriority.Int64), Enabled: true,
			}
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate model governance inventory dimension candidates: %w", err)
	}
	return candidates, nil
}

func listModelGovernanceInventoryChannels(ctx context.Context, tx *sql.Tx) (map[int64]*service.InventoryChannelCandidate, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT c.id, c.billing_model_source, c.restrict_models, c.model_mapping,
		       cmp.platform, cmp.models
		FROM channels c
		LEFT JOIN channel_model_pricing cmp ON cmp.channel_id = c.id
		WHERE c.status = 'active'
		ORDER BY c.id, cmp.id
	`)
	if err != nil {
		return nil, fmt.Errorf("list model governance inventory channels: %w", err)
	}
	defer func() { _ = rows.Close() }()

	channels := make(map[int64]*service.InventoryChannelCandidate)
	for rows.Next() {
		var id int64
		var billingModelSource string
		var restrictModels bool
		var mappingJSON []byte
		var pricingPlatform sql.NullString
		var pricingModels []byte
		if err := rows.Scan(&id, &billingModelSource, &restrictModels, &mappingJSON, &pricingPlatform, &pricingModels); err != nil {
			return nil, fmt.Errorf("scan model governance inventory channel: %w", err)
		}
		channel := channels[id]
		if channel == nil {
			channel = &service.InventoryChannelCandidate{
				ID: id, BillingModelSource: billingModelSource, RestrictModels: restrictModels,
				ModelMapping: make(map[string]map[string]string),
			}
			var rawMapping map[string]any
			if err := json.Unmarshal(mappingJSON, &rawMapping); err != nil {
				return nil, fmt.Errorf("decode model governance inventory channel %d mapping: %w", id, err)
			}
			for platform, rawPlatformValue := range rawMapping {
				rawPlatformMapping, ok := rawPlatformValue.(map[string]any)
				if !ok {
					continue
				}
				platformMapping := make(map[string]string)
				for source, rawTarget := range rawPlatformMapping {
					if target, ok := rawTarget.(string); ok {
						platformMapping[source] = target
					}
				}
				if len(platformMapping) > 0 {
					channel.ModelMapping[platform] = platformMapping
				}
			}
			channels[id] = channel
		}
		if pricingPlatform.Valid {
			var models []string
			if len(pricingModels) > 0 {
				if err := json.Unmarshal(pricingModels, &models); err != nil {
					continue
				}
			}
			channel.Pricing = append(channel.Pricing, service.InventoryChannelPricingCandidate{Platform: pricingPlatform.String, Models: models})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate model governance inventory channels: %w", err)
	}
	return channels, nil
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

func listModelGovernanceInventoryObservations(ctx context.Context, tx *sql.Tx) ([]service.InventoryModelObservation, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT mo.account_id, mo.upstream_model_id
		FROM model_observations mo
		JOIN accounts a ON a.id = mo.account_id
		WHERE a.status = 'active' AND a.schedulable = TRUE AND a.deleted_at IS NULL
		ORDER BY mo.account_id, mo.upstream_model_id
	`)
	if err != nil {
		return nil, fmt.Errorf("list model governance inventory observations: %w", err)
	}
	defer func() { _ = rows.Close() }()

	observations := make([]service.InventoryModelObservation, 0)
	for rows.Next() {
		var observation service.InventoryModelObservation
		if err := rows.Scan(&observation.AccountID, &observation.UpstreamModelID); err != nil {
			return nil, fmt.Errorf("scan model governance inventory observation: %w", err)
		}
		observations = append(observations, observation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate model governance inventory observations: %w", err)
	}
	return observations, nil
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
		"target_platform":   item.TargetPlatform,
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
WITH approved_dimensions AS (
	SELECT projection.account_id, projection.group_id, projection.channel_id,
	       projection.target_platform, projection.upstream_model_ids
	FROM jsonb_to_recordset($4::jsonb) AS projection(
		account_id bigint, group_id bigint, channel_id bigint,
		target_platform text, upstream_model_ids jsonb
	)
),
account_mapping_models AS (
	SELECT d.account_id, d.target_platform AS platform, d.group_id, d.channel_id,
	       model.value AS upstream_model_id
	FROM approved_dimensions d
	CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(d.upstream_model_ids, '[]'::jsonb)) model
),
usage_rows AS (
    SELECT ul.account_id,
           COALESCE(CASE WHEN ul.governance_target_platform IN ('anthropic', 'openai', 'gemini', 'antigravity', 'grok')
                         THEN ul.governance_target_platform END,
                    CASE WHEN g.platform = 'composite' THEN a.platform
                         ELSE COALESCE(NULLIF(g.platform, ''), a.platform) END) AS platform,
	       ul.group_id, ul.channel_id,
           COALESCE(NULLIF(ul.upstream_model, ''), ul.model) AS upstream_model_id,
           ul.api_key_id, ul.actual_cost, ul.settlement_currency, ul.created_at
    FROM usage_logs ul
    JOIN accounts a ON a.id = ul.account_id
	LEFT JOIN groups g ON g.id = ul.group_id
    WHERE ul.created_at >= $2 AND ul.created_at < $3
),
inventory_keys AS (
    SELECT account_id, platform, group_id, channel_id, upstream_model_id FROM account_mapping_models
    UNION
    SELECT account_id, platform, group_id, channel_id, upstream_model_id FROM usage_rows
),
usage_summary AS (
    SELECT account_id, platform, group_id, channel_id, upstream_model_id,
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
    GROUP BY account_id, platform, group_id, channel_id, upstream_model_id
)
SELECT k.account_id, k.group_id, k.channel_id, k.platform, k.upstream_model_id,
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
 AND EXISTS (SELECT 1 FROM approved_dimensions d WHERE d.account_id = mo.account_id)
LEFT JOIN usage_summary us
  ON us.account_id = k.account_id
 AND us.platform = k.platform
 AND us.group_id IS NOT DISTINCT FROM k.group_id
 AND us.channel_id IS NOT DISTINCT FROM k.channel_id
 AND us.upstream_model_id = k.upstream_model_id
WHERE k.upstream_model_id <> ''
ORDER BY k.account_id, k.group_id NULLS FIRST, k.channel_id NULLS FIRST, k.platform, k.upstream_model_id
`
