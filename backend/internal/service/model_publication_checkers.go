package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

// channelPriceChecker checks exact channel price presence via DB.
type channelPriceChecker struct {
	db *sql.DB
}

func NewChannelPriceChecker(db *sql.DB) ChannelPriceChecker {
	return &channelPriceChecker{db: db}
}

func (c *channelPriceChecker) HasExactPrice(ctx context.Context, canonicalID string, provider GovernanceProvider) (bool, error) {
	if c.db == nil {
		return false, fmt.Errorf("db not configured")
	}
	// Exact price requires a channel_model_pricing row where models JSONB contains the canonical as exact string
	// and the pricing is billable (input_price or output_price not null). We check existence.
	var exists bool
	// Use jsonb containment: models @> '["canonical"]'
	canonicalJSON, _ := json.Marshal([]string{canonicalID})
	err := c.db.QueryRowContext(ctx, `
SELECT EXISTS(
  SELECT 1 FROM channel_model_pricing
  WHERE models @> $1::jsonb
    AND (input_price IS NOT NULL OR output_price IS NOT NULL OR cache_write_price IS NOT NULL OR cache_read_price IS NOT NULL)
)`, string(canonicalJSON)).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

// billingMappingChecker checks billing mapping unambiguity.
type billingMappingChecker struct {
	db *sql.DB
}

func NewBillingMappingChecker(db *sql.DB) BillingMappingChecker {
	return &billingMappingChecker{db: db}
}

func (b *billingMappingChecker) IsUnambiguous(ctx context.Context, canonicalID string) (bool, error) {
	if b.db == nil {
		return false, fmt.Errorf("db not configured")
	}
	// Ambiguity: if any channel's model_mapping contains wildcard that matches the canonical,
	// or multiple channels map the same canonical to different billing models, it would be ambiguous.
	// Simplified real check: ensure no channel maps the canonical via wildcard and no duplicate precise mappings.
	var count int
	err := b.db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM channels
WHERE model_mapping::text LIKE '%' || $1 || '%'
`, canonicalID).Scan(&count)
	if err != nil {
		return false, err
	}
	// If count ==0 or 1, unambiguous; if >1, check if they map identically (simplified: treat >1 as ambiguous).
	if count > 1 {
		return false, nil
	}
	return true, nil
}

// resourceChecker checks connection/account/group/channel enablement.
type resourceChecker struct {
	db *sql.DB
}

func NewResourceChecker(db *sql.DB) ResourceChecker {
	return &resourceChecker{db: db}
}

func (r *resourceChecker) AreEnabled(ctx context.Context, accountID int64) (bool, bool, bool, bool, error) {
	if r.db == nil {
		return false, false, false, false, fmt.Errorf("db not configured")
	}
	var (
		connEnabled    = true
		accountEnabled = true
		groupEnabled   = true
		channelEnabled = true
	)
	// Account schedulable and status
	var schedulable bool
	var status string
	var connID sql.NullInt64
	err := r.db.QueryRowContext(ctx, `SELECT status, schedulable, connection_id FROM accounts WHERE id = $1`, accountID).Scan(&status, &schedulable, &connID)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, false, false, false, fmt.Errorf("account not found")
		}
		return false, false, false, false, err
	}
	accountEnabled = status == "active" && schedulable
	if connID.Valid {
		var connStatus string
		err = r.db.QueryRowContext(ctx, `SELECT status FROM upstream_connections WHERE id = $1`, connID.Int64).Scan(&connStatus)
		if err != nil {
			connEnabled = false
		} else {
			connEnabled = connStatus == "active"
		}
	}
	// Group and channel enablement: check if account's group and channel are active.
	// Simplified: query account_groups and channel_groups to find associated group/channel status.
	var groupID sql.NullInt64
	err = r.db.QueryRowContext(ctx, `SELECT group_id FROM account_groups WHERE account_id = $1 LIMIT 1`, accountID).Scan(&groupID)
	if err == nil && groupID.Valid {
		var groupStatus string
		if err := r.db.QueryRowContext(ctx, `SELECT status FROM groups WHERE id = $1`, groupID.Int64).Scan(&groupStatus); err == nil {
			groupEnabled = groupStatus == "active"
		}
	}
	var channelID sql.NullInt64
	err = r.db.QueryRowContext(ctx, `SELECT channel_id FROM channel_groups WHERE group_id = $1 LIMIT 1`, groupID.Int64).Scan(&channelID)
	if err == nil && channelID.Valid {
		var chStatus string
		if err := r.db.QueryRowContext(ctx, `SELECT status FROM channels WHERE id = $1`, channelID.Int64).Scan(&chStatus); err == nil {
			channelEnabled = chStatus == "active"
		}
	}
	return connEnabled, accountEnabled, groupEnabled, channelEnabled, nil
}
