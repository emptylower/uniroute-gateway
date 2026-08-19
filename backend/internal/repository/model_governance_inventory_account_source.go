package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

type modelGovernanceInventoryAccountSource struct {
	db *sql.DB
}

// NewModelGovernanceInventoryAccountSource loads configuration-enabled runtime accounts for finite mapping projection.
func NewModelGovernanceInventoryAccountSource(db *sql.DB) service.ModelGovernanceInventoryAccountSource {
	return &modelGovernanceInventoryAccountSource{db: db}
}

func (s *modelGovernanceInventoryAccountSource) ListInventoryAccounts(ctx context.Context) ([]service.Account, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, platform, type, credentials
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
		var credentials []byte
		if err := rows.Scan(&account.ID, &account.Platform, &account.Type, &credentials); err != nil {
			return nil, fmt.Errorf("scan model governance inventory account: %w", err)
		}
		if err := json.Unmarshal(credentials, &account.Credentials); err != nil {
			return nil, fmt.Errorf("decode model governance inventory account %d credentials: %w", account.ID, err)
		}
		accounts = append(accounts, account)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate model governance inventory accounts: %w", err)
	}
	return accounts, nil
}
