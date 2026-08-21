package repository

import (
	"context"
	"database/sql"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

type aggregatorReuseRepository struct {
	client *dbent.Client
	db     *sql.DB
}

func NewAggregatorReuseRepository(client *dbent.Client, db *sql.DB) service.AggregatorReuseRepository {
	return &aggregatorReuseRepository{client: client, db: db}
}

func (r *aggregatorReuseRepository) FindByScope(ctx context.Context, connectionID int64, provider service.GovernanceProvider, protocol service.AccountProtocol, endpoint, clientRequestID string) (int64, bool, error) {
	if r.db == nil {
		return 0, false, nil
	}
	var accountID sql.NullInt64
	err := r.db.QueryRowContext(ctx, `SELECT account_id FROM aggregator_reuse_requests WHERE connection_id=$1 AND provider=$2 AND protocol=$3 AND normalized_endpoint_path=$4 AND client_request_id=$5`, connectionID, string(provider), string(protocol), endpoint, clientRequestID).Scan(&accountID)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if !accountID.Valid {
		return 0, true, nil
	}
	return accountID.Int64, true, nil
}

func (r *aggregatorReuseRepository) Create(ctx context.Context, connectionID int64, provider service.GovernanceProvider, protocol service.AccountProtocol, endpoint, clientRequestID string, accountID int64) error {
	if r.db == nil {
		return nil
	}
	_, err := r.db.ExecContext(ctx, `INSERT INTO aggregator_reuse_requests (connection_id, provider, protocol, normalized_endpoint_path, client_request_id, account_id, status) VALUES ($1,$2,$3,$4,$5,$6,'pending')`, connectionID, string(provider), string(protocol), endpoint, clientRequestID, accountID)
	if err != nil {
		// Unique violation indicates concurrent hit – caller will handle via FindByScope
		return err
	}
	return nil
}

var _ service.AggregatorReuseRepository = (*aggregatorReuseRepository)(nil)
