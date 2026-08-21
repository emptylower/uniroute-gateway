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

func NewAggregatorReuseRepository(client *dbent.Client, db *sql.DB) *aggregatorReuseRepository {
	return &aggregatorReuseRepository{client: client, db: db}
}

func (r *aggregatorReuseRepository) CreateReuseRequest(ctx context.Context, req *service.ReuseAggregatorConnectionInput, accountID *int64) error {
	// Stub: would insert into aggregator_reuse_requests with idempotency scope uniqueness
	return nil
}

func (r *aggregatorReuseRepository) GetByScope(ctx context.Context, connectionID int64, provider service.GovernanceProvider, protocol service.AccountProtocol, endpoint, clientRequestID string) (interface{}, error) {
	return nil, nil
}

var _ = context.Background
var _ = sql.ErrNoRows
