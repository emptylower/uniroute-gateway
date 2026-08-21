package repository

import (
	"context"
	"database/sql"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

type shadowDecisionRepository struct {
	db *sql.DB
}

func NewShadowDecisionRepository(db *sql.DB) service.ShadowDecisionRepository {
	return &shadowDecisionRepository{db: db}
}

func (r *shadowDecisionRepository) RecordShadowDecisions(ctx context.Context, batchID string, decisions []service.ShadowDecision) error {
	if r.db == nil {
		return sql.ErrConnDone
	}
	if strings.TrimSpace(batchID) == "" {
		return sql.ErrTxDone
	}
	if len(decisions) == 0 {
		return nil
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	for _, d := range decisions {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO model_shadow_decisions (batch_id, upstream_model_id, canonical_id, provider, classification, eligibility, reason)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`, batchID, d.UpstreamModelID, d.CanonicalID, nullableProvider(d.Provider), d.Classification, d.Eligibility, d.Reason)
		if err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

func nullableProvider(p *service.GovernanceProvider) any {
	if p == nil {
		return nil
	}
	return string(*p)
}
