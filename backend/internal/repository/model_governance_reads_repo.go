package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

// modelGovernanceReadRepository is the read-only evidence view over the
// Phase 5 governance tables (Phase 7 Task 0 G2). It never writes.
type modelGovernanceReadRepository struct {
	db *sql.DB
}

func NewModelGovernanceReadRepository(db *sql.DB) service.ModelGovernanceReadRepository {
	return &modelGovernanceReadRepository{db: db}
}

const quarantinePoolQuery = `
SELECT e.account_id, COALESCE(a.name, ''), e.channel_id, COALESCE(c.name, ''),
       e.canonical_model_id, e.reason, e.registry_version, e.channel_version,
       e.quarantine_batch_id, e.batch_id, e.created_at, e.updated_at
FROM model_publication_eligibility e
LEFT JOIN accounts a ON a.id = e.account_id
LEFT JOIN channels c ON c.id = e.channel_id
WHERE e.eligibility = 'quarantined'
ORDER BY e.updated_at DESC, e.id DESC
LIMIT $1 OFFSET $2`

func (r *modelGovernanceReadRepository) ListQuarantinePool(
	ctx context.Context, page, pageSize int,
) (*service.GovernancePage[service.QuarantinePoolItem], error) {
	page, pageSize = normalizeGovernancePage(page, pageSize)
	if r == nil || r.db == nil {
		return nil, errors.New("model governance read repository is not configured")
	}
	var total int64
	if err := r.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM model_publication_eligibility WHERE eligibility = 'quarantined'`,
	).Scan(&total); err != nil {
		return nil, fmt.Errorf("count quarantine pool: %w", err)
	}
	rows, err := r.db.QueryContext(ctx, quarantinePoolQuery, pageSize, (page-1)*pageSize)
	if err != nil {
		return nil, fmt.Errorf("list quarantine pool: %w", err)
	}
	defer rows.Close()

	items := make([]service.QuarantinePoolItem, 0, pageSize)
	for rows.Next() {
		item := service.QuarantinePoolItem{}
		var quarantineBatchID, batchID sql.NullString
		if err := rows.Scan(
			&item.AccountID, &item.AccountName, &item.ChannelID, &item.ChannelName,
			&item.CanonicalModelID, &item.Reason, &item.RegistryVersion, &item.ChannelVersion,
			&quarantineBatchID, &batchID, &item.CreatedAt, &item.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan quarantine pool row: %w", err)
		}
		if quarantineBatchID.Valid {
			v := quarantineBatchID.String
			item.QuarantineBatchID = &v
		}
		if batchID.Valid {
			v := batchID.String
			item.BatchID = &v
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate quarantine pool: %w", err)
	}
	return &service.GovernancePage[service.QuarantinePoolItem]{
		Items: items, Total: total, Page: page, PageSize: pageSize,
	}, nil
}

const publicationEventsQuery = `
SELECT id, event_type, actor_id, registry_version, created_at,
       account_id, canonical_model_id, channel_id, eligibility, reason,
       channel_version, batch_id, quarantine_batch_id
FROM model_publication_events
ORDER BY created_at DESC, id DESC
LIMIT $1 OFFSET $2`

const publicationEventsCountQuery = `SELECT COUNT(*) FROM model_publication_events`

const registryEventsQuery = `
SELECT e.id, e.event_type, e.actor_id, e.registry_version, e.created_at,
       COALESCE(e.payload->>'canonical_id', '')
FROM model_registry_events e
ORDER BY e.created_at DESC, e.id DESC
LIMIT $1 OFFSET $2`

const registryEventsCountQuery = `SELECT COUNT(*) FROM model_registry_events`

func (r *modelGovernanceReadRepository) ListGovernanceEvents(
	ctx context.Context, stream string, page, pageSize int,
) (*service.GovernancePage[service.GovernanceEventItem], error) {
	page, pageSize = normalizeGovernancePage(page, pageSize)
	stream = strings.TrimSpace(stream)
	if stream == "" {
		stream = service.GovernanceEventStreamPublication
	}
	if stream != service.GovernanceEventStreamPublication && stream != service.GovernanceEventStreamRegistry {
		return nil, infraerrors.BadRequest(
			"GOVERNANCE_EVENT_STREAM_INVALID",
			fmt.Sprintf("unsupported governance event stream %q", stream),
		)
	}
	if r == nil || r.db == nil {
		return nil, errors.New("model governance read repository is not configured")
	}

	countQuery, listQuery := publicationEventsCountQuery, publicationEventsQuery
	if stream == service.GovernanceEventStreamRegistry {
		countQuery, listQuery = registryEventsCountQuery, registryEventsQuery
	}
	var total int64
	if err := r.db.QueryRowContext(ctx, countQuery).Scan(&total); err != nil {
		return nil, fmt.Errorf("count governance events: %w", err)
	}
	rows, err := r.db.QueryContext(ctx, listQuery, pageSize, (page-1)*pageSize)
	if err != nil {
		return nil, fmt.Errorf("list governance events: %w", err)
	}
	defer rows.Close()

	items := make([]service.GovernanceEventItem, 0, pageSize)
	scanPublication := func() error {
		item := service.GovernanceEventItem{Stream: service.GovernanceEventStreamPublication}
		var accountID, channelID, channelVersion int64
		var quarantineBatchID sql.NullString
		if err := rows.Scan(
			&item.ID, &item.EventType, &item.ActorID, &item.RegistryVersion, &item.CreatedAt,
			&accountID, &item.CanonicalModelID, &channelID, &item.Eligibility, &item.Reason,
			&channelVersion, &item.BatchID, &quarantineBatchID,
		); err != nil {
			return err
		}
		item.AccountID, item.ChannelID, item.ChannelVersion = &accountID, &channelID, &channelVersion
		if quarantineBatchID.Valid {
			v := quarantineBatchID.String
			item.QuarantineBatchID = &v
		}
		items = append(items, item)
		return nil
	}
	scanRegistry := func() error {
		item := service.GovernanceEventItem{Stream: service.GovernanceEventStreamRegistry}
		if err := rows.Scan(
			&item.ID, &item.EventType, &item.ActorID, &item.RegistryVersion, &item.CreatedAt,
			&item.CanonicalID,
		); err != nil {
			return err
		}
		items = append(items, item)
		return nil
	}
	scan := scanPublication
	if stream == service.GovernanceEventStreamRegistry {
		scan = scanRegistry
	}
	for rows.Next() {
		if err := scan(); err != nil {
			return nil, fmt.Errorf("scan governance event row: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate governance events: %w", err)
	}
	return &service.GovernancePage[service.GovernanceEventItem]{
		Items: items, Total: total, Page: page, PageSize: pageSize,
	}, nil
}

const connectionsQuery = `
SELECT id, kind, provider, COALESCE(base_url, ''), status, credential_version
FROM upstream_connections
ORDER BY id
LIMIT $1 OFFSET $2`

func (r *modelGovernanceReadRepository) ListConnections(
	ctx context.Context, page, pageSize int,
) (*service.GovernancePage[service.UpstreamConnectionItem], error) {
	page, pageSize = normalizeGovernancePage(page, pageSize)
	if r == nil || r.db == nil {
		return nil, errors.New("model governance read repository is not configured")
	}
	var total int64
	if err := r.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM upstream_connections`,
	).Scan(&total); err != nil {
		return nil, fmt.Errorf("count connections: %w", err)
	}
	rows, err := r.db.QueryContext(ctx, connectionsQuery, pageSize, (page-1)*pageSize)
	if err != nil {
		return nil, fmt.Errorf("list connections: %w", err)
	}
	defer rows.Close()

	items := make([]service.UpstreamConnectionItem, 0, pageSize)
	for rows.Next() {
		item := service.UpstreamConnectionItem{}
		var provider sql.NullString
		if err := rows.Scan(
			&item.ConnectionID, &item.Kind, &provider, &item.BaseURL,
			&item.Status, &item.CredentialVersion,
		); err != nil {
			return nil, fmt.Errorf("scan connection row: %w", err)
		}
		if provider.Valid {
			v := provider.String
			item.Provider = &v
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate connections: %w", err)
	}
	return &service.GovernancePage[service.UpstreamConnectionItem]{
		Items: items, Total: total, Page: page, PageSize: pageSize,
	}, nil
}

func normalizeGovernancePage(page, pageSize int) (int, int) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 50
	}
	if pageSize > 200 {
		pageSize = 200
	}
	return page, pageSize
}
