package repository

import (
	"context"
	"database/sql"
	"fmt"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	dbconn "github.com/Wei-Shaw/sub2api/ent/upstreamconnection"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

type upstreamConnectionRepository struct {
	client *dbent.Client
	sql    *sql.DB
}

func NewUpstreamConnectionRepository(client *dbent.Client, sqlDB *sql.DB) service.UpstreamConnectionRepository {
	return &upstreamConnectionRepository{client: client, sql: sqlDB}
}

func (r *upstreamConnectionRepository) Create(ctx context.Context, conn *service.UpstreamConnection, encryptedCredential string) error {
	if conn == nil {
		return fmt.Errorf("nil connection")
	}
	builder := r.client.UpstreamConnection.Create().
		SetKind(conn.Kind).
		SetBaseURL(conn.BaseURL).
		SetEncryptedCredential(encryptedCredential).
		SetCredentialVersion(conn.CredentialVersion).
		SetStatus(conn.Status)
	if conn.Provider != nil {
		builder.SetProvider(string(*conn.Provider))
	}
	if conn.ProxyID != nil {
		builder.SetProxyID(*conn.ProxyID)
	}
	if conn.EvidenceRef != nil {
		builder.SetEvidenceRef(*conn.EvidenceRef)
	}
	created, err := builder.Save(ctx)
	if err != nil {
		return err
	}
	conn.ID = created.ID
	return nil
}

func (r *upstreamConnectionRepository) GetByID(ctx context.Context, id int64) (*service.UpstreamConnection, string, error) {
	entConn, err := r.client.UpstreamConnection.Query().Where(dbconn.IDEQ(id)).Only(ctx)
	if err != nil {
		return nil, "", err
	}
	var provider *service.GovernanceProvider
	if entConn.Provider != nil && *entConn.Provider != "" {
		p := service.GovernanceProvider(*entConn.Provider)
		provider = &p
	}
	conn := &service.UpstreamConnection{
		ID:                entConn.ID,
		Kind:              entConn.Kind,
		Provider:          provider,
		BaseURL:           entConn.BaseURL,
		CredentialVersion: entConn.CredentialVersion,
		ProxyID:           entConn.ProxyID,
		Status:            entConn.Status,
	}
	if entConn.EvidenceRef != nil {
		conn.EvidenceRef = entConn.EvidenceRef
	}
	return conn, entConn.EncryptedCredential, nil
}

func (r *upstreamConnectionRepository) UpdateCredential(ctx context.Context, id int64, expectedVersion int64, encryptedCredential string) (int64, error) {
	// Optimistic locking via credential_version
	res, err := r.client.UpstreamConnection.Update().
		Where(dbconn.IDEQ(id), dbconn.CredentialVersionEQ(expectedVersion)).
		SetEncryptedCredential(encryptedCredential).
		SetCredentialVersion(expectedVersion + 1).
		Save(ctx)
	if err != nil {
		return 0, err
	}
	if res == 0 {
		return 0, fmt.Errorf("credential version conflict: expected %d", expectedVersion)
	}
	return expectedVersion + 1, nil
}

func (r *upstreamConnectionRepository) BatchGetByIDs(ctx context.Context, ids []int64) (map[int64]*service.UpstreamConnection, error) {
	if len(ids) == 0 {
		return map[int64]*service.UpstreamConnection{}, nil
	}
	ents, err := r.client.UpstreamConnection.Query().Where(dbconn.IDIn(ids...)).All(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[int64]*service.UpstreamConnection, len(ents))
	for _, e := range ents {
		var provider *service.GovernanceProvider
		if e.Provider != nil && *e.Provider != "" {
			p := service.GovernanceProvider(*e.Provider)
			provider = &p
		}
		out[e.ID] = &service.UpstreamConnection{
			ID:                e.ID,
			Kind:              e.Kind,
			Provider:          provider,
			BaseURL:           e.BaseURL,
			CredentialVersion: e.CredentialVersion,
			ProxyID:           e.ProxyID,
			Status:            e.Status,
			EvidenceRef:       e.EvidenceRef,
		}
	}
	return out, nil
}

func (r *upstreamConnectionRepository) ListAll(ctx context.Context) ([]*service.UpstreamConnection, error) {
	ents, err := r.client.UpstreamConnection.Query().All(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*service.UpstreamConnection, 0, len(ents))
	for _, e := range ents {
		var provider *service.GovernanceProvider
		if e.Provider != nil && *e.Provider != "" {
			p := service.GovernanceProvider(*e.Provider)
			provider = &p
		}
		out = append(out, &service.UpstreamConnection{
			ID:                e.ID,
			Kind:              e.Kind,
			Provider:          provider,
			BaseURL:           e.BaseURL,
			CredentialVersion: e.CredentialVersion,
			ProxyID:           e.ProxyID,
			Status:            e.Status,
			EvidenceRef:       e.EvidenceRef,
		})
	}
	return out, nil
}

var _ service.UpstreamConnectionRepository = (*upstreamConnectionRepository)(nil)
