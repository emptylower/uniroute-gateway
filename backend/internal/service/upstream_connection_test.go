package service

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeEncryptor struct{}

func (fakeEncryptor) Encrypt(p string) (string, error) { return "enc:" + p, nil }
func (fakeEncryptor) Decrypt(c string) (string, error) { return strings.TrimPrefix(c, "enc:"), nil }

type fakeUpstreamConnRepo struct {
	store  map[int64]*UpstreamConnection
	encs   map[int64]string
	events map[string]bool
	nextID int64
}

func newFakeUpstreamConnRepo() *fakeUpstreamConnRepo {
	return &fakeUpstreamConnRepo{store: map[int64]*UpstreamConnection{}, encs: map[int64]string{}, events: map[string]bool{}, nextID: 1}
}
func (f *fakeUpstreamConnRepo) Create(ctx context.Context, conn *UpstreamConnection, enc string) error {
	conn.ID = f.nextID
	f.nextID++
	cp := *conn
	f.store[conn.ID] = &cp
	f.encs[conn.ID] = enc
	return nil
}
func (f *fakeUpstreamConnRepo) GetByID(ctx context.Context, id int64) (*UpstreamConnection, string, error) {
	c, ok := f.store[id]
	if !ok {
		return nil, "", ErrAccountNotFound
	}
	return c, f.encs[id], nil
}
func (f *fakeUpstreamConnRepo) UpdateCredential(ctx context.Context, id int64, expectedVersion int64, enc string) (int64, error) {
	c, ok := f.store[id]
	if !ok {
		return 0, ErrAccountNotFound
	}
	if c.CredentialVersion != expectedVersion {
		return 0, errFakeOptimisticLockConflict
	}
	c.CredentialVersion = expectedVersion + 1
	f.encs[id] = enc
	return c.CredentialVersion, nil
}
func (f *fakeUpstreamConnRepo) BatchGetByIDs(ctx context.Context, ids []int64) (map[int64]*UpstreamConnection, error) {
	out := map[int64]*UpstreamConnection{}
	for _, id := range ids {
		if c, ok := f.store[id]; ok {
			out[id] = c
		}
	}
	return out, nil
}
func (f *fakeUpstreamConnRepo) ListAll(ctx context.Context) ([]*UpstreamConnection, error) {
	out := []*UpstreamConnection{}
	for _, v := range f.store {
		out = append(out, v)
	}
	return out, nil
}
func (f *fakeUpstreamConnRepo) FindEventByIdempotencyKey(ctx context.Context, key string) (bool, error) {
	if f.events == nil {
		return false, nil
	}
	_, ok := f.events[key]
	return ok, nil
}
func (f *fakeUpstreamConnRepo) TransitionToAggregator(ctx context.Context, id int64, expectedVersion int64, evidenceRef, actorID, idempotencyKey string) error {
	if f.events != nil {
		if _, exists := f.events[idempotencyKey]; exists {
			return nil
		}
	}
	c, ok := f.store[id]
	if !ok {
		return ErrAccountNotFound
	}
	if c.Kind != "first_party" {
		return errFakeInvalidArgument
	}
	if c.CredentialVersion != expectedVersion {
		return errFakeOptimisticLockConflict
	}
	c.Kind = "aggregator"
	c.Provider = nil
	c.CredentialVersion = expectedVersion + 1
	if f.events == nil {
		f.events = map[string]bool{}
	}
	f.events[idempotencyKey] = true
	return nil
}
func (f *fakeUpstreamConnRepo) CountAccountsByConnectionID(ctx context.Context, connectionID int64) (int, error) { return 1, nil }

var errFakeOptimisticLockConflict = fakeFmtError("version conflict")
var errFakeInvalidArgument = fakeFmtError("invalid argument")

type fakeFmtError string
func (e fakeFmtError) Error() string { return string(e) }

func TestUpstreamConnectionKindProviderAffinity(t *testing.T) {
	repo := newFakeUpstreamConnRepo()
	svc := NewUpstreamConnectionService(repo, fakeEncryptor{})
	anthropic := GovernanceProviderAnthropic
	// first_party requires provider
	_, err := svc.Create(context.Background(), "first_party", nil, "https://api.anthropic.com", "sk-test", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "governed provider")
	// aggregator must have null provider
	_, err = svc.Create(context.Background(), "aggregator", &anthropic, "https://api.anthropic.com", "sk-test", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "null provider")
	// valid first_party
	conn, err := svc.Create(context.Background(), "first_party", &anthropic, "https://api.anthropic.com/", "sk-test", nil)
	require.NoError(t, err)
	require.Equal(t, "first_party", conn.Kind)
	require.NotNil(t, conn.Provider)
	// valid aggregator
	conn2, err := svc.Create(context.Background(), "aggregator", nil, "https://api.openai.com", "sk-agg", nil)
	require.NoError(t, err)
	require.Equal(t, "aggregator", conn2.Kind)
	require.Nil(t, conn2.Provider)
}

func TestUpstreamConnectionURLNormalization(t *testing.T) {
	normalized, err := NormalizeBaseURL("https://API.ANTHROPIC.COM/v1/")
	require.NoError(t, err)
	require.Equal(t, "https://api.anthropic.com/v1", normalized)
	_, err = NormalizeBaseURL("not-a-url")
	require.Error(t, err)
	_, err = NormalizeBaseURL("")
	require.Error(t, err)
	ep, err := NormalizeEndpointPath("v1/chat/completions/")
	require.NoError(t, err)
	require.Equal(t, "/v1/chat/completions", ep)
	ep, err = NormalizeEndpointPath("/v1/messages/")
	require.NoError(t, err)
	require.Equal(t, "/v1/messages", ep)
}

func TestUpstreamConnectionEncryptionRoundTrip(t *testing.T) {
	repo := newFakeUpstreamConnRepo()
	svc := NewUpstreamConnectionService(repo, fakeEncryptor{})
	anthropic := GovernanceProviderAnthropic
	conn, err := svc.Create(context.Background(), "first_party", &anthropic, "https://api.anthropic.com", "secret123", nil)
	require.NoError(t, err)
	// Retrieve and decrypt
	_, enc, err := repo.GetByID(context.Background(), conn.ID)
	require.NoError(t, err)
	require.Equal(t, "enc:secret123", enc)
	dec, err := fakeEncryptor{}.Decrypt(enc)
	require.NoError(t, err)
	require.Equal(t, "secret123", dec)
	// Rotate
	newVer, err := svc.RotateCredential(context.Background(), conn.ID, 1, "newSecret")
	require.NoError(t, err)
	require.Equal(t, int64(2), newVer)
	// Conflict
	_, err = svc.RotateCredential(context.Background(), conn.ID, 1, "another")
	require.Error(t, err)
	require.Contains(t, err.Error(), "version conflict")
}

func TestUpstreamConnectionDTORedaction(t *testing.T) {
	repo := newFakeUpstreamConnRepo()
	svc := NewUpstreamConnectionService(repo, fakeEncryptor{})
	anthropic := GovernanceProviderAnthropic
	conn, _ := svc.Create(context.Background(), "first_party", &anthropic, "https://api.anthropic.com", "sk-secret", nil)
	dto := svc.ToDTO(conn)
	require.NotNil(t, dto)
	require.True(t, dto.CredentialRedacted)
	// Ensure DTO does not expose encrypted credential
	require.NotContains(t, dto.BaseURL, "sk-secret")
	require.Equal(t, "https://api.anthropic.com", dto.BaseURL)
}
