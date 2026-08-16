package service

import (
	"context"
	"strings"
	"testing"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/stretchr/testify/require"
)

type platformAPIKeyRepoStub struct {
	projection  *PlatformAPIKeyProjection
	creates     int
	updates     int
	placeholder string
}

func (r *platformAPIKeyRepoStub) FindProjectedByPlatformKeyID(context.Context, string) (*PlatformAPIKeyProjection, error) {
	if r.projection == nil {
		return nil, nil
	}
	copy := *r.projection
	return &copy, nil
}

func (r *platformAPIKeyRepoStub) CreateProjected(_ context.Context, projection *PlatformAPIKeyProjection, placeholder string) error {
	r.creates++
	r.placeholder = placeholder
	copy := *projection
	copy.GatewayAPIKeyID = 55
	r.projection = &copy
	projection.GatewayAPIKeyID = copy.GatewayAPIKeyID
	return nil
}

func (r *platformAPIKeyRepoStub) UpdateProjected(_ context.Context, projection *PlatformAPIKeyProjection, expectedVersion int64) (bool, error) {
	if r.projection == nil || r.projection.Version != expectedVersion {
		return false, nil
	}
	r.updates++
	copy := *projection
	r.projection = &copy
	return true, nil
}

type platformAPIKeyCacheStub struct{ hashes []string }

func (c *platformAPIKeyCacheStub) InvalidateAuthCacheByHash(_ context.Context, hash string) {
	c.hashes = append(c.hashes, hash)
}

func testPlatformAPIKeyService(repo *platformAPIKeyRepoStub, cache *platformAPIKeyCacheStub) *PlatformAPIKeyService {
	return &PlatformAPIKeyService{repo: repo, cache: cache}
}

func validPlatformAPIKeyInput(version int64) PlatformAPIKeyUpsert {
	return PlatformAPIKeyUpsert{
		PlatformKeyID: "shipany-key-1",
		KeySHA256:     strings.Repeat("a", 64),
		KeyPrefix:     "sk-live-abcd",
		Status:        StatusAPIKeyActive,
		Version:       version,
		Name:          "Primary",
	}
}

func TestPlatformAPIKeyUpsertIsIdempotentAndMonotonic(t *testing.T) {
	repo := &platformAPIKeyRepoStub{}
	cache := &platformAPIKeyCacheStub{}
	svc := testPlatformAPIKeyService(repo, cache)

	created, isCreated, err := svc.Upsert(context.Background(), 42, validPlatformAPIKeyInput(1))
	require.NoError(t, err)
	require.True(t, isCreated)
	require.EqualValues(t, 42, created.GatewayUserID)
	require.Equal(t, 1, repo.creates)
	require.True(t, strings.HasPrefix(repo.placeholder, "__projected__"))

	retry, isCreated, err := svc.Upsert(context.Background(), 42, validPlatformAPIKeyInput(1))
	require.NoError(t, err)
	require.False(t, isCreated)
	require.Equal(t, created, retry)
	require.Zero(t, repo.updates)

	rotated := validPlatformAPIKeyInput(2)
	rotated.KeySHA256 = strings.Repeat("b", 64)
	updated, isCreated, err := svc.Upsert(context.Background(), 42, rotated)
	require.NoError(t, err)
	require.False(t, isCreated)
	require.Equal(t, rotated.KeySHA256, updated.KeySHA256)
	require.Equal(t, []string{strings.Repeat("a", 64), strings.Repeat("a", 64), strings.Repeat("b", 64)}, cache.hashes)

	_, _, err = svc.Upsert(context.Background(), 42, validPlatformAPIKeyInput(1))
	require.True(t, infraerrors.IsConflict(err))
}

func TestPlatformAPIKeyRejectsOwnershipAndSameVersionMutation(t *testing.T) {
	repo := &platformAPIKeyRepoStub{projection: projectionFromInput(42, validPlatformAPIKeyInput(3))}
	svc := testPlatformAPIKeyService(repo, &platformAPIKeyCacheStub{})

	_, _, err := svc.Upsert(context.Background(), 99, validPlatformAPIKeyInput(4))
	require.True(t, infraerrors.IsConflict(err))

	changed := validPlatformAPIKeyInput(3)
	changed.Status = StatusAPIKeyDisabled
	_, _, err = svc.Upsert(context.Background(), 42, changed)
	require.True(t, infraerrors.IsConflict(err))
}

func TestPlatformAPIKeyRevokeIsIdempotentAndInvalidatesVerifier(t *testing.T) {
	input := validPlatformAPIKeyInput(3)
	repo := &platformAPIKeyRepoStub{projection: projectionFromInput(42, input)}
	cache := &platformAPIKeyCacheStub{}
	svc := testPlatformAPIKeyService(repo, cache)

	revoked, changed, err := svc.Revoke(context.Background(), 42, input.PlatformKeyID, 4)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, StatusAPIKeyDisabled, revoked.Status)
	require.Equal(t, []string{input.KeySHA256}, cache.hashes)

	retry, changed, err := svc.Revoke(context.Background(), 42, input.PlatformKeyID, 4)
	require.NoError(t, err)
	require.False(t, changed)
	require.Equal(t, revoked, retry)
}

func TestPlatformAPIKeyNeverAcceptsPlaintextOrMalformedVerifier(t *testing.T) {
	svc := testPlatformAPIKeyService(&platformAPIKeyRepoStub{}, &platformAPIKeyCacheStub{})
	input := validPlatformAPIKeyInput(1)
	input.KeySHA256 = "sk-this-is-plaintext"
	_, _, err := svc.Upsert(context.Background(), 42, input)
	require.True(t, infraerrors.IsBadRequest(err))
}
