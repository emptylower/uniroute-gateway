package service

import (
	"context"
	"testing"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/stretchr/testify/require"
)

type platformIdentityRepoStub struct {
	identity *PlatformIdentity
	creates  int
}

func (s *platformIdentityRepoStub) FindByPlatformUserID(context.Context, string) (*PlatformIdentity, error) {
	return s.identity, nil
}

func (s *platformIdentityRepoStub) CreatePlatformUser(_ context.Context, platformUserID, _ string) (*PlatformIdentity, error) {
	s.creates++
	s.identity = &PlatformIdentity{GatewayUserID: 42, PlatformUserID: platformUserID, Status: StatusActive, CreatedAt: time.Now()}
	return s.identity, nil
}

func TestPlatformIdentityServiceUpsertIsIdempotentByPlatformUserID(t *testing.T) {
	repo := &platformIdentityRepoStub{}
	svc := NewPlatformIdentityService(repo)

	first, created, err := svc.Upsert(context.Background(), "shipany-user-1", "User")
	require.NoError(t, err)
	require.True(t, created)
	require.EqualValues(t, 42, first.GatewayUserID)

	second, created, err := svc.Upsert(context.Background(), "shipany-user-1", "Changed Name")
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, first, second)
	require.Equal(t, 1, repo.creates)
}

func TestPlatformIdentityServiceRejectsInvalidAndDeletedMappings(t *testing.T) {
	svc := NewPlatformIdentityService(&platformIdentityRepoStub{})
	_, _, err := svc.Upsert(context.Background(), "contains spaces", "")
	require.True(t, infraerrors.IsBadRequest(err))

	deletedAt := time.Now()
	svc = NewPlatformIdentityService(&platformIdentityRepoStub{identity: &PlatformIdentity{
		GatewayUserID: 1, PlatformUserID: "deleted-user", DeletedAt: &deletedAt,
	}})
	_, _, err = svc.Upsert(context.Background(), "deleted-user", "")
	require.True(t, infraerrors.IsConflict(err))
}
