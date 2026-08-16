package service

import (
	"context"
	"regexp"
	"strings"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
)

const (
	PlatformIdentityReadScope     = "identity:read"
	PlatformIdentityWriteScope    = "identity:write"
	PlatformDataReadScope         = "gateway:data:read"
	PlatformPreferencesWriteScope = "gateway:preferences:write"
	PlatformGatewayAdminScope     = "gateway:admin"
	PlatformKeysWriteScope        = "gateway:keys:write"
)

var platformUserIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

var (
	ErrPlatformIdentityNotFound = infraerrors.NotFound("PLATFORM_IDENTITY_NOT_FOUND", "platform identity not found")
	ErrPlatformIdentityDeleted  = infraerrors.Conflict("PLATFORM_IDENTITY_DELETED", "platform identity belongs to a deleted gateway user")
)

type PlatformIdentity struct {
	GatewayUserID  int64      `json:"gateway_user_id"`
	PlatformUserID string     `json:"platform_user_id"`
	Status         string     `json:"status"`
	CreatedAt      time.Time  `json:"created_at"`
	DeletedAt      *time.Time `json:"-"`
}

type PlatformIdentityRepository interface {
	FindByPlatformUserID(ctx context.Context, platformUserID string) (*PlatformIdentity, error)
	CreatePlatformUser(ctx context.Context, platformUserID, username string) (*PlatformIdentity, error)
}

type platformIdentityStatusRepository interface {
	UpdatePlatformUserStatus(ctx context.Context, platformUserID, status string) (*PlatformIdentity, error)
}

type PlatformIdentityService struct {
	repo PlatformIdentityRepository
}

func NewPlatformIdentityService(repo PlatformIdentityRepository) *PlatformIdentityService {
	return &PlatformIdentityService{repo: repo}
}

func NormalizePlatformUserID(value string) (string, error) {
	value = strings.TrimSpace(value)
	if !platformUserIDPattern.MatchString(value) {
		return "", infraerrors.BadRequest(
			"INVALID_PLATFORM_USER_ID",
			"platform_user_id must be 1-128 characters using letters, numbers, dot, underscore, colon, or hyphen",
		)
	}
	return value, nil
}

func (s *PlatformIdentityService) Get(ctx context.Context, platformUserID string) (*PlatformIdentity, error) {
	normalized, err := NormalizePlatformUserID(platformUserID)
	if err != nil {
		return nil, err
	}
	identity, err := s.repo.FindByPlatformUserID(ctx, normalized)
	if err != nil {
		return nil, err
	}
	if identity == nil || identity.DeletedAt != nil {
		return nil, ErrPlatformIdentityNotFound
	}
	return identity, nil
}

func (s *PlatformIdentityService) Upsert(ctx context.Context, platformUserID, username string, requestedStatus ...string) (*PlatformIdentity, bool, error) {
	normalized, err := NormalizePlatformUserID(platformUserID)
	if err != nil {
		return nil, false, err
	}
	username = strings.TrimSpace(username)
	if len(username) > 100 {
		return nil, false, infraerrors.BadRequest("INVALID_USERNAME", "username must be at most 100 characters")
	}
	status := "active"
	if len(requestedStatus) > 0 && strings.TrimSpace(requestedStatus[0]) != "" {
		status = strings.TrimSpace(requestedStatus[0])
	}
	if status != "active" && status != "disabled" {
		return nil, false, infraerrors.BadRequest("INVALID_PLATFORM_STATUS", "status must be active or disabled")
	}

	existing, err := s.repo.FindByPlatformUserID(ctx, normalized)
	if err != nil {
		return nil, false, err
	}
	if existing != nil {
		if existing.DeletedAt != nil {
			return nil, false, ErrPlatformIdentityDeleted
		}
		if existing.Status != status {
			statusRepo, ok := s.repo.(platformIdentityStatusRepository)
			if !ok {
				return nil, false, infraerrors.InternalServer("PLATFORM_STATUS_UNAVAILABLE", "platform status projection is unavailable")
			}
			updated, updateErr := statusRepo.UpdatePlatformUserStatus(ctx, normalized, status)
			return updated, false, updateErr
		}
		return existing, false, nil
	}

	created, err := s.repo.CreatePlatformUser(ctx, normalized, username)
	if err != nil {
		return nil, false, err
	}
	if created.DeletedAt != nil {
		return nil, false, ErrPlatformIdentityDeleted
	}
	if created.Status != status {
		statusRepo, ok := s.repo.(platformIdentityStatusRepository)
		if !ok {
			return nil, false, infraerrors.InternalServer("PLATFORM_STATUS_UNAVAILABLE", "platform status projection is unavailable")
		}
		updated, updateErr := statusRepo.UpdatePlatformUserStatus(ctx, normalized, status)
		return updated, true, updateErr
	}
	return created, true, nil
}
