package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"html"
	"regexp"
	"strings"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
)

var (
	platformKeyIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	keySHA256Pattern     = regexp.MustCompile(`^[0-9a-f]{64}$`)
	keyPrefixPattern     = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,32}$`)

	ErrPlatformAPIKeyNotFound = infraerrors.NotFound("PLATFORM_API_KEY_NOT_FOUND", "platform api key projection not found")
	ErrPlatformAPIKeyConflict = infraerrors.Conflict("PLATFORM_API_KEY_CONFLICT", "platform api key version or ownership conflicts with the stored projection")
)

type PlatformAPIKeyUpsert struct {
	PlatformKeyID string
	KeySHA256     string
	KeyPrefix     string
	Status        string
	Version       int64
	Name          string
}

type PlatformAPIKeyProjection struct {
	GatewayAPIKeyID int64  `json:"gateway_api_key_id"`
	GatewayUserID   int64  `json:"gateway_user_id"`
	PlatformKeyID   string `json:"platform_key_id"`
	KeySHA256       string `json:"-"`
	KeyPrefix       string `json:"key_prefix"`
	Status          string `json:"status"`
	Version         int64  `json:"version"`
	Name            string `json:"name"`
}

type PlatformAPIKeyRepository interface {
	FindProjectedByPlatformKeyID(ctx context.Context, platformKeyID string) (*PlatformAPIKeyProjection, error)
	CreateProjected(ctx context.Context, projection *PlatformAPIKeyProjection, keyPlaceholder string) error
	UpdateProjected(ctx context.Context, projection *PlatformAPIKeyProjection, expectedVersion int64) (bool, error)
}

type platformAPIKeyCacheInvalidator interface {
	InvalidateAuthCacheByHash(ctx context.Context, keySHA256 string)
}

type PlatformAPIKeyService struct {
	repo  PlatformAPIKeyRepository
	cache platformAPIKeyCacheInvalidator
}

func NewPlatformAPIKeyService(repo PlatformAPIKeyRepository, apiKeys *APIKeyService) *PlatformAPIKeyService {
	service := &PlatformAPIKeyService{repo: repo}
	if apiKeys != nil {
		service.cache = apiKeys
	}
	return service
}

func (s *PlatformAPIKeyService) Upsert(ctx context.Context, userID int64, input PlatformAPIKeyUpsert) (*PlatformAPIKeyProjection, bool, error) {
	input, err := normalizePlatformAPIKeyUpsert(input)
	if err != nil {
		return nil, false, err
	}
	existing, err := s.repo.FindProjectedByPlatformKeyID(ctx, input.PlatformKeyID)
	if err != nil {
		return nil, false, err
	}
	if existing == nil {
		projection := projectionFromInput(userID, input)
		if err := s.repo.CreateProjected(ctx, projection, platformKeyPlaceholder(input.PlatformKeyID)); err != nil {
			// A concurrent retry may have won the unique insert.
			existing, lookupErr := s.repo.FindProjectedByPlatformKeyID(ctx, input.PlatformKeyID)
			if lookupErr != nil || existing == nil {
				return nil, false, err
			}
			return s.applyUpsert(ctx, existing, userID, input)
		}
		s.invalidateHash(ctx, projection.KeySHA256)
		return projection, true, nil
	}
	return s.applyUpsert(ctx, existing, userID, input)
}

func (s *PlatformAPIKeyService) applyUpsert(ctx context.Context, existing *PlatformAPIKeyProjection, userID int64, input PlatformAPIKeyUpsert) (*PlatformAPIKeyProjection, bool, error) {
	if existing.GatewayUserID != userID {
		return nil, false, ErrPlatformAPIKeyConflict
	}
	if input.Version < existing.Version {
		return nil, false, ErrPlatformAPIKeyConflict
	}
	if input.Version == existing.Version {
		if existing.KeySHA256 == input.KeySHA256 && existing.KeyPrefix == input.KeyPrefix &&
			existing.Status == input.Status && existing.Name == input.Name {
			return existing, false, nil
		}
		return nil, false, ErrPlatformAPIKeyConflict
	}
	updated := *existing
	oldHash := updated.KeySHA256
	updated.KeySHA256 = input.KeySHA256
	updated.KeyPrefix = input.KeyPrefix
	updated.Status = input.Status
	updated.Version = input.Version
	updated.Name = input.Name
	affected, err := s.repo.UpdateProjected(ctx, &updated, existing.Version)
	if err != nil {
		return nil, false, err
	}
	if !affected {
		return nil, false, ErrPlatformAPIKeyConflict
	}
	s.invalidateHash(ctx, oldHash)
	s.invalidateHash(ctx, updated.KeySHA256)
	return &updated, false, nil
}

func (s *PlatformAPIKeyService) Revoke(ctx context.Context, userID int64, platformKeyID string, version int64) (*PlatformAPIKeyProjection, bool, error) {
	platformKeyID = strings.TrimSpace(platformKeyID)
	if !platformKeyIDPattern.MatchString(platformKeyID) || version <= 0 {
		return nil, false, infraerrors.BadRequest("INVALID_PLATFORM_API_KEY", "platform_key_id and a positive version are required")
	}
	existing, err := s.repo.FindProjectedByPlatformKeyID(ctx, platformKeyID)
	if err != nil {
		return nil, false, err
	}
	if existing == nil {
		return nil, false, ErrPlatformAPIKeyNotFound
	}
	if existing.GatewayUserID != userID || version < existing.Version {
		return nil, false, ErrPlatformAPIKeyConflict
	}
	if existing.Status == StatusAPIKeyDisabled && version == existing.Version {
		return existing, false, nil
	}
	updated := *existing
	updated.Status = StatusAPIKeyDisabled
	updated.Version = version
	affected, err := s.repo.UpdateProjected(ctx, &updated, existing.Version)
	if err != nil {
		return nil, false, err
	}
	if !affected {
		return nil, false, ErrPlatformAPIKeyConflict
	}
	s.invalidateHash(ctx, updated.KeySHA256)
	return &updated, true, nil
}

func normalizePlatformAPIKeyUpsert(input PlatformAPIKeyUpsert) (PlatformAPIKeyUpsert, error) {
	input.PlatformKeyID = strings.TrimSpace(input.PlatformKeyID)
	input.KeySHA256 = strings.ToLower(strings.TrimSpace(input.KeySHA256))
	input.KeyPrefix = strings.TrimSpace(input.KeyPrefix)
	input.Status = strings.TrimSpace(input.Status)
	input.Name = strings.TrimSpace(input.Name)
	if !platformKeyIDPattern.MatchString(input.PlatformKeyID) || !keySHA256Pattern.MatchString(input.KeySHA256) ||
		!keyPrefixPattern.MatchString(input.KeyPrefix) || input.Version <= 0 {
		return input, infraerrors.BadRequest("INVALID_PLATFORM_API_KEY", "platform_key_id, lowercase key_sha256, key_prefix, and a positive version are required")
	}
	if input.Status == "" {
		input.Status = StatusAPIKeyActive
	}
	if input.Status != StatusAPIKeyActive && input.Status != StatusAPIKeyDisabled {
		return input, infraerrors.BadRequest("INVALID_PLATFORM_API_KEY_STATUS", "status must be active or disabled")
	}
	if input.Name == "" {
		input.Name = "Platform key " + input.KeyPrefix
	}
	if len(input.Name) > 100 {
		return input, infraerrors.BadRequest("INVALID_PLATFORM_API_KEY_NAME", "name must be at most 100 characters")
	}
	input.Name = html.EscapeString(input.Name)
	return input, nil
}

func projectionFromInput(userID int64, input PlatformAPIKeyUpsert) *PlatformAPIKeyProjection {
	return &PlatformAPIKeyProjection{GatewayUserID: userID, PlatformKeyID: input.PlatformKeyID, KeySHA256: input.KeySHA256,
		KeyPrefix: input.KeyPrefix, Status: input.Status, Version: input.Version, Name: input.Name}
}

func platformKeyPlaceholder(platformKeyID string) string {
	sum := sha256.Sum256([]byte(platformKeyID))
	return "__projected__" + hex.EncodeToString(sum[:])
}

func (s *PlatformAPIKeyService) invalidateHash(ctx context.Context, hash string) {
	if s.cache != nil {
		s.cache.InvalidateAuthCacheByHash(ctx, hash)
	}
}
