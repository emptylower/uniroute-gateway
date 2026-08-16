package service

import (
	"context"
	"strings"
)

// InvalidateAuthCacheByKey 清除指定 API Key 的认证缓存
func (s *APIKeyService) InvalidateAuthCacheByKey(ctx context.Context, key string) {
	if key == "" {
		return
	}
	cacheKey := s.authCacheKey(key)
	s.deleteAuthCache(ctx, cacheKey)
}

// InvalidateAuthCacheByHash invalidates a cache entry when the caller only
// possesses the verifier hash, as is required for platform-owned projections.
func (s *APIKeyService) InvalidateAuthCacheByHash(ctx context.Context, keySHA256 string) {
	if len(keySHA256) != 64 {
		return
	}
	s.deleteAuthCache(ctx, keySHA256)
}

// InvalidateAuthCacheByUserID 清除用户相关的 API Key 认证缓存
func (s *APIKeyService) InvalidateAuthCacheByUserID(ctx context.Context, userID int64) {
	if userID <= 0 {
		return
	}
	keys, err := s.apiKeyRepo.ListKeysByUserID(ctx, userID)
	if err != nil {
		return
	}
	s.deleteAuthCacheByKeys(ctx, keys)
}

// InvalidateAuthCacheByGroupID 清除分组相关的 API Key 认证缓存
func (s *APIKeyService) InvalidateAuthCacheByGroupID(ctx context.Context, groupID int64) {
	if groupID <= 0 {
		return
	}
	keys, err := s.apiKeyRepo.ListKeysByGroupID(ctx, groupID)
	if err != nil {
		return
	}
	s.deleteAuthCacheByKeys(ctx, keys)
}

func (s *APIKeyService) deleteAuthCacheByKeys(ctx context.Context, keys []string) {
	if len(keys) == 0 {
		return
	}
	for _, key := range keys {
		if key == "" {
			continue
		}
		if hash, ok := strings.CutPrefix(key, "sha256:"); ok && len(hash) == 64 {
			s.deleteAuthCache(ctx, hash)
			continue
		}
		s.deleteAuthCache(ctx, s.authCacheKey(key))
	}
}
