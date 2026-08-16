package middleware

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/redis/go-redis/v9"
)

const platformAssertionContextKey = "platform_assertion"

type PlatformAssertion struct {
	Subject string
	JWTID   string
	Scopes  map[string]struct{}
	Expires time.Time
}

func (a *PlatformAssertion) HasScope(scope string) bool {
	_, ok := a.Scopes[scope]
	return ok
}

func PlatformAssertionFromContext(c *gin.Context) (*PlatformAssertion, bool) {
	value, ok := c.Get(platformAssertionContextKey)
	if !ok {
		return nil, false
	}
	assertion, ok := value.(*PlatformAssertion)
	return assertion, ok && assertion != nil
}

func RequirePlatformAssertion(cfg config.PlatformIdentityConfig, requiredScope string, redisClient *redis.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		authorization := strings.TrimSpace(c.GetHeader("Authorization"))
		parts := strings.Fields(authorization)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			response.Unauthorized(c, "invalid internal assertion")
			c.Abort()
			return
		}

		assertion, err := ValidatePlatformAssertion(parts[1], cfg)
		if err != nil {
			response.Unauthorized(c, "invalid internal assertion")
			c.Abort()
			return
		}
		if !assertion.HasScope(requiredScope) {
			response.Forbidden(c, "internal assertion scope denied")
			c.Abort()
			return
		}
		if redisClient == nil {
			response.Error(c, http.StatusServiceUnavailable, "internal assertion replay protection unavailable")
			c.Abort()
			return
		}
		remaining := time.Until(assertion.Expires) + 5*time.Second
		if remaining <= 0 {
			response.Unauthorized(c, "invalid internal assertion")
			c.Abort()
			return
		}
		digest := sha256.Sum256([]byte(cfg.Version + ":" + assertion.JWTID))
		replayKey := "platform_identity:assertion:" + hex.EncodeToString(digest[:])
		fresh, replayErr := redisClient.SetNX(c.Request.Context(), replayKey, "1", remaining).Result()
		if replayErr != nil {
			response.Error(c, http.StatusServiceUnavailable, "internal assertion replay protection unavailable")
			c.Abort()
			return
		}
		if !fresh {
			response.Unauthorized(c, "internal assertion already used")
			c.Abort()
			return
		}
		c.Set(platformAssertionContextKey, assertion)
		c.Next()
	}
}

func ValidatePlatformAssertion(raw string, cfg config.PlatformIdentityConfig) (*PlatformAssertion, error) {
	if !cfg.Enabled {
		return nil, errors.New("platform identity bridge is disabled")
	}
	if strings.TrimSpace(raw) == "" {
		return nil, errors.New("assertion is empty")
	}

	claims := jwt.MapClaims{}
	parser := jwt.NewParser(
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithExpirationRequired(),
		jwt.WithIssuer(cfg.Issuer),
		jwt.WithAudience(cfg.Audience),
		jwt.WithIssuedAt(),
		jwt.WithLeeway(5*time.Second),
	)
	token, err := parser.ParseWithClaims(raw, claims, func(token *jwt.Token) (any, error) {
		if token.Method != jwt.SigningMethodHS256 {
			return nil, errors.New("unexpected signing method")
		}
		kid, ok := token.Header["kid"].(string)
		if !ok || kid != cfg.Version {
			return nil, errors.New("unexpected assertion key version")
		}
		return []byte(cfg.Secret), nil
	})
	if err != nil || token == nil || !token.Valid {
		return nil, fmt.Errorf("validate assertion: %w", err)
	}

	subject, err := claims.GetSubject()
	if err != nil || strings.TrimSpace(subject) == "" {
		return nil, errors.New("assertion subject is required")
	}
	issuedAt, err := claims.GetIssuedAt()
	if err != nil || issuedAt == nil {
		return nil, errors.New("assertion iat is required")
	}
	expiresAt, err := claims.GetExpirationTime()
	if err != nil || expiresAt == nil {
		return nil, errors.New("assertion exp is required")
	}
	lifetime := expiresAt.Time.Sub(issuedAt.Time)
	if lifetime <= 0 || lifetime > 60*time.Second {
		return nil, errors.New("assertion lifetime must be between 1 and 60 seconds")
	}
	jti, ok := claims["jti"].(string)
	if !ok || strings.TrimSpace(jti) == "" {
		return nil, errors.New("assertion jti is required")
	}

	scopes, err := assertionScopes(claims["scope"])
	if err != nil || len(scopes) == 0 {
		return nil, errors.New("assertion scope is required")
	}
	return &PlatformAssertion{Subject: subject, JWTID: jti, Scopes: scopes, Expires: expiresAt.Time}, nil
}

func assertionScopes(value any) (map[string]struct{}, error) {
	out := make(map[string]struct{})
	switch scopes := value.(type) {
	case string:
		for _, scope := range strings.Fields(scopes) {
			out[scope] = struct{}{}
		}
	case []any:
		for _, raw := range scopes {
			scope, ok := raw.(string)
			if !ok || strings.TrimSpace(scope) == "" {
				return nil, errors.New("invalid assertion scope")
			}
			out[strings.TrimSpace(scope)] = struct{}{}
		}
	default:
		return nil, errors.New("invalid assertion scope")
	}
	return out, nil
}
