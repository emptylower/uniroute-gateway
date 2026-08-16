package middleware

import (
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

func platformIdentityTestConfig() config.PlatformIdentityConfig {
	return config.PlatformIdentityConfig{
		Enabled:  true,
		Issuer:   "shipany-test",
		Audience: "gateway-test",
		Secret:   "0123456789abcdef0123456789abcdef",
		Version:  "v7",
	}
}

func signPlatformAssertion(t *testing.T, cfg config.PlatformIdentityConfig, claims jwt.MapClaims, kid string) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token.Header["kid"] = kid
	signed, err := token.SignedString([]byte(cfg.Secret))
	require.NoError(t, err)
	return signed
}

func validPlatformAssertionClaims(now time.Time) jwt.MapClaims {
	return jwt.MapClaims{
		"iss":   "shipany-test",
		"aud":   "gateway-test",
		"sub":   "01912345-abcd-7000-8000-0123456789ab",
		"scope": "identity:read identity:write",
		"iat":   now.Unix(),
		"exp":   now.Add(60 * time.Second).Unix(),
		"jti":   "assertion-123",
	}
}

func TestValidatePlatformAssertionAcceptsStrictShortLivedHS256Token(t *testing.T) {
	cfg := platformIdentityTestConfig()
	now := time.Now().UTC().Truncate(time.Second)
	raw := signPlatformAssertion(t, cfg, validPlatformAssertionClaims(now), cfg.Version)

	assertion, err := ValidatePlatformAssertion(raw, cfg)
	require.NoError(t, err)
	require.Equal(t, "01912345-abcd-7000-8000-0123456789ab", assertion.Subject)
	require.Equal(t, "assertion-123", assertion.JWTID)
	require.True(t, assertion.HasScope("identity:read"))
	require.True(t, assertion.HasScope("identity:write"))
}

func TestValidatePlatformAssertionRejectsLifetimeOver60Seconds(t *testing.T) {
	cfg := platformIdentityTestConfig()
	now := time.Now().UTC().Truncate(time.Second)
	claims := validPlatformAssertionClaims(now)
	claims["exp"] = now.Add(61 * time.Second).Unix()
	raw := signPlatformAssertion(t, cfg, claims, cfg.Version)

	_, err := ValidatePlatformAssertion(raw, cfg)
	require.ErrorContains(t, err, "lifetime")
}

func TestValidatePlatformAssertionRejectsWrongKidAndMissingJTI(t *testing.T) {
	cfg := platformIdentityTestConfig()
	now := time.Now().UTC().Truncate(time.Second)

	wrongKid := signPlatformAssertion(t, cfg, validPlatformAssertionClaims(now), "v6")
	_, err := ValidatePlatformAssertion(wrongKid, cfg)
	require.Error(t, err)

	claims := validPlatformAssertionClaims(now)
	delete(claims, "jti")
	missingJTI := signPlatformAssertion(t, cfg, claims, cfg.Version)
	_, err = ValidatePlatformAssertion(missingJTI, cfg)
	require.ErrorContains(t, err, "jti")
}

func TestValidatePlatformAssertionRejectsMissingOrMismatchedRequiredClaims(t *testing.T) {
	cfg := platformIdentityTestConfig()
	now := time.Now().UTC().Truncate(time.Second)
	tests := map[string]func(jwt.MapClaims){
		"issuer":   func(claims jwt.MapClaims) { claims["iss"] = "other-issuer" },
		"audience": func(claims jwt.MapClaims) { claims["aud"] = "other-audience" },
		"subject":  func(claims jwt.MapClaims) { delete(claims, "sub") },
		"scope":    func(claims jwt.MapClaims) { delete(claims, "scope") },
		"issued_at": func(claims jwt.MapClaims) {
			delete(claims, "iat")
		},
		"expires_at": func(claims jwt.MapClaims) { delete(claims, "exp") },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			claims := validPlatformAssertionClaims(now)
			mutate(claims)
			raw := signPlatformAssertion(t, cfg, claims, cfg.Version)
			_, err := ValidatePlatformAssertion(raw, cfg)
			require.Error(t, err)
		})
	}
}

func TestValidatePlatformAssertionRejectsWhenBridgeDisabled(t *testing.T) {
	cfg := platformIdentityTestConfig()
	cfg.Enabled = false

	_, err := ValidatePlatformAssertion("unused", cfg)
	require.ErrorContains(t, err, "disabled")
}
