package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

const (
	canonicalWalletSummaryPath  = "/api/internal/v2/wallet/summary"
	canonicalWalletUsageTimeout = 15 * time.Second
)

var canonicalWalletUSDDecimalPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)(\.[0-9]{1,8})?$`)

// CanonicalWalletUsageReader reads the canonical balance without acquiring funds.
type CanonicalWalletUsageReader struct {
	control *canonicalWalletHTTPClient
}

type CanonicalWalletUsageSummary struct {
	PlatformUserID         string       `json:"platform_user_id"`
	PlatformKeyID          *string      `json:"platform_key_id,omitempty"`
	Currency               string       `json:"currency"`
	UnitVersion            string       `json:"unit_version"`
	USDWalletPolicyVersion string       `json:"usd_wallet_policy_version"`
	AvailableUSD           *json.Number `json:"-"`
	Completeness           string       `json:"completeness"`
	AvailableIsLowerBound  bool         `json:"available_is_lower_bound"`
	AsOf                   time.Time    `json:"as_of"`
}

func NewCanonicalWalletUsageReader(cfg *config.Config) *CanonicalWalletUsageReader {
	if cfg == nil {
		return nil
	}
	client := &http.Client{
		Timeout:       canonicalWalletUsageTimeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
	}
	return &CanonicalWalletUsageReader{control: newCanonicalWalletHTTPClient(cfg.CanonicalWallet, client)}
}

func (r *CanonicalWalletUsageReader) Summary(ctx context.Context, owner string, keyID *string) (*CanonicalWalletUsageSummary, error) {
	if r == nil || r.control == nil {
		return nil, errors.New("canonical wallet summary reader unavailable")
	}
	c := r.control
	if c.cfg.ControlPlaneURL == "" || c.cfg.Secret == "" || c.cfg.Issuer == "" || c.cfg.Audience == "" || c.cfg.Version == "" {
		return nil, errors.New("canonical wallet summary is not configured")
	}
	if normalized, err := NormalizePlatformUserID(owner); err != nil || normalized != owner {
		return nil, errors.New("invalid canonical wallet owner")
	}
	if keyID != nil && !platformKeyIDPattern.MatchString(*keyID) {
		return nil, errors.New("invalid canonical wallet key")
	}
	payload, err := json.Marshal(struct {
		PlatformUserID         string  `json:"platform_user_id"`
		PlatformKeyID          *string `json:"platform_key_id,omitempty"`
		Currency               string  `json:"currency"`
		UnitVersion            string  `json:"unit_version"`
		USDWalletPolicyVersion string  `json:"usd_wallet_policy_version"`
	}{owner, keyID, CurrencyUSD, CanonicalWalletUnitVersion, config.CanonicalUSDWalletPolicyVersion})
	if err != nil {
		return nil, err
	}
	now := c.now()
	digest := sha256.Sum256(payload)
	claims := jwt.MapClaims{
		"iss": c.cfg.Issuer, "aud": c.cfg.Audience, "sub": "sub2api-gateway",
		"iat": now.Unix(), "exp": now.Add(30 * time.Second).Unix(), "jti": uuid.NewString(),
		"scope": "wallet:usage-read", "purpose": "usage-wallet-summary",
		"platform_user_id": owner, "http_method": http.MethodPost,
		"http_path": canonicalWalletSummaryPath, "body_sha256": hex.EncodeToString(digest[:]),
	}
	if keyID != nil {
		claims["platform_key_id"] = *keyID
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token.Header["kid"] = c.cfg.Version
	assertion, err := token.SignedString([]byte(c.cfg.Secret))
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.cfg.ControlPlaneURL, "/")+canonicalWalletSummaryPath, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+assertion)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("read canonical wallet summary: %w", err)
	}
	// Closing the read-only response is cleanup; read failures are handled below.
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil || len(body) > 1<<20 {
		return nil, errors.New("invalid canonical wallet summary body")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("canonical wallet summary returned status %d", resp.StatusCode)
	}
	var envelope struct {
		Code json.RawMessage `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	if json.Unmarshal(body, &envelope) != nil || (string(envelope.Code) != "0" && string(envelope.Code) != `"0"`) || len(envelope.Data) == 0 || string(envelope.Data) == "null" {
		return nil, errors.New("invalid canonical wallet summary envelope")
	}
	var wire struct {
		PlatformUserID         string    `json:"platform_user_id"`
		PlatformKeyID          *string   `json:"platform_key_id"`
		Currency               string    `json:"currency"`
		UnitVersion            string    `json:"unit_version"`
		USDWalletPolicyVersion string    `json:"usd_wallet_policy_version"`
		AvailableUSD           *string   `json:"available_usd"`
		Completeness           string    `json:"completeness"`
		AvailableIsLowerBound  *bool     `json:"available_is_lower_bound"`
		AsOf                   time.Time `json:"as_of"`
	}
	receivedAt := c.now()
	if json.Unmarshal(envelope.Data, &wire) != nil || wire.PlatformUserID != owner || (keyID == nil) != (wire.PlatformKeyID == nil) || (keyID != nil && *keyID != *wire.PlatformKeyID) ||
		wire.Currency != CurrencyUSD || wire.UnitVersion != CanonicalWalletUnitVersion || wire.USDWalletPolicyVersion != config.CanonicalUSDWalletPolicyVersion || wire.AvailableIsLowerBound == nil ||
		(wire.Completeness != "complete" && wire.Completeness != "unknown") || wire.AsOf.IsZero() || wire.AsOf.After(receivedAt.Add(5*time.Second)) || receivedAt.Sub(wire.AsOf) > 30*time.Second {
		return nil, errors.New("canonical wallet summary identity, version or freshness mismatch")
	}
	out := &CanonicalWalletUsageSummary{PlatformUserID: owner, PlatformKeyID: keyID, Currency: wire.Currency, UnitVersion: wire.UnitVersion,
		USDWalletPolicyVersion: wire.USDWalletPolicyVersion, Completeness: wire.Completeness, AvailableIsLowerBound: *wire.AvailableIsLowerBound, AsOf: wire.AsOf}
	if wire.AvailableUSD != nil {
		value := *wire.AvailableUSD
		if len(value) > 29 || !canonicalWalletUSDDecimalPattern.MatchString(value) {
			return nil, errors.New("invalid canonical wallet USD decimal")
		}
		parts := strings.SplitN(value, ".", 2)
		fraction := ""
		if len(parts) == 2 {
			fraction = parts[1]
		}
		units := strings.TrimLeft(parts[0]+fraction+strings.Repeat("0", 8-len(fraction)), "0")
		if units == "" {
			units = "0"
		}
		if _, err := strconv.ParseInt(units, 10, 64); err != nil {
			return nil, errors.New("canonical wallet USD decimal exceeds int64 units")
		}
		number := json.Number(value)
		out.AvailableUSD = &number
	}
	if out.Completeness == "complete" && !out.AvailableIsLowerBound && out.AvailableUSD == nil {
		return nil, errors.New("complete canonical wallet summary has no balance")
	}
	return out, nil
}
