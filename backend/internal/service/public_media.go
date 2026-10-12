package service

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
)

func normalizeMediaCostCap(s string) (int64, string, error) {
	invalid := func() (int64, string, error) {
		return 0, "", infraerrors.BadRequest("INVALID_COST_CAP", "max_cost_usd must be a non-negative decimal string with at most eight fractional digits")
	}
	if s == "" {
		return 0, "", nil
	}
	parts := strings.Split(s, ".")
	if len(parts) > 2 || len(parts[0]) == 0 {
		return invalid()
	}
	fraction := ""
	if len(parts) == 2 {
		fraction = parts[1]
		if len(fraction) == 0 || len(fraction) > 8 {
			return invalid()
		}
	}
	for _, c := range parts[0] + fraction {
		if c < '0' || c > '9' {
			return invalid()
		}
	}
	whole, err := strconv.ParseInt(strings.TrimLeft("0"+parts[0], "0"), 10, 64)
	if strings.Trim(parts[0], "0") == "" {
		whole = 0
		err = nil
	}
	if err != nil || whole > math.MaxInt64/mediaUnitsPerUSD {
		return invalid()
	}
	f, _ := strconv.ParseInt(fraction+strings.Repeat("0", 8-len(fraction)), 10, 64)
	units := whole * mediaUnitsPerUSD
	if units > math.MaxInt64-f {
		return invalid()
	}
	units += f
	return units, mediaUSD(units), nil
}
func validatePublicMediaKey(key *APIKey, cfg *config.Config, newTask bool) error {
	if err := validateMediaActor(key); err != nil {
		return err
	}
	if key.PlatformKeyID == nil || strings.TrimSpace(*key.PlatformKeyID) == "" || key.RoutingMode != APIKeyRoutingModeAutoChannels || len(key.ChannelIDs) > 0 || key.GroupID != nil || strings.TrimSpace(key.User.PlatformUserID) == "" {
		return infraerrors.Forbidden("MEDIA_KEY_INELIGIBLE", "use a platform-managed unified API key with automatic channels and no channel restrictions")
	}
	if cfg == nil || cfg.CanonicalWallet.Mode != config.CanonicalWalletModeEnforce || !cfg.CanonicalWallet.USDWalletEnabled || cfg.CanonicalWallet.USDPolicyVersion != config.CanonicalUSDWalletPolicyVersion {
		return ErrMediaUnavailable
	}
	if newTask && (key.Status == StatusAPIKeyExpired || key.IsExpired()) {
		return infraerrors.Forbidden("API_KEY_EXPIRED", "API key has expired")
	}
	if newTask && (key.Status == StatusAPIKeyQuotaExhausted || key.IsQuotaExhausted()) {
		return infraerrors.New(429, "API_KEY_QUOTA_EXHAUSTED", "API key allowance is exhausted")
	}
	return nil
}
func (s *MediaTaskService) PublicEnabled(keyID int64) bool {
	if s == nil || s.cfg == nil || !s.cfg.MediaTasks.PublicAPIEnabled {
		return false
	}
	ids := s.cfg.MediaTasks.CanaryKeyIDs
	if len(ids) == 0 {
		return true
	}
	for _, id := range ids {
		if id == keyID {
			return true
		}
	}
	return false
}
func (s *MediaTaskService) PublicCatalog(key *APIKey) ([]MediaModel, error) {
	if err := validatePublicMediaKey(key, s.cfg, false); err != nil {
		return nil, err
	}
	return PublicMediaTaskCatalog(), nil
}
func (s *MediaTaskService) PublicQuote(key *APIKey, model, option string) (MediaQuote, error) {
	if err := validatePublicMediaKey(key, s.cfg, false); err != nil {
		return MediaQuote{}, err
	}
	return mediaQuote(model, option)
}
func (s *MediaTaskService) screenMedia(ctx context.Context, user string, in MediaCreateInput) error {
	if in.MediaType == "music" {
		return nil
	}
	if s.bridge == nil {
		return ErrMediaUnavailable
	}
	client, ok := s.bridge.control.(*canonicalWalletHTTPClient)
	if !ok {
		return ErrMediaUnavailable
	}
	copyClient := *client
	httpClient := *client.client
	httpClient.Timeout = 10 * time.Second
	copyClient.client = &httpClient
	bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var approval struct {
		Allowed bool `json:"allowed"`
	}
	body := struct {
		UserID    string `json:"user_id"`
		Model     string `json:"model"`
		MediaType string `json:"media_type"`
		Prompt    string `json:"prompt"`
	}{user, in.Model, in.MediaType, in.Prompt}
	if err := copyClient.doJSON(bounded, http.MethodPost, "/api/internal/v2/media/moderation", "media:moderation", "", body, &approval); err != nil {
		var status *canonicalWalletStatusError
		if errors.As(err, &status) && (status.Status == 400 || status.Status == 403) {
			return infraerrors.Forbidden("MEDIA_PROMPT_BLOCKED", "generation prompt was rejected")
		}
		return infraerrors.ServiceUnavailable("MEDIA_MODERATION_UNAVAILABLE", "generation moderation is temporarily unavailable")
	}
	if !approval.Allowed {
		return infraerrors.Forbidden("MEDIA_PROMPT_BLOCKED", "generation prompt was rejected")
	}
	return nil
}

func IsPublicMediaPath(path string) bool {
	return path == "/v1/media/catalog" || path == "/v1/media/quote" || path == "/v1/media/tasks" || strings.HasPrefix(path, "/v1/media/tasks/")
}

func publicMediaBudgetError(err error) error {
	if errors.Is(err, ErrAPIKeyBudgetExceeded) {
		return infraerrors.New(429, "API_KEY_ALLOWANCE_EXCEEDED", "API key allowance exceeded including in-flight requests")
	}
	if errors.Is(err, ErrAPIKeyBudgetConflict) {
		return infraerrors.Conflict("API_KEY_ALLOWANCE_CONFLICT", "API key allowance authorization refused; check key status")
	}
	if errors.Is(err, ErrAPIKeyBudgetReconciliation) {
		return infraerrors.ServiceUnavailable("API_KEY_ALLOWANCE_RECONCILIATION", "API key has unsettled historical usage; contact support")
	}
	return err
}

func validateMediaActor(key *APIKey) error {
	if key == nil || key.User == nil || key.ID <= 0 || key.UserID != key.User.ID || !key.User.IsActive() {
		return infraerrors.Forbidden("MEDIA_KEY_INELIGIBLE", "an active platform user is required")
	}
	if !key.IsActive() && key.Status != StatusAPIKeyExpired && key.Status != StatusAPIKeyQuotaExhausted {
		return infraerrors.Unauthorized("API_KEY_DISABLED", "API key is disabled")
	}
	return nil
}

// Call after task/segment locks; the retained Key lock serializes preference
// changes and revocation with both admission and the provider write fence.
func validateMediaEligibilityTx(ctx context.Context, tx *sql.Tx, r *mediaTaskRecord) error {
	var eligible bool
	err := tx.QueryRowContext(ctx, `SELECT k.user_id=$2 AND k.deleted_at IS NULL AND k.status='active' AND (k.expires_at IS NULL OR k.expires_at>now()) AND k.platform_key_id IS NOT NULL AND k.routing_mode='auto_channels' AND k.group_id IS NULL AND NOT EXISTS(SELECT 1 FROM api_key_channels WHERE api_key_id=k.id) AND EXISTS(SELECT 1 FROM users u WHERE u.id=$2 AND u.status='active' AND u.platform_user_id=$3 AND u.deleted_at IS NULL) FROM api_keys k WHERE k.id=$1 FOR UPDATE OF k`, r.APIKeyID, r.UserID, r.PlatformUserID).Scan(&eligible)
	if err != nil {
		return err
	}
	if !eligible {
		return infraerrors.Forbidden("MEDIA_KEY_INELIGIBLE", "use an active platform-managed unified API key with automatic channels and no channel restrictions")
	}
	return nil
}
