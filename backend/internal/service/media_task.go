package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
)

var (
	ErrMediaTaskNotFound        = infraerrors.NotFound("MEDIA_TASK_NOT_FOUND", "media task not found")
	ErrMediaIdempotencyConflict = infraerrors.Conflict("MEDIA_IDEMPOTENCY_CONFLICT", "idempotency key belongs to another request")
	ErrMediaUnavailable         = infraerrors.ServiceUnavailable("MEDIA_UNAVAILABLE", "media generation is temporarily unavailable")
)

type MediaBillingView struct {
	Currency      string  `json:"currency"`
	State         string  `json:"state"`
	QuotedUSD     string  `json:"quoted_usd"`
	HeldUSD       string  `json:"held_usd"`
	ChargedUSD    *string `json:"charged_usd"`
	ReleasedUSD   *string `json:"released_usd"`
	PolicyVersion string  `json:"policy_version"`
}
type MediaTaskView struct {
	TaskID       string           `json:"task_id"`
	Model        string           `json:"model"`
	MediaType    string           `json:"media_type"`
	Option       string           `json:"option"`
	Prompt       string           `json:"prompt"`
	Status       string           `json:"status"`
	Billing      MediaBillingView `json:"billing"`
	URLs         []MediaURL       `json:"urls"`
	Title        string           `json:"title,omitempty"`
	CoverURL     string           `json:"cover_url,omitempty"`
	ErrorCode    string           `json:"error_code,omitempty"`
	ErrorMessage string           `json:"error_message,omitempty"`
	CreatedAt    time.Time        `json:"created_at"`
	UpdatedAt    time.Time        `json:"updated_at"`
}
type MediaTaskList struct {
	Tasks      []MediaTaskView `json:"tasks"`
	NextCursor *string         `json:"next_cursor"`
}

func mediaTaskView(r *mediaTaskRecord) MediaTaskView {
	v := MediaTaskView{TaskID: r.ID, Model: r.Model, MediaType: r.MediaType, Option: r.Option, Prompt: r.Prompt, Status: "pending", URLs: r.Result.URLs, Title: r.Result.Title, CoverURL: r.Result.CoverURL, ErrorCode: r.ErrorCode, ErrorMessage: r.ErrorMessage, CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt, Billing: MediaBillingView{Currency: "USD", State: "held", QuotedUSD: mediaUSD(r.QuotedUnits), HeldUSD: mediaUSD(r.HeldUnits), PolicyVersion: config.CanonicalUSDWalletPolicyVersion}}
	if v.URLs == nil {
		v.URLs = []MediaURL{}
	}
	switch r.Status {
	case "submitting", "processing":
		v.Status = "processing"
	case "settling":
		v.Status = "success"
		v.Billing.State = "settling"
	case "completed":
		v.Status = "success"
		v.Billing.State = "charged"
		cost := mediaUSD(*r.ActualUnits)
		v.Billing.ChargedUSD = &cost
	case "releasing":
		v.Status = "failed"
		v.Billing.State = "settling"
	case "failed":
		v.Status = "failed"
		v.Billing.State = "released"
		released := mediaUSD(r.HeldUnits)
		v.Billing.ReleasedUSD = &released
	case "indeterminate":
		v.Status = "indeterminate"
		v.Billing.State = "indeterminate"
	}
	return v
}

type MediaTaskService struct {
	cfg        *config.Config
	db         *sql.DB
	store      *mediaTaskStore
	bridge     *CanonicalWalletBridge
	snapshots  *BillingSnapshotService
	authorizer *CanonicalWalletAuthorizer
	keys       *PlatformAPIKeyService
	apiKeys    *APIKeyService
	users      UserRepository
	provider   mediaProvider
	accountID  int64
	stop       chan struct{}
	stopOnce   sync.Once
	loops      sync.WaitGroup
}

func NewMediaTaskService(cfg *config.Config, db *sql.DB, bridge *CanonicalWalletBridge, snapshots *BillingSnapshotService, keys *PlatformAPIKeyService, apiKeys *APIKeyService, users UserRepository, upstream HTTPUpstream) (*MediaTaskService, error) {
	s := &MediaTaskService{cfg: cfg, db: db, store: &mediaTaskStore{db}, snapshots: snapshots, keys: keys, apiKeys: apiKeys, users: users, stop: make(chan struct{})}
	s.bridge = bridge
	if db == nil {
		return s, nil
	}
	err := db.QueryRow(`SELECT account_id FROM gateway_media_provider WHERE provider='kie'`).Scan(&s.accountID)
	if err != nil {
		return nil, err
	}
	s.authorizer = NewCanonicalWalletAuthorizer(cfg, s.bridge, snapshots)
	if cfg != nil && strings.TrimSpace(cfg.MediaTasks.KIEAPIKey) != "" && upstream != nil {
		s.provider = &kieMediaProvider{upstream: upstream, baseURL: cfg.MediaTasks.KIEBaseURL, apiKey: cfg.MediaTasks.KIEAPIKey, accountID: s.accountID}
	}
	// Recovery is intentionally independent of Enabled. Turning off new creates
	// cannot abandon a task, unpin funds, or stop delivery of accepted usage.
	if s.bridge != nil {
		s.loops.Add(1)
		go s.run()
	}
	return s, nil
}
func (s *MediaTaskService) Stop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() { close(s.stop) })
	s.loops.Wait()
}
func (s *MediaTaskService) Enabled() bool {
	return s != nil && s.cfg != nil && s.cfg.MediaTasks.Enabled && s.provider != nil && s.bridge != nil && s.bridge.HoldsEnabled() && s.cfg.CanonicalWallet.Mode == config.CanonicalWalletModeEnforce && s.cfg.CanonicalWallet.USDWalletEnabled && s.cfg.CanonicalWallet.USDPolicyVersion == config.CanonicalUSDWalletPolicyVersion
}
func (s *MediaTaskService) Quote(model, option string) (MediaQuote, error) {
	if !s.Enabled() {
		return MediaQuote{}, ErrMediaUnavailable
	}
	return mediaQuote(model, option)
}

func (s *MediaTaskService) internalKey(ctx context.Context, user *User) (*APIKey, error) {
	hash := sha256.Sum256([]byte(user.PlatformUserID))
	id := "playground:" + hex.EncodeToString(hash[:])
	projection, err := s.keys.repo.FindProjectedByPlatformKeyID(ctx, id)
	if err != nil {
		return nil, err
	}
	if projection == nil {
		var nonce [32]byte
		if _, err = rand.Read(nonce[:]); err != nil {
			return nil, err
		}
		verifier := sha256.Sum256(nonce[:])
		projection, _, err = s.keys.Upsert(ctx, user.ID, PlatformAPIKeyUpsert{PlatformKeyID: id, KeySHA256: hex.EncodeToString(verifier[:]), KeyPrefix: "internal-playground", Status: StatusActive, Version: 1, Name: "Playground (internal)"})
		if err != nil {
			projection, err = s.keys.repo.FindProjectedByPlatformKeyID(ctx, id)
			if err != nil || projection == nil {
				return nil, ErrMediaUnavailable
			}
		}
	}
	if projection.GatewayUserID != user.ID || projection.Status != StatusActive {
		return nil, ErrMediaUnavailable
	}
	key, err := s.apiKeys.GetByID(ctx, projection.GatewayAPIKeyID)
	if err != nil {
		return nil, err
	}
	if key.UserID != user.ID || key.Status != StatusActive {
		return nil, ErrMediaUnavailable
	}
	return key, nil
}
func (s *MediaTaskService) Create(ctx context.Context, userID int64, idempotency string, in MediaCreateInput) (MediaTaskView, error) {
	if !s.Enabled() {
		return MediaTaskView{}, ErrMediaUnavailable
	}
	if idempotency == "" || len(idempotency) > 128 || strings.ContainsAny(idempotency, "\r\n") {
		return MediaTaskView{}, infraerrors.BadRequest("INVALID_IDEMPOTENCY_KEY", "Idempotency-Key is required (maximum 128 characters)")
	}
	in, _, units, input, err := normalizeMediaCreate(in)
	if err != nil {
		return MediaTaskView{}, err
	}
	user, err := s.users.GetByID(ctx, userID)
	if err != nil {
		return MediaTaskView{}, err
	}
	fx, enabled, err := canonicalUSDWalletSnapshot(user, s.cfg)
	if err != nil || !enabled || user.Status != StatusActive {
		return MediaTaskView{}, ErrMediaUnavailable
	}
	key, err := s.internalKey(ctx, user)
	if err != nil {
		return MediaTaskView{}, err
	}
	snapshotID, err := newBillingSnapshotID()
	if err != nil {
		return MediaTaskView{}, err
	}
	authID, err := newAuthorizationID()
	if err != nil {
		return MediaTaskView{}, err
	}
	jobID := "media_" + strings.TrimPrefix(authID, "auth_")
	snap := &BillingSnapshot{ID: snapshotID, Version: BillingSnapshotVersion, FrozenAt: time.Now().UTC(), Family: BillingFamily("media"), UserID: user.ID, APIKeyID: key.ID, AccountID: s.accountID, RequestedModel: in.Model, BillingModel: in.Model, Pricing: BillingSnapshotPricing{Mode: BillingModePerRequest, Source: mediaPricingVersion, DefaultPerRequestPrice: float64(units) / float64(mediaUnitsPerUSD)}, Multipliers: BillingSnapshotMultipliers{Base: 1, Text: 1, Image: 1, Video: 1, WebSearch: 1, Account: 1}, FX: fx, Flags: BillingSnapshotFlags{USDWalletPolicyVersion: config.CanonicalUSDWalletPolicyVersion, BillingCurrency: CurrencyCNY, MultiplierCurrency: CurrencyUSD}}
	normalized, _ := json.Marshal(in)
	requestHash := sha256.Sum256(normalized)
	r, err := s.store.create(ctx, &mediaTaskRecord{ID: jobID, UserID: userID, PlatformUserID: user.PlatformUserID, APIKeyID: key.ID, IdempotencyKey: idempotency, RequestHash: hex.EncodeToString(requestHash[:]), Model: in.Model, MediaType: in.MediaType, Option: in.Option, Prompt: in.Prompt, RequestPayload: input, SnapshotID: snapshotID, QuotedUnits: units, AuthorizationID: authID, EventID: CanonicalWalletSettlementEventID(jobID, user.PlatformUserID, CurrencyCNY), DeadlineAt: time.Now().UTC().Add(time.Duration(s.cfg.MediaTasks.DeadlineSeconds) * time.Second)}, snap)
	if err != nil {
		return MediaTaskView{}, err
	}
	return mediaTaskView(r), nil
}
func (s *MediaTaskService) Get(ctx context.Context, userID int64, id string) (MediaTaskView, error) {
	r, err := s.store.get(ctx, userID, id)
	if err != nil {
		return MediaTaskView{}, mediaStoreError(err)
	}
	return mediaTaskView(r), nil
}
func (s *MediaTaskService) List(ctx context.Context, userID int64, limit int, cursor string) (MediaTaskList, error) {
	if limit < 1 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	var before time.Time
	var id string
	if cursor != "" {
		raw, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil {
			return MediaTaskList{}, infraerrors.BadRequest("INVALID_CURSOR", "invalid cursor")
		}
		parts := strings.SplitN(string(raw), "|", 2)
		if len(parts) != 2 {
			return MediaTaskList{}, infraerrors.BadRequest("INVALID_CURSOR", "invalid cursor")
		}
		before, err = time.Parse(time.RFC3339Nano, parts[0])
		if err != nil {
			return MediaTaskList{}, infraerrors.BadRequest("INVALID_CURSOR", "invalid cursor")
		}
		id = parts[1]
	}
	rows, err := s.store.list(ctx, userID, limit+1, before, id)
	if err != nil {
		return MediaTaskList{}, err
	}
	out := MediaTaskList{Tasks: []MediaTaskView{}}
	if len(rows) > limit {
		rows = rows[:limit]
		last := rows[len(rows)-1]
		c := base64.RawURLEncoding.EncodeToString([]byte(last.CreatedAt.Format(time.RFC3339Nano) + "|" + last.ID))
		out.NextCursor = &c
	}
	for _, r := range rows {
		out.Tasks = append(out.Tasks, mediaTaskView(r))
	}
	return out, nil
}
func (s *MediaTaskService) LegacyRead(ctx context.Context, taskID, model, kind string) (MediaProviderResult, error) {
	if s.provider == nil {
		return MediaProviderResult{}, ErrMediaUnavailable
	}
	m, _, _, err := resolveMediaQuote(model, "")
	if err != nil || m.MediaKind != kind || strings.TrimSpace(taskID) == "" || len(taskID) > 256 {
		return MediaProviderResult{}, infraerrors.BadRequest("INVALID_LEGACY_TASK", "invalid legacy task")
	}
	return s.provider.Read(ctx, taskID, kind)
}
func (b *CanonicalWalletBridge) mediaTaskActive(ctx context.Context, auth string) bool {
	if b == nil || b.outboxDB == nil {
		return false
	}
	active, err := (&mediaTaskStore{b.outboxDB}).active(ctx, auth)
	if err != nil {
		slog.Warn("media hold liveness unavailable", "error", err)
		return true
	}
	return active
}
func (b *CanonicalWalletBridge) mediaLeasePinned(ctx context.Context, user, lease string) (bool, error) {
	if b == nil || b.outboxDB == nil {
		return false, nil
	}
	return (&mediaTaskStore{b.outboxDB}).leasePinned(ctx, user, lease)
}
