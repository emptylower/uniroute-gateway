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
	"os"
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
	case "queued":
		v.Billing.State = "queued"
	case "authorizing":
		v.Billing.State = "authorizing"
	case "submitting", "processing":
		v.Status = "processing"
	case "settling":
		v.Status = "success"
		v.Billing.State = "settling"
	case "completed":
		v.Status = "success"
		v.Billing.State = "charged"
		if r.PublicAPI {
			v.Billing.HeldUSD = "0"
		}
		cost := mediaUSD(*r.ActualUnits)
		v.Billing.ChargedUSD = &cost
		if r.HeldUnits > *r.ActualUnits {
			released := mediaUSD(r.HeldUnits - *r.ActualUnits)
			v.Billing.ReleasedUSD = &released
		}
	case "releasing":
		v.Status = "failed"
		v.Billing.State = "settling"
	case "failed":
		v.Status = "failed"
		v.Billing.State = "released"
		v.Billing.HeldUSD = "0"
		released := mediaUSD(r.HeldUnits)
		v.Billing.ReleasedUSD = &released
	case "indeterminate":
		v.Status = "indeterminate"
		v.Billing.State = "indeterminate"
	}
	if r.FinancialState == "released_unknown" || r.FinancialState == "released_zero" && r.Status != "completed" {
		v.Billing.State = "released"
		if r.FinancialState == "released_unknown" && r.PublicAPI {
			v.Billing.State = "released_unknown"
		}
		v.Billing.HeldUSD = "0"
		released := mediaUSD(r.HeldUnits)
		v.Billing.ReleasedUSD = &released
	}
	if r.Status == "completed" && r.FinancialState == "released_zero" {
		v.Billing.HeldUSD = "0"
	}

	return v
}

type MediaTaskService struct {
	cfg          *config.Config
	db           *sql.DB
	store        *mediaTaskStore
	bridge       *CanonicalWalletBridge
	snapshots    *BillingSnapshotService
	authorizer   *CanonicalWalletAuthorizer
	keys         *PlatformAPIKeyService
	apiKeys      *APIKeyService
	users        UserRepository
	provider     mediaProvider
	accountID    int64
	stop         chan struct{}
	stopOnce     sync.Once
	loops        sync.WaitGroup
	journalMu    sync.Mutex
	journalOwner *walletReaderJournalOwner
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
	s.closeMediaJournalOwner()
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

// PlaygroundKey resolves a non-exportable, user-owned key projection. The
// caller must still run the ordinary gateway authentication and wallet gates.
func (s *MediaTaskService) PlaygroundKey(ctx context.Context, userID int64) (*APIKey, error) {
	if s == nil || s.users == nil || s.keys == nil || s.apiKeys == nil {
		return nil, ErrMediaUnavailable
	}
	user, err := s.users.GetByID(ctx, userID)
	if err != nil {
		return nil, err
	}
	if !user.IsActive() || strings.TrimSpace(user.PlatformUserID) == "" {
		return nil, ErrMediaUnavailable
	}
	return s.internalKey(ctx, user)
}
func (s *MediaTaskService) Create(ctx context.Context, userID int64, idempotency string, in MediaCreateInput) (MediaTaskView, error) {
	// Internal Console callers retain the dependency-free suspension guard.
	// Public callers use CreatePublicChecked and keep owned replay first.
	if err := CheckModelAvailability(in.Model, mediaUpstreamModel(in.Model)); err != nil {
		return MediaTaskView{}, err
	}
	if s == nil || s.users == nil || s.keys == nil || s.keys.repo == nil || s.apiKeys == nil || s.apiKeys.apiKeyRepo == nil || s.store == nil || s.store.db == nil {
		return MediaTaskView{}, ErrMediaUnavailable
	}
	user, err := s.users.GetByID(ctx, userID)
	if err != nil {
		return MediaTaskView{}, err
	}
	key, err := s.internalKey(ctx, user)
	if err != nil {
		return MediaTaskView{}, err
	}
	return s.createForKey(ctx, key, idempotency, in, false, nil)
}
func (s *MediaTaskService) CreatePublic(ctx context.Context, key *APIKey, idempotency string, in MediaCreateInput) (MediaTaskView, error) {
	if err := validateMediaActor(key); err != nil {
		return MediaTaskView{}, err
	}
	return s.CreatePublicChecked(ctx, key, idempotency, in, nil)
}
func (s *MediaTaskService) CreatePublicChecked(ctx context.Context, key *APIKey, idempotency string, in MediaCreateInput, check func(MediaCreateInput) error) (MediaTaskView, error) {
	if err := validateMediaActor(key); err != nil {
		return MediaTaskView{}, err
	}
	return s.createForKey(ctx, key, idempotency, in, true, check)
}
func (s *MediaTaskService) createForKey(ctx context.Context, key *APIKey, idempotency string, in MediaCreateInput, public bool, check func(MediaCreateInput) error) (MediaTaskView, error) {
	if idempotency == "" || len(idempotency) > 128 || strings.ContainsAny(idempotency, "\r\n") {
		return MediaTaskView{}, infraerrors.BadRequest("INVALID_IDEMPOTENCY_KEY", "Idempotency-Key is required (maximum 128 characters)")
	}
	in, _, units, input, err := normalizeMediaCreateUnchecked(in)
	if err != nil {
		return MediaTaskView{}, err
	}
	if in.MaxCostUSD != nil {
		if *in.MaxCostUSD == "" {
			return MediaTaskView{}, infraerrors.BadRequest("INVALID_COST_CAP", "max_cost_usd must be a decimal string")
		}
		_, normalized, e := normalizeMediaCostCap(*in.MaxCostUSD)
		if e != nil {
			return MediaTaskView{}, e
		}
		in.MaxCostUSD = &normalized
	}
	normalized, _ := json.Marshal(in)
	hash := sha256.Sum256(normalized)
	if s.store == nil || s.store.db == nil {
		return MediaTaskView{}, ErrMediaUnavailable
	}
	if existing, e := s.store.idempotent(ctx, key.UserID, key.ID, idempotency); e == nil {
		if existing.RequestHash != hex.EncodeToString(hash[:]) {
			return MediaTaskView{}, ErrMediaIdempotencyConflict
		}
		return mediaTaskView(existing), nil
	} else if e != sql.ErrNoRows {
		return MediaTaskView{}, e
	}
	if public && strings.EqualFold(strings.TrimSpace(os.Getenv("USD_LEDGER_MAINTENANCE")), "true") {
		return MediaTaskView{}, infraerrors.ServiceUnavailable("USD_LEDGER_MAINTENANCE", "USD wallet maintenance in progress")
	}
	if !s.Enabled() || (public && !s.PublicEnabled(key.ID)) {
		return MediaTaskView{}, ErrMediaUnavailable
	}
	if err = CheckModelAvailability(in.Model, mediaUpstreamModel(in.Model)); err != nil {
		return MediaTaskView{}, err
	}
	user, err := s.users.GetByID(ctx, key.UserID)
	if err != nil {
		return MediaTaskView{}, err
	}
	actorKey := *key
	actorKey.User = user
	key = &actorKey
	if public {
		if err = validatePublicMediaKey(key, s.cfg, true); err != nil {
			return MediaTaskView{}, err
		}
	}
	fx, enabled, err := canonicalUSDWalletSnapshot(user, s.cfg)
	if err != nil || !enabled || user.Status != StatusActive {
		return MediaTaskView{}, ErrMediaUnavailable
	}
	if mediaCostPlusPricingEnabled() && mediaVariableSupplierModel(in.Model) {
		if in.MaxCostUSD == nil {
			return MediaTaskView{}, infraerrors.BadRequest("COST_CAP_REQUIRED", "max_cost_usd is required for usage-priced media")
		}
		cap, _, e := normalizeMediaCostCap(*in.MaxCostUSD)
		err = e
		if err != nil {
			return MediaTaskView{}, err
		}
		if units > cap {
			return MediaTaskView{}, infraerrors.BadRequest("MAX_COST_EXCEEDED", "quoted USD cost exceeds max_cost_usd")
		}
		units = cap
	} else if in.MaxCostUSD != nil {
		cap, _, _ := normalizeMediaCostCap(*in.MaxCostUSD)
		if units > cap {
			return MediaTaskView{}, infraerrors.BadRequest("MAX_COST_EXCEEDED", "quoted USD cost exceeds max_cost_usd")
		}
	}
	if check != nil {
		if err = check(in); err != nil {
			return MediaTaskView{}, err
		}
	}
	if public || s.cfg.MediaTasks.PublicAPIEnabled {
		if err = s.screenMedia(ctx, user.PlatformUserID, in); err != nil {
			return MediaTaskView{}, err
		}
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
	snap := &BillingSnapshot{ID: snapshotID, Version: BillingSnapshotVersion, FrozenAt: time.Now().UTC(), Family: BillingFamily("media"), UserID: user.ID, APIKeyID: key.ID, AccountID: s.accountID, RequestedModel: in.Model, BillingModel: in.Model, Pricing: BillingSnapshotPricing{Mode: BillingModePerRequest, Source: mediaPricingVersion, DefaultPerRequestPrice: float64(units) / float64(mediaUnitsPerUSD)}, Multipliers: BillingSnapshotMultipliers{Base: 1, Text: 1, Image: 1, Video: 1, WebSearch: 1, Account: 1}, FX: fx, Flags: BillingSnapshotFlags{USDWalletPolicyVersion: config.CanonicalUSDWalletPolicyVersion, BillingCurrency: CurrencyUSD, MultiplierCurrency: CurrencyUSD}}
	snap.Pricing.MediaSupplierPricing = newMediaSupplierPricing(in.Model, units)
	normalized, _ = json.Marshal(in)
	requestHash := sha256.Sum256(normalized)
	r, err := s.store.create(ctx, &mediaTaskRecord{PublicAPI: public, ID: jobID, UserID: user.ID, PlatformUserID: user.PlatformUserID, APIKeyID: key.ID, IdempotencyKey: idempotency, RequestHash: hex.EncodeToString(requestHash[:]), Model: in.Model, MediaType: in.MediaType, Option: in.Option, Prompt: in.Prompt, RequestPayload: input, SnapshotID: snapshotID, QuotedUnits: units, AuthorizationID: authID, EventID: CanonicalWalletSettlementEventID(jobID, user.PlatformUserID, CurrencyUSD), DeadlineAt: time.Now().UTC().Add(time.Duration(s.cfg.MediaTasks.DeadlineSeconds) * time.Second)}, snap, s.bridge)
	if err != nil {
		return MediaTaskView{}, publicMediaBudgetError(err)
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
	return s.listForKey(ctx, userID, 0, limit, cursor)
}
func (s *MediaTaskService) listForKey(ctx context.Context, userID, keyID int64, limit int, cursor string) (MediaTaskList, error) {
	if limit < 1 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	var before time.Time
	var id string
	if len(cursor) > 4096 {
		return MediaTaskList{}, infraerrors.BadRequest("INVALID_CURSOR", "invalid cursor")
	}
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
	rows, err := s.store.listForKey(ctx, userID, keyID, limit+1, before, id)
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
	result, err := s.provider.Read(ctx, taskID, kind)
	result.ProviderModel, result.CreditsConsumed = "", nil
	return result, err
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

func (s *MediaTaskService) GetPublic(ctx context.Context, key *APIKey, id string) (MediaTaskView, error) {
	r, err := s.store.getForKey(ctx, key.UserID, key.ID, id)
	if err != nil {
		return MediaTaskView{}, mediaStoreError(err)
	}
	return mediaTaskView(r), nil
}
func (s *MediaTaskService) ListPublic(ctx context.Context, key *APIKey, limit int, cursor string) (MediaTaskList, error) {
	return s.listForKey(ctx, key.UserID, key.ID, limit, cursor)
}
