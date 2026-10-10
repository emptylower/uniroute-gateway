//go:build media_integration

package media_integration

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

var mediaTemplateOnce sync.Once
var mediaTemplateErr error

func mediaDatabase(t *testing.T) (*sql.DB, *redis.Client) {
	t.Helper()
	dsn := os.Getenv("UNIROUTE_MEDIA_TEST_POSTGRES_DSN")
	addr := os.Getenv("UNIROUTE_MEDIA_TEST_REDIS_ADDR")
	require.NotEmpty(t, dsn, "external isolated PostgreSQL is required; no skip/fallback")
	require.Equal(t, "127.0.0.1:56379", addr, "only the isolated test Redis endpoint is permitted")
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(strings.TrimPrefix(u.Path, "/"), "uniroute_media_test_"), "refuse non-test database")
	require.Equal(t, "127.0.0.1", u.Hostname())
	require.Equal(t, "55432", u.Port(), "only the isolated test PostgreSQL endpoint is permitted")
	mediaTemplateOnce.Do(func() {
		base, e := sql.Open("postgres", dsn)
		if e != nil {
			mediaTemplateErr = e
			return
		}
		defer base.Close()
		mediaTemplateErr = repository.ApplyMigrations(context.Background(), base)
		if mediaTemplateErr == nil {
			mediaTemplateErr = repository.ApplyMigrations(context.Background(), base)
		}
	})
	require.NoError(t, mediaTemplateErr, "full migration and idempotent replay")
	template := strings.TrimPrefix(u.Path, "/")
	adminURL := *u
	adminURL.Path = "/postgres"
	admin, err := sql.Open("postgres", adminURL.String())
	require.NoError(t, err)
	defer admin.Close()
	name := "uniroute_media_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = admin.Exec(`CREATE DATABASE "` + name + `" TEMPLATE "` + template + `"`)
	require.NoError(t, err)
	u.Path = "/" + name
	db, err := sql.Open("postgres", u.String())
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
		admin, _ := sql.Open("postgres", adminURL.String())
		defer admin.Close()
		_, _ = admin.Exec(`DROP DATABASE "` + name + `" WITH (FORCE)`)
	})
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: 15})
	require.NoError(t, rdb.FlushDB(context.Background()).Err())
	t.Cleanup(func() { rdb.Close() })
	return db, rdb
}

type mediaUsers struct {
	service.UserRepository
	db *sql.DB
}

func (r *mediaUsers) GetByID(ctx context.Context, id int64) (*service.User, error) {
	u := &service.User{}
	err := r.db.QueryRowContext(ctx, `SELECT id,platform_user_id,billing_currency,status,balance FROM users WHERE id=$1`, id).Scan(&u.ID, &u.PlatformUserID, &u.BillingCurrency, &u.Status, &u.Balance)
	return u, err
}

type mediaKeys struct {
	service.APIKeyRepository
	db *sql.DB
}

func (r *mediaKeys) GetByID(ctx context.Context, id int64) (*service.APIKey, error) {
	k := &service.APIKey{}
	err := r.db.QueryRowContext(ctx, `SELECT id,user_id,status FROM api_keys WHERE id=$1`, id).Scan(&k.ID, &k.UserID, &k.Status)
	return k, err
}

type mediaProjection struct{ db *sql.DB }

func (r *mediaProjection) FindProjectedByPlatformKeyID(ctx context.Context, id string) (*service.PlatformAPIKeyProjection, error) {
	p := &service.PlatformAPIKeyProjection{}
	err := r.db.QueryRowContext(ctx, `SELECT id,user_id,platform_key_id,key_sha256,key_prefix,status,platform_key_version,name FROM api_keys WHERE platform_key_id=$1`, id).Scan(&p.GatewayAPIKeyID, &p.GatewayUserID, &p.PlatformKeyID, &p.KeySHA256, &p.KeyPrefix, &p.Status, &p.Version, &p.Name)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return p, err
}
func (r *mediaProjection) CreateProjected(ctx context.Context, p *service.PlatformAPIKeyProjection, key string) error {
	return r.db.QueryRowContext(ctx, `INSERT INTO api_keys(user_id,key,name,status,routing_mode,platform_key_id,key_sha256,key_prefix,platform_key_version) VALUES($1,$2,$3,$4,'auto_channels',$5,$6,$7,$8) RETURNING id`, p.GatewayUserID, key, p.Name, p.Status, p.PlatformKeyID, p.KeySHA256, p.KeyPrefix, p.Version).Scan(&p.GatewayAPIKeyID)
}
func (r *mediaProjection) UpdateProjected(context.Context, *service.PlatformAPIKeyProjection, int64) (bool, error) {
	return false, fmt.Errorf("unexpected projection mutation")
}

type mediaHTTP struct {
	service.HTTPUpstream
	client *http.Client
}

func (u *mediaHTTP) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	return u.client.Do(req)
}

type testControl struct {
	zeroReceipts        map[string]map[string]any
	secret              string
	mu                  sync.Mutex
	budget, captured    int64
	expires             time.Time
	events              map[string]int64
	pins                map[string]map[string]any
	pinAckLost          bool
	pinRefused          bool
	pinFinishRefused    bool
	pinAccepted         chan struct{}
	issued              bool
	pool                []*testFundingLease
	unleased            int64
	pinStatus           map[string]string
	expiryAckLost       bool
	expiryRefused       bool
	expiryRefusedAuth   string
	forceOverCapture    bool
	overCaptureHeadroom int64
	fundingReturns      map[string]service.WalletFundingReturnReceipt
	sourceReceipts      map[string]service.WalletFundingSourceReceipt
	// survivingPin makes the signed close behave as the Worker does while a D1 pin
	// is still active on the lease: the close is refused with a funding conflict.
	survivingPin bool
}

func amount(n int64) map[string]any {
	return map[string]any{"amount_units": strconv.FormatInt(n, 10), "currency": "USD", "scale": 8, "unit_version": "usd-e8-v1"}
}
func units(v any) int64 {
	m, _ := v.(map[string]any)
	raw, _ := m["amount_units"].(string)
	n, _ := strconv.ParseInt(raw, 10, 64)
	return n
}

type testFundingLease struct {
	id                                       string
	budget, captured                         int64
	expires                                  time.Time
	drained                                  bool
	closed                                   bool
	scope, owner, key                        string
	returned, returnRevision, budgetRevision int64
	frozen                                   bool
}

func (c *testControl) lease(id string) *testFundingLease {
	for _, l := range c.pool {
		if l.id == id {
			return l
		}
	}
	return nil
}
func (c *testControl) wire(id, user string) map[string]any {
	budget, captured, expiry := c.budget, c.captured, c.expires
	if l := c.lease(id); l != nil {
		budget, captured, expiry = l.budget, l.captured, l.expires
	}
	out := map[string]any{"lease_id": id, "platform_user_id": user, "currency": "USD", "unit_version": "usd-e8-v1", "scale": 8, "budget": amount(budget), "reserved": amount(0), "captured": amount(captured), "released": amount(0), "headroom": amount(budget - captured), "capture_seq": len(c.events), "status": "active", "expires_at": expiry, "outcome": "reused", "usd_wallet_policy_version": "usd-wallet-v1", "drained_at": nil}
	// The canonical pool view always reports the active/live pin counts from the
	// same snapshot (the Gateway fails closed if they are absent or invalid).
	active, live := int64(0), int64(0)
	for auth, pin := range c.pins {
		if pin["lease_id"] == id && c.pinStatus[auth] == "active" {
			active++
			if pin["authorization_kind"] == "live" {
				live++
			}
		}
	}
	out["active_pin_count"], out["active_live_pin_count"] = active, live
	if l := c.lease(id); l != nil {
		out["funding_scope"], out["funding_owner_id"], out["funding_issuance_key"] = l.scope, l.owner, l.key
		out["return_revision"], out["budget_revision"] = l.returnRevision, l.budgetRevision
		out["released"], out["headroom"] = amount(l.returned), amount(l.budget-l.returned-l.captured)
		if l.frozen {
			out["funding_frozen_at"] = time.Now().UTC()
		}
		if l.closed {
			out["status"] = "closed"
		}
	}
	return out
}
func (c *testControl) handler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(405)
		return
	}
	var in map[string]any
	if json.NewDecoder(r.Body).Decode(&in) != nil {
		w.WriteHeader(400)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	data := map[string]any{}
	user, _ := in["platform_user_id"].(string)
	switch r.URL.Path {
	case "/api/internal/v2/wallet/leases/pool":
		leases := []any{}
		if len(c.pool) > 0 {
			for _, l := range c.pool {
				if l.closed {
					continue // the Worker's pool view lists active leases only
				}
				v := c.wire(l.id, user)
				if l.drained {
					v["drained_at"] = time.Now()
				}
				leases = append(leases, v)
			}
		} else if c.issued {
			leases = append(leases, c.wire("lease-test", user))
		}
		data = map[string]any{"leases": leases, "captured_event_ids": []string{}}
	case "/api/internal/v2/wallet/leases/ensure":
		scope, _ := in["funding_scope"].(string)
		if scope == "media" || scope == "settle" {
			owner, _ := in["funding_owner_id"].(string)
			key, _ := in["funding_issuance_key"].(string)
			minimum := units(in["min_headroom"])
			excluded := map[string]bool{}
			if ids, ok := in["exclude_lease_ids"].([]any); ok {
				for _, id := range ids {
					if s, ok := id.(string); ok {
						excluded[s] = true
					}
				}
			}
			// Worker lease-ensure: only an issuance key replays one immutable grant.
			// A keyless settle ensure reuses a covering settle/legacy lease (active,
			// unexpired, unfrozen, not excluded, headroom >= min_headroom) or issues
			// a fresh one; it never hands back an exhausted lease.
			for _, l := range c.pool {
				if key != "" && l.scope == scope && l.owner == owner && l.key == key {
					data = c.wire(l.id, user)
					break
				}
				if key == "" && scope == "settle" && (l.scope == "settle" || l.scope == "legacy") && !excluded[l.id] && !l.frozen && !l.drained && !l.closed && time.Now().Before(l.expires) && minimum > 0 && l.budget-l.returned-l.captured >= minimum {
					data = c.wire(l.id, user)
					break
				}
			}
			if len(data) > 0 {
				break
			}
			requested := units(in["requested_budget"])
			if requested < minimum {
				requested = minimum
			}
			if len(c.pool) == 0 {
				if c.issued {
					c.pool = append(c.pool, &testFundingLease{id: "lease-test", budget: c.budget, captured: c.captured, expires: c.expires, scope: "llm"})
				} else {
					c.unleased = c.budget - c.captured
				}
			}
			// Worker clampBudget: grant max(requested, min_headroom) clamped to the
			// spendable balance; refuse only when that is below min_headroom.
			if requested > c.unleased {
				requested = c.unleased
			}
			if requested <= 0 || requested < minimum {
				w.WriteHeader(409)
				_ = json.NewEncoder(w).Encode(map[string]any{"code": 409, "data": map[string]string{"reason": "insufficient_balance"}})
				return
			}
			sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%d", user, scope, owner, key, len(c.pool))))
			id := "lease-scoped-" + hex.EncodeToString(sum[:8])
			c.pool = append(c.pool, &testFundingLease{id: id, budget: requested, expires: time.Now().Add(5 * time.Minute), scope: scope, owner: owner, key: key})
			c.unleased -= requested
			data = c.wire(id, user)
			break
		}
		c.issued = true
		id := "lease-test"
		if target, _ := in["top_up_lease_id"].(string); target != "" {
			id = target
		}
		if len(c.pool) > 0 && id == "lease-test" {
			minimum := units(in["min_headroom"])
			if c.unleased < minimum {
				if in["purpose"] == "settle" && c.unleased > 0 {
					minimum = c.unleased
				} else {
					w.WriteHeader(409)
					_ = json.NewEncoder(w).Encode(map[string]any{"code": 409, "data": map[string]string{"reason": "insufficient_balance"}})
					return
				}
			}
			id = fmt.Sprintf("lease-cold-%d", len(c.pool))
			c.pool = append(c.pool, &testFundingLease{id: id, budget: minimum, expires: time.Now().Add(5 * time.Minute), scope: "llm"})
			c.unleased -= minimum
		}
		if value, _ := in["minimum_budget_units"].(string); value != "" {
			target, _ := strconv.ParseInt(value, 10, 64)
			if l := c.lease(id); l != nil {
				extra := target - l.budget
				if extra > c.unleased {
					w.WriteHeader(409)
					_ = json.NewEncoder(w).Encode(map[string]any{"code": 409, "data": map[string]string{"reason": "insufficient_balance"}})
					return
				}
				if extra > 0 {
					c.unleased -= extra
					l.budget = target
				}
			} else if target > c.budget {
				c.budget = target
			}
		}
		data = c.wire(id, user)
	case "/api/internal/v2/wallet/leases/freeze-source":
		// The signed source fence the Gateway commits before it asks for any
		// return. Same wire contract as the real Worker route: an immutable freeze
		// id per lease, replay returns the original receipt, and the allocations
		// conserve the funded principal.
		leaseID, _ := in["lease_id"].(string)
		freezeID, _ := in["freeze_id"].(string)
		l := c.lease(leaseID)
		if l == nil {
			w.WriteHeader(404)
			return
		}
		if previous, ok := c.sourceReceipts[freezeID]; ok {
			data = map[string]any{"receipt": previous}
			break
		}
		funded := units(in["expected_funded"])
		revision, _ := in["expected_budget_revision"].(float64)
		if freezeID != leaseID+".source-freeze.v1" || funded <= 0 || funded != l.budget+l.returned || int64(revision) != l.budgetRevision {
			w.WriteHeader(409)
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 409, "data": map[string]string{"reason": "funding_conflict"}})
			return
		}
		text := func(n int64) string { return strconv.FormatInt(n, 10) }
		receipt := service.WalletFundingSourceReceipt{Protocol: "wallet-funding-source-v1", FreezeID: freezeID, PlatformUserID: user, LeaseID: leaseID, FundingScope: l.scope, FundingOwnerID: l.owner, FundingIssuanceKey: l.key, FundedUnits: text(funded), BudgetRevision: l.budgetRevision, CommittedAt: time.Now().UTC().Format(time.RFC3339Nano),
			Sources: []service.WalletFundingSource{{AllocationID: leaseID + ":original-allocation", CreditID: leaseID + ":original-credit", AllocatedUnits: text(funded)}}}
		sources, _ := json.Marshal(receipt.Sources)
		payload, _ := json.Marshal([]string{receipt.Protocol, receipt.FreezeID, user, leaseID, l.scope, l.owner, l.key, receipt.FundedUnits, text(receipt.BudgetRevision), receipt.CommittedAt, string(sources)})
		secret := c.secret
		if secret == "" {
			secret = strings.Repeat("s", 32)
		}
		mac := hmac.New(sha256.New, []byte(secret))
		_, _ = mac.Write(payload)
		receipt.Signature = hex.EncodeToString(mac.Sum(nil))
		l.frozen = true
		if c.sourceReceipts == nil {
			c.sourceReceipts = map[string]service.WalletFundingSourceReceipt{}
		}
		c.sourceReceipts[freezeID] = receipt
		data = map[string]any{"receipt": receipt}
	case "/api/internal/v2/wallet/leases/return":
		leaseID, _ := in["lease_id"].(string)
		l := c.lease(leaseID)
		if l == nil {
			w.WriteHeader(404)
			return
		}
		if c.survivingPin && l.scope == "media" {
			w.WriteHeader(409)
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 409, "data": map[string]string{"reason": "funding_conflict"}})
			return
		}
		base, _ := in["base_return_revision"].(float64)
		mode, _ := in["mode"].(string)
		returnKey := leaseID + ":" + strconv.FormatInt(int64(base), 10)
		if previous, ok := c.fundingReturns[returnKey]; ok {
			data = map[string]any{"receipt": previous}
			break
		}
		if int64(base) != l.returnRevision || mode != "partial" && mode != "close" {
			w.WriteHeader(409)
			return
		}
		consumed, released := units(in["gateway_consumed"]), units(in["gateway_released"])
		held := int64(0)
		if holds, ok := in["holds"].([]any); ok {
			for _, raw := range holds {
				h, _ := raw.(map[string]any)
				held += units(h["held"])
			}
		}
		activePins := int64(0)
		for auth, pin := range c.pins {
			if pin["lease_id"] == leaseID && c.pinStatus[auth] == "active" {
				activePins += units(pin["held"])
			}
		}
		net := consumed - released
		if net != l.captured+held || activePins != held || mode == "close" && held != 0 {
			w.WriteHeader(409)
			return
		}
		free := l.budget - l.returned - net
		if free < 0 {
			w.WriteHeader(409)
			return
		}
		text := func(n int64) string { return strconv.FormatInt(n, 10) }
		receipt := service.WalletFundingReturnReceipt{Protocol: "wallet-funding-return-v1", ReceiptID: returnKey, PlatformUserID: user, LeaseID: leaseID, FundingScope: l.scope, FundingOwnerID: l.owner, FundingIssuanceKey: l.key, ReturnRevision: l.returnRevision + 1, BudgetRevision: l.budgetRevision, CaptureSeq: int64(len(c.events)), FundedUnits: text(l.budget), CapturedUnits: text(l.captured), ReturnedBeforeUnits: text(l.returned), ReturnedUnits: text(free), ReturnedAfterUnits: text(l.returned + free), CreditedUnits: text(free), WriteOffUnits: "0", HeldUnits: text(held), GatewayConsumedUnits: text(consumed), GatewayReleasedUnits: text(released), Mode: mode, CommittedAt: time.Now().UTC().Format("2006-01-02T15:04:05.000Z"), Sources: []service.WalletFundingReturnSource{}}
		if free > 0 {
			receipt.Sources = append(receipt.Sources, service.WalletFundingReturnSource{AllocationID: leaseID + ":original-allocation", CreditID: leaseID + ":original-credit", ReturnedUnits: text(free), CreditedUnits: text(free)})
		}
		sources, _ := json.Marshal(receipt.Sources)
		payload, _ := json.Marshal([]string{receipt.Protocol, receipt.ReceiptID, user, leaseID, l.scope, l.owner, l.key, text(receipt.ReturnRevision), text(receipt.BudgetRevision), text(receipt.CaptureSeq), receipt.FundedUnits, receipt.CapturedUnits, receipt.ReturnedBeforeUnits, receipt.ReturnedUnits, receipt.ReturnedAfterUnits, receipt.CreditedUnits, receipt.WriteOffUnits, receipt.HeldUnits, receipt.GatewayConsumedUnits, receipt.GatewayReleasedUnits, mode, receipt.CommittedAt, string(sources)})
		secret := c.secret
		if secret == "" {
			secret = strings.Repeat("s", 32)
		}
		mac := hmac.New(sha256.New, []byte(secret))
		_, _ = mac.Write(payload)
		receipt.Signature = hex.EncodeToString(mac.Sum(nil))
		l.returned += free
		l.returnRevision++
		l.frozen = true
		l.closed = mode == "close"
		c.unleased += free
		if c.fundingReturns == nil {
			c.fundingReturns = map[string]service.WalletFundingReturnReceipt{}
		}
		c.fundingReturns[returnKey] = receipt
		data = map[string]any{"receipt": receipt}
	case "/api/internal/v2/wallet/settlements":
		id, _ := in["event_id"].(string)
		lease, _ := in["lease_id"].(string)
		n := units(in["amount"])
		duplicate := c.events[id] > 0
		if !duplicate && c.forceOverCapture {
			w.WriteHeader(409)
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 409, "data": map[string]any{"reason": "lease_over_capture", "headroom": amount(c.overCaptureHeadroom)}})
			return
		}
		if !duplicate {
			if l := c.lease(lease); l != nil && l.closed {
				w.WriteHeader(409)
				_ = json.NewEncoder(w).Encode(map[string]any{"code": 409, "data": map[string]string{"reason": "lease_not_capturable"}})
				return
			}
		}
		if !duplicate {
			c.events[id] = n
			if l := c.lease(lease); l != nil {
				l.captured += n
			} else {
				c.captured += n
			}
		}
		data = map[string]any{"accepted": true, "duplicate": duplicate, "canonical_balance": amount(c.budget - c.captured), "event": map[string]any{"lease_id": lease, "amount": amount(n), "lease_capture_seq": len(c.events)}}
	case "/api/internal/v2/wallet/task-pins/expire":
		auth, _ := in["authorization_id"].(string)
		lease, _ := in["lease_id"].(string)
		event, _ := in["settlement_event_id"].(string)
		if c.expiryRefused || c.expiryRefusedAuth == auth || c.events[event] > 0 {
			w.WriteHeader(409)
			return
		}
		if c.pinStatus == nil {
			c.pinStatus = map[string]string{}
		}
		if c.pinStatus[auth] != "active" && c.pinStatus[auth] != "expired_unknown" {
			w.WriteHeader(409)
			return
		}
		if l := c.lease(lease); l != nil && time.Now().Before(l.expires.Add(1800*time.Second)) {
			w.WriteHeader(409)
			return
		}
		c.pinStatus[auth] = "expired_unknown"
		// A swept lease closes only after its final active pin expires; expired
		// admission time alone never invalidates Live or in-flight capture.
		if l := c.lease(lease); l != nil {
			active := false
			for id, pin := range c.pins {
				if pin["lease_id"] == lease && c.pinStatus[id] == "active" {
					active = true
				}
			}
			if !active {
				l.closed = true
			}
		}
		data = map[string]any{}
		for _, key := range []string{"authorization_id", "gateway_job_id", "platform_user_id", "lease_id", "billing_snapshot_id", "settlement_event_id", "held", "usd_wallet_policy_version", "authorization_kind", "expiry_version", "expiry_deadline", "legacy_completion_proof"} {
			if val, ok := in[key]; ok {
				data[key] = val
			}
		}
		data["status"] = "expired_unknown"
		data["expiry_receipt_id"] = auth + ":expiry:1"
		if c.expiryAckLost {
			c.expiryAckLost = false
			if hijack, ok := w.(http.Hijacker); ok {
				conn, _, _ := hijack.Hijack()
				conn.Close()
				return
			}
		}
	case "/api/internal/v2/wallet/task-pins/create", "/api/internal/v2/wallet/task-pins/finish":
		if c.pinFinishRefused && r.URL.Path == "/api/internal/v2/wallet/task-pins/finish" {
			w.WriteHeader(503)
			return
		}
		if c.pinRefused {
			if r.URL.Path == "/api/internal/v2/wallet/task-pins/create" {
				w.WriteHeader(409)
			} else {
				w.WriteHeader(404)
			}
			return
		}
		job, _ := in["gateway_job_id"].(string)
		auth, _ := in["authorization_id"].(string)
		lease, _ := in["lease_id"].(string)
		resolution, _ := in["resolution"].(string)
		if c.pinStatus == nil {
			c.pinStatus = map[string]string{}
		}
		status := c.pinStatus[auth]
		if status == "" {
			status = "active"
		}
		if resolution != "" {
			event, _ := in["settlement_event_id"].(string)
			if resolution == "settled" && c.events[event] != units(in["actual"]) {
				w.WriteHeader(409)
				return
			}
			if resolution == "released" && c.events[event] > 0 {
				w.WriteHeader(409)
				return
			}
			status = resolution
		}
		c.pinStatus[auth] = status
		c.pins[auth] = in
		data = map[string]any{"gateway_job_id": job, "authorization_id": auth, "lease_id": lease, "status": status}
		captured := []string{}
		for event := range c.events {
			captured = append(captured, event)
		}
		data["captured_event_ids"] = captured
		budget, cap, expiry := c.budget, c.captured, c.expires
		if l := c.lease(lease); l != nil {
			budget, cap, expiry = l.budget, l.captured, l.expires
		}
		data["lease"] = map[string]any{"lease_id": lease, "budget_units": strconv.FormatInt(budget, 10), "captured_units": strconv.FormatInt(cap, 10), "released_units": "0", "reserved_units": "0", "expires_at": expiry, "status": "active"}
		if l := c.lease(lease); l != nil {
			view := data["lease"].(map[string]any)
			view["funding_scope"], view["funding_owner_id"], view["funding_issuance_key"] = l.scope, l.owner, l.key
			view["return_revision"], view["budget_revision"] = l.returnRevision, l.budgetRevision
			view["released_units"] = strconv.FormatInt(l.returned, 10)
			if l.frozen {
				view["funding_frozen_at"] = time.Now().UTC()
			}
		}
		if resolution == "released" && in["authorization_token"] != "" && in["authorization_token"] != nil {
			if c.zeroReceipts == nil {
				c.zeroReceipts = map[string]map[string]any{}
			}
			if original := c.zeroReceipts[auth]; original != nil {
				data = original
			} else {
				for _, name := range []string{"platform_user_id", "billing_snapshot_id", "settlement_event_id", "held", "authorization_kind", "authorization_token", "usd_wallet_policy_version"} {
					data[name] = in[name]
				}
				data["actual"] = amount(0)
				data["expiry_version"] = 0
				data["released_at"] = time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
				data["finish_receipt_id"] = auth + ":finish:1"
				data["reason"] = "gateway_confirmed_zero"
				heldObject, _ := in["held"].(map[string]any)
				parts := []string{"wallet-task-pin-receipt-v2", "released", auth, job, user, lease, in["billing_snapshot_id"].(string), in["settlement_event_id"].(string), heldObject["amount_units"].(string), in["authorization_kind"].(string), in["authorization_token"].(string), "0", "", "", "", data["released_at"].(string), auth + ":finish:1", "", "0"}
				var buffer bytes.Buffer
				encoder := json.NewEncoder(&buffer)
				encoder.SetEscapeHTML(false)
				_ = encoder.Encode(parts)
				key := c.secret
				if key == "" {
					key = strings.Repeat("s", 32)
				}
				mac := hmac.New(sha256.New, []byte(key))
				_, _ = mac.Write(bytes.TrimSuffix(buffer.Bytes(), []byte("\n")))
				data["receipt_signature"] = hex.EncodeToString(mac.Sum(nil))
				c.zeroReceipts[auth] = data
			}
		}
		if c.pinAckLost && resolution == "" {
			c.pinAckLost = false
			if c.pinAccepted != nil {
				close(c.pinAccepted)
			}
			if h, ok := w.(http.Hijacker); ok {
				conn, _, _ := h.Hijack()
				conn.Close()
				return
			}
		}
	default:
		w.WriteHeader(404)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": data})
}

type mediaFixture struct {
	db               *sql.DB
	rdb              *redis.Client
	cfg              *config.Config
	bridge           *service.CanonicalWalletBridge
	svc              *service.MediaTaskService
	control          *testControl
	userID           int64
	platformUserID   string
	creates          atomic.Int64
	mode             atomic.Int64
	upstream         *httptest.Server
	users            *mediaUsers
	keys             *service.PlatformAPIKeyService
	apiKeys          *service.APIKeyService
	snapshots        *service.BillingSnapshotService
	readHook         func(http.ResponseWriter, *http.Request)
	readerJournalDir string
}

func newMediaFixture(t *testing.T, configure ...func(*config.Config)) *mediaFixture {
	t.Helper()
	return newMediaFixtureForUser(t, "media-user", configure...)
}

func newMediaFixtureForUser(t *testing.T, platformUserID string, configure ...func(*config.Config)) *mediaFixture {
	t.Helper()
	db, rdb := mediaDatabase(t)
	f := &mediaFixture{db: db, rdb: rdb, platformUserID: platformUserID, readerJournalDir: t.TempDir()}
	f.control = &testControl{budget: 72000000000, expires: time.Now().Add(5 * time.Minute), events: map[string]int64{}, pins: map[string]map[string]any{}}
	control := httptest.NewServer(http.HandlerFunc(f.control.handler))
	t.Cleanup(control.Close)
	f.upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/jobs/createTask" {
			n := f.creates.Add(1)
			if f.mode.Load() == 1 {
				w.WriteHeader(500)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 200, "data": map[string]any{"taskId": fmt.Sprintf("vendor-%d", n)}})
			return
		}
		if f.readHook != nil {
			f.readHook(w, r)
			return
		}
		state := "success"
		if f.mode.Load() == 2 {
			state = "fail"
		}
		if f.mode.Load() == 3 {
			state = "generating"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 200, "data": map[string]any{"taskId": r.URL.Query().Get("taskId"), "state": state, "resultJson": `{"resultUrls":["https://example.com/result"]}`}})
	}))
	t.Cleanup(f.upstream.Close)
	f.cfg = &config.Config{}
	f.cfg.PlatformIdentity.Enabled = true
	f.cfg.CanonicalWallet = config.CanonicalWalletConfig{Mode: "enforce", Holds: "on", EnforceReady: true, BillingSnapshotMode: "settle", USDWalletEnabled: true, USDPolicyVersion: "usd-wallet-v1", ControlPlaneURL: control.URL, Issuer: "sub2api-gateway", Audience: "shipany-control-plane", Secret: strings.Repeat("s", 32), Version: "v1", RequestTimeoutMS: 1000, LeaseTTLSeconds: 300, LeaseBudgetUnits: 500000000, ExpirySkewMarginMS: 10, OrphanGraceSeconds: 2, OrphanSweepIntervalSeconds: 1, OrphanSweepBatch: 100, ReceivableRedriveIntervalSeconds: 3600, ReceivableRedriveMaxAttempts: 3, RetentionDays: 45}
	f.cfg.CanonicalWallet.ReaderJournalDirectory = f.readerJournalDir
	f.cfg.MediaTasks = config.MediaTasksConfig{Enabled: true, KIEAPIKey: "isolated-test-key", KIEBaseURL: f.upstream.URL, PollSeconds: 1, DeadlineSeconds: 86400}
	f.cfg.Gateway.ConcurrencySlotTTLMinutes = 30
	for _, configureFixture := range configure {
		configureFixture(f.cfg)
	}
	f.control.secret = f.cfg.CanonicalWallet.Secret
	require.NoError(t, db.QueryRow(`INSERT INTO users(email,password_hash,platform_user_id,billing_currency,status,balance) VALUES('media@example.test','test',$1,'USD','active',999) RETURNING id`, f.platformUserID).Scan(&f.userID))
	f.users = &mediaUsers{db: db}
	f.apiKeys = service.NewAPIKeyService(&mediaKeys{db: db}, f.users, nil, nil, nil, nil, f.cfg)
	f.keys = service.NewPlatformAPIKeyService(&mediaProjection{db}, f.apiKeys)
	f.snapshots = service.NewBillingSnapshotService(f.cfg, nil, nil, nil, repository.ProvideBillingSnapshotStore(db))
	wallet := repository.NewGatewayCache(rdb).(service.CanonicalWalletLeaseStore)
	f.bridge = service.NewCanonicalWalletBridge(f.cfg, wallet, db, repository.ProvideWalletOutboxStore(db))
	t.Cleanup(f.bridge.Close)
	f.svc = f.newService(t)
	return f
}
func (f *mediaFixture) newService(t *testing.T) *service.MediaTaskService {
	svc, err := service.NewMediaTaskService(f.cfg, f.db, f.bridge, f.snapshots, f.keys, f.apiKeys, f.users, service.NewAuthorizingHTTPUpstream(&mediaHTTP{client: f.upstream.Client()}, f.cfg))
	require.NoError(t, err)
	t.Cleanup(svc.Stop)
	return svc
}
func (f *mediaFixture) create(t *testing.T, key, model, option string) service.MediaTaskView {
	v, err := f.svc.Create(context.Background(), f.userID, key, service.MediaCreateInput{Model: model, Option: option, Prompt: "A colorful garden"})
	require.NoError(t, err)
	return v
}
func (f *mediaFixture) wait(t *testing.T, id, state string) service.MediaTaskView {
	var result service.MediaTaskView
	require.Eventually(t, func() bool {
		v, err := f.svc.Get(context.Background(), f.userID, id)
		result = v
		return err == nil && v.Billing.State == state
	}, 90*time.Second, 50*time.Millisecond)
	return result
}

func TestExternalMediaAllModelsDurableExactlyOnce(t *testing.T) {
	f := newMediaFixture(t)
	catalog := service.MediaTaskCatalog()
	require.NotEmpty(t, catalog)
	tasks := []service.MediaTaskView{}
	for i, m := range catalog {
		tasks = append(tasks, f.create(t, fmt.Sprint(i), m.ModelID, m.Param.Options[0].Value))
	}
	for _, task := range tasks {
		v := f.wait(t, task.TaskID, "charged")
		require.Equal(t, "success", v.Status)
		require.Equal(t, v.Billing.QuotedUSD, *v.Billing.ChargedUSD)
		var taskUsage, taskOutbox int
		require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_logs WHERE request_id=$1`, task.TaskID).Scan(&taskUsage))
		require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_settlement_outbox WHERE gateway_request_id=$1`, task.TaskID).Scan(&taskOutbox))
		require.Equal(t, 1, taskUsage, "each task must record usage exactly once")
		require.Equal(t, 1, taskOutbox, "each task must enqueue settlement exactly once")
	}
	// The catalog now includes the extended models; 11 was only the original
	// seed catalog. Retain exact cardinality and per-task uniqueness checks.
	require.Equal(t, int64(len(catalog)), f.creates.Load())
	var usage, outbox int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_logs`).Scan(&usage))
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_settlement_outbox`).Scan(&outbox))
	require.Equal(t, len(catalog), usage)
	require.Equal(t, len(catalog), outbox)
	var providerTasks int
	require.NoError(t, f.db.QueryRow(`SELECT count(DISTINCT provider_task_id) FROM gateway_media_task`).Scan(&providerTasks))
	require.Equal(t, len(catalog), providerTasks, "each model must have a distinct accepted provider task")
	var balance string
	require.NoError(t, f.db.QueryRow(`SELECT balance::text FROM users WHERE id=$1`, f.userID).Scan(&balance))
	require.Equal(t, "999.00000000", balance, "media must not debit native user balance")
	_, err := f.svc.Get(context.Background(), f.userID+1, tasks[0].TaskID)
	require.Error(t, err)
}
func TestExternalMediaIdempotentConcurrentCreateAndPayloadConflict(t *testing.T) {
	f := newMediaFixture(t)
	ids := make(chan string, 20)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			task, err := f.svc.Create(context.Background(), f.userID, "same-key", service.MediaCreateInput{Model: "google/nano-banana", Prompt: "A colorful garden", Option: "1:1"})
			if err != nil {
				ids <- "error"
				return
			}
			ids <- task.TaskID
		}()
	}
	wg.Wait()
	close(ids)
	id := ""
	for v := range ids {
		require.NotEqual(t, "error", v)
		if id == "" {
			id = v
		}
		require.Equal(t, id, v)
	}
	f.wait(t, id, "charged")
	require.Equal(t, int64(1), f.creates.Load())
	_, err := f.svc.Create(context.Background(), f.userID, "same-key", service.MediaCreateInput{Model: "google/nano-banana", Prompt: "changed payload"})
	require.ErrorIs(t, err, service.ErrMediaIdempotencyConflict)
}
func TestExternalMediaUnknownCreateSurvivesRestartAndRedisFlush(t *testing.T) {
	f := newMediaFixture(t)
	f.mode.Store(1)
	task := f.create(t, "unknown", "google/nano-banana", "1:1")
	v := f.wait(t, task.TaskID, "indeterminate")
	require.Nil(t, v.Billing.ReleasedUSD)
	f.svc.Stop()
	require.NoError(t, f.rdb.FlushDB(context.Background()).Err())
	f.svc = f.newService(t)
	_, err := f.db.Exec(`UPDATE gateway_media_task SET next_poll_at=now(),claimed_by=NULL,claim_until=NULL WHERE id=$1`, task.TaskID)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		keys, _ := f.rdb.Keys(context.Background(), "canonical_wallet:hold:*").Result()
		return len(keys) == 1
	}, 10*time.Second, 50*time.Millisecond)
	keys, err := f.rdb.Keys(context.Background(), "canonical_wallet:hold:*").Result()
	require.NoError(t, err)
	ttl, err := f.rdb.TTL(context.Background(), keys[0]).Result()
	require.NoError(t, err)
	require.Equal(t, time.Duration(-1), ttl)
	require.Equal(t, int64(1), f.creates.Load())
	var status string
	require.NoError(t, f.db.QueryRow(`SELECT pin_state FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&status))
	require.Equal(t, "active", status)
}
func TestExternalMediaDefiniteFailureReleasesOnce(t *testing.T) {
	f := newMediaFixture(t)
	f.mode.Store(2)
	task := f.create(t, "failure", "google/nano-banana", "1:1")
	v := f.wait(t, task.TaskID, "released")
	require.Equal(t, "0.0273", *v.Billing.ReleasedUSD)
	var outbox int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_settlement_outbox`).Scan(&outbox))
	require.Zero(t, outbox)
	// The quote is funded by the task's own isolated media lease. A definite
	// provider failure must return exactly the quote, once, and close that lease.
	f.control.mu.Lock()
	var isolated *testFundingLease
	for _, l := range f.control.pool {
		if l.scope == "media" {
			require.Nil(t, isolated, "exactly one isolated media lease funds one quote")
			isolated = l
		}
	}
	require.NotNil(t, isolated)
	returned, closed, unleased := isolated.returned, isolated.closed, f.control.unleased
	f.control.mu.Unlock()
	require.Equal(t, int64(2730000), returned)
	require.True(t, closed)
	require.Equal(t, f.control.budget-f.control.captured, unleased, "the whole quote went back to spendable credit")
	f.create(t, "failure", "google/nano-banana", "1:1")
	require.Equal(t, int64(1), f.creates.Load())
}
func TestExternalMediaPinAckLossCannotSubmitBeforeRecoveredAck(t *testing.T) {
	f := newMediaFixture(t)
	f.poolFunds(t, 10000000, 10000000, 0)
	f.control.mu.Lock()
	f.control.pinAckLost = true
	f.control.pinAccepted = make(chan struct{})
	ack := f.control.pinAccepted
	f.control.mu.Unlock()
	task := f.create(t, "pin-lost", "google/nano-banana", "1:1")
	select {
	case <-ack:
	case <-time.After(10 * time.Second):
		t.Fatal("pin was not reached")
	}
	require.Zero(t, f.creates.Load())
	f.wait(t, task.TaskID, "charged")
	require.Equal(t, int64(1), f.creates.Load())
}

func TestExternalMediaDefinitePinRefusalReleasesAllUnsubmittedSegments(t *testing.T) {
	f := newMediaFixture(t)
	f.poolFunds(t, 10000000, 10000000, 0)
	f.control.mu.Lock()
	f.control.pinRefused = true
	f.control.mu.Unlock()
	task := f.create(t, "pin-refused", "google/nano-banana", "1:1")
	v := f.wait(t, task.TaskID, "released")
	require.Equal(t, "PIN_AUTHORIZATION_REFUSED", v.ErrorCode)
	require.Zero(t, f.creates.Load())
	var events int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_settlement_outbox`).Scan(&events))
	require.Zero(t, events)
	// The Worker never created a pin, so there is no signed zero receipt to wait
	// for. The money must still come back: the task's isolated lease is closed by
	// the signed close and its whole quote returns to spendable credit, instead of
	// the task being stranded with the quote tied up in an unreturned lease.
	var quote int64
	require.NoError(t, f.db.QueryRow(`SELECT quoted_units FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&quote))
	require.Eventually(t, func() bool {
		f.control.mu.Lock()
		defer f.control.mu.Unlock()
		for _, l := range f.control.pool {
			if l.scope == "media" {
				return l.closed && l.returned == quote
			}
		}
		return false
	}, 10*time.Second, 50*time.Millisecond, "the isolated media lease is closed and returns the whole quote")
	var unreleased int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=(SELECT authorization_id FROM gateway_media_task WHERE id=$1) AND NOT (state='finished' AND pin_state='finished')`, task.TaskID).Scan(&unreleased))
	require.Zero(t, unreleased, "every share ended")
	wallet := repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore)
	for _, id := range []string{"lease-a", "lease-b"} {
		l, err := wallet.GetCanonicalWalletLeaseByID(context.Background(), "media-user", id)
		require.NoError(t, err)
		require.Equal(t, l.BudgetUnits, l.RemainingUnits())
	}
}
func TestExternalMediaFeatureDisabledKeepsAcceptedRecovery(t *testing.T) {
	f := newMediaFixture(t)
	f.mode.Store(3)
	task := f.create(t, "disable", "google/nano-banana", "1:1")
	// The view also shows "processing" for the pre-write 'submitting' checkpoint,
	// and v5's Stop cancels in-flight lane work, so a stop there correctly ends
	// unsent and released. The subject here is an ACCEPTED task: wait until the
	// provider acceptance is durable before disabling.
	require.Eventually(t, func() bool {
		v, _ := f.svc.Get(context.Background(), f.userID, task.TaskID)
		var accepted bool
		_ = f.db.QueryRow(`SELECT provider_task_id IS NOT NULL AND status='processing' FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&accepted)
		return v.Status == "processing" && accepted
	}, 10*time.Second, 50*time.Millisecond)
	f.svc.Stop()
	copyCfg := *f.cfg
	copyCfg.MediaTasks.Enabled = false
	f.cfg = &copyCfg
	f.svc = f.newService(t)
	_, err := f.svc.Create(context.Background(), f.userID, "blocked", service.MediaCreateInput{Model: "google/nano-banana", Prompt: "new request"})
	require.ErrorIs(t, err, service.ErrMediaUnavailable)
	f.mode.Store(0)
	f.wait(t, task.TaskID, "charged")
	require.Equal(t, int64(1), f.creates.Load())
}

func (f *mediaFixture) poolFunds(t *testing.T, a, b, direct int64) {
	f.control.mu.Lock()
	// The real Worker reports a non-empty funding scope on every lease; shared
	// leases issued before scoped funding are "legacy".
	f.control.pool = []*testFundingLease{{id: "lease-a", budget: a, expires: f.control.expires, scope: "legacy"}, {id: "lease-b", budget: b, expires: f.control.expires, scope: "legacy"}}
	f.control.unleased = direct
	if b == 0 {
		f.control.pool = f.control.pool[:1]
	}
	f.control.mu.Unlock()
	wallet := repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore)
	for _, lease := range f.control.pool {
		require.NoError(t, wallet.InstallCanonicalWalletLease(context.Background(), service.CanonicalWalletLease{LeaseID: lease.id, PlatformUserID: "media-user", Currency: "USD", BudgetUnits: lease.budget, FundingScope: lease.scope, ExpiresAt: lease.expires}))
	}
}
func (f *mediaFixture) authorize(t *testing.T, units int64, family service.BillingFamily) (*service.AuthorizationHandle, *service.BillingSnapshot) {
	f.svc.Stop()
	task := f.create(t, "auth-fixture", "google/nano-banana", "1:1")
	var snapshotID string
	require.NoError(t, f.db.QueryRow(`SELECT billing_snapshot_id FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&snapshotID))
	snap, err := repository.ProvideBillingSnapshotStore(f.db).GetBillingSnapshot(context.Background(), snapshotID)
	require.NoError(t, err)
	// This helper synthesizes a selected LLM account from a media template.
	// Persist its own frozen facts without rewriting the immutable media payload.
	snap.ID = "llm-fixture-" + uuid.NewString()
	snap.Family = family
	snap.ProviderPlatform = service.PlatformOpenAI
	require.NoError(t, f.snapshots.Persist(context.Background(), snap))
	_, err = f.db.Exec(`UPDATE gateway_media_task SET status='failed',pin_state='finished',actual_units=0 WHERE id=$1`, task.TaskID)
	require.NoError(t, err)
	user, err := f.users.GetByID(context.Background(), f.userID)
	require.NoError(t, err)
	h, err := service.NewCanonicalWalletAuthorizer(f.cfg, f.bridge, f.snapshots).Authorize(context.Background(), service.AuthorizeInput{Snapshot: snap, User: user, FixedEstimateUnits: units})
	require.NoError(t, err)
	return h, snap
}
func TestExternalPoolMixedLeasesAndDirectFundsReallySpend(t *testing.T) {
	for _, family := range []service.BillingFamily{service.BillingFamilyOpenAI, service.BillingFamilyLive} {
		t.Run(string(family), func(t *testing.T) {
			f := newMediaFixture(t)
			f.poolFunds(t, 4*720000000, 4*720000000, 2*720000000)
			h, snap := f.authorize(t, 9*720000000, family)
			require.Len(t, h.Segments, 2)
			require.Equal(t, 9*int64(720000000), h.HeldUnits)
			f.control.mu.Lock()
			require.Equal(t, int64(720000000), f.control.unleased)
			require.Len(t, f.control.pins, 2)
			f.control.mu.Unlock()
			require.True(t, f.bridge.ObserveSettlement(service.CanonicalWalletSettlementEvent{GatewayRequestID: "pool-spend", PlatformUserID: "media-user", Currency: "USD", AmountUnits: 9 * 720000000, AuthorizationID: h.ID, BillingSnapshotID: snap.ID}))
			f.svc = f.newService(t)
			require.Eventually(t, func() bool {
				var n int
				_ = f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND state='finished'`, h.ID).Scan(&n)
				return n == 2
			}, 10*time.Second, 50*time.Millisecond)
			var total, count int64
			require.NoError(t, f.db.QueryRow(`SELECT count(*),sum(amount_units) FROM wallet_settlement_outbox WHERE gateway_request_id='pool-spend'`).Scan(&count, &total))
			require.Equal(t, int64(2), count)
			require.Equal(t, 9*int64(720000000), total)
		})
	}
}
func TestExternalMediaSegmentsSingleProviderSingleUsageAndAtomicOutbox(t *testing.T) {
	f := newMediaFixture(t)
	// v5 funds media with isolated leases. Two shared free tails would both be
	// reclaimed at once and fund the 2.73M quote with ONE lease, so keep two
	// funding segments this way: the first isolated grant is clamped to 2M of
	// unleased credit, the 0.73M rest is issued after reclaiming lease-a.
	f.poolFunds(t, 2000000, 0, 2000000)
	task := f.create(t, "segmented", "google/nano-banana", "1:1")
	f.wait(t, task.TaskID, "charged")
	require.Equal(t, int64(1), f.creates.Load())
	var usage, outbox, pins int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_logs WHERE request_id=$1`, task.TaskID).Scan(&usage))
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_settlement_outbox WHERE gateway_request_id=$1`, task.TaskID).Scan(&outbox))
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment a JOIN gateway_media_task m ON m.authorization_id=a.parent_authorization_id WHERE m.id=$1 AND a.pin_state='finished'`, task.TaskID).Scan(&pins))
	require.Equal(t, 1, usage)
	require.Equal(t, 2, outbox)
	require.Equal(t, 2, pins)
}
func TestExternalPoolActualCostFIFOReleasesUnusedSegments(t *testing.T) {
	f := newMediaFixture(t)
	f.poolFunds(t, 4*720000000, 4*720000000, 0)
	h, snap := f.authorize(t, 8*720000000, service.BillingFamilyOpenAI)
	require.True(t, f.bridge.ObserveSettlement(service.CanonicalWalletSettlementEvent{GatewayRequestID: "lower-actual", PlatformUserID: "media-user", Currency: "USD", AmountUnits: 3 * 720000000, AuthorizationID: h.ID, BillingSnapshotID: snap.ID}))
	f.svc = f.newService(t)
	require.Eventually(t, func() bool {
		var n int
		_ = f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND state='finished'`, h.ID).Scan(&n)
		return n == 2
	}, 10*time.Second, 50*time.Millisecond)
	var total int64
	require.NoError(t, f.db.QueryRow(`SELECT sum(amount_units) FROM wallet_settlement_outbox WHERE gateway_request_id='lower-actual'`).Scan(&total))
	require.Equal(t, 3*int64(720000000), total)
	wallet := repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore)
	a, err := wallet.GetCanonicalWalletLeaseByID(context.Background(), "media-user", "lease-a")
	require.NoError(t, err)
	b, err := wallet.GetCanonicalWalletLeaseByID(context.Background(), "media-user", "lease-b")
	require.NoError(t, err)
	require.Equal(t, 5*int64(720000000), a.RemainingUnits()+b.RemainingUnits())
}
func TestExternalPoolUnknownRetainsEverySegmentAcrossExpiryFlushAndRestart(t *testing.T) {
	f := newMediaFixture(t)
	f.poolFunds(t, 4*720000000, 4*720000000, 0)
	h, _ := f.authorize(t, 8*720000000, service.BillingFamilyLive)
	for _, segment := range h.Segments {
		_, err := f.rdb.Keys(context.Background(), "canonical_wallet:hold:*").Result()
		require.NoError(t, err)
		require.NotEmpty(t, segment.AuthorizationID)
	}
	require.NoError(t, f.rdb.FlushDB(context.Background()).Err())
	f.control.mu.Lock()
	for _, lease := range f.control.pool {
		lease.expires = time.Now().Add(-time.Hour)
	}
	f.control.mu.Unlock()
	f.svc = f.newService(t)
	require.Eventually(t, func() bool {
		keys, _ := f.rdb.Keys(context.Background(), "canonical_wallet:hold:*").Result()
		return len(keys) == 2
	}, 10*time.Second, 50*time.Millisecond)
	keys, err := f.rdb.Keys(context.Background(), "canonical_wallet:hold:*").Result()
	require.NoError(t, err)
	for _, key := range keys {
		ttl, err := f.rdb.TTL(context.Background(), key).Result()
		require.NoError(t, err)
		require.Equal(t, time.Duration(-1), ttl)
	}
	var finished int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND state='finished'`, h.ID).Scan(&finished))
	require.Zero(t, finished)
	require.Zero(t, f.creates.Load())
}

func TestExternalMediaTerminalTransactionRollsBackEverySegment(t *testing.T) {
	f := newMediaFixture(t)
	// Two isolated funding segments (2M + 0.73M), as in the segmented test. The
	// fault targets the second segment's lease, whose id is issued at runtime.
	f.poolFunds(t, 2000000, 0, 2000000)
	_, err := f.db.Exec(`CREATE FUNCTION reject_media_second() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.lease_id IN (SELECT lease_id FROM wallet_authorization_segment WHERE ordinal=1) THEN RAISE EXCEPTION 'isolated terminal transaction fault'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_media_second BEFORE INSERT ON wallet_settlement_outbox FOR EACH ROW EXECUTE FUNCTION reject_media_second()`)
	require.NoError(t, err)
	task := f.create(t, "tx-rollback", "google/nano-banana", "1:1")
	require.Eventually(t, func() bool {
		var n int
		_ = f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE actual_units>0`).Scan(&n)
		return n == 2
	}, 10*time.Second, 50*time.Millisecond)
	var usage, outbox int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_logs`).Scan(&usage))
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_settlement_outbox`).Scan(&outbox))
	// v5 commits the usage row in the frozen fee-plan transaction together with
	// every segment actual (persistMediaFeePlan); the outbox rows of all segments
	// are one later transaction. So the fault leaves exactly one usage row and
	// no outbox row for any segment.
	require.Equal(t, 1, usage)
	require.Zero(t, outbox)
	_, err = f.db.Exec(`DROP TRIGGER reject_media_second ON wallet_settlement_outbox;DROP FUNCTION reject_media_second()`)
	require.NoError(t, err)
	f.wait(t, task.TaskID, "charged")
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM usage_logs`).Scan(&usage))
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_settlement_outbox`).Scan(&outbox))
	require.Equal(t, 1, usage)
	require.Equal(t, 2, outbox)
	require.Equal(t, int64(1), f.creates.Load())
}
func TestExternalMediaExpiredClaimCannotReleaseNewOwnerSuccess(t *testing.T) {
	f := newMediaFixture(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var reads atomic.Int64
	defer close(release)
	f.readHook = func(w http.ResponseWriter, r *http.Request) {
		n := reads.Add(1)
		state := "success"
		if n == 1 {
			close(entered)
			<-release
			state = "fail"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 200, "data": map[string]any{"taskId": r.URL.Query().Get("taskId"), "state": state, "resultJson": `{"resultUrls":["https://example.com/result"]}`}})
	}
	task := f.create(t, "stale-worker", "google/nano-banana", "1:1")
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("first owner did not poll")
	}
	var oldOwner string
	require.NoError(t, f.db.QueryRow(`SELECT claimed_by FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&oldOwner))
	require.True(t, strings.HasPrefix(oldOwner, "media-"))
	_, err := f.db.Exec(`UPDATE gateway_media_task SET claim_until=now()-interval '1 second',next_poll_at=now() WHERE id=$1`, task.TaskID)
	require.NoError(t, err)
	newer := f.newService(t)
	_ = newer
	f.wait(t, task.TaskID, "charged")
	var outbox int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_settlement_outbox WHERE gateway_request_id=$1`, task.TaskID).Scan(&outbox))
	require.Equal(t, 1, outbox)
	require.Equal(t, int64(1), f.creates.Load())
}
func TestExternalPoolAvailabilityExactRawCountersAndCapturedDedup(t *testing.T) {
	f := newMediaFixture(t)
	f.poolFunds(t, 720000000, 720000000, 0)
	h, _ := f.authorize(t, 1080000000, service.BillingFamilyOpenAI)
	input := service.WalletAvailabilityInput{UnitVersion: "usd-e8-v1", USDPolicyVersion: "usd-wallet-v1", CapturedEventIDs: []string{}}
	for _, l := range f.control.pool {
		input.Leases = append(input.Leases, service.WalletAvailabilityLeaseInput{LeaseID: l.id, BudgetUnits: strconv.FormatInt(l.budget, 10), CapturedUnits: "0", ReleasedUnits: "0", ReservedUnits: "0", Status: "active", ExpiresAt: l.expires})
	}
	availability, err := f.svc.Availability(context.Background(), f.userID, input)
	require.NoError(t, err)
	require.Equal(t, "complete", availability.Completeness)
	require.Len(t, availability.Obligations, 2)
	require.Equal(t, "720000000", availability.Leases[0].HeldUnits)
	require.Equal(t, "360000000", availability.Leases[1].HeldUnits)
	input.CapturedEventIDs = []string{h.Segments[0].EventID}
	input.Leases[0].CapturedUnits = "720000000"
	availability, err = f.svc.Availability(context.Background(), f.userID, input)
	require.NoError(t, err)
	require.Equal(t, "complete", availability.Completeness)
	require.Equal(t, "0", availability.Leases[0].HeldUnits)
	capturedSeen := false
	for _, obligation := range availability.Obligations {
		if obligation.AuthorizationID == h.Segments[0].AuthorizationID {
			capturedSeen = obligation.Captured
		}
	}
	require.True(t, capturedSeen)
	walletKeys, err := f.rdb.Keys(context.Background(), "canonical_wallet:lease:*").Result()
	require.NoError(t, err)
	for _, key := range walletKeys {
		lease, _ := f.rdb.HGet(context.Background(), key, "lease_id").Result()
		if lease == "lease-a" {
			require.NoError(t, f.rdb.HIncrBy(context.Background(), key, "consumed_units", 1).Err())
		}
	}
	availability, err = f.svc.Availability(context.Background(), f.userID, input)
	require.NoError(t, err)
	require.Equal(t, "unknown", availability.Completeness, "an unexplained counter tail cannot fabricate free funds")
}
func TestExternalPoolMissingDrainedAndExpiredCannotAuthorize(t *testing.T) {
	for _, bad := range []string{"missing", "drained", "expired"} {
		t.Run(bad, func(t *testing.T) {
			f := newMediaFixture(t)
			f.svc.Stop()
			f.poolFunds(t, 4*720000000, 4*720000000, 0)
			task := f.create(t, "invalid-basis", "google/nano-banana", "1:1")
			var id string
			require.NoError(t, f.db.QueryRow(`SELECT billing_snapshot_id FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&id))
			snapshot, err := repository.ProvideBillingSnapshotStore(f.db).GetBillingSnapshot(context.Background(), id)
			require.NoError(t, err)
			user, err := f.users.GetByID(context.Background(), f.userID)
			require.NoError(t, err)
			if bad == "missing" {
				require.NoError(t, f.rdb.FlushDB(context.Background()).Err())
			} else {
				f.control.mu.Lock()
				for _, l := range f.control.pool {
					if bad == "drained" {
						l.drained = true
					} else {
						l.expires = time.Now().Add(-time.Hour)
					}
				}
				f.control.mu.Unlock()
			}
			_, err = service.NewCanonicalWalletAuthorizer(f.cfg, f.bridge, f.snapshots).Authorize(context.Background(), service.AuthorizeInput{Snapshot: snapshot, User: user, FixedEstimateUnits: 8 * 720000000})
			require.Error(t, err)
			require.ErrorIs(t, err, service.ErrAuthorizationRefused)
			require.Zero(t, f.creates.Load())
		})
	}
}
func TestExternalPoolColdAndOrdinaryHoldTopupCanSpendTenUSD(t *testing.T) {
	t.Run("cold-eleven", func(t *testing.T) {
		f := newMediaFixture(t)
		f.control.mu.Lock()
		f.control.budget = 11 * 720000000
		f.control.mu.Unlock()
		h, _ := f.authorize(t, 10*720000000, service.BillingFamilyOpenAI)
		require.Equal(t, 10*int64(720000000), h.HeldUnits)
		require.Len(t, h.Segments, 1)
	})
	t.Run("ordinary-hold-nine-free-two-direct", func(t *testing.T) {
		f := newMediaFixture(t)
		f.poolFunds(t, 10*720000000, 0, 2*720000000)
		first, _ := f.authorize(t, 720000000, service.BillingFamilyOpenAI)
		second, _ := f.authorize(t, 10*720000000, service.BillingFamilyOpenAI)
		require.Equal(t, int64(720000000), first.HeldUnits)
		require.Equal(t, 10*int64(720000000), second.HeldUnits)
		f.control.mu.Lock()
		require.Equal(t, int64(720000000), f.control.unleased)
		f.control.mu.Unlock()
		lease, err := repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore).GetCanonicalWalletLeaseByID(context.Background(), "media-user", "lease-a")
		require.NoError(t, err)
		require.Equal(t, 11*int64(720000000), lease.ConsumedUnits)
		require.Zero(t, lease.RemainingUnits())
	})
}
func TestExternalPoolConcurrentLLMAndMediaNeverOverspend(t *testing.T) {
	f := newMediaFixture(t)
	f.svc.Stop()
	f.poolFunds(t, 10000000, 9656000, 0)
	seed := f.create(t, "concurrency-seed", "google/nano-banana", "1:1")
	var snapshotID string
	require.NoError(t, f.db.QueryRow(`SELECT billing_snapshot_id FROM gateway_media_task WHERE id=$1`, seed.TaskID).Scan(&snapshotID))
	snapshot, err := repository.ProvideBillingSnapshotStore(f.db).GetBillingSnapshot(context.Background(), snapshotID)
	require.NoError(t, err)
	snapshot.Family = service.BillingFamilyOpenAI
	user, err := f.users.GetByID(context.Background(), f.userID)
	require.NoError(t, err)
	_, err = f.db.Exec(`UPDATE gateway_media_task SET status='failed',pin_state='finished',actual_units=0 WHERE id=$1`, seed.TaskID)
	require.NoError(t, err)
	task := f.create(t, "competing-media", "google/nano-banana", "1:1")
	f.svc = f.newService(t)
	h, authErr := service.NewCanonicalWalletAuthorizer(f.cfg, f.bridge, f.snapshots).Authorize(context.Background(), service.AuthorizeInput{Snapshot: snapshot, User: user, FixedEstimateUnits: 19656000})
	if authErr == nil {
		require.True(t, f.bridge.ObserveSettlement(service.CanonicalWalletSettlementEvent{GatewayRequestID: "competing-llm", PlatformUserID: "media-user", Currency: "USD", AmountUnits: 19656000, AuthorizationID: h.ID, BillingSnapshotID: snapshot.ID}))
	}
	require.Eventually(t, func() bool {
		v, e := f.svc.Get(context.Background(), f.userID, task.TaskID)
		return e == nil && (v.Billing.State == "charged" || v.Billing.State == "released")
	}, 10*time.Second, 50*time.Millisecond)
	f.control.mu.Lock()
	var total int64
	for _, amount := range f.control.events {
		total += amount
	}
	f.control.mu.Unlock()
	require.LessOrEqual(t, total, int64(19656000))
	v, err := f.svc.Get(context.Background(), f.userID, task.TaskID)
	require.NoError(t, err)
	if authErr == nil {
		require.Equal(t, "released", v.Billing.State)
		require.Zero(t, f.creates.Load())
	} else {
		require.ErrorIs(t, authErr, service.ErrAuthorizationRefused)
		require.Equal(t, "charged", v.Billing.State)
		require.Equal(t, int64(1), f.creates.Load())
	}
}

func TestExternalPoolUnknownWriteCannotReplaySameHandle(t *testing.T) {
	f := newMediaFixture(t)
	h, _ := f.authorize(t, 720000000, service.BillingFamilyOpenAI)
	var requests atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		conn, _, _ := w.(http.Hijacker).Hijack()
		conn.Close()
	}))
	defer upstream.Close()
	decorated := service.NewAuthorizingHTTPUpstream(&mediaHTTP{client: upstream.Client()}, f.cfg)
	req, err := http.NewRequestWithContext(service.WithAuthorizationHandle(context.Background(), h), "POST", upstream.URL, strings.NewReader("{}"))
	require.NoError(t, err)
	_, err = decorated.Do(req, "", 0, 1)
	require.Error(t, err)
	first := h.LastWriteToken()
	require.Equal(t, service.AuthorizationOutcomeIndeterminate, h.Writes()[0].Outcome)
	_, err = decorated.Do(req, "", 0, 1)
	require.ErrorIs(t, err, service.ErrWalletUnknownCostRetry)
	require.Equal(t, int64(1), requests.Load())
	var token, state string
	require.NoError(t, f.db.QueryRow(`SELECT authorization_token,state FROM wallet_authorization_segment WHERE parent_authorization_id=$1`, h.ID).Scan(&token, &state))
	require.Equal(t, first, token)
	require.Equal(t, "indeterminate", state)
	h.RecordOutcome(first, service.AuthorizationOutcomeNotWritten, context.Canceled)
	require.Equal(t, service.AuthorizationOutcomeIndeterminate, h.Writes()[0].Outcome, "a later classification cannot erase an uncertain write")
}

func TestExternalPoolReliableZeroProofAndTokenlessAbort(t *testing.T) {
	for _, proof := range []string{"connection-refused", "http-400", "context-canceled", "tokenless-abort"} {
		t.Run(proof, func(t *testing.T) {
			f := newMediaFixture(t)
			f.poolFunds(t, 720000000, 720000000, 0)
			h, _ := f.authorize(t, 1440000000, service.BillingFamilyOpenAI)
			if proof == "tokenless-abort" {
				_, err := f.db.Exec(`UPDATE wallet_authorization_segment SET created_at=now()-interval '2 minutes' WHERE parent_authorization_id=$1`, h.ID)
				require.NoError(t, err)
			} else {
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(400) }))
				defer upstream.Close()
				if proof == "connection-refused" {
					upstream.Close()
				}
				ctx := service.WithAuthorizationHandle(context.Background(), h)
				client := upstream.Client()
				if proof == "context-canceled" {
					client = &http.Client{Transport: mediaRoundTripFunc(func(*http.Request) (*http.Response, error) { return nil, context.Canceled })}
				}
				decorated := service.NewAuthorizingHTTPUpstream(&mediaHTTP{client: client}, f.cfg)
				req, err := http.NewRequestWithContext(ctx, "POST", upstream.URL, strings.NewReader("{}"))
				require.NoError(t, err)
				resp, callErr := decorated.Do(req, "", 0, 1)
				if proof == "http-400" {
					require.NoError(t, callErr)
					require.Equal(t, 400, resp.StatusCode)
					resp.Body.Close()
				} else {
					require.Error(t, callErr)
				}
			}
			f.svc = f.newService(t)
			if proof == "context-canceled" {
				time.Sleep(2200 * time.Millisecond)
				var pending int
				require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND state='indeterminate' AND pin_state='active'`, h.ID).Scan(&pending))
				require.Equal(t, 2, pending)
				return
			}
			require.Eventually(t, func() bool {
				var n int
				_ = f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND state='finished' AND pin_state='finished' AND actual_units=0`, h.ID).Scan(&n)
				return n == 2
			}, 10*time.Second, 50*time.Millisecond)
			wallet := repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore)
			for _, segment := range h.Segments {
				l, err := wallet.GetCanonicalWalletLeaseByID(context.Background(), "media-user", segment.LeaseID)
				require.NoError(t, err)
				require.Equal(t, l.BudgetUnits, l.RemainingUnits())
			}
			var outbox int
			require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_settlement_outbox`).Scan(&outbox))
			require.Zero(t, outbox)
		})
	}
}

type mediaRoundTripFunc func(*http.Request) (*http.Response, error)

func (f mediaRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestExternalPoolRecoveryRotatesPastThirtyTwoUnknownGroups(t *testing.T) {
	f := newMediaFixture(t)
	for i := 0; i < 32; i++ {
		h, _ := f.authorize(t, 1000000, service.BillingFamilyOpenAI)
		_, err := f.db.Exec(`UPDATE wallet_authorization_segment SET state='indeterminate',authorization_token=$2,updated_at=now()-interval '1 hour' WHERE parent_authorization_id=$1`, h.ID, h.ID+".1")
		require.NoError(t, err)
	}
	h, snap := f.authorize(t, 1000000, service.BillingFamilyOpenAI)
	require.True(t, f.bridge.ObserveSettlement(service.CanonicalWalletSettlementEvent{GatewayRequestID: "after-thirty-two", PlatformUserID: "media-user", Currency: "USD", AmountUnits: 1000000, AuthorizationID: h.ID, BillingSnapshotID: snap.ID}))
	f.svc = f.newService(t)
	require.Eventually(t, func() bool {
		var state string
		_ = f.db.QueryRow(`SELECT state FROM wallet_authorization_segment WHERE parent_authorization_id=$1`, h.ID).Scan(&state)
		return state == "finished"
	}, 10*time.Second, 50*time.Millisecond)
	var unknown int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE state='indeterminate' AND pin_state='active'`).Scan(&unknown))
	require.Equal(t, 32, unknown)
}

func TestExternalPoolConcurrentPositiveAndZeroReleaseIsWholeGroup(t *testing.T) {
	f := newMediaFixture(t)
	f.poolFunds(t, 720000000, 720000000, 0)
	h, snap := f.authorize(t, 1440000000, service.BillingFamilyOpenAI)
	gate, err := f.db.Conn(context.Background())
	require.NoError(t, err)
	defer gate.Close()
	_, err = gate.ExecContext(context.Background(), `SELECT pg_advisory_lock(781240)`)
	require.NoError(t, err)
	defer gate.ExecContext(context.Background(), `SELECT pg_advisory_unlock(781240)`)
	_, err = f.db.Exec(`CREATE FUNCTION pause_positive_attempt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.actual_units>0 THEN PERFORM pg_advisory_xact_lock(781240); END IF; RETURN NEW; END $$; CREATE TRIGGER pause_positive_attempt BEFORE UPDATE ON wallet_authorization_segment FOR EACH ROW EXECUTE FUNCTION pause_positive_attempt()`)
	require.NoError(t, err)
	transportEntered := make(chan struct{})
	transportRelease := make(chan struct{})
	closed := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	closed.Close()
	client := &http.Client{Transport: mediaRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		close(transportEntered)
		<-transportRelease
		return closed.Client().Transport.RoundTrip(r)
	})}
	decorated := service.NewAuthorizingHTTPUpstream(&mediaHTTP{client: client}, f.cfg)
	req, err := http.NewRequestWithContext(service.WithAuthorizationHandle(context.Background(), h), "POST", closed.URL, strings.NewReader("{}"))
	require.NoError(t, err)
	zero := make(chan error, 1)
	go func() { _, e := decorated.Do(req, "", 0, 1); zero <- e }()
	<-transportEntered
	positive := make(chan bool, 1)
	go func() {
		positive <- f.bridge.ObserveSettlement(service.CanonicalWalletSettlementEvent{GatewayRequestID: "positive-race", PlatformUserID: "media-user", Currency: "USD", AmountUnits: 360000000, AuthorizationID: h.ID, AuthorizationToken: h.LastWriteToken(), BillingSnapshotID: snap.ID})
	}()
	require.Eventually(t, func() bool {
		var blocked bool
		_ = f.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND objid=781240 AND NOT granted)`).Scan(&blocked)
		return blocked
	}, time.Second, 10*time.Millisecond)
	close(transportRelease)
	_, err = gate.ExecContext(context.Background(), `SELECT pg_advisory_unlock(781240)`)
	require.NoError(t, err)
	require.True(t, <-positive)
	require.Error(t, <-zero)
	f.svc = f.newService(t)
	require.Eventually(t, func() bool {
		var n int
		_ = f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND state='finished'`, h.ID).Scan(&n)
		return n == 2
	}, 10*time.Second, 50*time.Millisecond)
	var charged int64
	require.NoError(t, f.db.QueryRow(`SELECT sum(amount_units) FROM wallet_settlement_outbox WHERE gateway_request_id='positive-race'`).Scan(&charged))
	require.Equal(t, int64(360000000), charged)
	wallet := repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore)
	var free int64
	for _, segment := range h.Segments {
		l, e := wallet.GetCanonicalWalletLeaseByID(context.Background(), "media-user", segment.LeaseID)
		require.NoError(t, e)
		free += l.RemainingUnits()
	}
	require.Equal(t, int64(1080000000), free)
}

func TestExternalMediaCacheLossAndDefinitePinRefusalResolveKnownZero(t *testing.T) {
	f := newMediaFixture(t)
	f.svc.Stop()
	f.poolFunds(t, 10000000, 10000000, 0)
	task := f.create(t, "missing-cache-pin-refused", "google/nano-banana", "1:1")
	var auth, snapshotID, event string
	var quote int64
	require.NoError(t, f.db.QueryRow(`SELECT authorization_id,billing_snapshot_id,settlement_event_id,quoted_units FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&auth, &snapshotID, &event, &quote))
	snap, err := repository.ProvideBillingSnapshotStore(f.db).GetBillingSnapshot(context.Background(), snapshotID)
	require.NoError(t, err)
	user, err := f.users.GetByID(context.Background(), f.userID)
	require.NoError(t, err)
	// Media funding binds to the persisted task: the amount is the task's own
	// immutable quote (PG refuses to change it) and the funding owner is the task
	// id. The quote is funded by one isolated lease here; multi-segment funding is
	// covered by the scoped-funding cases.
	h, err := service.NewCanonicalWalletAuthorizer(f.cfg, f.bridge, f.snapshots).Authorize(service.WithMediaFundingOwner(context.Background(), task.TaskID), service.AuthorizeInput{Snapshot: snap, User: user, FixedEstimateUnits: quote, DurableAuthorizationID: auth})
	require.NoError(t, err)
	for i, segment := range h.Segments {
		id := event
		if i > 0 {
			id = service.CanonicalWalletSettlementEventID(task.TaskID+":"+segment.AuthorizationID, "media-user", "USD")
		}
		_, err = f.db.Exec(`UPDATE wallet_authorization_segment SET event_id=$2 WHERE authorization_id=$1`, segment.AuthorizationID, id)
		require.NoError(t, err)
	}
	basis, err := json.Marshal(h.Segments[0].Basis)
	require.NoError(t, err)
	_, err = f.db.Exec(`UPDATE gateway_media_task SET status='authorizing',held_units=$2,lease_id=$3,lease_basis=$4::jsonb WHERE id=$1`, task.TaskID, h.HeldUnits, h.LeaseID, string(basis))
	require.NoError(t, err)
	require.NoError(t, f.rdb.FlushDB(context.Background()).Err())
	f.control.mu.Lock()
	f.control.pinRefused = true
	f.control.mu.Unlock()
	f.svc = f.newService(t)
	f.wait(t, task.TaskID, "released")
	require.Zero(t, f.creates.Load())
	var unfinished, outbox int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND pin_state<>'finished'`, auth).Scan(&unfinished))
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_settlement_outbox`).Scan(&outbox))
	require.Zero(t, unfinished)
	require.Zero(t, outbox)
	// The Redis cache was wiped, so the task's isolated lease had to be rebuilt
	// from the persisted basis before the signed close. The money must actually
	// have come back: a rebuild that returned without closing would leave the
	// whole quote tied up in an unreturned lease while the task still reads
	// "released".
	require.Eventually(t, func() bool {
		f.control.mu.Lock()
		defer f.control.mu.Unlock()
		for _, l := range f.control.pool {
			if l.scope == "media" {
				return l.closed && l.returned == quote
			}
		}
		return false
	}, 10*time.Second, 50*time.Millisecond, "the rebuilt isolated media lease is closed and returns the whole quote")
}

func TestExternalPoolLiveNextWindowActivationSurvivesAgeAndRedisLoss(t *testing.T) {
	f := newMediaFixture(t)
	f.poolFunds(t, 4*720000000, 4*720000000, 2*720000000)
	first, snap := f.authorize(t, 72000000, service.BillingFamilyLive)
	store := service.ProvideLiveProvisionalStore(f.cfg, f.db)
	ctx := context.Background()
	row := &service.LiveProvisionalRecord{Token: first.ID, AuthorizationID: first.ID, PlatformUserID: "media-user", UserID: f.userID, APIKeyID: 1, AccountID: 1, BillingCurrency: "USD", BillingSnapshotID: snap.ID, EstimatedUnits: 9 * 720000000, Status: service.LiveProvisionalStatusProvisional, Windows: []service.LiveWindow{{WindowSeq: 1, LeaseID: first.LeaseID, Token: first.ID}}}
	require.NoError(t, store.Save(ctx, row))
	require.NoError(t, store.Activate(ctx, first.ID, "real-next-window", time.Now()))
	require.True(t, f.bridge.ObserveSettlement(service.CanonicalWalletSettlementEvent{GatewayRequestID: "live-first-window", PlatformUserID: "media-user", Currency: "USD", AmountUnits: 72000000, AuthorizationID: first.ID, BillingSnapshotID: snap.ID}))
	next, _ := f.authorize(t, 9*720000000, service.BillingFamilyLive)
	require.Len(t, next.Segments, 2)
	window := service.LiveWindow{WindowSeq: 2, LeaseID: next.LeaseID, Token: next.ID, OpenedAtMS: time.Now().UnixMilli()}
	require.NoError(t, store.AdvanceLiveWindow(ctx, first.ID, 1, 72000000, window))
	// A failed repeated advance must not alter the group's activation identity.
	require.ErrorIs(t, store.AdvanceLiveWindow(ctx, first.ID, 1, 72000000, window), service.ErrLiveWindowCASLost)
	_, err := f.db.Exec(`UPDATE wallet_authorization_segment SET created_at=now()-interval '2 minutes',updated_at=now()-interval '1 hour' WHERE parent_authorization_id=$1`, next.ID)
	require.NoError(t, err)
	require.NoError(t, f.rdb.FlushDB(ctx).Err())
	f.control.mu.Lock()
	for _, lease := range f.control.pool {
		lease.expires = time.Now().Add(-time.Hour)
	}
	f.control.mu.Unlock()
	f.svc = f.newService(t)
	require.Eventually(t, func() bool {
		keys, _ := f.rdb.Keys(ctx, "canonical_wallet:hold:*").Result()
		return len(keys) >= 2
	}, 10*time.Second, 50*time.Millisecond)
	var active int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND state='indeterminate' AND authorization_token=$2 AND pin_state='active'`, next.ID, next.ID+":live-window").Scan(&active))
	require.Equal(t, 2, active)
	require.NoError(t, store.SetLiveWindowPending(ctx, first.ID, 2, 9*720000000))
	require.True(t, f.bridge.ObserveSettlement(service.CanonicalWalletSettlementEvent{GatewayRequestID: "live-next-window", PlatformUserID: "media-user", Currency: "USD", AmountUnits: 9 * 720000000, AuthorizationID: next.ID, AuthorizationToken: next.ID, BillingSnapshotID: snap.ID}))
	require.Eventually(t, func() bool {
		var n int
		_ = f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND state='finished'`, next.ID).Scan(&n)
		return n == 2
	}, 10*time.Second, 50*time.Millisecond)
	claimed, err := store.ClaimFinalization(ctx, first.ID, time.Now())
	require.NoError(t, err)
	require.True(t, claimed)
	require.NoError(t, store.CompleteFinalization(ctx, first.ID, next.Segments[0].EventID, 9*720000000, 2, time.Now()))
	var total int64
	require.NoError(t, f.db.QueryRow(`SELECT sum(amount_units) FROM wallet_settlement_outbox WHERE gateway_request_id='live-next-window'`).Scan(&total))
	require.Equal(t, 9*int64(720000000), total)
}

func TestExternalPoolKnownHTTPRejectionRenewsOnlyAfterZeroPinACK(t *testing.T) {
	f := newMediaFixture(t)
	f.poolFunds(t, 720000000, 720000000, 0)
	h, snap := f.authorize(t, 1440000000, service.BillingFamilyOpenAI)
	old := h.ID
	var requests atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) <= 2 {
			w.WriteHeader(400)
			return
		}
		w.WriteHeader(200)
	}))
	defer upstream.Close()
	decorated := service.NewAuthorizingHTTPUpstream(&mediaHTTP{client: upstream.Client()}, f.cfg)
	req, err := http.NewRequestWithContext(service.WithAuthorizationHandle(context.Background(), h), "POST", upstream.URL, strings.NewReader("{}"))
	require.NoError(t, err)
	resp, err := decorated.Do(req, "", 0, 1)
	require.NoError(t, err)
	require.Equal(t, 400, resp.StatusCode)
	resp.Body.Close()
	resp, err = decorated.Do(req, "", 0, 1)
	require.NoError(t, err)
	require.Equal(t, 400, resp.StatusCode)
	resp.Body.Close()
	intermediate := service.AuthorizationIDOf(h)
	require.NotEqual(t, old, intermediate)
	input := service.WalletAvailabilityInput{UnitVersion: "usd-e8-v1", USDPolicyVersion: "usd-wallet-v1", CapturedEventIDs: []string{}}
	f.control.mu.Lock()
	for _, lease := range f.control.pool {
		input.Leases = append(input.Leases, service.WalletAvailabilityLeaseInput{LeaseID: lease.id, BudgetUnits: strconv.FormatInt(lease.budget, 10), CapturedUnits: "0", ReleasedUnits: "0", ReservedUnits: "0", Status: "active", ExpiresAt: lease.expires})
	}
	f.control.mu.Unlock()
	availability, err := f.svc.Availability(context.Background(), f.userID, input)
	require.NoError(t, err)
	require.Equal(t, "complete", availability.Completeness)
	for _, lease := range availability.Leases {
		require.Equal(t, "1440000000", lease.ConsumedUnits)
		require.Equal(t, "1440000000", lease.ReleasedUnits)
		require.Equal(t, "720000000", lease.FreeUnits)
		require.Equal(t, "0", lease.HeldUnits)
	}
	resp, err = decorated.Do(req, "", 0, 1)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()
	fresh := service.AuthorizationIDOf(h)
	require.NotEqual(t, old, fresh)
	require.NotEqual(t, intermediate, fresh)
	require.Equal(t, old, h.ID, "the original parent remains immutable")
	f.control.mu.Lock()
	for _, segment := range h.Segments {
		require.Equal(t, "released", f.control.pinStatus[segment.AuthorizationID])
	}
	f.control.mu.Unlock()
	// A late callback to the old token cannot release the fresh shares.
	h.RecordOutcome(h.Writes()[0].Token, service.AuthorizationOutcomeNotWritten, context.Canceled)
	require.True(t, f.bridge.ObserveSettlement(service.CanonicalWalletSettlementEvent{GatewayRequestID: "corrected-http", PlatformUserID: "media-user", Currency: "USD", AmountUnits: 1440000000, AuthorizationID: fresh, AuthorizationToken: service.AuthorizationTokenOf(h), BillingSnapshotID: snap.ID}))
	f.svc = f.newService(t)
	require.Eventually(t, func() bool {
		var n int
		_ = f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=$1 AND state='finished'`, fresh).Scan(&n)
		return n == 2
	}, 10*time.Second, 50*time.Millisecond)
	var count, total int64
	require.NoError(t, f.db.QueryRow(`SELECT count(*),sum(amount_units) FROM wallet_settlement_outbox WHERE gateway_request_id='corrected-http'`).Scan(&count, &total))
	require.Equal(t, int64(2), count)
	require.Equal(t, int64(1440000000), total)
	var oldEvents int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_settlement_outbox WHERE authorization_id IN (SELECT authorization_id FROM wallet_authorization_segment WHERE parent_authorization_id=$1)`, old).Scan(&oldEvents))
	require.Zero(t, oldEvents)
	wallet := repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore)
	for _, segment := range h.Segments {
		lease, e := wallet.GetCanonicalWalletLeaseByID(context.Background(), "media-user", segment.LeaseID)
		require.NoError(t, e)
		require.Equal(t, 3*lease.BudgetUnits, lease.ConsumedUnits)
		require.Equal(t, 2*lease.BudgetUnits, lease.ReleasedUnits)
		require.Zero(t, lease.RemainingUnits())
		require.NoError(t, wallet.InstallCanonicalWalletLease(context.Background(), *lease))
	}
}

func TestExternalPoolClaimCarriesFrozenSnapshotIdentity(t *testing.T) {
	f := newMediaFixture(t)
	h, snap := f.authorize(t, 720000000, service.BillingFamilyOpenAI)
	f.bridge.Close()
	require.True(t, f.bridge.ObserveSettlement(service.CanonicalWalletSettlementEvent{GatewayRequestID: "claim-snapshot", PlatformUserID: "media-user", Currency: "USD", AmountUnits: 720000000, AuthorizationID: h.ID, BillingSnapshotID: snap.ID}))
	claimed, err := repository.ProvideWalletOutboxStore(f.db).ClaimPendingOutboxEvents(context.Background(), "isolated-claim-proof", 1)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.Equal(t, snap.ID, claimed[0].BillingSnapshotID)
	require.Equal(t, h.ID, claimed[0].AuthorizationID)
}

func TestExternalPoolHTTPRenewalStopsWithoutOldZeroPinACK(t *testing.T) {
	f := newMediaFixture(t)
	h, _ := f.authorize(t, 720000000, service.BillingFamilyOpenAI)
	var requests atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); w.WriteHeader(400) }))
	defer upstream.Close()
	decorated := service.NewAuthorizingHTTPUpstream(&mediaHTTP{client: upstream.Client()}, f.cfg)
	req, err := http.NewRequestWithContext(service.WithAuthorizationHandle(context.Background(), h), "POST", upstream.URL, strings.NewReader("{}"))
	require.NoError(t, err)
	resp, err := decorated.Do(req, "", 0, 1)
	require.NoError(t, err)
	resp.Body.Close()
	f.control.mu.Lock()
	f.control.pinFinishRefused = true
	f.control.mu.Unlock()
	_, err = decorated.Do(req, "", 0, 1)
	require.ErrorIs(t, err, service.ErrAuthorizationRefused)
	require.Equal(t, int64(1), requests.Load())
	require.Equal(t, h.ID, service.AuthorizationIDOf(h))
}

// A 409 on pin creation is not proof that no pin exists (it also answers a pin that
// exists but conflicts). The pinless release therefore may not declare the quote
// refunded on its own: it is final only once the signed close of the isolated lease
// is accepted, and the Worker refuses that close while a pin is still active.
func TestExternalMediaPinRefusalWithASurvivingPinIsNotReleasedUntilTheSignedCloseIsAccepted(t *testing.T) {
	f := newMediaFixture(t)
	f.poolFunds(t, 10000000, 10000000, 0)
	f.control.mu.Lock()
	f.control.pinRefused = true
	f.control.survivingPin = true
	f.control.mu.Unlock()
	task := f.create(t, "pin-refused-survivor", "google/nano-banana", "1:1")
	var quote int64
	require.NoError(t, f.db.QueryRow(`SELECT quoted_units FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&quote))
	isolatedClosed := func() bool {
		f.control.mu.Lock()
		defer f.control.mu.Unlock()
		for _, l := range f.control.pool {
			if l.scope == "media" {
				return l.closed
			}
		}
		return false
	}
	// Wait until the refusal has been processed (the task left authorization), then
	// hold the line: with a pin still alive the quote must not be reported refunded.
	require.Eventually(t, func() bool {
		var code string
		_ = f.db.QueryRow(`SELECT COALESCE(error_code,'') FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&code)
		return code == "PIN_AUTHORIZATION_REFUSED"
	}, 10*time.Second, 50*time.Millisecond)
	require.Never(t, func() bool {
		var state string
		_ = f.db.QueryRow(`SELECT financial_state FROM gateway_media_task WHERE id=$1`, task.TaskID).Scan(&state)
		return state == "released_zero" || isolatedClosed()
	}, 4*time.Second, 100*time.Millisecond, "a refused close must leave the task unreleased and the lease open")
	var unfinished int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE parent_authorization_id=(SELECT authorization_id FROM gateway_media_task WHERE id=$1) AND state='finished'`, task.TaskID).Scan(&unfinished))
	require.Zero(t, unfinished, "no share is finished before the signed close is accepted")
	// The pin is gone (finished by its own recovery): the close is now accepted.
	f.control.mu.Lock()
	f.control.survivingPin = false
	f.control.mu.Unlock()
	f.wait(t, task.TaskID, "released")
	require.Eventually(t, isolatedClosed, 10*time.Second, 50*time.Millisecond, "the isolated lease is closed once the Worker accepts the close")
	var returned int64
	f.control.mu.Lock()
	for _, l := range f.control.pool {
		if l.scope == "media" {
			returned = l.returned
		}
	}
	f.control.mu.Unlock()
	require.Equal(t, quote, returned, "the whole quote returns to spendable credit")
}
