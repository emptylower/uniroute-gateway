//go:build media_integration

package media_integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func immediateV5WireConfig(t *testing.T) (string, string) {
	t.Helper()
	base, secret := os.Getenv("UNIROUTE_WALLET_WIRE_FIXTURE_URL"), os.Getenv("UNIROUTE_WALLET_WIRE_FIXTURE_SECRET")
	require.NotEmpty(t, base, "actual isolated Worker/D1 wire fixture is required; no mock or skip")
	u, err := url.Parse(base)
	require.NoError(t, err)
	require.Equal(t, "http", u.Scheme)
	require.Equal(t, "127.0.0.1", u.Hostname(), "refuse nonlocal or production wallet endpoint")
	require.GreaterOrEqual(t, len(secret), 32)
	return strings.TrimSuffix(base, "/"), secret
}

func immediateV5WirePost(t *testing.T, base, secret, path string, body any, fixture bool) (int, []byte) {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, base+path, bytes.NewReader(raw))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	if fixture {
		req.Header.Set("x-fixture-secret", secret)
	} else {
		now := time.Now()
		token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"iss": "sub2api", "aud": "shipany-wallet", "sub": "sub2api-gateway", "iat": now.Unix(), "exp": now.Add(30 * time.Second).Unix(), "jti": uuid.NewString(), "scope": "wallet:task-pin"})
		token.Header["kid"] = "v1"
		signed, err := token.SignedString([]byte(secret))
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+signed)
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	response, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	require.NoError(t, err)
	return resp.StatusCode, response
}

func immediateV5Bridge(t *testing.T, f *mediaFixture, base, secret, mode string) *service.CanonicalWalletBridge {
	t.Helper()
	cfg := *f.cfg
	cfg.CanonicalWallet.ControlPlaneURL = base
	cfg.CanonicalWallet.Secret = secret
	cfg.CanonicalWallet.Issuer = "sub2api"
	cfg.CanonicalWallet.Audience = "shipany-wallet"
	cfg.CanonicalWallet.RequestTimeoutMS = 3000
	cfg.CanonicalWallet.LLMImmediateReleaseMode = mode
	cfg.CanonicalWallet.ReaderJournalDirectory = f.readerJournalDir
	bridge := service.NewCanonicalWalletBridge(&cfg, repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore), f.db, repository.ProvideWalletOutboxStore(f.db))
	t.Cleanup(bridge.Close)
	return bridge
}

func immediateV5Prepared(t *testing.T) (*mediaFixture, *service.AuthorizationHandle, *service.BillingSnapshot, string, string) {
	t.Helper()
	base, secret := immediateV5WireConfig(t)
	f := newMediaFixture(t)
	f.svc.Stop()
	// The fixture control plane supplies the pre-existing PG/Redis funding;
	// financial expire/create/finish RPCs below execute against actual Worker D1.
	for _, budget := range []int64{300000000, 300000000} {
		leaseID := "v5-" + uuid.NewString()
		lease := &testFundingLease{id: leaseID, budget: budget, expires: time.Now().Add(5 * time.Minute)}
		f.control.pool = append(f.control.pool, lease)
		require.NoError(t, repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore).InstallCanonicalWalletLease(context.Background(), service.CanonicalWalletLease{LeaseID: leaseID, PlatformUserID: "media-user", Currency: "USD", BudgetUnits: budget, ExpiresAt: lease.expires}))
		status, body := immediateV5WirePost(t, base, secret, "/__fixture/seed", map[string]any{"platform_user_id": "media-user", "grant_units": strconv.FormatInt(budget, 10), "lease_id": leaseID, "budget_units": strconv.FormatInt(budget, 10)}, true)
		require.Less(t, status, 300, string(body))
	}
	f.control.unleased = 0
	h, snapshot := f.authorize(t, 500000000, service.BillingFamilyOpenAI)
	token := h.ID + ".1"
	_, err := f.db.Exec(`UPDATE wallet_authorization_segment SET state='indeterminate',authorization_token=$2,first_write_at=now(),write_ended_at=now(),write_active_until=NULL,expiry_deadline=now()+interval '30 minutes',terminal_sealed_at=now(),terminal_evidence=jsonb_build_object('source','fixture-owner','authorization_id',$1::text,'authorization_token',$2::text),evidence_pending=false,fee_pending=false,known_fee_units=NULL WHERE parent_authorization_id=$1`, h.ID, token)
	require.NoError(t, err)
	for _, segment := range h.Segments {
		status, body := immediateV5WirePost(t, base, secret, "/api/internal/v2/wallet/task-pins/create", map[string]any{"authorization_id": segment.AuthorizationID, "authorization_kind": "llm", "gateway_job_id": h.ID, "platform_user_id": "media-user", "lease_id": segment.LeaseID, "held": amount(segment.HeldUnits), "billing_snapshot_id": snapshot.ID, "settlement_event_id": segment.EventID, "usd_wallet_policy_version": "usd-wallet-v1"}, false)
		require.Less(t, status, 300, string(body))
	}
	return f, h, snapshot, base, secret
}

func TestExternalImmediateV5PartialACKCountsSignedTimeAndResumesOff(t *testing.T) {
	f, h, snapshot, base, secret := immediateV5Prepared(t)
	var denied atomic.Bool
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		if r.URL.Path == "/api/internal/v2/wallet/task-pins/expire" && body["authorization_id"] == h.Segments[1].AuthorizationID && denied.CompareAndSwap(false, true) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		forward, _ := http.NewRequestWithContext(r.Context(), r.Method, base+r.URL.RequestURI(), bytes.NewReader(raw))
		forward.Header = r.Header.Clone()
		resp, err := http.DefaultClient.Do(forward)
		if err != nil {
			w.WriteHeader(502)
			return
		}
		defer resp.Body.Close()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
	defer proxy.Close()
	bridge := immediateV5Bridge(t, f, proxy.URL, secret, "enabled")
	deadline := time.Now().Add(-time.Second).UTC().Truncate(time.Millisecond)
	proof := strings.Repeat("a", 64)
	claimed, err := bridge.ClaimImmediateWalletExpiry(context.Background(), h.ID, deadline, proof)
	require.NoError(t, err)
	require.True(t, claimed)
	handled, err := bridge.RecoverImmediateWalletExpiry(context.Background(), h.ID)
	require.True(t, handled)
	require.Error(t, err)
	var count int
	var total int64
	var firstSignedTime time.Time
	require.NoError(t, f.db.QueryRow(`SELECT count(*),COALESCE(sum(held_units),0)::bigint,min(released_at) FROM wallet_unknown_release_counter WHERE parent_authorization_id=$1`, h.ID).Scan(&count, &total, &firstSignedTime))
	require.Equal(t, 1, count)
	require.Equal(t, h.Segments[0].HeldUnits, total, "only the acknowledged segment is counted")
	store := repository.NewGatewayCache(f.rdb).(service.CanonicalWalletLeaseStore)
	first, err := store.GetCanonicalWalletHold(context.Background(), "media-user", h.Segments[0].AuthorizationID)
	require.NoError(t, err)
	require.Equal(t, "released", first.State)
	second, err := store.GetCanonicalWalletHold(context.Background(), "media-user", h.Segments[1].AuthorizationID)
	require.NoError(t, err)
	require.Equal(t, "armed", second.State)
	// Restart recovery with both flags off: a committed intent still finishes.
	resumed := immediateV5Bridge(t, f, base, secret, "off")
	handled, err = resumed.RecoverImmediateWalletExpiry(context.Background(), h.ID)
	require.True(t, handled)
	require.NoError(t, err)
	_, err = resumed.RecoverImmediateWalletExpiry(context.Background(), h.ID)
	require.NoError(t, err)
	var firstAfter time.Time
	require.NoError(t, f.db.QueryRow(`SELECT count(*),sum(held_units)::bigint,min(released_at) FROM wallet_unknown_release_counter WHERE parent_authorization_id=$1`, h.ID).Scan(&count, &total, &firstAfter))
	require.Equal(t, 2, count)
	require.EqualValues(t, 500000000, total)
	require.True(t, firstSignedTime.Equal(firstAfter), "replay and later ACK must not refresh the first signed release")
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_unknown_release_counter c JOIN wallet_authorization_segment s USING(authorization_id) WHERE c.parent_authorization_id=$1 AND c.released_at=s.expiry_released_at AND c.receipt_id=s.expiry_receipt_id AND c.receipt_signature=s.expiry_receipt_signature AND c.billing_snapshot_id=$2`, h.ID, snapshot.ID).Scan(&count))
	require.Equal(t, 2, count)
	risk := immediateV5Bridge(t, f, base, secret, "enabled")
	err = risk.CheckUnknownReleaseSoftLimit(context.Background(), "media-user")
	status, retry, ok := service.WalletRiskRefusalDetails(err)
	require.True(t, ok)
	require.Equal(t, http.StatusTooManyRequests, status)
	require.Greater(t, retry, 23*3600)
	require.NoError(t, risk.AdmitWalletRisk(context.Background(), service.WalletRiskAdmission{ParentAuthorizationID: "existing-v5-job", PlatformUserID: "another-user", BillingSnapshotID: snapshot.ID, Kind: "media"}))
	_, err = f.db.Exec(`UPDATE wallet_risk_admission SET platform_user_id='media-user' WHERE parent_authorization_id='existing-v5-job'`)
	require.Error(t, err, "durable admission identity is immutable")
}

func TestExternalImmediateV5ProofFeeBarrierAndV1EvidenceStayImmutable(t *testing.T) {
	f, h, _, base, secret := immediateV5Prepared(t)
	deadline := time.Now().Add(-time.Second).UTC().Truncate(time.Millisecond)
	proof := strings.Repeat("b", 64)
	for _, mode := range []string{"off", "shadow"} {
		claimed, err := immediateV5Bridge(t, f, base, secret, mode).ClaimImmediateWalletExpiry(context.Background(), h.ID, deadline, proof)
		require.NoError(t, err)
		require.False(t, claimed)
	}
	bridge := immediateV5Bridge(t, f, base, secret, "enabled")
	for _, barrier := range []string{"fee_pending=true", "evidence_pending=true", "known_fee_units=0"} {
		_, err := f.db.Exec(`UPDATE wallet_authorization_segment SET `+barrier+` WHERE authorization_id=$1`, h.ID)
		require.NoError(t, err)
		claimed, err := bridge.ClaimImmediateWalletExpiry(context.Background(), h.ID, deadline, proof)
		require.NoError(t, err)
		require.False(t, claimed, barrier)
		_, err = f.db.Exec(`UPDATE wallet_authorization_segment SET fee_pending=false,evidence_pending=false,known_fee_units=NULL WHERE authorization_id=$1`, h.ID)
		require.NoError(t, err)
	}
	var legacyDeadline time.Time
	require.NoError(t, f.db.QueryRow(`SELECT expiry_deadline FROM wallet_authorization_segment WHERE authorization_id=$1`, h.ID).Scan(&legacyDeadline))
	claimed, err := bridge.ClaimImmediateWalletExpiry(context.Background(), h.ID, deadline, proof)
	require.NoError(t, err)
	require.True(t, claimed)
	_, err = bridge.ClaimImmediateWalletExpiry(context.Background(), h.ID, deadline, strings.Repeat("c", 64))
	require.Error(t, err)
	for _, change := range []string{"expiry_intent_version=1", "expiry_deadline=expiry_deadline+interval '1 second'", "expiry_v2_deadline=expiry_v2_deadline+interval '1 second'", "authorization_token='other'", "kind='media'", "expiry_terminal_proof='" + strings.Repeat("c", 64) + "'"} {
		_, err = f.db.Exec(`UPDATE wallet_authorization_segment SET `+change+` WHERE authorization_id=$1`, h.ID)
		require.Error(t, err, change)
	}
	var after time.Time
	require.NoError(t, f.db.QueryRow(`SELECT expiry_deadline FROM wallet_authorization_segment WHERE authorization_id=$1`, h.ID).Scan(&after))
	require.True(t, legacyDeadline.Equal(after))
	var counters int
	require.NoError(t, f.db.QueryRow(`SELECT count(*) FROM wallet_unknown_release_counter`).Scan(&counters))
	require.Zero(t, counters, "intent and shadow eligibility are not financial releases")
}
