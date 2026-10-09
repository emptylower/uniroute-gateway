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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestExternalFundingV7ActualActiveLivePinPreservesItsOriginalBackingAndSlot(t *testing.T) {
	x := newFundingV7Fixture(t)
	llm := x.authorize(t, 100000000)
	snapshot := *x.snapshot
	snapshot.ID, snapshot.Family = "funding-v7-live-snapshot-"+uuid.NewString(), service.BillingFamilyLive
	require.NoError(t, x.f.snapshots.Persist(context.Background(), &snapshot))
	live, err := service.NewCanonicalWalletAuthorizer(x.f.cfg, x.f.bridge, x.f.snapshots).Authorize(context.Background(), service.AuthorizeInput{Snapshot: &snapshot, User: x.owner, FixedEstimateUnits: 100000000})
	require.NoError(t, err)
	require.Len(t, live.Segments, 1)
	require.Equal(t, "live", live.Segments[0].Kind)
	require.Equal(t, x.source, live.LeaseID)
	_, err = x.mediaFunding(t, 200000000)
	require.Error(t, err, "the actual canonical source-return route preserves an active Live pin before isolated media funding")
	x.signedZero(t, llm)
	var revision, returned int64
	var closed bool
	require.NoError(t, x.f.db.QueryRow(`SELECT return_revision,returned_units,closed FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, x.source).Scan(&revision, &returned, &closed))
	require.Zero(t, revision)
	require.Zero(t, returned)
	require.False(t, closed)
	hold, err := x.wallet.GetCanonicalWalletHold(context.Background(), x.f.platformUserID, live.ID)
	require.NoError(t, err)
	require.Equal(t, "armed", hold.State)
	require.Equal(t, int64(100000000), hold.HeldUnits)
	var kind, state, pin, mode string
	require.NoError(t, x.f.db.QueryRow(`SELECT kind,state,pin_state FROM wallet_authorization_segment WHERE authorization_id=$1`, live.ID).Scan(&kind, &state, &pin))
	require.Equal(t, "live", kind)
	require.Equal(t, "held", state)
	require.Equal(t, "active", pin)
	require.NoError(t, x.f.db.QueryRow(`SELECT requested_mode FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, x.source).Scan(&mode))
	require.Equal(t, "partial", mode)
	code, raw := fundingV5WirePost(t, x.base, x.secret, "/api/internal/v2/wallet/leases/pool", "wallet:lease", map[string]string{"platform_user_id": x.f.platformUserID, "usd_wallet_policy_version": "usd-wallet-v1", "funding_scope": "all"})
	require.Equal(t, http.StatusOK, code, string(raw))
	var pool struct {
		Code int `json:"code"`
		Data struct {
			Leases []struct {
				ID   string `json:"lease_id"`
				Pins *int64 `json:"active_pin_count"`
				Live *int64 `json:"active_live_pin_count"`
			} `json:"leases"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(raw, &pool))
	require.Zero(t, pool.Code)
	var observed bool
	for _, lease := range pool.Data.Leases {
		if lease.ID == x.source {
			require.NotNil(t, lease.Pins)
			require.NotNil(t, lease.Live)
			require.Equal(t, int64(1), *lease.Pins)
			require.Equal(t, int64(1), *lease.Live)
			observed = true
		}
	}
	require.True(t, observed)
	require.Zero(t, fundingV7Available(t, x), "all original10 remains allocated; active Live forbids reclaim of this source")
	source, err := x.wallet.GetCanonicalWalletLeaseByID(context.Background(), x.f.platformUserID, x.source)
	require.NoError(t, err)
	require.Equal(t, int64(1000000000), source.BudgetUnits)
	require.Equal(t, int64(1000000000), source.FundingPrincipalUnits())
	require.Zero(t, source.ReturnedUnits)
	var isolatedMediaHolds int
	require.NoError(t, x.f.db.QueryRow(`SELECT count(*) FROM wallet_authorization_segment WHERE platform_user_id=$1 AND kind='media'`, x.f.platformUserID).Scan(&isolatedMediaHolds))
	require.Zero(t, isolatedMediaHolds, "reclaim refusal precedes any isolated media hold/provider POST")
	var generation, applied int64
	require.NoError(t, x.f.db.QueryRow(`SELECT generation,applied_generation FROM wallet_funding_terminal_work WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, x.source).Scan(&generation, &applied))
	require.Greater(t, generation, applied, "active Live cannot consume the original source's still-pending causal work")
	require.Never(t, func() bool {
		_ = x.f.db.QueryRow(`SELECT return_revision,closed FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, x.source).Scan(&revision, &closed)
		return revision != 0 || closed
	}, 700*time.Millisecond, 20*time.Millisecond, "active Live cannot be released or create unchanged zero-money return loops")
}

func TestExternalFundingV7RunningWorkerNewOriginalACKSurvivesOldPartialApplyGeneration(t *testing.T) {
	x := newFundingV7Fixture(t)
	x.f.bridge.Close()
	target, err := url.Parse(x.base)
	require.NoError(t, err)
	committed := make(chan []byte, 1)
	allow := make(chan struct{})
	var once sync.Once
	resume := func() { once.Do(func() { close(allow) }) }
	t.Cleanup(resume)
	pause := &atomic.Bool{}
	pause.Store(true)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		forwarded := r.Clone(r.Context())
		forwarded.URL.Scheme, forwarded.URL.Host, forwarded.RequestURI = target.Scheme, target.Host, ""
		forwarded.Body = io.NopCloser(bytes.NewReader(raw))
		response, err := http.DefaultClient.Do(forwarded)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		if r.URL.Path == "/api/internal/v2/wallet/leases/return" && response.StatusCode == http.StatusOK {
			var request struct {
				Source   string `json:"lease_id"`
				Mode     string `json:"mode"`
				Revision int64  `json:"base_return_revision"`
			}
			_ = json.Unmarshal(raw, &request)
			if request.Source == x.source && request.Mode == "partial" && request.Revision == 1 && pause.CompareAndSwap(true, false) {
				// Only delay a genuine already-committed canonical response. The
				// later original ACK happens while this old worker remains live.
				committed <- body
				select {
				case <-allow:
				case <-r.Context().Done():
					return
				}
			}
		}
		for key, values := range response.Header {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(response.StatusCode)
		_, _ = w.Write(body)
	}))
	t.Cleanup(proxy.Close)
	x.f.cfg.CanonicalWallet.ControlPlaneURL = proxy.URL
	x.f.bridge = service.NewCanonicalWalletBridge(x.f.cfg, x.wallet, x.f.db, repository.ProvideWalletOutboxStore(x.f.db))
	t.Cleanup(x.f.bridge.Close)
	first, second := x.authorize(t, 100000000), x.authorize(t, 100000000)
	_, err = x.mediaFunding(t, 200000000)
	require.NoError(t, err)
	x.signedZero(t, first)
	var originalResponse []byte
	select {
	case originalResponse = <-committed:
	case <-time.After(5 * time.Second):
		t.Fatal("the running worker did not commit genuine partial revision2")
	}
	var partial struct {
		Code int `json:"code"`
		Data struct {
			Receipt service.WalletFundingReturnReceipt `json:"receipt"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(originalResponse, &partial))
	require.Zero(t, partial.Code)
	require.Equal(t, int64(2), partial.Data.Receipt.ReturnRevision)
	require.Equal(t, "100000000", partial.Data.Receipt.HeldUnits)
	require.Equal(t, "900000000", partial.Data.Receipt.ReturnedAfterUnits)
	var oldGeneration, newGeneration, applied int64
	require.NoError(t, x.f.db.QueryRow(`SELECT generation FROM wallet_funding_terminal_work WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, x.source).Scan(&oldGeneration))
	started := time.Now()
	x.signedZero(t, second)
	require.NoError(t, x.f.db.QueryRow(`SELECT generation,applied_generation FROM wallet_funding_terminal_work WHERE platform_user_id=$1 AND lease_id=$2`, x.f.platformUserID, x.source).Scan(&newGeneration, &applied))
	require.Greater(t, newGeneration, oldGeneration, "second actual signed ACK and successful cleanup are later causal generations")
	require.Less(t, applied, newGeneration, "old in-flight worker cannot swallow the newly committed causal work")
	resume()
	var closed bool
	var revision, returned int64
	require.Eventually(t, func() bool {
		return x.f.db.QueryRow(`SELECT f.closed,f.return_revision,f.returned_units,w.generation,w.applied_generation FROM wallet_funding_freeze f JOIN wallet_funding_terminal_work w USING(platform_user_id,lease_id) WHERE f.platform_user_id=$1 AND f.lease_id=$2`, x.f.platformUserID, x.source).Scan(&closed, &revision, &returned, &newGeneration, &applied) == nil && closed && revision == 3 && returned == 1000000000 && applied == newGeneration
	}, 6*time.Second, 20*time.Millisecond)
	require.Equal(t, int64(800000000), fundingV7Available(t, x), "two real zeros and isolated held media2 conserve all original10")
	fresh := x.authorize(t, 800000000)
	require.NotEqual(t, x.source, fresh.LeaseID)
	require.Less(t, time.Since(started), 2500*time.Millisecond, "the later original ACK proceeds through old pending replay and fresh authorization within2s plus isolated jitter")
}
