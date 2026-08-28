//go:build integration

package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeEnsureControlPlane implements redesign §3 in memory: one transaction per
// call, serialized per process (a mutex — the prototype's stand-in for
// ShipAny's per-user serialization), the covering / slot-holding partition,
// prefer_lease_id, the cap on authorize only, the server-verified drain
// (closed iff gateway_consumed_units == captured_units), the balance clamp,
// and the v1 settlements route (still *_micros) so captures land on
// captured_units. Its covering predicate is budget − captured − released ≥
// min — it knows NOTHING about gateway reservations, exactly as §3 step 1
// and §4 ("ensure may answer reused on a lease the gateway has fully reserved
// locally") require; tests that need a lease to be non-covering set Captured
// under f.mu. Refusals use ShipAny's real wire shape: 409 +
// {"code":-1,"message":…,"data":{"reason":…}}. The clamp issues the exact
// clamped units; §3 step 4's unitsToCreditsCeil rounding is the ShipAny half's
// test 4, not modelled here.
type fakeLease struct {
	ID        string
	Purpose   string
	Budget    int64
	Captured  int64
	Released  int64
	ExpiresAt time.Time
	Status    string // active | closed
}

type fakeEnsureControlPlane struct {
	mu             sync.Mutex
	now            func() time.Time
	cap            int
	perLeaseMax    int64
	grace          time.Duration
	balance        map[string]int64 // units available per user
	leases         map[string][]*fakeLease
	seq            int
	issuances      int
	ensureCalls    int
	requests       []canonicalWalletEnsureRequest
	ensureHeaders  []http.Header // ensure requests only — the settlement client sends its own Idempotency-Key
	contentionOnce bool
	Server         *httptest.Server
}

func newFakeEnsureControlPlane(t *testing.T, now func() time.Time) *fakeEnsureControlPlane {
	t.Helper()
	f := &fakeEnsureControlPlane{now: now, cap: 3, perLeaseMax: 10_000_000_000, grace: 30 * time.Minute, balance: map[string]int64{}, leases: map[string][]*fakeLease{}}
	f.Server = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.Server.Close)
	return f
}

func (f *fakeEnsureControlPlane) fund(user string, units int64) {
	f.mu.Lock()
	f.balance[user] += units
	f.mu.Unlock()
}

// seedLease installs a server-side lease directly (for cap/settle scenarios).
func (f *fakeEnsureControlPlane) seedLease(user, id, purpose string, budget, captured int64, expires time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leases[user] = append(f.leases[user], &fakeLease{ID: id, Purpose: purpose, Budget: budget, Captured: captured, ExpiresAt: expires, Status: "active"})
}

// setCaptured is the server-side settlement a test applies directly.
func (f *fakeEnsureControlPlane) setCaptured(user, id string, captured int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lease(user, id).Captured = captured
}

func (f *fakeEnsureControlPlane) setContentionOnce() {
	f.mu.Lock()
	f.contentionOnce = true
	f.mu.Unlock()
}

func (f *fakeEnsureControlPlane) status(user, id string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lease(user, id).Status
}

func (f *fakeEnsureControlPlane) purpose(user, id string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lease(user, id).Purpose
}

// lease is called with f.mu held.
func (f *fakeEnsureControlPlane) lease(user, id string) *fakeLease {
	for _, l := range f.leases[user] {
		if l.ID == id {
			return l
		}
	}
	return nil
}

func (l *fakeLease) headroom() int64 { return l.Budget - l.Captured - l.Released }

func (f *fakeEnsureControlPlane) refuse(w http.ResponseWriter, reason string) {
	w.WriteHeader(http.StatusConflict)
	_ = json.NewEncoder(w).Encode(map[string]any{"code": -1, "message": reason, "data": map[string]string{"reason": reason}})
}

func (f *fakeEnsureControlPlane) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch r.URL.Path {
	case "/api/internal/v2/wallet/leases/ensure":
		var req canonicalWalletEnsureRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f.ensureCalls++
		f.ensureHeaders = append(f.ensureHeaders, r.Header.Clone())
		f.requests = append(f.requests, req)
		if f.contentionOnce {
			f.contentionOnce = false
			f.refuse(w, "lease_contention")
			return
		}
		now := f.now()
		user := strings.TrimSpace(req.PlatformUserID)
		// drain: close the named lease iff the gateway's consumed equals our captured (§3.3)
		for _, d := range req.Drained {
			if l := f.lease(user, d.LeaseID); l != nil && l.Status == "active" && d.GatewayConsumedUnits == l.Captured {
				l.Status = "closed"
				l.Released = l.Budget - l.Captured
				f.balance[user] += l.Released
			}
		}
		// step 1: partition
		var covering []*fakeLease
		slotHolding := 0
		for _, l := range f.leases[user] {
			if l.Status != "active" {
				continue
			}
			if l.ExpiresAt.After(now) && l.headroom() >= req.MinHeadroomUnits {
				covering = append(covering, l)
			}
			if l.Purpose == "authorize" && (l.ExpiresAt.After(now) || (l.headroom() > 0 && l.ExpiresAt.After(now.Add(-f.grace)))) {
				slotHolding++
			}
		}
		// step 2: reuse
		var pick *fakeLease
		for _, l := range covering {
			if req.PreferLeaseID != "" && l.ID == req.PreferLeaseID {
				pick = l
				break
			}
		}
		if pick == nil {
			for _, l := range covering {
				if pick == nil || l.ExpiresAt.After(pick.ExpiresAt) || (l.ExpiresAt.Equal(pick.ExpiresAt) && l.headroom() > pick.headroom()) {
					pick = l
				}
			}
		}
		outcome := "reused"
		if pick == nil {
			// step 3: cap on authorize only
			if req.Purpose == "authorize" && slotHolding >= f.cap {
				f.refuse(w, "lease_cap_reached")
				return
			}
			// step 4: issue with the clamp
			budget := req.RequestedBudgetUnits
			if req.MinHeadroomUnits > budget {
				budget = req.MinHeadroomUnits
			}
			if budget > f.perLeaseMax {
				budget = f.perLeaseMax
			}
			if budget > f.balance[user] {
				budget = f.balance[user]
			}
			if budget < req.MinHeadroomUnits {
				f.refuse(w, "insufficient_balance")
				return
			}
			f.seq++
			f.issuances++
			f.balance[user] -= budget
			ttl := time.Duration(req.RequestedTTLSeconds) * time.Second
			pick = &fakeLease{ID: "srv-lease-" + itoa(f.seq), Purpose: req.Purpose, Budget: budget, ExpiresAt: now.Add(ttl), Status: "active"}
			f.leases[user] = append(f.leases[user], pick)
			outcome = "issued"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{
			"lease_id": pick.ID, "platform_user_id": user, "currency": "CNY",
			"budget_units": pick.Budget, "captured_units": pick.Captured, "released_units": pick.Released,
			"headroom_units": pick.headroom(), "expires_at": pick.ExpiresAt.UTC().Format(time.RFC3339Nano),
			"capture_seq": 0, "outcome": outcome, "clamped_by": "none",
		}})
	case "/api/internal/v1/wallet/settlements":
		var req canonicalWalletSettlementWireRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		l := f.lease(strings.TrimSpace(req.PlatformUserID), req.LeaseID)
		if l == nil {
			f.refuse(w, "lease_missing")
			return
		}
		units := req.AmountMicros * 100
		if units > l.headroom() {
			f.refuse(w, "lease_over_capture")
			return
		}
		l.Captured += units
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"accepted": true, "duplicate": false}})
	default:
		http.NotFound(w, r)
	}
}

func itoa(n int) string { return strconv.Itoa(n) }
