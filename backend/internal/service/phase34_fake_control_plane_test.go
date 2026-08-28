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
	// DrainedAt (redesign §9.1): set when a gateway drain of this lease did
	// NOT verify — the lease is never covering again (it stays slot-holding
	// until captures resolve it or the grace closes it). The server's own
	// row, never a gateway reservation figure.
	DrainedAt *time.Time
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
	// grantBelowMinOnce (§9.3): the NEXT issue path grants headroom one unit
	// below min_headroom — the non-conformant server the local under-grant
	// guard exists for — once, then behaves again.
	grantBelowMinOnce bool
	// withoutEnsureRoute (§9.6 item 6): every method on the ensure path
	// answers 404 — the pre-3.4a-S control plane.
	withoutEnsureRoute bool
	// probeCalls counts GET requests on the ensure path (§9.6 item 6's probe).
	probeCalls int
	// Phase 3.5 (§11.2/§11.10): the v2 settlements route's state.
	events         map[string]map[string]fakeSettlementEvent // user → event_id → the captured event
	captureSeqs    map[string]int64                          // lease id → applied settlement count
	settlementReqs []canonicalWalletSettlementWireRequest    // every settlement request, in order (G5's wire evidence)
	omitHeadroom   bool                                      // answer lease_over_capture WITHOUT data.headroom (the absent-field leg)
	responses      map[string]*fakeCannedResponse            // path → canned response (f.respondWith)
	Server         *httptest.Server
}

// fakeSettlementEvent is the fake's wallet_settlement_event row.
type fakeSettlementEvent struct {
	LeaseID string
	Units   int64
}

// fakeCannedResponse is f.respondWith's entry: answer this path with
// status/body, `times` times (-1 = until cleared).
type fakeCannedResponse struct {
	Status int
	Body   string
	Times  int
}

func newFakeEnsureControlPlane(t *testing.T, now func() time.Time) *fakeEnsureControlPlane {
	t.Helper()
	f := &fakeEnsureControlPlane{
		now: now, cap: 3, perLeaseMax: 10_000_000_000, grace: 30 * time.Minute,
		balance: map[string]int64{}, leases: map[string][]*fakeLease{},
		events: map[string]map[string]fakeSettlementEvent{}, captureSeqs: map[string]int64{},
		responses: map[string]*fakeCannedResponse{},
	}
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

// drainedAt (§9.1) reads the mark under the mutex; nil = never marked.
func (f *fakeEnsureControlPlane) drainedAt(user, id string) *time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lease(user, id).DrainedAt
}

// probeCallsLocked reads the probe counter under the mutex.
func (f *fakeEnsureControlPlane) probeCallsLocked() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.probeCalls
}

// close (Phase 3.5, §11.4): set a lease's Status to closed — the grace
// sweep or a verified drain already ran.
func (f *fakeEnsureControlPlane) close(user, id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lease(user, id).Status = "closed"
}

// respondWith (Phase 3.5, test 37): answer `path` with status/body, `times`
// times (-1 = until cleared). Consulted before the route's own handling.
func (f *fakeEnsureControlPlane) respondWith(path string, status int, body string, times int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.responses[path] = &fakeCannedResponse{Status: status, Body: body, Times: times}
}

// clearResponse removes a canned response.
func (f *fakeEnsureControlPlane) clearResponse(path string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.responses, path)
}

// canned answers for r.URL.Path, if one remains; called with f.mu held.
func (f *fakeEnsureControlPlane) canned(w http.ResponseWriter, r *http.Request) bool {
	c := f.responses[r.URL.Path]
	if c == nil {
		return false
	}
	if c.Times > 0 {
		c.Times--
		if c.Times == 0 {
			delete(f.responses, r.URL.Path)
		}
	}
	w.WriteHeader(c.Status)
	_, _ = w.Write([]byte(c.Body))
	return true
}

// fakeLeaseWireView is leaseWireView's twelve fields (lease-wire.ts) — the
// same view the ensure handler builds, shared by the v2 settlements route.
// Called with f.mu held.
func (f *fakeEnsureControlPlane) fakeLeaseWireView(user string, l *fakeLease) map[string]any {
	return map[string]any{
		"lease_id": l.ID, "platform_user_id": user, "currency": "CNY", "unit_version": "cny-e8-v1", "scale": 8,
		"budget": fakeAmountObject(l.Budget), "reserved": fakeAmountObject(0), "captured": fakeAmountObject(l.Captured),
		"released": fakeAmountObject(l.Released), "capture_seq": f.captureSeqs[l.ID], "status": l.Status,
		"expires_at": l.ExpiresAt.UTC().Format(time.RFC3339Nano),
	}
}

// fakeCanonicalBalance is the v2 route's canonical_balance: grant balance
// plus every ACTIVE lease's open headroom (what the user can still spend).
func (f *fakeEnsureControlPlane) fakeCanonicalBalance(user string) int64 {
	total := f.balance[user]
	for _, l := range f.leases[user] {
		if l.Status == "active" {
			total += l.headroom()
		}
	}
	return total
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

// fakeAmountObject renders an int64 as Phase 0's four-field amount object —
// the ONLY shape the v2 wire carries (§9.2).
func fakeAmountObject(units int64) map[string]any {
	return map[string]any{"amount_units": strconv.FormatInt(units, 10), "currency": "CNY", "scale": 8, "unit_version": "cny-e8-v1"}
}

func (f *fakeEnsureControlPlane) refuse(w http.ResponseWriter, reason string) {
	f.refuseStatus(w, http.StatusConflict, reason)
}

// refuseStatus is ShipAny's fail(status, reason, message) shape with the
// caller's status (404 lease_not_found vs the 409 family, §11.5).
func (f *fakeEnsureControlPlane) refuseStatus(w http.ResponseWriter, status int, reason string) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"code": -1, "message": reason, "data": map[string]any{"reason": reason}})
}

func (f *fakeEnsureControlPlane) refuseClamped(w http.ResponseWriter, reason, clampedBy string) {
	w.WriteHeader(http.StatusConflict)
	data := map[string]any{"reason": reason}
	if clampedBy != "" {
		data["clamped_by"] = clampedBy
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": -1, "message": reason, "data": data})
}

func (f *fakeEnsureControlPlane) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch r.URL.Path {
	case "/api/internal/v2/wallet/leases/ensure":
		if r.Method == http.MethodGet {
			// §9.6 item 6's probe: body-less 405 = the route exists (POST-only);
			// 404 = the control plane predates 3.4a-S.
			f.probeCalls++
			if f.withoutEnsureRoute {
				w.WriteHeader(http.StatusNotFound)
			} else {
				w.WriteHeader(http.StatusMethodNotAllowed)
			}
			return
		}
		if f.withoutEnsureRoute {
			w.WriteHeader(http.StatusNotFound)
			return
		}
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
		minHeadroom := mustUnits(req.MinHeadroom)
		// (1) drain (§3.3, §9.1; 3.4b §10.2): a VERIFIED drain closes the lease
		// as today — the identity is consumed == captured + gateway_released,
		// with the RELEASED figure taken from the ENTRY (the gateway's own
		// number), never from this fake's l.Released (the server's close
		// accounting — conflating them would make the check circular); an
		// unverified drain MARKS it — the server's own row, idempotent.
		for _, d := range req.Drained {
			l := f.lease(user, d.LeaseID)
			if l == nil || l.Status != "active" {
				continue
			}
			gatewayReleased := int64(0)
			if d.GatewayReleased != nil {
				gatewayReleased = mustUnits(*d.GatewayReleased)
			}
			if mustUnits(d.GatewayConsumed) == l.Captured+gatewayReleased {
				l.Status = "closed"
				l.Released = l.Budget - l.Captured
				f.balance[user] += l.Released
			} else if l.DrainedAt == nil {
				t := now
				l.DrainedAt = &t
			}
		}
		// (2) §9.1 early close: a marked lease whose captures consumed its whole
		// budget is closed now (released = budget − captured = 0 — nothing left),
		// so a resolved slot is not held for the rest of the grace.
		for _, l := range f.leases[user] {
			if l.Status == "active" && l.DrainedAt != nil && l.Captured+l.Released == l.Budget {
				l.Status = "closed"
				l.Released = 0
			}
		}
		// (3) step 1: partition — covering excludes every marked lease (§9.1);
		// slot-holding unchanged (a marked lease holds its slot until resolution
		// or the grace).
		var covering []*fakeLease
		slotHolding := 0
		for _, l := range f.leases[user] {
			if l.Status != "active" {
				continue
			}
			if l.DrainedAt == nil && l.ExpiresAt.After(now) && l.headroom() >= minHeadroom {
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
			budget := mustUnits(req.RequestedBudget)
			if minHeadroom > budget {
				budget = minHeadroom
			}
			if budget > f.perLeaseMax {
				budget = f.perLeaseMax
			}
			if budget > f.balance[user] {
				budget = f.balance[user]
			}
			if budget < minHeadroom {
				f.refuseClamped(w, "insufficient_balance", "balance")
				return
			}
			if f.grantBelowMinOnce {
				// §9.3's unreachable-against-conformance case: a server bug
				// that issues below min_headroom_units anyway.
				f.grantBelowMinOnce = false
				budget = minHeadroom - 1
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
			"lease_id": pick.ID, "platform_user_id": user, "currency": "CNY", "unit_version": "cny-e8-v1", "scale": 8,
			"budget": fakeAmountObject(pick.Budget), "reserved": fakeAmountObject(0), "captured": fakeAmountObject(pick.Captured),
			"released": fakeAmountObject(pick.Released), "headroom": fakeAmountObject(pick.headroom()),
			"capture_seq": 0, "status": pick.Status, "expires_at": pick.ExpiresAt.UTC().Format(time.RFC3339Nano),
			"outcome": outcome, "clamped_by": "none",
		}})
	case "/api/internal/v2/wallet/settlements":
		// Phase 3.5 (§11.2): the fake's units-native v2 settlements route —
		// per-event identity, named_lease_id, lease_over_capture with
		// data.headroom, lease_not_capturable on a closed lease,
		// settlement_payload_conflict, and the strict amount-object wire.
		if f.canned(w, r) {
			return
		}
		var req canonicalWalletSettlementWireRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f.settlementReqs = append(f.settlementReqs, req)
		user := strings.TrimSpace(req.PlatformUserID)
		units := mustUnits(req.Amount)
		if f.events[user] == nil {
			f.events[user] = map[string]fakeSettlementEvent{}
		}
		if stored, known := f.events[user][req.EventID]; known {
			// Per-event identity first: a different amount is a payload
			// conflict; the same amount is a duplicate, and named_lease_id
			// names the lease the redelivery asked for when that is not the
			// lease the event actually captured on (§11.2).
			if stored.Units != units {
				f.refuse(w, "settlement_payload_conflict")
				return
			}
			l := f.lease(user, stored.LeaseID)
			var named any
			if stored.LeaseID != req.LeaseID {
				named = req.LeaseID
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{
				"accepted": true, "duplicate": true, "named_lease_id": named,
				"event": map[string]any{
					"event_id": req.EventID, "lease_id": stored.LeaseID, "amount": fakeAmountObject(stored.Units),
					"lease_capture_seq":     f.captureSeqs[stored.LeaseID],
					"lease_captured_before": fakeAmountObject(l.Captured), "lease_captured_after": fakeAmountObject(l.Captured),
					"occurred_at": req.OccurredAt,
				},
				"lease":             f.fakeLeaseWireView(user, l),
				"canonical_balance": fakeAmountObject(f.fakeCanonicalBalance(user)),
			}})
			return
		}
		l := f.lease(user, req.LeaseID)
		if l == nil {
			f.refuseStatus(w, http.StatusNotFound, "lease_not_found")
			return
		}
		if l.Status != "active" {
			f.refuse(w, "lease_not_capturable")
			return
		}
		if units > l.headroom() {
			if f.omitHeadroom {
				f.refuse(w, "lease_over_capture")
				return
			}
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]any{"code": -1, "message": "lease_over_capture", "data": map[string]any{"reason": "lease_over_capture", "headroom": fakeAmountObject(l.headroom())}})
			return
		}
		before := l.Captured
		l.Captured += units
		f.captureSeqs[l.ID]++
		seq := f.captureSeqs[l.ID]
		f.events[user][req.EventID] = fakeSettlementEvent{LeaseID: l.ID, Units: units}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{
			"accepted": true, "duplicate": false, "named_lease_id": nil,
			"event": map[string]any{
				"event_id": req.EventID, "lease_id": l.ID, "amount": fakeAmountObject(units),
				"lease_capture_seq":     seq,
				"lease_captured_before": fakeAmountObject(before),
				"lease_captured_after":  fakeAmountObject(l.Captured),
				"occurred_at":           req.OccurredAt,
			},
			"lease":             f.fakeLeaseWireView(user, l),
			"canonical_balance": fakeAmountObject(f.fakeCanonicalBalance(user)),
		}})
	default:
		http.NotFound(w, r)
	}
}

func itoa(n int) string { return strconv.Itoa(n) }
