package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
)

type WalletAvailabilityLeaseInput struct {
	LeaseID       string     `json:"lease_id"`
	BudgetUnits   string     `json:"budget_units"`
	CapturedUnits string     `json:"captured_units"`
	ReleasedUnits string     `json:"released_units"`
	ReservedUnits string     `json:"reserved_units"`
	Status        string     `json:"status"`
	DrainedAt     *time.Time `json:"drained_at"`
	ExpiresAt     time.Time  `json:"expires_at"`
	CaptureSeq    int64      `json:"capture_seq"`
}
type WalletAvailabilityInput struct {
	UnitVersion      string                         `json:"unit_version"`
	USDPolicyVersion string                         `json:"usd_wallet_policy_version"`
	Leases           []WalletAvailabilityLeaseInput `json:"leases"`
	CapturedEventIDs []string                       `json:"captured_event_ids"`
}
type WalletAvailabilityLease struct {
	LeaseID       string `json:"lease_id"`
	ConsumedUnits string `json:"consumed_units"`
	ReleasedUnits string `json:"released_units"`
	FreeUnits     string `json:"free_units"`
	HeldUnits     string `json:"held_units"`
	CacheStatus   string `json:"cache_status"`
	Sealed        bool   `json:"sealed"`
}
type WalletAvailabilityObligation struct {
	Kind            string `json:"kind"`
	AuthorizationID string `json:"authorization_id"`
	EventID         string `json:"event_id,omitempty"`
	LeaseID         string `json:"lease_id"`
	AmountUnits     string `json:"amount_units"`
	Captured        bool   `json:"captured"`
	CoveredByRedis  bool   `json:"covered_by_redis"`
}
type WalletAvailability struct {
	AsOf         time.Time                      `json:"as_of"`
	Completeness string                         `json:"completeness"`
	Leases       []WalletAvailabilityLease      `json:"leases"`
	Obligations  []WalletAvailabilityObligation `json:"obligations"`
}

func strictAvailabilityUnits(raw string) (int64, error) {
	if raw == "" || strings.TrimSpace(raw) != raw {
		return 0, errors.New("invalid wallet units")
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < 0 || strconv.FormatInt(n, 10) != raw {
		return 0, errors.New("invalid wallet units")
	}
	return n, nil
}
func (s *MediaTaskService) Availability(ctx context.Context, userID int64, in WalletAvailabilityInput) (WalletAvailability, error) {
	out := WalletAvailability{AsOf: time.Now().UTC(), Completeness: "unknown", Leases: []WalletAvailabilityLease{}, Obligations: []WalletAvailabilityObligation{}}
	if in.UnitVersion != CanonicalWalletUnitVersion || in.USDPolicyVersion != "usd-wallet-v1" || len(in.Leases) > 1000 || len(in.CapturedEventIDs) > 10000 {
		return out, infraerrors.BadRequest("INVALID_WALLET_SNAPSHOT", "invalid wallet snapshot")
	}
	user, err := s.users.GetByID(ctx, userID)
	if err != nil {
		return out, err
	}
	if strings.TrimSpace(user.PlatformUserID) == "" {
		return out, ErrMediaUnavailable
	}
	if s.bridge == nil || s.db == nil {
		return out, nil
	}
	stateStore, ok := s.bridge.store.(MediaWalletStateStore)
	if !ok {
		return out, nil
	}
	ids := []string{}
	seenLease := map[string]bool{}
	captured := map[string]bool{}
	for _, id := range in.CapturedEventIDs {
		captured[id] = true
	}
	for _, l := range in.Leases {
		if l.LeaseID == "" || seenLease[l.LeaseID] || l.CaptureSeq < 0 {
			return out, infraerrors.BadRequest("INVALID_WALLET_SNAPSHOT", "invalid lease identity")
		}
		seenLease[l.LeaseID] = true
		ids = append(ids, l.LeaseID)
		for _, value := range []string{l.BudgetUnits, l.CapturedUnits, l.ReleasedUnits, l.ReservedUnits} {
			if _, e := strictAvailabilityUnits(value); e != nil {
				return out, infraerrors.BadRequest("INVALID_WALLET_SNAPSHOT", "invalid lease amount")
			}
		}
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return out, nil
	}
	defer tx.Rollback()
	obligations := []WalletAvailabilityObligation{}
	seenAuth := map[string]bool{}
	events := []string{}
	add := func(kind, auth, event, lease string, units int64, isCaptured bool) {
		if units <= 0 || seenAuth[auth] && auth != "" {
			return
		}
		if auth != "" {
			seenAuth[auth] = true
		}
		obligations = append(obligations, WalletAvailabilityObligation{Kind: kind, AuthorizationID: auth, EventID: event, LeaseID: lease, AmountUnits: strconv.FormatInt(units, 10), Captured: isCaptured || captured[event]})
		if event != "" {
			events = append(events, event)
		}
	}
	rows, err := tx.QueryContext(ctx, `SELECT a.authorization_id,COALESCE(a.event_id,''),a.lease_id,a.held_units,COALESCE(o.status,'') FROM gateway_media_task m JOIN wallet_authorization_segment a ON a.parent_authorization_id=m.authorization_id LEFT JOIN wallet_settlement_outbox o ON o.event_id=a.event_id WHERE m.platform_user_id=$1 AND m.pin_state<>'finished'`, user.PlatformUserID)
	if err != nil {
		return out, nil
	}
	for rows.Next() {
		var auth, event, lease, status string
		var units int64
		if err = rows.Scan(&auth, &event, &lease, &units, &status); err != nil {
			rows.Close()
			return out, nil
		}
		add("media", auth, event, lease, units, status == "delivered")
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return out, nil
	}
	rows, err = tx.QueryContext(ctx, `SELECT a.authorization_id,COALESCE(a.event_id,''),a.lease_id,CASE WHEN a.state='settling' THEN a.actual_units ELSE a.held_units END,COALESCE(o.status,'') FROM wallet_authorization_segment a LEFT JOIN wallet_settlement_outbox o ON o.event_id=a.event_id WHERE a.platform_user_id=$1 AND a.kind<>'media' AND a.state NOT IN ('finished','released')`, user.PlatformUserID)
	if err != nil {
		return out, nil
	}
	for rows.Next() {
		var auth, event, lease, status string
		var units int64
		if err = rows.Scan(&auth, &event, &lease, &units, &status); err != nil {
			rows.Close()
			return out, nil
		}
		add("hold", auth, event, lease, units, status == "delivered")
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return out, nil
	}
	rows, err = tx.QueryContext(ctx, `SELECT authorization_id,windows FROM wallet_live_provisional WHERE platform_user_id=$1 AND status IN ('provisional','active','finalizing')`, user.PlatformUserID)
	if err != nil {
		return out, nil
	}
	for rows.Next() {
		var auth string
		var raw []byte
		if err = rows.Scan(&auth, &raw); err != nil {
			rows.Close()
			return out, nil
		}
		var windows []LiveWindow
		if json.Unmarshal(raw, &windows) != nil {
			rows.Close()
			return out, nil
		}
		for _, w := range windows {
			if w.PendingUnits > w.SettledUnits {
				token := w.Token
				if token == "" {
					token = auth
				}
				add("live", token, "", w.LeaseID, w.PendingUnits-w.SettledUnits, false)
			}
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return out, nil
	}
	rows, err = tx.QueryContext(ctx, `SELECT COALESCE(authorization_id,''),event_id,COALESCE(lease_id,''),amount_units,status FROM wallet_settlement_outbox WHERE platform_user_id=$1 AND status<>'delivered'`, user.PlatformUserID)
	if err != nil {
		return out, nil
	}
	for rows.Next() {
		var auth, event, lease, status string
		var units int64
		if err = rows.Scan(&auth, &event, &lease, &units, &status); err != nil {
			rows.Close()
			return out, nil
		}
		add("settlement", auth, event, lease, units, false)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return out, nil
	}
	rows, err = tx.QueryContext(ctx, `SELECT authorization_id,lease_id,held_units FROM wallet_hold_outcome WHERE platform_user_id=$1 AND resolution IS NULL`, user.PlatformUserID)
	if err != nil {
		return out, nil
	}
	for rows.Next() {
		var auth, lease string
		var units int64
		if err = rows.Scan(&auth, &lease, &units); err != nil {
			rows.Close()
			return out, nil
		}
		add("indeterminate", auth, "", lease, units, false)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return out, nil
	}
	first, err := stateStore.ReadMediaWalletState(ctx, user.PlatformUserID, ids, events)
	if err != nil {
		return out, nil
	}
	leases := map[string]MediaWalletRawLease{}
	holds := map[string]CanonicalWalletHold{}
	for _, l := range first.Leases {
		leases[l.LeaseID] = l
	}
	for _, h := range first.Holds {
		holds[h.AuthorizationID] = h
		if h.State == "armed" {
			add("hold", h.AuthorizationID, h.EventID, h.LeaseID, h.HeldUnits, false)
		}
	}
	complete := true
	heldTotals := map[string]int64{}
	coveredTotals := map[string]int64{}
	for i := range obligations {
		o := &obligations[i]
		units, e := strictAvailabilityUnits(o.AmountUnits)
		if e != nil {
			return out, nil
		}
		raw, exists := leases[o.LeaseID]
		h, armed := holds[o.AuthorizationID]
		o.CoveredByRedis = exists && raw.Present && ((armed && h.State == "armed" && h.HeldUnits >= units && h.LeaseID == o.LeaseID) || (o.EventID != "" && first.Reservations[o.EventID] == o.LeaseID))
		if !o.Captured {
			heldTotals[o.LeaseID], e = AddUnits(heldTotals[o.LeaseID], units)
			if e != nil {
				return out, nil
			}
			if o.CoveredByRedis {
				coveredTotals[o.LeaseID], e = AddUnits(coveredTotals[o.LeaseID], units)
				if e != nil {
					return out, nil
				}
			}
		}
		if o.LeaseID == "" {
			complete = false
		}
	}
	for _, l := range in.Leases {
		raw := leases[l.LeaseID]
		view := WalletAvailabilityLease{LeaseID: l.LeaseID, ConsumedUnits: strconv.FormatInt(raw.ConsumedUnits, 10), ReleasedUnits: strconv.FormatInt(raw.ReleasedUnits, 10), FreeUnits: "0", HeldUnits: strconv.FormatInt(heldTotals[l.LeaseID], 10), CacheStatus: "missing", Sealed: raw.Sealed}
		if !raw.Present {
			if (l.Status == "active" && l.DrainedAt == nil && l.ExpiresAt.After(out.AsOf)) || heldTotals[l.LeaseID] > 0 {
				complete = false
			}
		} else {
			view.CacheStatus = "present"
			budget, _ := strictAvailabilityUnits(l.BudgetUnits)
			capUnits, _ := strictAvailabilityUnits(l.CapturedUnits)
			if raw.Recovered || raw.BudgetUnits != budget || raw.ReleasedUnits > raw.ConsumedUnits || raw.ConsumedUnits < 0 {
				complete = false
			}
			net := raw.ConsumedUnits - raw.ReleasedUnits
			covered, e := AddUnits(capUnits, coveredTotals[l.LeaseID])
			if e != nil || net != covered || net > raw.BudgetUnits {
				complete = false
			}
			if !raw.Sealed && l.Status == "active" && l.DrainedAt == nil && l.ExpiresAt.After(out.AsOf) && raw.ExpiresAtMS > out.AsOf.UnixMilli() && net <= raw.BudgetUnits {
				view.FreeUnits = strconv.FormatInt(raw.BudgetUnits-net, 10)
			}
		}
		view.HeldUnits = strconv.FormatInt(coveredTotals[l.LeaseID], 10)
		out.Leases = append(out.Leases, view)
	}
	second, err := stateStore.ReadMediaWalletState(ctx, user.PlatformUserID, ids, events)
	if err != nil || second.Fingerprint != first.Fingerprint {
		complete = false
	}
	if err = tx.Commit(); err != nil {
		complete = false
	}
	out.Obligations = obligations
	if complete {
		out.Completeness = "complete"
	}
	return out, nil
}
