package service

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

// AuthorizationSegment binds one share of an attempt to its original funding
// lease. A group never moves principal between leases or reprices its snapshot.
type AuthorizationSegment struct {
	Kind            string                          `json:"kind"`
	State           string                          `json:"state"`
	Payload         *CanonicalWalletSettlementEvent `json:"-"`
	Remainder       *CanonicalWalletSettlementEvent `json:"-"`
	AuthorizationID string                          `json:"authorization_id"`
	LeaseID         string                          `json:"lease_id"`
	HeldUnits       int64                           `json:"held_units"`
	Basis           CanonicalWalletLease            `json:"basis"`
	EventID         string                          `json:"event_id,omitempty"`
	ActualUnits     int64                           `json:"actual_units"`
	PinState        string                          `json:"pin_state,omitempty"`
}
type CanonicalWalletPoolStore interface {
	ArmCanonicalWalletPool(context.Context, string, []AuthorizationSegment, int64, time.Time) error
}

func (b *CanonicalWalletBridge) authorizationSegments(ctx context.Context, parent string) ([]AuthorizationSegment, error) {
	rows, err := b.outboxDB.QueryContext(ctx, `SELECT authorization_id,lease_id,held_units,lease_basis,COALESCE(event_id,''),actual_units,pin_state,kind,state,settlement_payload,remainder_payload FROM wallet_authorization_segment WHERE parent_authorization_id=$1 ORDER BY ordinal`, parent)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []AuthorizationSegment{}
	for rows.Next() {
		var s AuthorizationSegment
		var raw, payload, remainder []byte
		if err = rows.Scan(&s.AuthorizationID, &s.LeaseID, &s.HeldUnits, &raw, &s.EventID, &s.ActualUnits, &s.PinState, &s.Kind, &s.State, &payload, &remainder); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &s.Basis); err != nil {
			return nil, err
		}
		if len(payload) > 0 {
			if err = json.Unmarshal(payload, &s.Payload); err != nil {
				return nil, err
			}
		}
		if len(remainder) > 0 {
			if err = json.Unmarshal(remainder, &s.Remainder); err != nil {
				return nil, err
			}
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
func (b *CanonicalWalletBridge) saveAuthorizationPlan(ctx context.Context, parent, user, snapshot, kind string, segments []AuthorizationSegment) error {
	tx, err := b.outboxDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for i, s := range segments {
		raw, e := json.Marshal(s.Basis)
		if e != nil {
			return e
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO wallet_authorization_segment(parent_authorization_id,ordinal,authorization_id,platform_user_id,billing_snapshot_id,lease_id,held_units,lease_basis,kind,event_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,NULLIF($10,''))`, parent, i, s.AuthorizationID, user, snapshot, s.LeaseID, s.HeldUnits, string(raw), kind, s.EventID)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}
func (b *CanonicalWalletBridge) saveAuthorizationSegments(ctx context.Context, parent string, segments []AuthorizationSegment) error {
	tx, err := b.outboxDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, s := range segments {
		raw, e := json.Marshal(s.Basis)
		if e != nil {
			return e
		}
		_, err = tx.ExecContext(ctx, `UPDATE wallet_authorization_segment SET lease_basis=$3::jsonb,event_id=CASE WHEN kind='media' THEN NULLIF($4,'') ELSE event_id END,actual_units=GREATEST(actual_units,$5),pin_state=CASE WHEN pin_state='finished' THEN pin_state ELSE $6 END WHERE parent_authorization_id=$1 AND authorization_id=$2`, parent, s.AuthorizationID, string(raw), s.EventID, s.ActualUnits, s.PinState)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}
func (b *CanonicalWalletBridge) fundingPool(ctx context.Context, user string) ([]CanonicalWalletLease, error) {
	client, ok := b.control.(*canonicalWalletHTTPClient)
	if !ok {
		return nil, errors.New("canonical wallet pool control unavailable")
	}
	var response struct {
		Leases []struct {
			canonicalWalletLeaseWireView
			Policy    string     `json:"usd_wallet_policy_version"`
			DrainedAt *time.Time `json:"drained_at"`
		} `json:"leases"`
	}
	if err := client.doJSON(ctx, http.MethodPost, "/api/internal/v2/wallet/leases/pool", "wallet:lease", user, map[string]string{"platform_user_id": user, "usd_wallet_policy_version": config.CanonicalUSDWalletPolicyVersion}, &response); err != nil {
		return nil, err
	}
	if len(response.Leases) > 1000 {
		return nil, errors.New("wallet pool exceeds safety bound")
	}
	leases := []CanonicalWalletLease{}
	seen := map[string]bool{}
	for _, wire := range response.Leases {
		if wire.PlatformUserID != user || wire.LeaseID == "" || seen[wire.LeaseID] || wire.UnitVersion != CanonicalWalletUnitVersion || wire.Scale != 8 || wire.Currency != "USD" || wire.Policy != config.CanonicalUSDWalletPolicyVersion {
			return nil, ErrCanonicalUSDWalletPolicy
		}
		seen[wire.LeaseID] = true
		if wire.Status != "active" || wire.DrainedAt != nil || !wire.ExpiresAt.After(b.clock().Add(time.Duration(b.cfg.ExpirySkewMarginMS)*time.Millisecond)) {
			continue
		}
		budget, err := parseCanonicalWalletAmountObject("budget", wire.Budget)
		if err != nil {
			return nil, err
		}
		captured, err := parseCanonicalWalletAmountObject("captured", wire.Captured)
		if err != nil {
			return nil, err
		}
		cached, err := b.store.GetCanonicalWalletLeaseByID(ctx, user, wire.LeaseID)
		if err != nil {
			return nil, err
		}
		if cached.Currency != "USD" || cached.PlatformUserID != user || cached.BudgetUnits > budget || cached.ConsumedUnits-cached.ReleasedUnits < captured {
			return nil, errors.New("wallet pool cache basis mismatch")
		}
		// Signed top-ups can raise budget, but never replace any raw C/R or holds.
		cached.BudgetUnits = budget
		cached.RequireCachedLease = true
		if wire.ExpiresAt.Before(cached.ExpiresAt) {
			cached.ExpiresAt = wire.ExpiresAt
		}
		if err = b.store.InstallCanonicalWalletLease(ctx, *cached); err != nil {
			return nil, err
		}
		if cached.Sealed || b.leaseExpiredAt(cached, b.clock()) {
			continue
		}
		leases = append(leases, *cached)
	}
	return leases, nil
}
func (b *CanonicalWalletBridge) authorizePool(ctx context.Context, h *AuthorizationHandle, user string, units int64) error {
	store, ok := b.store.(CanonicalWalletPoolStore)
	if !ok {
		return errors.New("wallet pool atomic authorization unavailable")
	}
	saved, err := b.authorizationSegments(ctx, h.ID)
	if err != nil {
		return err
	}
	if len(saved) > 0 {
		var total int64
		for _, s := range saved {
			total, err = AddUnits(total, s.HeldUnits)
			if err != nil {
				return err
			}
		}
		if total != units {
			return errors.New("persisted authorization bound mismatch")
		}
		if err = store.ArmCanonicalWalletPool(ctx, user, saved, b.graceMS(), b.clock()); err != nil {
			return err
		}
		h.Segments = saved
		h.LeaseID = saved[0].LeaseID
		h.HoldArmed = true
		h.HeldUnits = total
		b.installPoolOutcome(h, user)
		return nil
	}
	leases, err := b.fundingPool(ctx, user)
	if err != nil {
		return err
	}
	var total int64
	for _, l := range leases {
		total, err = AddUnits(total, l.RemainingUnits())
		if err != nil {
			return err
		}
	}
	if total < units {
		missing := units - total
		if len(leases) == 0 {
			budget := units
			if b.cfg.LeaseBudgetUnits > budget {
				budget = b.cfg.LeaseBudgetUnits
			}
			request := canonicalWalletEnsureRequest{PlatformUserID: user, Currency: "USD", Purpose: "authorize", USDWalletPolicyVersion: config.CanonicalUSDWalletPolicyVersion, MinHeadroom: newCanonicalWalletAmountObject(units), RequestedBudget: newCanonicalWalletAmountObject(budget), RequestedTTLSeconds: b.cfg.LeaseTTLSeconds, CallerSlotTTLSeconds: b.callerSlotTTLSeconds}
			var result *canonicalWalletEnsureResult
			result, err = b.control.EnsureLease(ctx, request)
			if err == nil {
				if result == nil || result.USDWalletPolicyVersion != config.CanonicalUSDWalletPolicyVersion || result.Lease.PlatformUserID != user || result.Lease.RemainingUnits() < units || b.leaseExpiredAt(&result.Lease, b.clock()) {
					return errors.New("wallet pool funding proof mismatch")
				}
				result.Lease.RetainUntil = result.Lease.ExpiresAt.Add(time.Duration(b.callerSlotTTLSeconds) * time.Second)
				err = b.store.InstallCanonicalWalletLease(ctx, result.Lease)
			}

		} else {
			target := leases[0]
			minimum, e := AddUnits(target.BudgetUnits, missing)
			if e != nil {
				return e
			}
			minHeadroom, e := AddUnits(target.RemainingUnits(), missing)
			if e != nil {
				return e
			}
			request := canonicalWalletEnsureRequest{PlatformUserID: user, Currency: "USD", Purpose: "authorize", USDWalletPolicyVersion: config.CanonicalUSDWalletPolicyVersion, TopUpLeaseID: target.LeaseID, PreferLeaseID: target.LeaseID, MinimumBudgetUnits: strconv.FormatInt(minimum, 10), MinHeadroom: newCanonicalWalletAmountObject(minHeadroom), RequestedBudget: newCanonicalWalletAmountObject(minimum), RequestedTTLSeconds: b.cfg.LeaseTTLSeconds, CallerSlotTTLSeconds: b.callerSlotTTLSeconds}
			var result *canonicalWalletEnsureResult
			result, err = b.control.EnsureLease(ctx, request)
			if err == nil {
				if result == nil || result.USDWalletPolicyVersion != config.CanonicalUSDWalletPolicyVersion || result.Lease.LeaseID != target.LeaseID || result.Lease.BudgetUnits < minimum || result.Lease.ExpiresAt.UnixMilli() > target.ExpiresAt.UnixMilli() {
					return errors.New("wallet pool top-up proof mismatch")
				}
				result.Lease.RequireCachedLease = true
				err = b.store.InstallCanonicalWalletLease(ctx, result.Lease)
			}
		}
		if err != nil {
			return err
		}
		leases, err = b.fundingPool(ctx, user)
		if err != nil {
			return err
		}
	}
	segments := []AuthorizationSegment{}
	remaining := units
	for _, l := range leases {
		free := l.RemainingUnits()
		if free <= 0 {
			continue
		}
		if free > remaining {
			free = remaining
		}
		auth := h.ID
		if len(segments) > 0 {
			auth, err = newAuthorizationID()
			if err != nil {
				return err
			}
		}
		segments = append(segments, AuthorizationSegment{AuthorizationID: auth, LeaseID: l.LeaseID, HeldUnits: free, Basis: l, PinState: "none", Kind: h.AttemptKind, State: "prepared", EventID: CanonicalWalletSettlementEventID(h.ID+":"+auth, user, "USD")})
		remaining -= free
		if remaining == 0 {
			break
		}
	}
	if remaining != 0 {
		return ErrCanonicalWalletBalanceShortfall
	}
	// This committed plan owns every hold even if the process dies after Lua.
	if err = b.saveAuthorizationPlan(ctx, h.ID, user, h.SnapshotID, h.AttemptKind, segments); err != nil {
		return err
	}
	if err = store.ArmCanonicalWalletPool(ctx, user, segments, b.graceMS(), b.clock()); err != nil {
		return err
	}
	h.Segments = segments
	h.LeaseID = segments[0].LeaseID
	h.HoldArmed = true
	h.HeldUnits = units
	b.installPoolOutcome(h, user)
	return nil
}
func (b *CanonicalWalletBridge) installPoolOutcome(h *AuthorizationHandle, user string) {
	h.onOutcome = func(token string, outcome AuthorizationOutcome, err error) {
		for _, s := range h.Segments {
			b.holdOutcome(user, s.AuthorizationID)(token, outcome, err)
		}
	}
}

// splitSettlement allocates actual usage FIFO, releasing every unused share.
// All positive events enter one transaction, retaining one total usage record.
func (b *CanonicalWalletBridge) observePoolSettlement(event CanonicalWalletSettlementEvent, segments []AuthorizationSegment) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(b.cfg.RequestTimeoutMS)*time.Millisecond)
	defer cancel()
	remaining := event.AmountUnits
	tx, err := b.outboxDB.BeginTx(ctx, nil)
	if err != nil {
		return false
	}
	defer tx.Rollback()
	if len(segments) == 0 {
		return false
	}
	if count, e := lockWalletAttempt(ctx, tx, segments[0].AuthorizationID); e != nil || count != len(segments) {
		return false
	}
	for i := range segments {
		segment := &segments[i]
		actual := remaining
		if actual > segment.HeldUnits {
			actual = segment.HeldUnits
		}
		remaining -= actual
		e := event
		e.AmountUnits = actual
		e.EventID = segment.EventID
		e.LeaseID = segment.LeaseID
		e.AuthorizationID = segment.AuthorizationID
		if segment.Payload != nil {
			if segment.Payload.AmountUnits != actual || segment.Payload.GatewayRequestID != event.GatewayRequestID {
				return false
			}
			e = *segment.Payload
			e.AuthorizationID = segment.AuthorizationID
			e.BillingSnapshotID = event.BillingSnapshotID
			e.AuthorizationToken = event.AuthorizationToken
		}
		state := "settling"
		if actual == 0 {
			state = "released"
		}
		raw, marshalErr := json.Marshal(e)
		if marshalErr != nil {
			return false
		}
		res, e2 := tx.ExecContext(ctx, `UPDATE wallet_authorization_segment SET actual_units=$2,state=$3,settlement_payload=$4::jsonb,updated_at=now() WHERE authorization_id=$1 AND state IN ('held','indeterminate','settling','released') AND (state<>'released' OR $2::bigint=0) AND (settlement_payload IS NULL OR settlement_payload->>'amount_units'=$5)`, segment.AuthorizationID, actual, state, string(raw), strconv.FormatInt(actual, 10))
		if e2 != nil {
			slog.Warn("wallet pooled settlement plan persistence failed", "authorization_id", segment.AuthorizationID, "error", e2)
			return false
		}
		n, _ := res.RowsAffected()
		if n != 1 && segment.State != "finished" {
			return false
		}
		segment.ActualUnits = actual
		segment.State = state
		segment.Payload = &e
	}
	if remaining > 0 {
		extra := event
		extra.EventID = CanonicalWalletSettlementEventID(segments[0].AuthorizationID+":overrun", event.PlatformUserID, event.Currency)
		extra.AuthorizationID = ""
		extra.LeaseID = ""
		extra.AmountUnits = remaining
		if segments[0].Remainder != nil {
			extra = *segments[0].Remainder
			if extra.AmountUnits != remaining {
				return false
			}
		}
		raw, e := json.Marshal(extra)
		if e != nil {
			return false
		}
		if _, err = tx.ExecContext(ctx, `UPDATE wallet_authorization_segment SET remainder_payload=$2::jsonb WHERE authorization_id=$1`, segments[0].AuthorizationID, string(raw)); err != nil {
			return false
		}
		segments[0].Remainder = &extra
	}
	if err = tx.Commit(); err != nil {
		return false
	}
	if err = b.enqueuePoolSettlements(ctx, event.PlatformUserID, event.BillingSnapshotID, segments); err != nil {
		slog.Warn("wallet pooled settlement deferred", "error", err)
		return false
	}
	return event.AmountUnits > 0
}

func (b *CanonicalWalletBridge) enqueuePoolSettlements(ctx context.Context, user, snapshot string, segments []AuthorizationSegment) error {
	events := []CanonicalWalletSettlementEvent{}
	for _, segment := range segments {
		if segment.Payload == nil {
			continue
		}
		if segment.State == "finished" {
			e := *segment.Payload
			e.AuthorizationID = segment.AuthorizationID
			e.BillingSnapshotID = snapshot
			if e.AmountUnits > 0 {
				events = append(events, e)
			}
			continue
		}
		if segment.ActualUnits == 0 {
			_, err := b.store.ReleaseCanonicalWalletHold(ctx, user, segment.AuthorizationID, "released", "zero_cost")
			if err != nil && !isHoldNotArmed(err) && !errors.Is(err, ErrCanonicalWalletHoldMissing) {
				return err
			}
			continue
		}
		e := *segment.Payload
		e.AuthorizationID = segment.AuthorizationID
		e.BillingSnapshotID = snapshot
		conv, err := b.store.ConvertCanonicalWalletHold(ctx, user, segment.AuthorizationID, e.EventID, segment.ActualUnits, b.clock())
		if err != nil || (conv.Code != 0 && !(conv.Code == 7 && conv.EventID == e.EventID)) {
			return errors.New("pooled settlement conversion unavailable")
		}
		events = append(events, e)
	}
	if len(segments) > 0 && segments[0].Remainder != nil {
		extra := *segments[0].Remainder
		extra.BillingSnapshotID = snapshot
		events = append(events, extra)
	}
	tx, err := b.outboxDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, e := range events {
		if err = b.outbox.InsertOutboxEventTx(ctx, tx, e); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	for _, e := range events {
		if b.holdOutcomes != nil && e.AuthorizationID != "" {
			_ = b.holdOutcomes.MarkSettled(ctx, e.AuthorizationID, e.EventID, b.clock())
		}
	}
	return nil
}
