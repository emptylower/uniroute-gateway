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
	ReturnRevision  int64      `json:"return_revision,omitempty"`
	FundingFrozenAt *time.Time `json:"funding_frozen_at,omitempty"`
	LeaseID         string     `json:"lease_id"`
	BudgetUnits     string     `json:"budget_units"`
	CapturedUnits   string     `json:"captured_units"`
	ReleasedUnits   string     `json:"released_units"`
	ReservedUnits   string     `json:"reserved_units"`
	Status          string     `json:"status"`
	DrainedAt       *time.Time `json:"drained_at"`
	ExpiresAt       time.Time  `json:"expires_at"`
	CaptureSeq      int64      `json:"capture_seq"`
}
type WalletAvailabilityInput struct {
	UnitVersion                      string                         `json:"unit_version"`
	USDPolicyVersion                 string                         `json:"usd_wallet_policy_version"`
	Leases                           []WalletAvailabilityLeaseInput `json:"leases"`
	CapturedEventIDs                 []string                       `json:"captured_event_ids"`
	FinancialReceiptAuthorizationIDs []string                       `json:"financial_receipt_authorization_ids,omitempty"`
}
type WalletAvailabilityLease struct {
	LeaseID       string `json:"lease_id"`
	ConsumedUnits string `json:"consumed_units"`
	ReleasedUnits string `json:"released_units"`
	FreeUnits     string `json:"free_units"`
	HeldUnits     string `json:"held_units"`
	CacheStatus   string `json:"cache_status"`
	Sealed        bool   `json:"sealed"`
	Completeness  string `json:"completeness,omitempty"`
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
	AsOf                       time.Time                      `json:"as_of"`
	Completeness               string                         `json:"completeness"`
	Leases                     []WalletAvailabilityLease      `json:"leases"`
	Obligations                []WalletAvailabilityObligation `json:"obligations"`
	CommittedFinancialReceipts []json.RawMessage              `json:"committed_financial_receipts,omitempty"`
}

func availabilityReceiptIDs(ids []string) ([]string, error) {
	if len(ids) > 128 {
		return nil, errors.New("too many financial receipt identities")
	}
	unique := make([]string, 0, len(ids))
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if id == "" || len(id) > 256 || strings.TrimSpace(id) != id {
			return nil, errors.New("invalid financial receipt identity")
		}
		if !seen[id] {
			unique = append(unique, id)
			seen[id] = true
		}
	}
	return unique, nil
}

// Only an ACKed original Worker receipt is returned. Neither an intent nor a
// terminal state without its signed receipt can authorize a D1 reread.
func committedAvailabilityReceipts(ctx context.Context, tx *sql.Tx, platformUser string, ids []string) ([]json.RawMessage, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	args := make([]any, 1, len(ids)+1)
	args[0] = platformUser
	placeholders := make([]string, len(ids))
	for i, id := range ids {
		args = append(args, id)
		placeholders[i] = "$" + strconv.Itoa(i+2)
	}
	rows, err := tx.QueryContext(ctx, `SELECT authorization_id,CASE WHEN zero_ack_at IS NOT NULL THEN zero_receipt ELSE expiry_receipt END FROM wallet_authorization_segment WHERE platform_user_id=$1 AND authorization_id IN (`+strings.Join(placeholders, ",")+`) AND ((zero_ack_at IS NOT NULL AND zero_receipt IS NOT NULL) OR (expiry_intent_version=2 AND expiry_ack_at IS NOT NULL AND expiry_receipt IS NOT NULL)) ORDER BY authorization_id`, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	receipts := make([]json.RawMessage, 0, len(ids))
	for rows.Next() {
		var authorization string
		var raw []byte
		if err = rows.Scan(&authorization, &raw); err != nil {
			return nil, err
		}
		var receipt WalletTaskPinReceipt
		if json.Unmarshal(raw, &receipt) != nil || receipt.AuthorizationID != authorization || receipt.PlatformUserID != platformUser || !walletProofHex(receipt.ReceiptSignature) || (receipt.Status != "released" && (receipt.Status != "expired_unknown" || receipt.ExpiryVersion != 2)) {
			return nil, errors.New("invalid committed financial receipt")
		}
		receipts = append(receipts, append(json.RawMessage(nil), raw...))
	}
	return receipts, rows.Err()
}

func availabilityLeaseFingerprint(state MediaWalletRawState, leaseID string) string {
	view := struct {
		Lease        MediaWalletRawLease
		Holds        map[string]CanonicalWalletHold
		Reservations map[string]string
	}{Holds: map[string]CanonicalWalletHold{}, Reservations: map[string]string{}}
	for _, lease := range state.Leases {
		if lease.LeaseID == leaseID {
			view.Lease = lease
			break
		}
	}
	for _, hold := range state.Holds {
		if hold.LeaseID == leaseID {
			view.Holds[hold.AuthorizationID] = hold
		}
	}
	for event, lease := range state.Reservations {
		if lease == leaseID {
			view.Reservations[event] = lease
		}
	}
	raw, _ := json.Marshal(view)
	return string(raw)
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
	receiptIDs, err := availabilityReceiptIDs(in.FinancialReceiptAuthorizationIDs)
	if err != nil {
		return out, infraerrors.BadRequest("INVALID_WALLET_SNAPSHOT", err.Error())
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
		if l.LeaseID == "" || seenLease[l.LeaseID] || l.CaptureSeq < 0 || l.ReturnRevision < 0 || (l.FundingFrozenAt != nil && l.FundingFrozenAt.IsZero()) {
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
	out.CommittedFinancialReceipts, err = committedAvailabilityReceipts(ctx, tx, user.PlatformUserID, receiptIDs)
	if err != nil {
		return out, nil
	}
	// The primary funding tombstone also fences stale Redis restores and the
	// intent-before-freeze window. Its existence never raises available money.
	type fundingProof struct{ funded, returned, revision int64 }
	funding := map[string]fundingProof{}
	if len(ids) > 0 {
		requested, marshalErr := json.Marshal(ids)
		if marshalErr != nil {
			return out, nil
		}
		rows, queryErr := tx.QueryContext(ctx, `SELECT lease_id,funded_units,returned_units,return_revision FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id IN (SELECT jsonb_array_elements_text($2::jsonb))`, user.PlatformUserID, string(requested))
		if queryErr != nil {
			return out, nil
		}
		for rows.Next() {
			var id string
			var proof fundingProof
			if rows.Scan(&id, &proof.funded, &proof.returned, &proof.revision) != nil {
				_ = rows.Close()
				return out, nil
			}
			funding[id] = proof
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return out, nil
		}
	}
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
	rows, err := tx.QueryContext(ctx, `SELECT a.authorization_id,COALESCE(a.event_id,''),a.lease_id,a.held_units,COALESCE(o.status,'') FROM gateway_media_task m JOIN wallet_authorization_segment a ON a.parent_authorization_id=m.authorization_id LEFT JOIN wallet_settlement_outbox o ON o.event_id=a.event_id WHERE m.platform_user_id=$1 AND m.pin_state<>'finished' AND a.expiry_ack_at IS NULL AND a.zero_ack_at IS NULL`, user.PlatformUserID)
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
	rows, err = tx.QueryContext(ctx, `SELECT a.authorization_id,COALESCE(a.event_id,''),COALESCE(NULLIF(o.lease_id,''),a.lease_id),CASE WHEN a.state='settling' OR a.expiry_ack_at IS NOT NULL THEN a.actual_units ELSE a.held_units END,COALESCE(o.status,'') FROM wallet_authorization_segment a LEFT JOIN wallet_settlement_outbox o ON o.event_id=a.event_id WHERE a.platform_user_id=$1 AND a.kind<>'media' AND a.state NOT IN ('finished','released','expired_unknown') AND (a.expiry_ack_at IS NULL OR a.actual_units>0)`, user.PlatformUserID)
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
	rows, err = tx.QueryContext(ctx, `SELECT authorization_id,lease_id,held_units FROM wallet_hold_outcome h WHERE platform_user_id=$1 AND resolution IS NULL AND NOT EXISTS(SELECT 1 FROM wallet_authorization_segment a WHERE a.authorization_id=h.authorization_id AND (a.expiry_ack_at IS NOT NULL OR a.zero_ack_at IS NOT NULL))`, user.PlatformUserID)
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
		leaseComplete := true
		proof, frozen := funding[l.LeaseID]
		view := WalletAvailabilityLease{LeaseID: l.LeaseID, ConsumedUnits: strconv.FormatInt(raw.ConsumedUnits, 10), ReleasedUnits: strconv.FormatInt(raw.ReleasedUnits, 10), FreeUnits: "0", HeldUnits: strconv.FormatInt(heldTotals[l.LeaseID], 10), CacheStatus: "missing", Sealed: raw.Sealed || raw.FundingFrozen || frozen, Completeness: "complete"}
		if !raw.Present {
			if (l.Status == "active" && l.DrainedAt == nil && l.ExpiresAt.After(out.AsOf)) || heldTotals[l.LeaseID] > 0 {
				leaseComplete = false
			}
		} else {
			view.CacheStatus = "present"
			budget, _ := strictAvailabilityUnits(l.BudgetUnits)
			capUnits, _ := strictAvailabilityUnits(l.CapturedUnits)
			returned, _ := strictAvailabilityUnits(l.ReleasedUnits)
			if (frozen && (proof.funded != budget || proof.returned != returned || proof.revision != l.ReturnRevision)) ||
				(l.ReturnRevision > 0 && (raw.FundedUnits != budget || raw.ReturnedUnits != returned)) ||
				raw.Recovered || returned > budget || raw.BudgetUnits != budget-returned || raw.ReturnRevision != l.ReturnRevision || raw.ReleasedUnits > raw.ConsumedUnits || raw.ConsumedUnits < 0 {
				leaseComplete = false
			}
			net := raw.ConsumedUnits - raw.ReleasedUnits
			covered, e := AddUnits(capUnits, coveredTotals[l.LeaseID])
			if e != nil || net != covered || net > raw.BudgetUnits {
				leaseComplete = false
			}
			if !frozen && !raw.Sealed && !raw.FundingFrozen && l.FundingFrozenAt == nil && l.Status == "active" && l.DrainedAt == nil && l.ExpiresAt.After(out.AsOf) && raw.ExpiresAtMS > out.AsOf.UnixMilli() && net <= raw.BudgetUnits {
				view.FreeUnits = strconv.FormatInt(raw.BudgetUnits-net, 10)
			}
		}
		view.HeldUnits = strconv.FormatInt(coveredTotals[l.LeaseID], 10)
		if !leaseComplete {
			view.Completeness = "unknown"
			complete = false
		}
		out.Leases = append(out.Leases, view)
	}
	second, err := stateStore.ReadMediaWalletState(ctx, user.PlatformUserID, ids, events)
	if err != nil || second.Fingerprint != first.Fingerprint {
		complete = false
		for i := range out.Leases {
			if err != nil || availabilityLeaseFingerprint(first, out.Leases[i].LeaseID) != availabilityLeaseFingerprint(second, out.Leases[i].LeaseID) {
				out.Leases[i].Completeness = "unknown"
			}
		}
	}
	if err = tx.Commit(); err != nil {
		complete = false
		out.CommittedFinancialReceipts = nil
		for i := range out.Leases {
			out.Leases[i].Completeness = "unknown"
		}
	}
	out.Obligations = obligations
	if complete {
		out.Completeness = "complete"
	}
	return out, nil
}
