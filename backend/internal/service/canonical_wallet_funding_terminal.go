package service

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"
)

var errFundingTerminalPending = errors.New("original funding terminal cleanup or capture is pending")

func fundingPoolPinCounts(wire canonicalWalletLeaseWireView) (int64, int64, error) {
	if wire.ActivePinCount == nil || wire.ActiveLivePinCount == nil || *wire.ActivePinCount < 0 ||
		*wire.ActiveLivePinCount < 0 || *wire.ActiveLivePinCount > *wire.ActivePinCount || *wire.ActivePinCount > 9007199254740991 {
		return 0, 0, errors.New("canonical funding pool pin counts are missing or invalid")
	}
	return *wire.ActivePinCount, *wire.ActiveLivePinCount, nil
}

func (b *CanonicalWalletBridge) wakeFundingTerminalRecovery() {
	if b == nil || b.fundingCleanupWake == nil {
		return
	}
	select {
	case b.fundingCleanupWake <- struct{}{}:
	default:
	}
}

// Acknowledgement discovery is durable in PG233. This channel only reduces
// latency; losing the process or turning admission flags off cannot lose work.
func (b *CanonicalWalletBridge) runFundingTerminalRecovery() {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var workers sync.WaitGroup
	defer workers.Wait()
	completed := make(chan struct{}, 4)
	active := 0
	for {
		select {
		case <-b.stop:
			return
		case <-completed:
			active--
		case <-b.fundingCleanupWake:
		case <-ticker.C:
		}
		if active == 4 {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		// Rotation is committed before dispatch. A dead process's bounded claim
		// naturally becomes due again; no PG lock is held during network traffic.
		rows, err := b.outboxDB.QueryContext(ctx, `WITH due AS (
		 SELECT platform_user_id,lease_id FROM wallet_funding_terminal_work
		 WHERE applied_generation<generation AND next_attempt_at<=now()
		 ORDER BY next_attempt_at,updated_at,platform_user_id,lease_id
		 LIMIT $1 FOR UPDATE SKIP LOCKED
		) UPDATE wallet_funding_terminal_work w SET next_attempt_at=now()+interval '2 seconds',updated_at=now()
		 FROM due WHERE w.platform_user_id=due.platform_user_id AND w.lease_id=due.lease_id
		 RETURNING w.platform_user_id,w.lease_id,w.generation`, 4-active)
		type work struct {
			user, lease string
			generation  int64
		}
		batch := []work{}
		if err == nil {
			for rows.Next() {
				var entry work
				if err = rows.Scan(&entry.user, &entry.lease, &entry.generation); err != nil {
					break
				}
				batch = append(batch, entry)
			}
			if err == nil {
				err = rows.Err()
			}
			_ = rows.Close()
		}
		cancel()
		if err != nil {
			continue
		}
		for _, entry := range batch {
			active++
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer func() { completed <- struct{}{} }()
				ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
				err := b.recoverFundingTerminal(ctx, entry.user, entry.lease)
				cancel()
				markCtx, markCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
				defer markCancel()
				if err == nil {
					// A concurrent cleanup/payload handoff wins a new generation.
					// A stale worker cannot consume it or delay its next observation.
					_, _ = b.outboxDB.ExecContext(markCtx, `UPDATE wallet_funding_terminal_work
					 SET applied_generation=$3,updated_at=now() WHERE platform_user_id=$1 AND lease_id=$2 AND generation=$3`, entry.user, entry.lease, entry.generation)
				} else {
					_, _ = b.outboxDB.ExecContext(markCtx, `UPDATE wallet_funding_terminal_work
					 SET next_attempt_at=now()+interval '250 milliseconds',updated_at=now()
					 WHERE platform_user_id=$1 AND lease_id=$2 AND generation=$3`, entry.user, entry.lease, entry.generation)
					if !errors.Is(err, errFundingTerminalPending) {
						slog.Warn("wallet source terminal recovery deferred", "lease_id", entry.lease, "error", err)
					}
				}
			}()
		}
	}
}

func (b *CanonicalWalletBridge) fundingTerminalPGReady(ctx context.Context, user, leaseID string) (bool, error) {
	var ready bool
	err := b.outboxDB.QueryRowContext(ctx, `SELECT NOT EXISTS(
	 SELECT 1 FROM wallet_authorization_segment a WHERE a.platform_user_id=$1 AND a.lease_id=$2 AND (
	  a.evidence_pending OR a.fee_pending
	  OR (a.kind<>'live' AND a.funding_terminal_cleanup_at IS NULL)
	  OR NOT ((a.state='finished' AND a.pin_state='finished')
	   OR (a.expiry_intent_version=2 AND a.expiry_ack_at IS NOT NULL AND a.expiry_cleanup_at IS NOT NULL AND a.pin_state='finished'))
	  OR (a.actual_units>0 AND (a.settlement_payload IS NULL OR NOT EXISTS(
	   SELECT 1 FROM wallet_settlement_outbox o WHERE o.event_id=a.settlement_payload->>'event_id'
	    AND o.status='delivered' AND o.platform_user_id=a.platform_user_id AND o.authorization_id=a.authorization_id
	    AND o.billing_snapshot_id=a.billing_snapshot_id AND o.amount_units=(a.settlement_payload->>'amount_units')::bigint
	    AND o.currency=a.settlement_payload->>'currency' AND o.gateway_request_id=a.settlement_payload->>'gateway_request_id')))
	  OR (a.remainder_payload IS NOT NULL AND NOT EXISTS(
	   SELECT 1 FROM wallet_settlement_outbox o WHERE o.event_id=a.remainder_payload->>'event_id'
	    AND o.status='delivered' AND o.platform_user_id=a.platform_user_id AND o.billing_snapshot_id=a.billing_snapshot_id
	    AND o.amount_units=(a.remainder_payload->>'amount_units')::bigint AND o.currency=a.remainder_payload->>'currency'
	    AND o.gateway_request_id=a.remainder_payload->>'gateway_request_id'))
	 ))`, user, leaseID).Scan(&ready)
	return ready, err
}

// All original obligations must be final before irreversible close mode is
// persisted. The pool returns counts and money in one canonical D1 snapshot.
func (b *CanonicalWalletBridge) fundingTerminalCloseReady(ctx context.Context, user, leaseID string) error {
	ready, err := b.fundingTerminalPGReady(ctx, user, leaseID)
	if err != nil {
		return err
	}
	if !ready {
		return errFundingTerminalPending
	}
	lease, err := b.store.GetCanonicalWalletLeaseByID(ctx, user, leaseID)
	if err != nil {
		return err
	}
	stateStore, ok := b.store.(MediaWalletStateStore)
	if !ok {
		return errors.New("original funding terminal Redis state unavailable")
	}
	raw, err := stateStore.ReadMediaWalletState(ctx, user, []string{leaseID}, nil)
	if err != nil {
		return err
	}
	if len(raw.Leases) != 1 || !raw.Leases[0].Present || raw.Leases[0].ConsumedUnits != lease.ConsumedUnits ||
		raw.Leases[0].ReleasedUnits != lease.ReleasedUnits || raw.Leases[0].FundedUnits != lease.FundingPrincipalUnits() ||
		raw.Leases[0].ReturnedUnits != lease.ReturnedUnits || raw.Leases[0].ReturnRevision != lease.ReturnRevision ||
		raw.Leases[0].BudgetRevision != lease.BudgetRevision || !lease.FundingFrozen || !raw.Leases[0].FundingFrozen ||
		lease.ConsumedUnits < lease.ReleasedUnits {
		return errors.New("original funding terminal cache changed or missing")
	}
	for _, hold := range raw.Holds {
		if hold.LeaseID == leaseID && hold.State == "armed" {
			return errFundingTerminalPending
		}
	}
	wire, err := b.fundingSourceWire(ctx, user, leaseID)
	if err != nil {
		return err
	}
	pins, live, err := fundingPoolPinCounts(*wire)
	if err != nil {
		return err
	}
	reserved, err := parseCanonicalWalletAmountObject("terminal reserved", wire.Reserved)
	if err != nil {
		return err
	}
	captured, err := parseCanonicalWalletAmountObject("terminal captured", wire.Captured)
	if err != nil {
		return err
	}
	funded, err := parseCanonicalWalletAmountObject("terminal funded", wire.Budget)
	if err != nil {
		return err
	}
	returned, err := parseCanonicalWalletAmountObject("terminal returned", wire.Released)
	if err != nil {
		return err
	}
	if pins != 0 || live != 0 || reserved != 0 || wire.FundingFrozenAt == nil || captured != lease.ConsumedUnits-lease.ReleasedUnits ||
		funded != lease.FundingPrincipalUnits() || returned != lease.ReturnedUnits || wire.ReturnRevision != lease.ReturnRevision ||
		wire.BudgetRevision != lease.BudgetRevision || wire.FundingScope != lease.FundingScope ||
		wire.FundingOwnerID != lease.FundingOwnerID || wire.FundingIssuanceKey != lease.FundingIssuanceKey {
		return errFundingTerminalPending
	}
	return nil
}

// Terminal cleanup is recovered from the original causal proof, including a
// crash after PG finished and before Redis Unprotect. It never releases Live.
func (b *CanonicalWalletBridge) cleanupFundingTerminal(ctx context.Context, user, leaseID string) error {
	rows, err := b.outboxDB.QueryContext(ctx, `SELECT parent_authorization_id,authorization_id,billing_snapshot_id,
	 COALESCE(authorization_token,''),zero_receipt,zero_ack_at IS NOT NULL,
	 expiry_intent_version,expiry_ack_at IS NOT NULL,kind,state,pin_state,held_units,actual_units,
	 lease_basis,COALESCE(event_id,''),settlement_payload FROM wallet_authorization_segment
	 WHERE platform_user_id=$1 AND lease_id=$2 AND kind<>'live'
	 AND funding_terminal_cleanup_at IS NULL
	 AND (zero_ack_at IS NOT NULL OR (expiry_intent_version=2 AND expiry_ack_at IS NOT NULL) OR settlement_payload IS NOT NULL
	  OR (state='finished' AND pin_state='finished'))
	 ORDER BY authorization_id LIMIT 128`, user, leaseID)
	if err != nil {
		return err
	}
	type terminal struct {
		parent, snapshot, token string
		zeroRaw                 []byte
		zero, expiry            bool
		version                 int
		segment                 AuthorizationSegment
	}
	batch := []terminal{}
	for rows.Next() {
		var entry terminal
		var basis, payload []byte
		s := &entry.segment
		if err = rows.Scan(&entry.parent, &s.AuthorizationID, &entry.snapshot, &entry.token, &entry.zeroRaw, &entry.zero,
			&entry.version, &entry.expiry, &s.Kind, &s.State, &s.PinState, &s.HeldUnits, &s.ActualUnits, &basis, &s.EventID, &payload); err != nil {
			break
		}
		s.LeaseID = leaseID
		if err = json.Unmarshal(basis, &s.Basis); err != nil {
			break
		}
		if len(payload) > 0 {
			if err = json.Unmarshal(payload, &s.Payload); err != nil {
				break
			}
		}
		batch = append(batch, entry)
	}
	if err == nil {
		err = rows.Err()
	}
	_ = rows.Close()
	if err != nil {
		return err
	}
	for _, entry := range batch {
		s := entry.segment
		job, jobErr := walletExpiryGatewayJobID(ctx, b.outboxDB, entry.parent, user, entry.snapshot, s.Kind)
		if jobErr != nil {
			return jobErr
		}
		if entry.zero {
			var receipt WalletTaskPinReceipt
			if err = json.Unmarshal(entry.zeroRaw, &receipt); err != nil {
				return err
			}
			_, err = VerifyWalletTaskPinReceipt(b.cfg.Secret, WalletTaskPinReceiptExpected{GatewayJobID: job,
				AuthorizationID: s.AuthorizationID, PlatformUserID: user, LeaseID: leaseID, BillingSnapshotID: entry.snapshot,
				SettlementEventID: s.EventID, HeldUnits: s.HeldUnits, AuthorizationKind: s.Kind, AuthorizationToken: entry.token,
				Status: "released"}, receipt)
			if err != nil {
				return err
			}
			var allAck bool
			if err = b.outboxDB.QueryRowContext(ctx, `SELECT bool_and(zero_ack_at IS NOT NULL AND known_fee_units=0 AND actual_units=0 AND NOT evidence_pending AND NOT fee_pending)
			 FROM wallet_authorization_segment WHERE parent_authorization_id=$1`, entry.parent).Scan(&allAck); err != nil {
				return err
			}
			if !allAck {
				return errFundingTerminalPending
			}
			_, err = b.store.ReleaseCanonicalWalletHold(ctx, user, s.AuthorizationID, "released", "zero_cost")
			if err != nil {
				var terminal *CanonicalWalletHoldNotArmedError
				if !errors.Is(err, ErrCanonicalWalletHoldMissing) && (!errors.As(err, &terminal) || terminal.State != "released" || terminal.EventID != "") {
					return err
				}
			}
			s.State, s.PinState = "released", "finished"
			if err = b.finishPoolSegment(ctx, user, s); err != nil {
				return err
			}
		} else if entry.expiry && entry.version == 2 {
			var receiptRaw []byte
			var deadline time.Time
			var proof, legacy string
			if err = b.outboxDB.QueryRowContext(ctx, `SELECT expiry_receipt,expiry_v2_deadline,expiry_terminal_proof,COALESCE(expiry_legacy_mapping_proof,'')
			 FROM wallet_authorization_segment WHERE authorization_id=$1 AND expiry_intent_version=2 AND expiry_ack_at IS NOT NULL`, s.AuthorizationID).Scan(&receiptRaw, &deadline, &proof, &legacy); err != nil {
				return err
			}
			var receipt WalletTaskPinReceipt
			if err = json.Unmarshal(receiptRaw, &receipt); err != nil {
				return err
			}
			if _, err = VerifyWalletTaskPinReceipt(b.cfg.Secret, WalletTaskPinReceiptExpected{GatewayJobID: job,
				AuthorizationID: s.AuthorizationID, PlatformUserID: user, LeaseID: leaseID, BillingSnapshotID: entry.snapshot,
				SettlementEventID: s.EventID, HeldUnits: s.HeldUnits, AuthorizationKind: s.Kind, AuthorizationToken: entry.token,
				Status: "expired_unknown", ExpiryDeadline: deadline, ExpiryTerminalProof: proof, LegacyCompletionProof: legacy}, receipt); err != nil {
				return err
			}
			if _, err = b.RecoverImmediateWalletExpiry(ctx, entry.parent); err != nil {
				return err
			}
		} else if s.Payload != nil && s.Payload.AmountUnits > 0 && s.ActualUnits > 0 {
			var delivered bool
			if err = b.outboxDB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM wallet_settlement_outbox
			 WHERE event_id=$1 AND platform_user_id=$2 AND authorization_id=$3 AND status='delivered'
			 AND billing_snapshot_id=$4 AND amount_units=$5 AND currency=$6 AND gateway_request_id=$7)`, s.Payload.EventID, user, s.AuthorizationID, entry.snapshot, s.Payload.AmountUnits, s.Payload.Currency, s.Payload.GatewayRequestID).Scan(&delivered); err != nil {
				return err
			}
			if !delivered {
				return errFundingTerminalPending
			}
			if s.State != "finished" {
				actual := s.ActualUnits
				svc := &MediaTaskService{bridge: b, db: b.outboxDB}
				record := &mediaTaskRecord{AuthorizationKind: s.Kind, ID: job, PlatformUserID: user, SnapshotID: entry.snapshot,
					AuthorizationID: s.AuthorizationID, LeaseID: leaseID, LeaseBasis: &s.Basis, HeldUnits: s.HeldUnits,
					EventID: s.EventID, PinState: s.PinState, Status: "settling", ActualUnits: &actual}
				if err = svc.pinSingle(ctx, record, true); err != nil {
					return err
				}
			}
			if err = b.finishPoolSegment(ctx, user, s); err != nil {
				return err
			}
		} else if s.State == "finished" && s.PinState == "finished" && s.ActualUnits == 0 && (s.Payload == nil || s.Payload.AmountUnits == 0) {
			// A historical V1 finished row, or a zero share of a charged split
			// group, already completed its own financial finish. Resume retention
			// cleanup only; never synthesize signed-zero evidence or release a hold
			// from this weaker historical state.
			if err = b.finishPoolSegment(ctx, user, s); err != nil {
				return err
			}
		} else if entry.version == 0 && s.State == "released" && s.ActualUnits == 0 && s.Payload != nil && s.Payload.AmountUnits == 0 {
			// A zero share of a CHARGED split group moves no money. Only its capacity
			// hold and its D1 pin remain; end both, then the segment. Never for a share
			// under an expiry intent (its pin is expiring or expired and a plain
			// release is refused by the Worker), and never for an all-zero attempt,
			// which ends through the signed zero finalizer instead.
			var charged bool
			if err = b.outboxDB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM wallet_authorization_segment p WHERE p.parent_authorization_id=$1
			 AND (p.actual_units>0 OR p.remainder_payload IS NOT NULL OR COALESCE((p.settlement_payload->>'amount_units')::bigint,0)>0))`, entry.parent).Scan(&charged); err != nil {
				return err
			}
			if charged {
				if _, err = b.store.ReleaseCanonicalWalletHold(ctx, user, s.AuthorizationID, "released", "zero_cost"); err != nil && !isHoldNotArmed(err) && !errors.Is(err, ErrCanonicalWalletHoldMissing) {
					return err
				}
				if s.PinState != "finished" {
					// The pin's gateway job id is the trusted mapping (the task id for media).
					if err = b.protectPoolAttempt(ctx, job, user, entry.snapshot, &s, true); err != nil {
						return err
					}
				}
				if err = b.finishPoolSegment(ctx, user, s); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (b *CanonicalWalletBridge) recoverFundingTerminal(ctx context.Context, user, leaseID string) error {
	// Exact first-winner financial requests always outrank a new observation.
	if err := b.recoverFundingReturn(ctx, user, leaseID); err != nil {
		return err
	}
	if err := b.cleanupFundingTerminal(ctx, user, leaseID); err != nil {
		return err
	}
	var closed bool
	if err := b.outboxDB.QueryRowContext(ctx, `SELECT closed FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, user, leaseID).Scan(&closed); err != nil {
		return err
	}
	if closed {
		return nil
	}
	ready, err := b.fundingTerminalPGReady(ctx, user, leaseID)
	if err != nil {
		return err
	}
	if ready {
		return b.returnFrozenFunding(ctx, user, CanonicalWalletLease{LeaseID: leaseID}, "close")
	}
	// Partial returns with unchanged held backing must not manufacture endless
	// zero-money revisions. An incomplete cleanup remains durably pending.
	store, ok := b.store.(CanonicalWalletFundingStore)
	if !ok {
		return errors.New("original funding terminal freeze store unavailable")
	}
	basis, err := store.FreezeCanonicalWalletFunding(ctx, user, leaseID)
	if err != nil {
		return err
	}
	if basis == nil {
		return ErrCanonicalWalletLeaseMissing
	}
	held := int64(0)
	for _, hold := range basis.Holds {
		held, err = AddUnits(held, hold.HeldUnits)
		if err != nil {
			return err
		}
	}
	// Atomic arm already includes every held unit in C. Adding H again would
	// strand a newly released share whenever another original hold survives.
	net := basis.Lease.ConsumedUnits - basis.Lease.ReleasedUnits
	if basis.Lease.ConsumedUnits < basis.Lease.ReleasedUnits || held > net || net > basis.Lease.BudgetUnits {
		return errors.New("original funding terminal obligations do not conserve")
	}
	if basis.Lease.BudgetUnits > net {
		if err = b.returnFrozenFunding(ctx, user, basis.Lease, "partial"); err != nil {
			return err
		}
	}
	return errFundingTerminalPending
}
