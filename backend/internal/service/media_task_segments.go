package service

import (
	"context"
	"errors"
	"time"
)

func (s *MediaTaskService) mediaSegments(ctx context.Context, r *mediaTaskRecord, fn func(context.Context, *mediaTaskRecord) error) error {
	if len(r.Segments) == 0 {
		return fn(ctx, r)
	}
	for i := range r.Segments {
		segment := &r.Segments[i]
		leaf := *r
		leaf.Segments = nil
		leaf.AuthorizationID = segment.AuthorizationID
		leaf.LeaseID = segment.LeaseID
		leaf.LeaseBasis = &segment.Basis
		leaf.HeldUnits = segment.HeldUnits
		leaf.EventID = segment.EventID
		leaf.PinState = segment.PinState
		if r.ActualUnits != nil {
			actual := segment.ActualUnits
			leaf.ActualUnits = &actual
		}
		if err := fn(ctx, &leaf); err != nil {
			return err
		}
		segment.Basis = *leaf.LeaseBasis
		if err := s.bridge.saveAuthorizationSegments(ctx, r.AuthorizationID, r.Segments); err != nil {
			return err
		}
	}
	return nil
}
func (s *MediaTaskService) protect(ctx context.Context, r *mediaTaskRecord) error {
	return s.mediaSegments(ctx, r, func(ctx context.Context, leaf *mediaTaskRecord) error {
		if leaf.PinState == "finished" {
			return nil
		}
		return s.protectSingle(ctx, leaf)
	})
}
func (s *MediaTaskService) unprotect(ctx context.Context, r *mediaTaskRecord) error {
	return s.mediaSegments(ctx, r, s.unprotectSingle)
}
func (s *MediaTaskService) pin(ctx context.Context, r *mediaTaskRecord, finish bool) error {
	if len(r.Segments) == 0 {
		return s.pinSingle(ctx, r, finish)
	}
	for i := range r.Segments {
		segment := &r.Segments[i]
		if finish && segment.PinState == "finished" {
			continue
		}
		leaf := *r
		leaf.Segments = nil
		leaf.AuthorizationID = segment.AuthorizationID
		leaf.LeaseID = segment.LeaseID
		leaf.LeaseBasis = &segment.Basis
		leaf.HeldUnits = segment.HeldUnits
		leaf.EventID = segment.EventID
		leaf.PinState = segment.PinState
		if r.ActualUnits != nil {
			actual := segment.ActualUnits
			leaf.ActualUnits = &actual
		}
		if err := s.pinSingle(ctx, &leaf, finish); err != nil {
			return err
		}
		segment.Basis = *leaf.LeaseBasis
		segment.PinState = "active"
		if finish {
			segment.PinState = "finished"
		}
		// Each accepted pin is recorded before attempting the next share. A lost
		// response still retries exactly that authorization, never provider create.
		if err := s.bridge.saveAuthorizationSegments(ctx, r.AuthorizationID, r.Segments); err != nil {
			return err
		}
	}
	return nil
}
func (s *MediaTaskService) mediaAuthorization(ctx context.Context, r *mediaTaskRecord, snapshot *BillingSnapshot, user *User) (*AuthorizationHandle, error) {
	if len(r.Segments) > 0 {
		for _, segment := range r.Segments {
			_, err := s.bridge.store.GetCanonicalWalletHold(ctx, r.PlatformUserID, segment.AuthorizationID)
			if errors.Is(err, ErrCanonicalWalletHoldMissing) {
				if err = s.protect(ctx, r); err != nil {
					return nil, err
				}
				break
			}
			if err != nil {
				return nil, err
			}
		}
	}
	handle, err := s.authorizer.Authorize(ctx, AuthorizeInput{Snapshot: snapshot, User: user, FixedEstimateUnits: r.QuotedUnits, DurableAuthorizationID: r.AuthorizationID})
	if err != nil {
		return nil, err
	}
	if handle == nil || !handle.HoldArmed || handle.HeldUnits != r.QuotedUnits {
		return nil, errors.New("media authorization did not arm its exact quote")
	}
	r.Segments = handle.Segments
	r.LeaseID = handle.LeaseID
	r.HeldUnits = handle.HeldUnits
	if len(r.Segments) == 0 {
		return nil, errors.New("media authorization requires durable funding segments")
	}
	for i := range r.Segments {
		segment := &r.Segments[i]
		segment.EventID = r.EventID
		if i > 0 {
			segment.EventID = CanonicalWalletSettlementEventID(r.ID+":"+segment.AuthorizationID, r.PlatformUserID, "USD")
		}
		basis, e := s.bridge.store.GetCanonicalWalletLeaseByID(ctx, r.PlatformUserID, segment.LeaseID)
		if e != nil {
			return nil, e
		}
		segment.Basis = *basis
	}
	r.LeaseBasis = &r.Segments[0].Basis
	if err = s.bridge.saveAuthorizationSegments(ctx, r.AuthorizationID, r.Segments); err != nil {
		return nil, err
	}
	return handle, nil
}
func (s *MediaTaskService) releaseMedia(ctx context.Context, r *mediaTaskRecord) error {
	return s.mediaSegments(ctx, r, func(ctx context.Context, leaf *mediaTaskRecord) error {
		if leaf.PinState == "finished" {
			return nil
		}
		_, err := s.bridge.store.GetCanonicalWalletLeaseByID(ctx, leaf.PlatformUserID, leaf.LeaseID)
		if errors.Is(err, ErrCanonicalWalletLeaseMissing) {
			// The durable releasing state proves zero usage; a missing cache
			// cannot expose these funds. Do not resurrect a lease from old
			// principal merely to release it. Canonical finish still must ACK.
			return nil
		}
		if err != nil {
			return err
		}
		if _, err = s.bridge.store.GetCanonicalWalletHold(ctx, leaf.PlatformUserID, leaf.AuthorizationID); errors.Is(err, ErrCanonicalWalletHoldMissing) {
			if err = s.protectSingle(ctx, leaf); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		_, err = s.bridge.store.ReleaseCanonicalWalletHold(ctx, leaf.PlatformUserID, leaf.AuthorizationID, "released", "zero_cost")
		if isHoldNotArmed(err) {
			return nil
		}
		return err
	})
}
func (s *MediaTaskService) mediaSettled(ctx context.Context, r *mediaTaskRecord) (bool, bool, error) {
	segments := r.Segments
	if len(segments) == 0 {
		segments = []AuthorizationSegment{{AuthorizationID: r.AuthorizationID, LeaseID: r.LeaseID, EventID: r.EventID, ActualUnits: *r.ActualUnits}}
	}
	all := true
	dead := false
	for _, segment := range segments {
		var status, lease, auth, snapshot string
		var amount int64
		err := s.db.QueryRowContext(ctx, `SELECT status,amount_units,COALESCE(lease_id,''),COALESCE(authorization_id,''),COALESCE(billing_snapshot_id,'') FROM wallet_settlement_outbox WHERE event_id=$1`, segment.EventID).Scan(&status, &amount, &lease, &auth, &snapshot)
		if err != nil {
			return false, false, err
		}
		if amount != segment.ActualUnits || lease != segment.LeaseID || auth != segment.AuthorizationID || snapshot != r.SnapshotID {
			return false, false, errors.New("media settlement acknowledgement mismatch")
		}
		if status == "dead_letter" {
			dead = true
		}
		if status != "delivered" {
			all = false
		}
	}
	return all, dead, nil
}
func (s *MediaTaskService) convertMedia(ctx context.Context, r *mediaTaskRecord) error {
	return s.mediaSegments(ctx, r, func(ctx context.Context, leaf *mediaTaskRecord) error {
		if leaf.PinState == "finished" {
			return nil
		}
		conv, err := s.bridge.store.ConvertCanonicalWalletHold(ctx, leaf.PlatformUserID, leaf.AuthorizationID, leaf.EventID, *leaf.ActualUnits, time.Now())
		if err != nil {
			return err
		}
		if conv.LeaseID != leaf.LeaseID || (conv.Code != 0 && !(conv.Code == 7 && conv.EventID == leaf.EventID)) {
			return errors.New("media hold conversion failed")
		}
		return nil
	})
}
