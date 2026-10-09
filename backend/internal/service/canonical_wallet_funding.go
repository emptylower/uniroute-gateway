package service

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

type mediaFundingOwnerContextKey struct{}

// Only the persisted media task service supplies this identity. The funding
// planner rechecks its user, parent authorization, snapshot and frozen quote.
func WithMediaFundingOwner(ctx context.Context, taskID string) context.Context {
	return context.WithValue(ctx, mediaFundingOwnerContextKey{}, taskID)
}

func walletFundingScopeForPurpose(purpose canonicalWalletLeasePurpose) string {
	if purpose == canonicalWalletLeasePurposeSettle {
		return "settle"
	}
	return "llm"
}

func walletFundingMatches(scope, owner, key string, request canonicalWalletEnsureRequest) bool {
	if request.FundingScope == "" {
		return scope == "" || scope == "legacy"
	}
	if request.FundingScope == "llm" && (scope == "" || scope == "legacy") {
		return true
	}
	if request.FundingScope == "settle" && (scope == "" || scope == "legacy") {
		return owner == "" && key == "" && request.FundingOwnerID == "" && request.FundingIssuanceKey == ""
	}
	return scope == request.FundingScope && owner == request.FundingOwnerID && key == request.FundingIssuanceKey
}

func (l CanonicalWalletLease) FundingPrincipalUnits() int64 {
	if l.FundedUnits > 0 {
		return l.FundedUnits
	}
	return l.BudgetUnits + l.ReturnedUnits
}

type CanonicalWalletFundingBasis struct {
	Lease CanonicalWalletLease  `json:"lease"`
	Holds []CanonicalWalletHold `json:"holds"`
}

type WalletFundingReturnSource struct {
	AllocationID   string `json:"allocation_id"`
	CreditID       string `json:"credit_id"`
	ReturnedUnits  string `json:"returned_units"`
	CreditedUnits  string `json:"credited_units"`
	WriteOffReason string `json:"write_off_reason"`
}

type WalletFundingReturnReceipt struct {
	Protocol             string                      `json:"protocol"`
	ReceiptID            string                      `json:"receipt_id"`
	PlatformUserID       string                      `json:"platform_user_id"`
	LeaseID              string                      `json:"lease_id"`
	FundingScope         string                      `json:"funding_scope"`
	FundingOwnerID       string                      `json:"funding_owner_id"`
	FundingIssuanceKey   string                      `json:"funding_issuance_key"`
	ReturnRevision       int64                       `json:"return_revision"`
	BudgetRevision       int64                       `json:"budget_revision"`
	CaptureSeq           int64                       `json:"capture_seq"`
	FundedUnits          string                      `json:"funded_units"`
	CapturedUnits        string                      `json:"captured_units"`
	ReturnedBeforeUnits  string                      `json:"returned_before_units"`
	ReturnedUnits        string                      `json:"returned_units"`
	ReturnedAfterUnits   string                      `json:"returned_after_units"`
	CreditedUnits        string                      `json:"credited_units"`
	WriteOffUnits        string                      `json:"write_off_units"`
	HeldUnits            string                      `json:"held_units"`
	GatewayConsumedUnits string                      `json:"gateway_consumed_units"`
	GatewayReleasedUnits string                      `json:"gateway_released_units"`
	Mode                 string                      `json:"mode"`
	CommittedAt          string                      `json:"committed_at"`
	Sources              []WalletFundingReturnSource `json:"sources"`
	Signature            string                      `json:"signature"`
}

type CanonicalWalletFundingStore interface {
	FreezeCanonicalWalletFunding(context.Context, string, string) (*CanonicalWalletFundingBasis, error)
	ApplyCanonicalWalletFundingReturn(context.Context, WalletFundingReturnReceipt) error
}

func fundingReturnSigningPayload(r WalletFundingReturnReceipt) ([]byte, error) {
	sources, err := json.Marshal(r.Sources)
	if err != nil {
		return nil, err
	}
	// JS JSON.stringify and Go's default HTML escaping differ for unusual IDs;
	// these identities have the wallet ASCII pattern, and protocol values are fixed.
	return json.Marshal([]string{r.Protocol, r.ReceiptID, r.PlatformUserID, r.LeaseID, r.FundingScope, r.FundingOwnerID, r.FundingIssuanceKey,
		strconv.FormatInt(r.ReturnRevision, 10), strconv.FormatInt(r.BudgetRevision, 10), strconv.FormatInt(r.CaptureSeq, 10),
		r.FundedUnits, r.CapturedUnits, r.ReturnedBeforeUnits, r.ReturnedUnits, r.ReturnedAfterUnits, r.CreditedUnits, r.WriteOffUnits,
		r.HeldUnits, r.GatewayConsumedUnits, r.GatewayReleasedUnits, r.Mode, r.CommittedAt, string(sources)})
}

func validateFundingReturnReceipt(secret string, r WalletFundingReturnReceipt) ([]int64, error) {
	raw, err := json.Marshal(r)
	if err != nil || len(raw) > walletFundingResponseLimit-16384 {
		return nil, errors.New("funding return receipt exceeds bounded protocol body")
	}
	if len(secret) < 32 || r.Protocol != "wallet-funding-return-v1" || r.PlatformUserID == "" || r.LeaseID == "" ||
		r.ReceiptID == "" || r.ReturnRevision <= 0 || r.BudgetRevision < 0 || r.CaptureSeq < 0 || (r.Mode != "partial" && r.Mode != "close") || len(r.Sources) > walletFundingMaxSources {
		return nil, errors.New("invalid signed funding return identity")
	}
	payload, err := fundingReturnSigningPayload(r)
	if err != nil {
		return nil, err
	}
	signature, err := hex.DecodeString(r.Signature)
	if err != nil || len(signature) != sha256.Size {
		return nil, errors.New("invalid funding signature")
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(payload)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return nil, errors.New("invalid funding signature")
	}
	values := []string{r.FundedUnits, r.CapturedUnits, r.ReturnedBeforeUnits, r.ReturnedUnits, r.ReturnedAfterUnits, r.CreditedUnits, r.WriteOffUnits, r.HeldUnits, r.GatewayConsumedUnits, r.GatewayReleasedUnits}
	units := make([]int64, len(values))
	for i, value := range values {
		units[i], err = parseCanonicalWalletAmountObject("funding receipt", newCanonicalWalletAmountObjectString(value))
		if err != nil {
			return nil, err
		}
	}
	if units[2] > units[0] || units[3] > units[0]-units[2] || units[4] != units[2]+units[3] ||
		units[5] > units[3] || units[6] != units[3]-units[5] || units[8] < units[9] ||
		units[1] > units[0]-units[4] || units[7] != units[0]-units[4]-units[1] || units[8]-units[9] != units[0]-units[4] || (r.Mode == "close" && units[7] != 0) {
		return nil, errors.New("signed funding return does not conserve exact obligations")
	}
	returned, credited := int64(0), int64(0)
	seen := map[string]bool{}
	for _, source := range r.Sources {
		if source.AllocationID == "" || len(source.AllocationID) > 128 || source.CreditID == "" || len(source.CreditID) > 128 || seen[source.AllocationID] {
			return nil, errors.New("invalid original funding allocation")
		}
		seen[source.AllocationID] = true
		amount, e := parseCanonicalWalletAmountObject("source returned", newCanonicalWalletAmountObjectString(source.ReturnedUnits))
		if e != nil {
			return nil, e
		}
		credit, e := parseCanonicalWalletAmountObject("source credited", newCanonicalWalletAmountObjectString(source.CreditedUnits))
		if e != nil {
			return nil, e
		}
		if amount <= 0 || credit > amount || (source.WriteOffReason == "" && credit != amount) || (source.WriteOffReason != "" && (credit != 0 || (source.WriteOffReason != "source_grant_expired" && source.WriteOffReason != "source_grant_revoked"))) {
			return nil, errors.New("invalid source return")
		}
		returned, e = AddUnits(returned, amount)
		if e != nil {
			return nil, e
		}
		credited, e = AddUnits(credited, credit)
		if e != nil {
			return nil, e
		}
	}
	if returned != units[3] || credited != units[5] {
		return nil, errors.New("source returns differ from signed total")
	}
	committed, err := time.Parse(time.RFC3339Nano, r.CommittedAt)
	if err != nil || committed.IsZero() {
		return nil, errors.New("invalid committed funding time")
	}
	return units, nil
}

func verifyFundingReturnReceipt(secret string, basis CanonicalWalletFundingBasis, mode string, r WalletFundingReturnReceipt) error {
	units, err := validateFundingReturnReceipt(secret, r)
	if err != nil {
		return err
	}
	if r.PlatformUserID != basis.Lease.PlatformUserID || r.LeaseID != basis.Lease.LeaseID ||
		r.Mode != mode || r.ReturnRevision != basis.Lease.ReturnRevision+1 || r.BudgetRevision != basis.Lease.BudgetRevision ||
		r.FundingScope != basis.Lease.FundingScope || r.FundingOwnerID != basis.Lease.FundingOwnerID || r.FundingIssuanceKey != basis.Lease.FundingIssuanceKey {
		return errors.New("signed funding return identity mismatch")
	}
	held := int64(0)
	for _, hold := range basis.Holds {
		held, err = AddUnits(held, hold.HeldUnits)
		if err != nil {
			return err
		}
	}
	if units[0] != basis.Lease.FundingPrincipalUnits() || units[2] != basis.Lease.ReturnedUnits || units[8] != basis.Lease.ConsumedUnits || units[9] != basis.Lease.ReleasedUnits || units[7] != held {
		return errors.New("signed funding return differs from frozen obligations")
	}
	return nil
}

func newCanonicalWalletAmountObjectString(units string) canonicalWalletAmountObject {
	return canonicalWalletAmountObject{AmountUnits: units, Currency: "USD", Scale: 8, UnitVersion: CanonicalWalletUnitVersion}
}

// The primary PG registry survives losing the Redis lease hash/tombstone.
// Installing a stale pre-return snapshot is prohibited even during flag rollback.
func (b *CanonicalWalletBridge) applyFundingRegistry(ctx context.Context, lease *CanonicalWalletLease) error {
	if b.outboxDB != nil {
		var returned, revision, funded int64
		var scope, owner, key string
		err := b.outboxDB.QueryRowContext(ctx, `SELECT returned_units,return_revision,funded_units,funding_scope,funding_owner_id,funding_issuance_key FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, lease.PlatformUserID, lease.LeaseID).Scan(&returned, &revision, &funded, &scope, &owner, &key)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil {
			if lease.ReturnRevision != revision || lease.ReturnedUnits != returned || lease.FundingPrincipalUnits() != funded ||
				lease.FundingScope != scope || lease.FundingOwnerID != owner || lease.FundingIssuanceKey != key {
				return errors.New("stale lease funding snapshot")
			}
			lease.FundingFrozen = true
		}
	}
	return nil
}

func (b *CanonicalWalletBridge) installFundingLease(ctx context.Context, lease CanonicalWalletLease) error {
	if err := b.applyFundingRegistry(ctx, &lease); err != nil {
		return err
	}
	return b.store.InstallCanonicalWalletLease(ctx, lease)
}

// Only lifecycle RPCs take this gate. Ordinary cached authorization, provider
// writes and recovery of another lease never acquire a hot per-user lock.
func (b *CanonicalWalletBridge) lockFundingLifecycle(ctx context.Context, user string) (*sql.Tx, error) {
	tx, err := b.outboxDB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "wallet-funding-lifecycle-v1:"+user); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return tx, nil
}

func (b *CanonicalWalletBridge) ensureFundingLifecycle(ctx context.Context, request canonicalWalletEnsureRequest) (*canonicalWalletEnsureResult, error) {
	_, funding := b.store.(CanonicalWalletFundingStore)
	if _, actual := b.control.(*canonicalWalletHTTPClient); !actual || b.outboxDB == nil || !funding {
		return b.control.EnsureLease(ctx, request)
	}
	tx, err := b.lockFundingLifecycle(ctx, request.PlatformUserID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	// A request selected before the gate cannot top up or drain a source whose
	// immutable principal has since been frozen. Other leases remain issuable.
	ids := []string{request.TopUpLeaseID, request.PreferLeaseID}
	for _, drain := range request.Drained {
		ids = append(ids, drain.LeaseID)
	}
	for _, id := range ids {
		if id == "" {
			continue
		}
		var frozen bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2 UNION ALL SELECT 1 FROM wallet_funding_source_intent WHERE platform_user_id=$1 AND lease_id=$2)`, request.PlatformUserID, id).Scan(&frozen); err != nil {
			return nil, err
		}
		if !frozen {
			continue
		}
		if id == request.TopUpLeaseID {
			return nil, ErrCanonicalWalletLeaseContention
		}
		for _, drain := range request.Drained {
			if drain.LeaseID == id {
				return nil, ErrCanonicalWalletLeaseContention
			}
		}
		if id == request.PreferLeaseID {
			request.PreferLeaseID = ""
		}
	}
	result, err := b.control.EnsureLease(ctx, request)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

// Refresh the exact D1 source while all cooperating ensure/top-up RPCs are
// excluded. Preserve Redis C/R and holds; a canonical pool response cannot
// reconstruct missing reservation state.
func (b *CanonicalWalletBridge) fundingSourceWire(ctx context.Context, user, leaseID string) (*canonicalWalletLeaseWireView, error) {
	client, ok := b.control.(*canonicalWalletHTTPClient)
	if !ok {
		return nil, errors.New("signed wallet funding control unavailable")
	}
	var response struct {
		Leases []canonicalWalletLeaseWireView `json:"leases"`
	}
	if err := client.doJSON(ctx, "POST", "/api/internal/v2/wallet/leases/pool", "wallet:lease", "", map[string]string{"platform_user_id": user, "usd_wallet_policy_version": config.CanonicalUSDWalletPolicyVersion, "funding_scope": "all"}, &response); err != nil {
		return nil, err
	}
	if len(response.Leases) > 128 {
		return nil, errors.New("funding source pool exceeds bound")
	}
	var source *canonicalWalletLeaseWireView
	for _, wire := range response.Leases {
		if wire.LeaseID != leaseID {
			continue
		}
		if source != nil || wire.PlatformUserID != user || wire.Status != "active" || wire.Currency != "USD" || wire.UnitVersion != CanonicalWalletUnitVersion || wire.Scale != 8 || wire.ExpiresAt.IsZero() || wire.ReturnRevision < 0 || wire.BudgetRevision < 0 || wire.CaptureSeq < 0 {
			return nil, errors.New("invalid canonical funding source")
		}
		source = &wire
	}
	if source == nil {
		return nil, ErrCanonicalWalletLeaseMissing
	}
	return source, nil
}

func (b *CanonicalWalletBridge) freshFundingSource(ctx context.Context, user, leaseID string) (*CanonicalWalletLease, error) {
	wire, err := b.fundingSourceWire(ctx, user, leaseID)
	if err != nil {
		return nil, err
	}
	funded, err := parseCanonicalWalletAmountObject("source funded", wire.Budget)
	if err != nil {
		return nil, err
	}
	returned, err := parseCanonicalWalletAmountObject("source returned", wire.Released)
	if err != nil {
		return nil, err
	}
	captured, err := parseCanonicalWalletAmountObject("source captured", wire.Captured)
	if err != nil {
		return nil, err
	}
	if funded <= 0 || returned > funded || captured > funded-returned {
		return nil, errors.New("invalid canonical funding conservation")
	}
	lease, err := b.store.GetCanonicalWalletLeaseByID(ctx, user, leaseID)
	if err != nil {
		return nil, err
	}
	if lease.Sealed && !lease.FundingFrozen {
		// A legacy drain may have replaced C with its budget. That is not
		// an exact source-return observation and must not become an intent.
		return nil, ErrCanonicalWalletLeaseContention
	}
	if lease.FundingScope != wire.FundingScope || lease.FundingOwnerID != wire.FundingOwnerID || lease.FundingIssuanceKey != wire.FundingIssuanceKey || lease.FundingPrincipalUnits() > funded || lease.ReturnedUnits > returned || lease.ReturnRevision > wire.ReturnRevision || lease.BudgetRevision > wire.BudgetRevision || lease.ConsumedUnits-lease.ReleasedUnits < captured {
		return nil, errors.New("canonical funding source identity or revision changed")
	}
	lease.FundedUnits, lease.ReturnedUnits, lease.BudgetUnits = funded, returned, funded-returned
	lease.ReturnRevision, lease.BudgetRevision = wire.ReturnRevision, wire.BudgetRevision
	lease.FundingFrozen = lease.FundingFrozen || wire.FundingFrozenAt != nil
	lease.RequireCachedLease = true
	if err = b.installFundingLease(ctx, *lease); err != nil {
		return nil, err
	}
	return b.store.GetCanonicalWalletLeaseByID(ctx, user, leaseID)
}

const walletFundingMaxSources = 10000
const walletFundingResponseLimit = 8 << 20

type WalletFundingSource struct {
	AllocationID   string `json:"allocation_id"`
	CreditID       string `json:"credit_id"`
	AllocatedUnits string `json:"allocated_units"`
}
type WalletFundingSourceReceipt struct {
	Protocol           string                `json:"protocol"`
	FreezeID           string                `json:"freeze_id"`
	PlatformUserID     string                `json:"platform_user_id"`
	LeaseID            string                `json:"lease_id"`
	FundingScope       string                `json:"funding_scope"`
	FundingOwnerID     string                `json:"funding_owner_id"`
	FundingIssuanceKey string                `json:"funding_issuance_key"`
	FundedUnits        string                `json:"funded_units"`
	BudgetRevision     int64                 `json:"budget_revision"`
	CommittedAt        string                `json:"committed_at"`
	Sources            []WalletFundingSource `json:"sources"`
	Signature          string                `json:"signature"`
}
type walletFundingSourceRequest struct {
	PlatformUserID         string                      `json:"platform_user_id"`
	LeaseID                string                      `json:"lease_id"`
	FreezeID               string                      `json:"freeze_id"`
	ExpectedFunded         canonicalWalletAmountObject `json:"expected_funded"`
	ExpectedBudgetRevision int64                       `json:"expected_budget_revision"`
	FundingScope           string                      `json:"funding_scope"`
	FundingOwnerID         string                      `json:"funding_owner_id"`
	FundingIssuanceKey     string                      `json:"funding_issuance_key"`
}

func fundingSourceSigningPayload(r WalletFundingSourceReceipt) ([]byte, error) {
	sources, err := json.Marshal(r.Sources)
	if err != nil {
		return nil, err
	}
	return json.Marshal([]string{r.Protocol, r.FreezeID, r.PlatformUserID, r.LeaseID, r.FundingScope, r.FundingOwnerID, r.FundingIssuanceKey, r.FundedUnits, strconv.FormatInt(r.BudgetRevision, 10), r.CommittedAt, string(sources)})
}
func verifyFundingSourceReceipt(secret string, request walletFundingSourceRequest, r WalletFundingSourceReceipt) error {
	raw, err := json.Marshal(r)
	if err != nil || len(raw) > walletFundingResponseLimit-16384 {
		return errors.New("funding source receipt exceeds bounded protocol body")
	}
	if len(secret) < 32 || r.Protocol != "wallet-funding-source-v1" || r.FreezeID != request.FreezeID || r.PlatformUserID != request.PlatformUserID || r.LeaseID != request.LeaseID || r.FundingScope != request.FundingScope || r.FundingOwnerID != request.FundingOwnerID || r.FundingIssuanceKey != request.FundingIssuanceKey || r.FundedUnits != request.ExpectedFunded.AmountUnits || r.BudgetRevision != request.ExpectedBudgetRevision || r.BudgetRevision < 0 || len(r.Sources) == 0 || len(r.Sources) > walletFundingMaxSources {
		return errors.New("funding source ACK differs from immutable request")
	}
	funded, err := parseCanonicalWalletAmountObject("source funded", request.ExpectedFunded)
	if err != nil || funded <= 0 {
		return errors.New("invalid funded source")
	}
	var sum int64
	seen := map[string]bool{}
	for _, source := range r.Sources {
		if source.AllocationID == "" || len(source.AllocationID) > 128 || source.CreditID == "" || len(source.CreditID) > 128 || seen[source.AllocationID] {
			return errors.New("invalid funding source identity")
		}
		seen[source.AllocationID] = true
		units, e := parseCanonicalWalletAmountObject("allocated source", newCanonicalWalletAmountObjectString(source.AllocatedUnits))
		if e != nil || units <= 0 || units > funded-sum {
			return errors.New("invalid funding source allocation")
		}
		sum += units
	}
	if sum != funded {
		return errors.New("funding source allocations do not conserve")
	}
	if _, err = time.Parse(time.RFC3339Nano, r.CommittedAt); err != nil {
		return err
	}
	payload, err := fundingSourceSigningPayload(r)
	if err != nil {
		return err
	}
	signature, err := hex.DecodeString(r.Signature)
	if err != nil {
		return err
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(payload)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return errors.New("invalid funding source signature")
	}
	return nil
}

// Every ambiguity retains the exact request. Only the route's atomic conflict
// clears an expectation; no PG principal is made immutable before its D1 fence.
func (b *CanonicalWalletBridge) freezeFundingSource(ctx context.Context, user, leaseID string) (*CanonicalWalletLease, error) {
	client, ok := b.control.(*canonicalWalletHTTPClient)
	if !ok {
		return nil, errors.New("signed wallet funding control unavailable")
	}
	for attempt := 0; attempt < 2; attempt++ {
		var raw, ack []byte
		err := b.outboxDB.QueryRowContext(ctx, `SELECT expected_request,source_receipt FROM wallet_funding_source_intent WHERE platform_user_id=$1 AND lease_id=$2`, user, leaseID).Scan(&raw, &ack)
		if err != nil {
			return nil, err
		}
		var request walletFundingSourceRequest
		if len(raw) == 0 {
			lease, e := b.freshFundingSource(ctx, user, leaseID)
			if e != nil {
				return nil, e
			}
			request = walletFundingSourceRequest{PlatformUserID: user, LeaseID: leaseID, FreezeID: leaseID + ".source-freeze.v1", ExpectedFunded: newCanonicalWalletAmountObject(lease.FundingPrincipalUnits()), ExpectedBudgetRevision: lease.BudgetRevision, FundingScope: lease.FundingScope, FundingOwnerID: lease.FundingOwnerID, FundingIssuanceKey: lease.FundingIssuanceKey}
			raw, err = json.Marshal(request)
			if err != nil {
				return nil, err
			}
			result, e := b.outboxDB.ExecContext(ctx, `UPDATE wallet_funding_source_intent SET expected_request=$3::jsonb,updated_at=now() WHERE platform_user_id=$1 AND lease_id=$2 AND expected_request IS NULL AND source_receipt IS NULL`, user, leaseID, string(raw))
			if e != nil {
				return nil, e
			}
			n, e := result.RowsAffected()
			if e != nil || n != 1 {
				return nil, errors.New("funding source expectation checkpoint conflict")
			}
		} else if err = json.Unmarshal(raw, &request); err != nil {
			return nil, err
		}
		if request.PlatformUserID != user || request.LeaseID != leaseID || request.FreezeID != leaseID+".source-freeze.v1" {
			return nil, errors.New("durable funding source request identity mismatch")
		}
		var receipt WalletFundingSourceReceipt
		if len(ack) > 0 {
			if err = json.Unmarshal(ack, &receipt); err != nil {
				return nil, err
			}
		} else {
			var response struct {
				Receipt WalletFundingSourceReceipt `json:"receipt"`
			}
			err = client.doJSON(ctx, "POST", "/api/internal/v2/wallet/leases/freeze-source", "wallet:lease", request.FreezeID, request, &response)
			if err != nil {
				var status *canonicalWalletStatusError
				if errors.As(err, &status) && status.Status == 409 && status.Reason == "funding_conflict" {
					_, clearErr := b.outboxDB.ExecContext(ctx, `UPDATE wallet_funding_source_intent SET expected_request=NULL,updated_at=now() WHERE platform_user_id=$1 AND lease_id=$2 AND expected_request=$3::jsonb AND source_receipt IS NULL`, user, leaseID, string(raw))
					if clearErr != nil {
						return nil, clearErr
					}
					continue
				}
				return nil, err
			}
			receipt = response.Receipt
		}
		if err = verifyFundingSourceReceipt(client.cfg.Secret, request, receipt); err != nil {
			return nil, err
		}
		if len(ack) == 0 {
			ack, err = json.Marshal(receipt)
			if err != nil {
				return nil, err
			}
			result, e := b.outboxDB.ExecContext(ctx, `UPDATE wallet_funding_source_intent SET source_receipt=$3::jsonb,updated_at=now() WHERE platform_user_id=$1 AND lease_id=$2 AND expected_request=$4::jsonb AND (source_receipt IS NULL OR source_receipt=$3::jsonb)`, user, leaseID, string(ack), string(raw))
			if e != nil {
				return nil, e
			}
			n, e := result.RowsAffected()
			if e != nil || n != 1 {
				return nil, errors.New("funding source ACK checkpoint conflict")
			}
		}
		funded, _ := parseCanonicalWalletAmountObject("funded", request.ExpectedFunded)
		lease, err := b.freshFundingSource(ctx, user, leaseID)
		if err != nil {
			return nil, err
		}
		if lease.FundingPrincipalUnits() != funded || lease.BudgetRevision != receipt.BudgetRevision || !lease.FundingFrozen {
			return nil, errors.New("canonical source differs from signed fence")
		}
		return lease, nil
	}
	return nil, errors.New("funding source freeze requires fresh canonical observation")
}

type walletFundingReturnHold struct {
	AuthorizationID string                      `json:"authorization_id"`
	Held            canonicalWalletAmountObject `json:"held"`
}
type walletFundingReturnRequest struct {
	PlatformUserID         string                      `json:"platform_user_id"`
	LeaseID                string                      `json:"lease_id"`
	Mode                   string                      `json:"mode"`
	BaseReturnRevision     int64                       `json:"base_return_revision"`
	SourceFreezeID         string                      `json:"source_freeze_id"`
	ExpectedFunded         canonicalWalletAmountObject `json:"expected_funded"`
	ExpectedBudgetRevision int64                       `json:"expected_budget_revision"`
	GatewayConsumed        canonicalWalletAmountObject `json:"gateway_consumed"`
	GatewayReleased        canonicalWalletAmountObject `json:"gateway_released"`
	Holds                  []walletFundingReturnHold   `json:"holds"`
}
type walletFundingPendingRequest struct {
	Protocol string                      `json:"protocol"`
	Basis    CanonicalWalletFundingBasis `json:"basis"`
	Request  walletFundingReturnRequest  `json:"request"`
}

func (b *CanonicalWalletBridge) returnFrozenFunding(ctx context.Context, user string, lease CanonicalWalletLease, mode string) error {
	store, ok := b.store.(CanonicalWalletFundingStore)
	if !ok {
		return errors.New("atomic wallet funding store unavailable")
	}
	if _, ok = b.control.(*canonicalWalletHTTPClient); !ok {
		return errors.New("signed wallet funding control unavailable")
	}
	if b.outboxDB == nil {
		return errors.New("durable funding registry unavailable")
	}
	for observation := 0; observation < 2; observation++ {
		if err := b.recoverFundingReturn(ctx, user, lease.LeaseID); err != nil {
			return err
		}
		var closed bool
		err := b.outboxDB.QueryRowContext(ctx, `SELECT closed FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, user, lease.LeaseID).Scan(&closed)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if closed {
			if mode == "close" {
				return nil
			}
			return errors.New("funding lease already closed")
		}
		if err = b.prepareFundingReturn(ctx, user, lease.LeaseID, mode, store); err != nil {
			return err
		}
		if err = b.recoverFundingReturn(ctx, user, lease.LeaseID); err != nil {
			return err
		}
		if mode == "partial" {
			return nil
		}
		// A concurrent partial-return request may have won this revision. Its
		// ACK cannot stand in for close-only; one fresh bounded observation closes
		// the zero/free residual and releases the task slot.
		if err = b.outboxDB.QueryRowContext(ctx, `SELECT closed FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, user, lease.LeaseID).Scan(&closed); err != nil {
			return err
		}
		if closed {
			return nil
		}

	}
	return errors.New("funding close requires another canonical observation")
}

func (b *CanonicalWalletBridge) prepareFundingReturn(ctx context.Context, user, leaseID, mode string, store CanonicalWalletFundingStore) error {
	tx, err := b.lockFundingLifecycle(ctx, user)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	// A failed close observation cannot persist its irreversible close mode.
	if mode == "close" {
		cached, readErr := b.store.GetCanonicalWalletLeaseByID(ctx, user, leaseID)
		if readErr != nil {
			return readErr
		}
		if cached.FundingScope == "legacy" || cached.FundingScope == "llm" {
			if err = b.fundingTerminalCloseReady(ctx, user, leaseID); err != nil {
				return err
			}
		}
	}
	_, err = b.outboxDB.ExecContext(ctx, `INSERT INTO wallet_funding_source_intent(platform_user_id,lease_id,freeze_id,requested_mode) VALUES($1,$2,$3,$4) ON CONFLICT(platform_user_id,lease_id) DO UPDATE SET requested_mode=CASE WHEN wallet_funding_source_intent.requested_mode='close' THEN 'close' ELSE EXCLUDED.requested_mode END,updated_at=now()`, user, leaseID, leaseID+".source-freeze.v1", mode)
	if err != nil {
		return err
	}
	if err = b.outboxDB.QueryRowContext(ctx, `SELECT requested_mode FROM wallet_funding_source_intent WHERE platform_user_id=$1 AND lease_id=$2`, user, leaseID).Scan(&mode); err != nil {
		return err
	}
	lease, err := b.freezeFundingSource(ctx, user, leaseID)
	if err != nil {
		return err
	}
	// The signed D1 source fence commits before immutable PG principal. The
	// second Redis observation retains the exact current C/R and original holds.
	_, err = b.outboxDB.ExecContext(ctx, `INSERT INTO wallet_funding_freeze(platform_user_id,lease_id,funding_scope,funding_owner_id,funding_issuance_key,funded_units,returned_units,return_revision,requested_mode) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(platform_user_id,lease_id) DO UPDATE SET requested_mode=CASE WHEN wallet_funding_freeze.requested_mode='close' THEN 'close' ELSE EXCLUDED.requested_mode END,updated_at=now()`, user, leaseID, lease.FundingScope, lease.FundingOwnerID, lease.FundingIssuanceKey, lease.FundingPrincipalUnits(), lease.ReturnedUnits, lease.ReturnRevision, mode)
	if err != nil {
		return err
	}
	if err = b.applyFundingRegistry(ctx, lease); err != nil {
		return err
	}
	basis, err := store.FreezeCanonicalWalletFunding(ctx, user, leaseID)
	if err != nil {
		return err
	}
	if basis == nil {
		return ErrCanonicalWalletLeaseMissing
	}
	request := walletFundingReturnRequest{PlatformUserID: user, LeaseID: leaseID, Mode: mode, BaseReturnRevision: basis.Lease.ReturnRevision, SourceFreezeID: leaseID + ".source-freeze.v1", ExpectedFunded: newCanonicalWalletAmountObject(basis.Lease.FundingPrincipalUnits()), ExpectedBudgetRevision: basis.Lease.BudgetRevision, GatewayConsumed: newCanonicalWalletAmountObject(basis.Lease.ConsumedUnits), GatewayReleased: newCanonicalWalletAmountObject(basis.Lease.ReleasedUnits), Holds: []walletFundingReturnHold{}}
	for _, hold := range basis.Holds {
		request.Holds = append(request.Holds, walletFundingReturnHold{AuthorizationID: hold.AuthorizationID, Held: newCanonicalWalletAmountObject(hold.HeldUnits)})
	}
	raw, err := json.Marshal(walletFundingPendingRequest{Protocol: "wallet-funding-request-v1", Basis: *basis, Request: request})
	if err != nil {
		return err
	}
	// Commit the exact request before any D1 return. A concurrent request uses
	// the first winner rather than overwriting its immutable C/R and hold basis.
	result, err := b.outboxDB.ExecContext(ctx, `UPDATE wallet_funding_freeze SET pending_request=$3::jsonb,updated_at=now() WHERE platform_user_id=$1 AND lease_id=$2 AND funded_units=$4 AND returned_units=$5 AND return_revision=$6 AND NOT closed AND (pending_request IS NULL OR pending_request=$3::jsonb)`, user, leaseID, string(raw), lease.FundingPrincipalUnits(), lease.ReturnedUnits, lease.ReturnRevision)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("funding pending request checkpoint conflict")
	}
	return tx.Commit()
}

// The row lock serializes RPC/ACK and a definitive noncommit response. It is
// acquired only after the exact request has committed in a separate write.
func (b *CanonicalWalletBridge) replayPendingFundingReturn(ctx context.Context, user, leaseID string) error {
	client, ok := b.control.(*canonicalWalletHTTPClient)
	if !ok {
		return errors.New("signed funding recovery control unavailable")
	}
	if _, ok := b.store.(CanonicalWalletFundingStore); !ok {
		return errors.New("funding recovery store unavailable")
	}
	tx, err := b.outboxDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var raw []byte
	var funded, returned, revision int64
	var scope, owner, key string
	err = tx.QueryRowContext(ctx, `SELECT pending_request,funded_units,returned_units,return_revision,funding_scope,funding_owner_id,funding_issuance_key FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2 FOR UPDATE`, user, leaseID).Scan(&raw, &funded, &returned, &revision, &scope, &owner, &key)
	if err != nil {
		return err
	}
	if len(raw) == 0 {
		return tx.Commit()
	}
	var pending walletFundingPendingRequest
	if err = json.Unmarshal(raw, &pending); err != nil {
		return err
	}
	basis := pending.Basis
	request := pending.Request
	if pending.Protocol != "wallet-funding-request-v1" || basis.Lease.PlatformUserID != user || basis.Lease.LeaseID != leaseID || !basis.Lease.FundingFrozen || basis.Lease.FundingPrincipalUnits() != funded || basis.Lease.ReturnedUnits != returned || basis.Lease.ReturnRevision != revision || basis.Lease.FundingScope != scope || basis.Lease.FundingOwnerID != owner || basis.Lease.FundingIssuanceKey != key || request.PlatformUserID != user || request.LeaseID != leaseID || request.BaseReturnRevision != revision || request.SourceFreezeID != leaseID+".source-freeze.v1" || request.ExpectedFunded.AmountUnits != strconv.FormatInt(funded, 10) || request.ExpectedBudgetRevision != basis.Lease.BudgetRevision || (request.Mode != "partial" && request.Mode != "close") || len(request.Holds) > 128 || len(request.Holds) != len(basis.Holds) {
		return errors.New("durable funding request identity mismatch")
	}
	principal, err := parseCanonicalWalletAmountObject("saved funding principal", request.ExpectedFunded)
	if err != nil || principal != funded {
		return errors.New("durable funding request principal mismatch")
	}
	consumed, err := parseCanonicalWalletAmountObject("saved funding consumed", request.GatewayConsumed)
	if err != nil {
		return err
	}
	released, err := parseCanonicalWalletAmountObject("saved funding released", request.GatewayReleased)
	if err != nil {
		return err
	}
	if consumed != basis.Lease.ConsumedUnits || released != basis.Lease.ReleasedUnits || released > consumed {
		return errors.New("durable funding request obligation mismatch")
	}
	seen := map[string]bool{}
	for i, hold := range request.Holds {
		units, e := parseCanonicalWalletAmountObject("saved funding held", hold.Held)
		if e != nil {
			return e
		}
		if hold.AuthorizationID == "" || seen[hold.AuthorizationID] || hold.AuthorizationID != basis.Holds[i].AuthorizationID || units <= 0 || units != basis.Holds[i].HeldUnits {
			return errors.New("durable funding request hold mismatch")
		}
		seen[hold.AuthorizationID] = true
	}
	var response struct {
		Receipt WalletFundingReturnReceipt `json:"receipt"`
	}
	err = client.doJSON(ctx, "POST", "/api/internal/v2/wallet/leases/return", "wallet:lease", "", request, &response)
	if err != nil {
		var status *canonicalWalletStatusError
		if errors.As(err, &status) && status.Status == 409 && status.Reason == "funding_conflict" {
			// No other RPC for this request can be in flight under the same primary
			// lock. A signed route's atomic noncommit can safely require a new basis.
			if _, clearErr := tx.ExecContext(ctx, `UPDATE wallet_funding_freeze SET pending_request=NULL,updated_at=now() WHERE platform_user_id=$1 AND lease_id=$2 AND return_revision=$3 AND pending_request=$4::jsonb`, user, leaseID, revision, string(raw)); clearErr != nil {
				return clearErr
			}
			if commitErr := tx.Commit(); commitErr != nil {
				return commitErr
			}
		}
		return err
	}
	if err = verifyFundingReturnReceipt(client.cfg.Secret, basis, request.Mode, response.Receipt); err != nil {
		return err
	}
	receiptRaw, err := json.Marshal(response.Receipt)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE wallet_funding_freeze SET returned_units=$3,return_revision=$4,receipt=$5::jsonb,closed=($6='close'),pending_request=NULL,updated_at=now() WHERE platform_user_id=$1 AND lease_id=$2 AND funded_units=$7 AND returned_units=$8 AND return_revision=$9 AND pending_request=$10::jsonb`, user, leaseID, response.Receipt.ReturnedAfterUnits, response.Receipt.ReturnRevision, string(receiptRaw), request.Mode, response.Receipt.FundedUnits, response.Receipt.ReturnedBeforeUnits, revision, string(raw))
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("funding ACK conflict")
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	// ACK precedes local budget reduction; an interrupted apply retries from PG.
	return b.applyFundingReturnACK(ctx, response.Receipt)
}

// Recover both D1-commit-before-PG-ACK and PG-ACK-before-Redis-apply before
// reading authorizable headroom. Recovery never creates a missing lease hash.
func (b *CanonicalWalletBridge) recoverFundingReturn(ctx context.Context, user, leaseID string) error {
	if b.outboxDB == nil {
		return nil
	}
	for attempt := 0; attempt < 2; attempt++ {
		var raw, pending []byte
		var funded, returned, revision int64
		var scope, owner, key string
		err := b.outboxDB.QueryRowContext(ctx, `SELECT receipt,funded_units,returned_units,return_revision,funding_scope,funding_owner_id,funding_issuance_key,pending_request FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, user, leaseID).Scan(&raw, &funded, &returned, &revision, &scope, &owner, &key, &pending)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if len(pending) > 0 {
			if err = b.replayPendingFundingReturn(ctx, user, leaseID); err != nil {
				return err
			}
			continue
		}
		if len(raw) == 0 {
			return nil
		}
		var receipt WalletFundingReturnReceipt
		if err = json.Unmarshal(raw, &receipt); err != nil {
			return err
		}
		client, ok := b.control.(*canonicalWalletHTTPClient)
		if !ok {
			return errors.New("signed funding recovery control unavailable")
		}
		units, err := validateFundingReturnReceipt(client.cfg.Secret, receipt)
		if err != nil {
			return err
		}
		if receipt.PlatformUserID != user || receipt.LeaseID != leaseID || receipt.ReturnRevision != revision || units[0] != funded || units[4] != returned || receipt.FundingScope != scope || receipt.FundingOwnerID != owner || receipt.FundingIssuanceKey != key {
			return errors.New("funding recovery differs from primary ACK")
		}
		return b.applyFundingReturnACK(ctx, receipt)
	}
	return errors.New("funding recovery requires another observation")
}

func (b *CanonicalWalletBridge) applyFundingReturnACK(ctx context.Context, receipt WalletFundingReturnReceipt) error {
	store, ok := b.store.(CanonicalWalletFundingStore)
	if !ok {
		return errors.New("funding recovery store unavailable")
	}
	if err := store.ApplyCanonicalWalletFundingReturn(ctx, receipt); err != nil {
		return err
	}
	_, err := b.outboxDB.ExecContext(ctx, `UPDATE wallet_funding_freeze SET redis_applied_revision=GREATEST(redis_applied_revision,$3) WHERE platform_user_id=$1 AND lease_id=$2 AND return_revision=$3`, receipt.PlatformUserID, receipt.LeaseID, receipt.ReturnRevision)
	return err
}

// This independent lane survives request cancellation and flag rollback. It
// retries exact persisted requests/ACKs without blocking provider fee dispatch.
func (b *CanonicalWalletBridge) runFundingRecovery() {
	cleanupDone := make(chan struct{})
	go func() {
		defer close(cleanupDone)
		b.runFundingTerminalRecovery()
	}()
	defer func() { <-cleanupDone }()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-b.stop:
			return
		case <-ticker.C:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		rows, err := b.outboxDB.QueryContext(ctx, `SELECT platform_user_id,lease_id,requested_mode,intent FROM (SELECT platform_user_id,lease_id,requested_mode,(pending_request IS NULL AND NOT closed AND (receipt IS NULL OR requested_mode='close')) AS intent,updated_at FROM wallet_funding_freeze WHERE pending_request IS NOT NULL OR (receipt IS NOT NULL AND redis_applied_revision<return_revision) OR (NOT closed AND (receipt IS NULL OR requested_mode='close')) UNION ALL SELECT s.platform_user_id,s.lease_id,s.requested_mode,true,s.updated_at FROM wallet_funding_source_intent s WHERE NOT EXISTS(SELECT 1 FROM wallet_funding_freeze f WHERE f.platform_user_id=s.platform_user_id AND f.lease_id=s.lease_id AND (f.requested_mode=s.requested_mode OR f.closed))) work ORDER BY updated_at,platform_user_id,lease_id LIMIT 16`)
		if err != nil {
			cancel()
			continue
		}
		leases := []struct {
			user, lease, mode string
			intent            bool
		}{}
		for rows.Next() {
			var entry struct {
				user, lease, mode string
				intent            bool
			}
			if err = rows.Scan(&entry.user, &entry.lease, &entry.mode, &entry.intent); err != nil {
				break
			}
			leases = append(leases, entry)
		}
		if err == nil {
			err = rows.Err()
		}
		_ = rows.Close()
		if err == nil {
			for _, entry := range leases {
				select {
				case <-b.stop:
					cancel()
					return
				default:
				}
				if entry.intent {
					err = b.returnFrozenFunding(ctx, entry.user, CanonicalWalletLease{LeaseID: entry.lease}, entry.mode)
				} else {
					err = b.recoverFundingReturn(ctx, entry.user, entry.lease)
				}
				if err != nil {
					slog.Warn("wallet funding recovery deferred", "lease_id", entry.lease, "error", err)
					// Rotate a persistent failure behind other pending work.
					markCtx, markCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
					_, _ = b.outboxDB.ExecContext(markCtx, `UPDATE wallet_funding_freeze SET updated_at=now() WHERE platform_user_id=$1 AND lease_id=$2`, entry.user, entry.lease)
					_, _ = b.outboxDB.ExecContext(markCtx, `UPDATE wallet_funding_source_intent SET updated_at=now() WHERE platform_user_id=$1 AND lease_id=$2`, entry.user, entry.lease)
					markCancel()
				}
				if ctx.Err() != nil {
					break
				}
			}
		}
		cancel()
	}
}

// New media uses only leases owned by its verified persisted task. Each stable
// ordinal issues once; a large quote may span leases without truncation.
func (b *CanonicalWalletBridge) authorizeMediaFunding(ctx context.Context, h *AuthorizationHandle, user string, units int64, store CanonicalWalletPoolStore) (retErr error) {
	taskID, _ := ctx.Value(mediaFundingOwnerContextKey{}).(string)
	if taskID == "" || b.outboxDB == nil {
		return errors.New("persisted media funding owner unavailable")
	}
	var parent, snapshot, owner string
	var quote int64
	if err := b.outboxDB.QueryRowContext(ctx, `SELECT authorization_id,platform_user_id,billing_snapshot_id,quoted_units FROM gateway_media_task WHERE id=$1`, taskID).Scan(&parent, &owner, &snapshot, &quote); err != nil {
		return err
	}
	if parent != h.ID || owner != user || snapshot != h.SnapshotID || quote != units {
		return errors.New("media funding owner differs from immutable task")
	}
	funded := []CanonicalWalletLease{}
	defer func() {
		if retErr == nil || len(funded) == 0 {
			return
		}
		// Issuance may precede plan/arm. A failed unsubmitted attempt must not
		// strand free principal or its task slot. The primary task proves no
		// provider write; the signed close RPC independently refuses any pin,
		// armed hold or pending conversion that won an ambiguous arm race.
		var unsubmitted bool
		err := b.outboxDB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM gateway_media_task WHERE id=$1 AND authorization_id=$2 AND platform_user_id=$3 AND billing_snapshot_id=$4 AND quoted_units=$5 AND held_units=0 AND write_started_at IS NULL AND provider_task_id IS NULL)`, taskID, h.ID, user, h.SnapshotID, units).Scan(&unsubmitted)
		if err != nil {
			retErr = fmt.Errorf("unsubmitted funding return pending: %w", err)
			return
		}
		if !unsubmitted {
			return
		}
		for _, lease := range funded {
			if err = b.returnFrozenFunding(ctx, user, lease, "close"); err != nil {
				retErr = fmt.Errorf("unsubmitted funding return pending: %w", err)
				return
			}
		}
	}()
	segments := []AuthorizationSegment{}
	remaining := units
	for ordinal := 0; remaining > 0 && ordinal < 128; ordinal++ {
		key := fmt.Sprintf("%s.funding.%d", taskID, ordinal)
		request := canonicalWalletEnsureRequest{PlatformUserID: user, Currency: "USD", Purpose: "authorize", FundingScope: "media", FundingOwnerID: taskID, FundingIssuanceKey: key,
			USDWalletPolicyVersion: config.CanonicalUSDWalletPolicyVersion, MinHeadroom: newCanonicalWalletAmountObject(1), RequestedBudget: newCanonicalWalletAmountObject(remaining), RequestedTTLSeconds: b.cfg.LeaseTTLSeconds, CallerSlotTTLSeconds: b.callerSlotTTLSeconds}
		result, err := b.ensureFundingLifecycle(ctx, request)
		if errors.Is(err, ErrCanonicalWalletBalanceShortfall) {
			if e := b.reclaimSharedFreeFunding(ctx, user); e != nil {
				return e
			}
			result, err = b.ensureFundingLifecycle(ctx, request)
		}
		if err != nil {
			return err
		}
		if result == nil || result.Lease.BudgetUnits <= 0 || result.Lease.BudgetUnits > remaining {
			return errors.New("invalid isolated media funding grant")
		}
		lease := result.Lease
		if lease.FundingScope != "media" || lease.FundingOwnerID != taskID || lease.FundingIssuanceKey != key || lease.FundingFrozen {
			return errors.New("media funding isolation mismatch")
		}
		lease.RetainUntil = lease.ExpiresAt.Add(time.Duration(b.callerSlotTTLSeconds) * time.Second)
		funded = append(funded, lease)
		if err = b.installFundingLease(ctx, lease); err != nil {
			return err
		}
		auth := h.ID
		if ordinal > 0 {
			auth, err = newAuthorizationID()
			if err != nil {
				return err
			}
		}
		segments = append(segments, AuthorizationSegment{AuthorizationID: auth, LeaseID: lease.LeaseID, HeldUnits: lease.BudgetUnits, Basis: lease, Kind: "media", State: "prepared", PinState: "none", EventID: CanonicalWalletSettlementEventID(h.ID+":"+auth, user, "USD")})
		remaining -= lease.BudgetUnits
	}
	if remaining != 0 {
		return errors.New("media quote exceeds bounded funding segment count")
	}
	if err := b.saveAuthorizationPlan(ctx, h.ID, user, h.SnapshotID, "media", segments); err != nil {
		return err
	}
	if err := store.ArmCanonicalWalletPool(ctx, user, segments, b.graceMS(), b.clock()); err != nil {
		return err
	}
	h.Segments = segments
	h.LeaseID = segments[0].LeaseID
	h.HoldArmed = true
	h.HeldUnits = units
	b.installPoolOutcome(h, user)
	return nil
}

func (b *CanonicalWalletBridge) reclaimSharedFreeFunding(ctx context.Context, user string) error {
	client, ok := b.control.(*canonicalWalletHTTPClient)
	if !ok {
		return errors.New("shared funding control unavailable")
	}
	var response struct {
		Leases []canonicalWalletLeaseWireView `json:"leases"`
	}
	if err := client.doJSON(ctx, "POST", "/api/internal/v2/wallet/leases/pool", "wallet:lease", "", map[string]string{"platform_user_id": user, "usd_wallet_policy_version": config.CanonicalUSDWalletPolicyVersion, "funding_scope": "llm"}, &response); err != nil {
		return err
	}
	if len(response.Leases) > 128 {
		return errors.New("shared funding pool exceeds return bound")
	}
	returned := false
	// One lease that cannot be reclaimed must not stop the others from being
	// reclaimed; its error is reported only when nothing at all could be returned.
	var firstErr error
	note := func(err error) {
		if firstErr == nil {
			firstErr = err
		}
	}
	for _, wire := range response.Leases {
		if wire.FundingScope == "media" || wire.PlatformUserID != user || wire.Status != "active" {
			continue
		}
		if err := b.recoverFundingReturn(ctx, user, wire.LeaseID); err != nil {
			note(err)
			continue
		}
		lease, err := b.store.GetCanonicalWalletLeaseByID(ctx, user, wire.LeaseID)
		if err != nil {
			note(err)
			continue
		}
		if lease.Sealed && !lease.FundingFrozen {
			continue
		}
		// Every return is a new signed revision on three stores. A lease with no
		// free principal has nothing to give back, and a user running short of
		// funds would otherwise mint one empty revision per lease per attempt.
		if lease.BudgetUnits <= lease.ConsumedUnits-lease.ReleasedUnits {
			continue
		}
		if err = b.returnFrozenFunding(ctx, user, *lease, "partial"); err != nil {
			note(err)
			continue
		}
		returned = true
	}
	if returned {
		return nil
	}
	if firstErr != nil {
		return firstErr
	}
	return ErrCanonicalWalletBalanceShortfall
}

// closePinlessMediaLease closes the isolated lease of a media share whose pin was
// never created. Unlike closeMediaFundingLeases it does not require a prior pin
// acknowledgement (none can exist). The signed close request carries the Redis
// consumed/released totals and the remaining holds, and the Worker refuses it
// while any pin is active or the captured total differs, so a pin that does
// exist can never be dropped by this path.
func (b *CanonicalWalletBridge) closePinlessMediaLease(ctx context.Context, user string, segment AuthorizationSegment) error {
	// D1 lists only active leases, so an absent isolated lease was already closed
	// and its unspent budget is back in the credits.
	if _, err := b.fundingSourceWire(ctx, user, segment.LeaseID); err != nil {
		if errors.Is(err, ErrCanonicalWalletLeaseMissing) {
			return nil
		}
		return err
	}
	lease, err := b.store.GetCanonicalWalletLeaseByID(ctx, user, segment.LeaseID)
	if errors.Is(err, ErrCanonicalWalletLeaseMissing) {
		// Redis lost the lease. Rebuild an empty isolated lease from the persisted
		// basis; no hold or consumption survives a lost cache, and the Worker still
		// checks D1's captured total and active pins when it accepts the close.
		basis := segment.Basis
		if basis.LeaseID != segment.LeaseID || basis.FundingScope != "media" {
			return errors.New("pinless media lease basis mismatch")
		}
		basis.ConsumedUnits, basis.ReleasedUnits = 0, 0
		if err = b.installFundingLease(ctx, basis); err != nil {
			return err
		}
		lease = &basis
	} else if err != nil {
		return err
	}
	return b.returnFrozenFunding(ctx, user, *lease, "close")
}

// Financial lifecycle callers use this only after signed PG ACK and cleanup.
// A shared historical lease with another obligation is safely refused by D1.
func (b *CanonicalWalletBridge) closeMediaFundingLeases(ctx context.Context, user, taskID string, ids []string) error {
	if b.outboxDB == nil {
		return errors.New("media finance acknowledgement unavailable")
	}
	var acknowledged bool
	if err := b.outboxDB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM gateway_media_task t WHERE t.id=$1 AND t.platform_user_id=$2
 AND t.financial_state IN ('released_zero','released_unknown','charged') AND NOT EXISTS(
  SELECT 1 FROM wallet_authorization_segment a WHERE a.parent_authorization_id=t.authorization_id AND NOT (
   (a.zero_ack_at IS NOT NULL AND a.pin_state='finished')
   OR (a.kind='media' AND a.state='finished' AND a.pin_state='finished' AND a.actual_units=0 AND a.zero_ack_at IS NULL AND a.expiry_ack_at IS NULL)
   OR (a.expiry_ack_at IS NOT NULL AND a.expiry_cleanup_at IS NOT NULL)
   OR (a.actual_units>0 AND a.pin_state='finished' AND EXISTS(SELECT 1 FROM wallet_settlement_outbox o WHERE o.event_id=a.event_id AND o.status='delivered' AND o.platform_user_id=a.platform_user_id AND o.billing_snapshot_id=a.billing_snapshot_id AND o.amount_units=a.actual_units)))))`, taskID, user).Scan(&acknowledged); err != nil {
		return err
	}
	if !acknowledged {
		return errors.New("media funding awaits complete financial ACK and cleanup")
	}
	seen := map[string]bool{}
	for _, id := range ids {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		if err := b.recoverFundingReturn(ctx, user, id); err != nil {
			return err
		}
		var closed bool
		var owner string
		registryErr := b.outboxDB.QueryRowContext(ctx, `SELECT closed,funding_owner_id FROM wallet_funding_freeze WHERE platform_user_id=$1 AND lease_id=$2`, user, id).Scan(&closed, &owner)
		if registryErr != nil && !errors.Is(registryErr, sql.ErrNoRows) {
			return registryErr
		}
		if closed {
			if owner != taskID {
				return errors.New("closed media funding owner mismatch")
			}
			continue
		}
		lease, err := b.store.GetCanonicalWalletLeaseByID(ctx, user, id)
		if errors.Is(err, ErrCanonicalWalletLeaseMissing) {
			return err
		}
		if err != nil {
			return err
		}
		if lease.FundingScope != "media" {
			continue
		}
		if lease.FundingOwnerID != taskID {
			return errors.New("media funding close owner mismatch")
		}
		if err = b.returnFrozenFunding(ctx, user, *lease, "close"); err != nil {
			return err
		}
	}
	return nil
}

// Primary PG joins also cover media overrun rows with no authorization_id.
func (b *CanonicalWalletBridge) lateFundingExclusions(ctx context.Context, e CanonicalWalletOutboxEvent) ([]string, string, error) {
	if b.outboxDB == nil {
		return nil, "", nil
	}
	rows, err := b.outboxDB.QueryContext(ctx, `SELECT DISTINCT a.lease_id,COALESCE(a.authorization_token,t.authorization_token,'')
 FROM wallet_authorization_segment a
 LEFT JOIN gateway_media_task t ON t.authorization_id=a.parent_authorization_id
 WHERE a.platform_user_id=$1 AND a.billing_snapshot_id=$2
 AND (a.expiry_ack_at IS NOT NULL OR a.zero_ack_at IS NOT NULL)
 AND ((NULLIF($3,'') IS NOT NULL AND a.parent_authorization_id IN
  (SELECT origin.parent_authorization_id FROM wallet_authorization_segment origin WHERE origin.authorization_id=$3 AND origin.platform_user_id=$1 AND origin.billing_snapshot_id=$2))
 OR (t.id=$4 AND t.platform_user_id=$1 AND t.billing_snapshot_id=$2)) ORDER BY a.lease_id`, e.PlatformUserID, e.BillingSnapshotID, e.AuthorizationID, e.GatewayRequestID)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = rows.Close() }()
	ids := []string{}
	token := ""
	for rows.Next() {
		var id, current string
		if err = rows.Scan(&id, &current); err != nil {
			return nil, "", err
		}
		if len(ids) >= 128 {
			return nil, "", errors.New("late original funding exceeds exclusion bound")
		}
		if token != "" && current != "" && token != current {
			return nil, "", errors.New("late original authorization token conflict")
		}
		if current != "" {
			token = current
		}
		ids = append(ids, id)
	}
	return ids, token, rows.Err()
}

func (b *CanonicalWalletBridge) ensureExcludedSettlementFunding(ctx context.Context, e CanonicalWalletOutboxEvent, excluded []string) (*CanonicalWalletLease, error) {
	request := canonicalWalletEnsureRequest{PlatformUserID: e.PlatformUserID, Currency: e.Currency, Purpose: "settle", FundingScope: "settle", ExcludeLeaseIDs: excluded,
		MinHeadroom: newCanonicalWalletAmountObject(e.AmountUnits), RequestedBudget: newCanonicalWalletAmountObject(e.AmountUnits), RequestedTTLSeconds: b.cfg.LeaseTTLSeconds, CallerSlotTTLSeconds: b.callerSlotTTLSeconds}
	result, err := b.ensureFundingLifecycle(ctx, request)
	if errors.Is(err, ErrCanonicalWalletBalanceShortfall) {
		if err = b.reclaimSharedFreeFunding(ctx, e.PlatformUserID); err != nil {
			return nil, err
		}
		result, err = b.ensureFundingLifecycle(ctx, request)
	}
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, ErrCanonicalWalletLeaseMissing
	}
	for _, id := range excluded {
		if result.Lease.LeaseID == id {
			return nil, errors.New("late funding reactivated original released lease")
		}
	}
	lease := result.Lease
	lease.RetainUntil = lease.ExpiresAt.Add(time.Duration(b.callerSlotTTLSeconds) * time.Second)
	if err = b.installFundingLease(ctx, lease); err != nil {
		return nil, err
	}
	return &lease, nil
}
