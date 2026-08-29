package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

const canonicalWalletLeasePrefix = "canonical_wallet:lease:"
const canonicalWalletCurrentPrefix = "canonical_wallet:current:"

// redisLuaMaxSafeInt64 is 2^53-1. Redis embeds Lua 5.1, which has exactly
// one numeric type — an IEEE-754 double, the same representation as
// JavaScript's `number` — so any value this script passes through
// `tonumber(...)` (every HGET read in both scripts below) loses exact
// precision above this bound, identical to JS's Number.MAX_SAFE_INTEGER
// (Phase 0's `exceeds_number_max_safe_integer` fixture case exists
// specifically to catch this class of bug). Rather than hand-rolling
// string-based bigint arithmetic inside Lua (a much larger undertaking),
// values are checked against this bound in Go BEFORE either script ever
// runs — this is a real, disclosed limitation, the same kind already
// accepted elsewhere in this project (lease.ts's planAllocation().limit(1000),
// MySQL's CHECK-constraint version dependency): at ~90,071,992 CNY per
// value, it is far beyond any realistic single lease/reservation amount
// (the default lease budget is 5 CNY — see Task 5), so this guard is not
// expected to ever fire in real operation, only to fail loudly instead of
// silently corrupting a value in the pathological case that it would.
const redisLuaMaxSafeInt64 = 9007199254740991

func ensureRedisLuaSafeInt64(values ...int64) error {
	for _, v := range values {
		if v < 0 || v > redisLuaMaxSafeInt64 {
			return fmt.Errorf(
				"canonical wallet Redis operation requires every amount within Lua's exact integer range (0..%d); got %d",
				redisLuaMaxSafeInt64, v,
			)
		}
	}
	return nil
}

// installCanonicalWalletLeaseScript writes/updates ONE lease's own hash
// (KEYS[1]) and repoints the user's "current lease" pointer (KEYS[2]) to
// it — but ONLY if this lease is at least as new (by expiry) as whatever
// the pointer currently targets. It never touches any other lease's own
// hash — an old lease with an in-flight reservation keeps living under its
// own key until its own TTL.
//
// The pointer comparison before advancing stops a delayed or reordered
// lease-issuance response (e.g. two concurrent renewals racing, or a
// retried HTTP call whose original response arrives late) from making an
// OLDER lease become "current" after a genuinely newer one already has —
// silently regressing fresh requests onto a lease with less remaining
// headroom than they should see. ARGV[7] is the lease-key prefix
// (everything before the lease id) so the script can read the
// CURRENTLY-pointed-to lease's own expiry — safe to build and access this
// key inside the script without declaring it as a separate `KEYS[]` entry
// because it shares the same `{user_hash}` hash tag as KEYS[1]/KEYS[2],
// so it always lands in the same Redis Cluster slot. An exact-millisecond
// expiry tie is broken by lexicographic lease_id comparison — deterministic
// regardless of arrival order, since it depends only on the two leases'
// own IDs.
var installCanonicalWalletLeaseScript = redis.NewScript(`
	local incoming_consumed = tonumber(ARGV[4])
	local incoming_expires = tonumber(ARGV[5])
	local lease_key_prefix = ARGV[7]
	if redis.call('EXISTS', KEYS[1]) == 1 then
		local current_consumed = tonumber(redis.call('HGET', KEYS[1], 'consumed_units') or '0')
		if current_consumed > incoming_consumed then
			incoming_consumed = current_consumed
		end
	end
	redis.call('HSET', KEYS[1],
		'lease_id', ARGV[1], 'platform_user_id', ARGV[2], 'currency', ARGV[3],
		'budget_units', ARGV[6], 'consumed_units', incoming_consumed, 'expires_at_ms', ARGV[5])
	redis.call('PEXPIREAT', KEYS[1], incoming_expires)

	local current_pointer_lease_id = redis.call('GET', KEYS[2])
	local should_advance_pointer = true
	if current_pointer_lease_id ~= false and current_pointer_lease_id ~= ARGV[1] then
		local pointer_key = lease_key_prefix .. current_pointer_lease_id
		local pointer_expires = tonumber(redis.call('HGET', pointer_key, 'expires_at_ms') or '0')
		if pointer_expires > incoming_expires then
			should_advance_pointer = false
		elseif pointer_expires == incoming_expires and current_pointer_lease_id > ARGV[1] then
			should_advance_pointer = false
		end
	end
	if should_advance_pointer then
		redis.call('SET', KEYS[2], ARGV[1])
		redis.call('PEXPIREAT', KEYS[2], incoming_expires)
	end
	return 1
`)

// reserveCanonicalWalletLeaseScript reserves against the EXACT lease named
// by KEYS[1] — never "whatever the current pointer says" — so a retry of an
// in-flight reservation resolves correctly even after a newer lease has
// become current for fresh requests. The duplicate check requires the
// stored reservation marker to equal THIS call's own lease id (read fresh
// from KEYS[1]); a mismatch returns code 6 (genuine conflict) instead of a
// false "duplicate" carrying the wrong lease's snapshot.
var reserveCanonicalWalletLeaseScript = redis.NewScript(`
	if redis.call('EXISTS', KEYS[1]) == 0 then return {1} end
	local stored_lease_id = redis.call('HGET', KEYS[1], 'lease_id')
	local currency = redis.call('HGET', KEYS[1], 'currency')
	local budget = tonumber(redis.call('HGET', KEYS[1], 'budget_units') or '0')
	local consumed = tonumber(redis.call('HGET', KEYS[1], 'consumed_units') or '0')
	-- released_units (3.4b, §10.2): freed budget is reusable on the gateway;
	-- consumed stays monotone and the guard subtracts released.
	local released = tonumber(redis.call('HGET', KEYS[1], 'released_units') or '0')
	local expires_at = tonumber(redis.call('HGET', KEYS[1], 'expires_at_ms') or '0')
	if currency ~= ARGV[1] then return {2} end
	if expires_at <= tonumber(ARGV[4]) then return {3} end
	local existing = redis.call('GET', KEYS[2])
	if existing ~= false then
		if existing == stored_lease_id then
			return {5, stored_lease_id, currency, budget, consumed, expires_at}
		end
		return {6}
	end
	local amount = tonumber(ARGV[2])
	if amount <= 0 or consumed - released + amount > budget then return {4} end
	local updated = consumed + amount
	redis.call('HSET', KEYS[1], 'consumed_units', updated)
	-- ABSOLUTE deadline, matching the lease hash's own PEXPIREAT. A relative
	-- PX computed from the GATEWAY's clock (ARGV[4]) is applied at REDIS's
	-- execution instant, so the marker outlived its lease by the round-trip
	-- latency plus any clock offset. Both keys now expire against one clock,
	-- which is what lets the dispatcher treat "lease gone" as "marker gone"
	-- when it releases a stale binding.
	redis.call('SET', KEYS[2], stored_lease_id)
	redis.call('PEXPIREAT', KEYS[2], expires_at)
	return {0, stored_lease_id, currency, budget, updated, expires_at}
`)

// sealCanonicalWalletLeaseScript (Phase 3.4, redesign §3.3) atomically closes
// KEYS[1] to NEW reservations on the gateway side ahead of a drain: it reads
// consumed_units, sets consumed_units = budget_units, and DELetes the current
// pointer KEYS[2] if it names this lease. It returns {0, pre_seal_consumed,
// released} — the consumed figure the gateway sends as
// gateway_consumed_units, and (3.4b, §10.2) the released figure it sends as
// gateway_released when non-zero. After the seal the guard refuses every
// further reservation when released_units is 0; with releases outstanding it
// still admits amounts up to released_units — the seal caps consumed at
// budget, it does not close a lease that has given budget back (redesign
// §10.2 leaves the seal untouched by design; the drain identity is unaffected
// because it is computed from the pre-seal figures). A retry carrying an
// existing marker still succeeds because the duplicate check runs before the
// budget check. Idempotent:
// sealing a sealed lease returns consumed == budget. The hash is NOT deleted
// — GetCanonicalWalletLeaseByID still finds a sealed lease, which is what
// keeps the explicit-id branch's marker resolution working.
var sealCanonicalWalletLeaseScript = redis.NewScript(`
	if redis.call('EXISTS', KEYS[1]) == 0 then return {1} end
	local budget = tonumber(redis.call('HGET', KEYS[1], 'budget_units') or '0')
	local consumed = tonumber(redis.call('HGET', KEYS[1], 'consumed_units') or '0')
	local released = tonumber(redis.call('HGET', KEYS[1], 'released_units') or '0')
	redis.call('HSET', KEYS[1], 'consumed_units', budget)
	local current = redis.call('GET', KEYS[2])
	if current ~= false and current == ARGV[1] then
		redis.call('DEL', KEYS[2])
	end
	return {0, consumed, released}
`)

// armCanonicalWalletHoldScript (3.4b, §10.4) — KEYS: lease hash, hold hash,
// user hold set. ARGV: currency, units, now_ms, authorization_id, grace_ms.
// Lease checks mirror the reserve script ({1} missing, {2} currency, {3}
// expired); an existing hold hash is the {5} idempotent re-arm; the guard is
// the reserve script's own (consumed − released + units > budget → {4}).
// On success: HINCRBY consumed, the hold hash {state=armed, class=""} with
// PEXPIREAT = the lease's expires_at + grace, SADD the per-user set. The
// untagged hold_users SADD happens in Go AFTER the script (another slot —
// §10.3's stated window).
var armCanonicalWalletHoldScript = redis.NewScript(`
	if redis.call('EXISTS', KEYS[1]) == 0 then return {1} end
	if redis.call('HGET', KEYS[1], 'currency') ~= ARGV[1] then return {2} end
	local budget = tonumber(redis.call('HGET', KEYS[1], 'budget_units') or '0')
	local consumed = tonumber(redis.call('HGET', KEYS[1], 'consumed_units') or '0')
	local released = tonumber(redis.call('HGET', KEYS[1], 'released_units') or '0')
	local expires_at = tonumber(redis.call('HGET', KEYS[1], 'expires_at_ms') or '0')
	if expires_at <= tonumber(ARGV[3]) then return {3} end
	if redis.call('EXISTS', KEYS[2]) == 1 then
		return {5, redis.call('HGET', KEYS[2], 'lease_id'), tonumber(redis.call('HGET', KEYS[2], 'held_units'))}
	end
	local units = tonumber(ARGV[2])
	if units <= 0 or consumed - released + units > budget then return {4} end
	redis.call('HSET', KEYS[1], 'consumed_units', consumed + units)
	redis.call('HSET', KEYS[2], 'lease_id', redis.call('HGET', KEYS[1], 'lease_id'), 'held_units', units, 'armed_at_ms', ARGV[3], 'class', '', 'state', 'armed', 'event_id', '')
	redis.call('PEXPIREAT', KEYS[2], expires_at + tonumber(ARGV[5]))
	redis.call('SADD', KEYS[3], ARGV[4])
	return {0, redis.call('HGET', KEYS[1], 'lease_id'), units}
`)

// releaseCanonicalWalletHoldScript (§10.5/§10.7) — KEYS: hold hash, user hold
// set, lease hash. ARGV: authorization_id, state_after ('released' |
// 'abandoned'), class_after (” keeps the stored class). Only an armed hold
// moves ({7} otherwise, carrying the stored state/event_id for the caller's
// branch-on-event_id rule); the lease's released_units rises by the held
// figure iff the lease hash still exists — a hold on an expired lease is the
// reaper's abandoned path and the release is a no-op on the (gone) lease.
var releaseCanonicalWalletHoldScript = redis.NewScript(`
	if redis.call('EXISTS', KEYS[1]) == 0 then return {1} end
	if redis.call('HGET', KEYS[1], 'state') ~= 'armed' then return {7, redis.call('HGET', KEYS[1], 'state'), redis.call('HGET', KEYS[1], 'event_id')} end
	local held = tonumber(redis.call('HGET', KEYS[1], 'held_units'))
	if redis.call('EXISTS', KEYS[3]) == 1 then redis.call('HINCRBY', KEYS[3], 'released_units', held) end
	redis.call('HSET', KEYS[1], 'state', ARGV[2])
	if ARGV[3] ~= '' then redis.call('HSET', KEYS[1], 'class', ARGV[3]) end
	redis.call('SREM', KEYS[2], ARGV[1])
	return {0, held}
`)

// convertCanonicalWalletHoldScript (§10.5) — KEYS: hold hash, user hold set,
// lease hash, the event's reservation marker. ARGV: authorization_id,
// event_id, actual_units, now_ms. Branches: {1} hold missing; {7, state,
// event_id, lease_id} not armed — the caller branches on the stored
// event_id, never on the code alone, and §13.2.7's fourth element carries
// the hold's lease so a retried submission restores event.LeaseID and the
// outbox dedups it as a same-payload duplicate; A ≤ E → released += E−A,
// marker = lease_id (PEXPIREAT = the lease's expires_at), settled, event_id
// — or {3} when the lease hash is already gone (nothing to release or mark;
// the event proceeds unbound); A > E within budget and unexpired →
// consumed += excess, marker, settled; otherwise (beyond budget, or the
// lease expired) → released += E, state= released, {4} — the money-safe
// direction, consistent with §10.5's expired-before-delivery reasoning (the
// overrun's expiry guard is an accepted fill of a §10 gap, recorded in the
// completion table).
var convertCanonicalWalletHoldScript = redis.NewScript(`
	if redis.call('EXISTS', KEYS[1]) == 0 then return {1} end
	local state = redis.call('HGET', KEYS[1], 'state')
	if state ~= 'armed' then return {7, state, redis.call('HGET', KEYS[1], 'event_id'), redis.call('HGET', KEYS[1], 'lease_id')} end
	local held = tonumber(redis.call('HGET', KEYS[1], 'held_units'))
	local actual = tonumber(ARGV[3])
	local lease_id = redis.call('HGET', KEYS[1], 'lease_id')
	local lease_exists = redis.call('EXISTS', KEYS[3]) == 1
	local expires_at = tonumber(redis.call('HGET', KEYS[3], 'expires_at_ms') or '0')
	if actual <= held then
		if lease_exists then
			redis.call('HINCRBY', KEYS[3], 'released_units', held - actual)
			redis.call('SET', KEYS[4], lease_id); redis.call('PEXPIREAT', KEYS[4], expires_at)
		end
		redis.call('HSET', KEYS[1], 'state', 'settled', 'event_id', ARGV[2]); redis.call('SREM', KEYS[2], ARGV[1])
		if not lease_exists then return {3} end
		return {0, lease_id}
	end
	local excess = actual - held
	if lease_exists then
		local budget = tonumber(redis.call('HGET', KEYS[3], 'budget_units') or '0')
		local consumed = tonumber(redis.call('HGET', KEYS[3], 'consumed_units') or '0')
		local released = tonumber(redis.call('HGET', KEYS[3], 'released_units') or '0')
		if expires_at > tonumber(ARGV[4]) and consumed - released + excess <= budget then
			redis.call('HINCRBY', KEYS[3], 'consumed_units', excess)
			redis.call('SET', KEYS[4], lease_id); redis.call('PEXPIREAT', KEYS[4], expires_at)
			redis.call('HSET', KEYS[1], 'state', 'settled', 'event_id', ARGV[2]); redis.call('SREM', KEYS[2], ARGV[1])
			return {0, lease_id}
		end
		redis.call('HINCRBY', KEYS[3], 'released_units', held)
	end
	redis.call('HSET', KEYS[1], 'state', 'released'); redis.call('SREM', KEYS[2], ARGV[1])
	return {4}
`)

// markCanonicalWalletHoldClassScript (§10.5's indeterminate mark): HSET class
// iff state = armed — the outcome row write (Task 4) needs the returned hold.
var markCanonicalWalletHoldClassScript = redis.NewScript(`
	if redis.call('EXISTS', KEYS[1]) == 0 then return {1} end
	local state = redis.call('HGET', KEYS[1], 'state')
	if state ~= 'armed' then return {7, state, redis.call('HGET', KEYS[1], 'event_id')} end
	redis.call('HSET', KEYS[1], 'class', ARGV[1])
	return {0, ARGV[1]}
`)

// releaseCanonicalWalletReservationScript (Phase 3.5, redesign §11.2/§11.3/
// §11.4) releases a settlement event's reservation against the lease named
// by the marker. KEYS: lease hash, event reservation marker, event release
// marker. ARGV: lease_id, units, drop_marker ('1' | '0').
// {1} = no marker (nothing owed — a re-run of the full form); {6} = the
// marker points at a different lease (a caller naming the wrong lease — no
// write); {7} = the partial form's release marker already exists (the
// idempotency gate: a redelivery or a crash-repair re-run must never raise
// released_units twice). The full form (drop_marker = '1' — named_lease_id
// and the late capture) DELs the marker, which is ITS idempotency; the
// partial form (drop_marker = '0' — the split) keeps the marker (the
// parent's redelivery must answer {5} on the reserve script) and writes the
// release marker with the lease's own PEXPIREAT so both age out together. A
// missing lease hash (a closed lease whose hash aged out, §11.4) writes no
// released_units but still follows its form — the money already returned
// through the lease's expiry.
var releaseCanonicalWalletReservationScript = redis.NewScript(`
	local marker = redis.call('GET', KEYS[2])
	if marker == false then return {1} end
	if marker ~= ARGV[1] then return {6} end
	if ARGV[3] == '0' and redis.call('EXISTS', KEYS[3]) == 1 then return {7} end
	local lease_exists = redis.call('EXISTS', KEYS[1]) == 1
	if lease_exists then redis.call('HINCRBY', KEYS[1], 'released_units', tonumber(ARGV[2])) end
	if ARGV[3] == '0' then
		redis.call('SET', KEYS[3], ARGV[1])
		local expires_at = tonumber(redis.call('HGET', KEYS[1], 'expires_at_ms') or '0')
		if expires_at > 0 then redis.call('PEXPIREAT', KEYS[3], expires_at) else redis.call('PEXPIRE', KEYS[3], 3600000) end
	else
		redis.call('DEL', KEYS[2])
	end
	return {0}
`)

func canonicalWalletLeaseKeyPrefix(platformUserID string) string {
	return canonicalWalletLeasePrefix + "{" + canonicalWalletUserHash(platformUserID) + "}:"
}

func canonicalWalletLeaseKey(platformUserID, leaseID string) string {
	return canonicalWalletLeaseKeyPrefix(platformUserID) + leaseID
}

func canonicalWalletCurrentKey(platformUserID string) string {
	return canonicalWalletCurrentPrefix + "{" + canonicalWalletUserHash(platformUserID) + "}"
}

func canonicalWalletReservationKey(platformUserID, eventID string) string {
	eventSum := sha256.Sum256([]byte(strings.TrimSpace(eventID)))
	return "canonical_wallet:reservation:{" + canonicalWalletUserHash(platformUserID) + "}:" + hex.EncodeToString(eventSum[:])
}

// canonicalWalletReleaseMarkerKey (§11.2): the partial release form's
// idempotency marker — same user hash tag (same cluster slot) as the
// reservation marker it gates, so the script touches one slot only.
func canonicalWalletReleaseMarkerKey(platformUserID, eventID string) string {
	eventSum := sha256.Sum256([]byte(strings.TrimSpace(eventID)))
	return "canonical_wallet:released:{" + canonicalWalletUserHash(platformUserID) + "}:" + hex.EncodeToString(eventSum[:])
}

// Hold keys (3.4b, §10.3). The per-hold hash and the per-user set share the
// user's {hash} tag with the lease keys, so every script touches one slot;
// hold_users and the reaper tick are deliberately untagged (global sets).
const canonicalWalletHoldPrefix = "canonical_wallet:hold:"
const canonicalWalletHoldSetPrefix = "canonical_wallet:holds:"
const canonicalWalletHoldUsersKey = "canonical_wallet:hold_users"
const canonicalWalletReaperTickKey = "canonical_wallet:reaper:tick"
const canonicalWalletHoldUserEmptyPrefix = "canonical_wallet:holds_empty:"

func canonicalWalletHoldKey(platformUserID, authorizationID string) string {
	return canonicalWalletHoldPrefix + "{" + canonicalWalletUserHash(platformUserID) + "}:" + authorizationID
}

func canonicalWalletHoldSetKey(platformUserID string) string {
	return canonicalWalletHoldSetPrefix + "{" + canonicalWalletUserHash(platformUserID) + "}"
}

func canonicalWalletHoldUserEmptyKey(platformUserID string) string {
	return canonicalWalletHoldUserEmptyPrefix + "{" + canonicalWalletUserHash(platformUserID) + "}"
}

func canonicalWalletUserHash(platformUserID string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(platformUserID)))
	return hex.EncodeToString(sum[:])
}

func (c *gatewayCache) InstallCanonicalWalletLease(ctx context.Context, lease service.CanonicalWalletLease) error {
	if c == nil || c.rdb == nil {
		return errors.New("canonical wallet Redis store unavailable")
	}
	if strings.TrimSpace(lease.PlatformUserID) == "" || strings.TrimSpace(lease.LeaseID) == "" || lease.BudgetUnits <= 0 || lease.ExpiresAt.IsZero() {
		return errors.New("invalid canonical wallet lease")
	}
	if lease.ConsumedUnits < 0 || lease.ConsumedUnits > lease.BudgetUnits {
		return errors.New("invalid canonical wallet lease consumption")
	}
	if err := ensureRedisLuaSafeInt64(lease.BudgetUnits, lease.ConsumedUnits); err != nil {
		return err
	}
	currency, err := service.RequireCNYBillingCurrency(lease.Currency)
	if err != nil {
		return err
	}
	return installCanonicalWalletLeaseScript.Run(ctx, c.rdb,
		[]string{canonicalWalletLeaseKey(lease.PlatformUserID, lease.LeaseID), canonicalWalletCurrentKey(lease.PlatformUserID)},
		lease.LeaseID, strings.TrimSpace(lease.PlatformUserID), currency,
		lease.ConsumedUnits, lease.ExpiresAt.UnixMilli(), lease.BudgetUnits, canonicalWalletLeaseKeyPrefix(lease.PlatformUserID),
	).Err()
}

func (c *gatewayCache) GetCanonicalWalletLease(ctx context.Context, platformUserID string) (*service.CanonicalWalletLease, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("canonical wallet Redis store unavailable")
	}
	currentID, err := c.rdb.Get(ctx, canonicalWalletCurrentKey(platformUserID)).Result()
	if err == redis.Nil {
		return nil, service.ErrCanonicalWalletLeaseMissing
	}
	if err != nil {
		return nil, err
	}
	return c.GetCanonicalWalletLeaseByID(ctx, platformUserID, currentID)
}

func (c *gatewayCache) GetCanonicalWalletLeaseByID(ctx context.Context, platformUserID, leaseID string) (*service.CanonicalWalletLease, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("canonical wallet Redis store unavailable")
	}
	values, err := c.rdb.HGetAll(ctx, canonicalWalletLeaseKey(platformUserID, leaseID)).Result()
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, service.ErrCanonicalWalletLeaseMissing
	}
	return parseCanonicalWalletLease(values)
}

func (c *gatewayCache) ReserveCanonicalWalletLease(ctx context.Context, platformUserID, leaseID, currency, eventID string, amountUnits int64, now time.Time) (*service.CanonicalWalletReservation, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("canonical wallet Redis store unavailable")
	}
	if strings.TrimSpace(eventID) == "" || amountUnits <= 0 || strings.TrimSpace(leaseID) == "" {
		return nil, errors.New("canonical wallet reservation requires a lease id, positive amount, and event id")
	}
	if err := ensureRedisLuaSafeInt64(amountUnits); err != nil {
		return nil, err
	}
	strictCurrency, err := service.RequireCNYBillingCurrency(currency)
	if err != nil {
		return nil, err
	}
	result, err := reserveCanonicalWalletLeaseScript.Run(ctx, c.rdb,
		[]string{canonicalWalletLeaseKey(platformUserID, leaseID), canonicalWalletReservationKey(platformUserID, eventID)},
		strictCurrency, amountUnits, eventID, now.UnixMilli(),
	).Slice()
	if err != nil {
		return nil, err
	}
	return decodeCanonicalWalletReservationResult(result, platformUserID)
}

func (c *gatewayCache) SealCanonicalWalletLease(ctx context.Context, platformUserID, leaseID string) (int64, int64, error) {
	if c == nil || c.rdb == nil {
		return 0, 0, errors.New("canonical wallet Redis store unavailable")
	}
	if strings.TrimSpace(platformUserID) == "" || strings.TrimSpace(leaseID) == "" {
		return 0, 0, errors.New("canonical wallet seal requires a platform user id and a lease id")
	}
	result, err := sealCanonicalWalletLeaseScript.Run(ctx, c.rdb,
		[]string{canonicalWalletLeaseKey(platformUserID, leaseID), canonicalWalletCurrentKey(platformUserID)},
		leaseID,
	).Slice()
	if err != nil {
		return 0, 0, err
	}
	if len(result) == 0 {
		return 0, 0, errors.New("canonical wallet seal returned no result")
	}
	code, err := redisResultInt64(result[0])
	if err != nil {
		return 0, 0, err
	}
	if code == 1 {
		return 0, 0, service.ErrCanonicalWalletLeaseMissing
	}
	if len(result) != 3 {
		return 0, 0, errors.New("canonical wallet seal returned an invalid result")
	}
	preSealConsumed, err := redisResultInt64(result[1])
	if err != nil {
		return 0, 0, err
	}
	released, err := redisResultInt64(result[2])
	if err != nil {
		return 0, 0, err
	}
	return preSealConsumed, released, nil
}

func (c *gatewayCache) ArmCanonicalWalletHold(ctx context.Context, platformUserID, leaseID, currency, authorizationID string, units int64, graceMS int64, now time.Time) (string, int64, bool, error) {
	if c == nil || c.rdb == nil {
		return "", 0, false, errors.New("canonical wallet Redis store unavailable")
	}
	if strings.TrimSpace(platformUserID) == "" || strings.TrimSpace(leaseID) == "" || strings.TrimSpace(authorizationID) == "" || units <= 0 || graceMS < 0 {
		return "", 0, false, errors.New("canonical wallet hold arm requires a platform user id, a lease id, an authorization id and positive units")
	}
	if err := ensureRedisLuaSafeInt64(units); err != nil {
		return "", 0, false, err
	}
	strictCurrency, err := service.RequireCNYBillingCurrency(currency)
	if err != nil {
		return "", 0, false, err
	}
	result, err := armCanonicalWalletHoldScript.Run(ctx, c.rdb,
		[]string{canonicalWalletLeaseKey(platformUserID, leaseID), canonicalWalletHoldKey(platformUserID, authorizationID), canonicalWalletHoldSetKey(platformUserID)},
		strictCurrency, units, now.UnixMilli(), authorizationID, graceMS,
	).Slice()
	if err != nil {
		return "", 0, false, err
	}
	if len(result) == 0 {
		return "", 0, false, errors.New("canonical wallet hold arm returned no result")
	}
	code, err := redisResultInt64(result[0])
	if err != nil {
		return "", 0, false, err
	}
	switch code {
	case 1:
		return "", 0, false, service.ErrCanonicalWalletLeaseMissing
	case 2:
		return "", 0, false, service.ErrCanonicalWalletLeaseCurrencyMismatch
	case 3:
		return "", 0, false, service.ErrCanonicalWalletLeaseExpired
	case 4:
		return "", 0, false, service.ErrCanonicalWalletLeaseExhausted
	case 5:
		if len(result) != 3 {
			return "", 0, false, errors.New("canonical wallet hold arm returned an invalid duplicate reply")
		}
		held, err := redisResultInt64(result[2])
		if err != nil {
			return "", 0, false, err
		}
		return fmt.Sprint(result[1]), held, true, nil
	case 0:
		if len(result) != 3 {
			return "", 0, false, errors.New("canonical wallet hold arm returned an invalid reply")
		}
		held, err := redisResultInt64(result[2])
		if err != nil {
			return "", 0, false, err
		}
		// §10.3's stated window: hold_users lives in another slot, so this
		// SADD runs AFTER the arm script. A crash in the window leaves a
		// hold the reaper never enumerates; the hash's PEXPIREAT is the
		// backstop and the two-tick prune covers the interleaving.
		if err := c.rdb.SAdd(ctx, canonicalWalletHoldUsersKey, strings.TrimSpace(platformUserID)).Err(); err != nil {
			return "", 0, false, err
		}
		return fmt.Sprint(result[1]), held, false, nil
	default:
		return "", 0, false, fmt.Errorf("unknown canonical wallet hold arm code %d", code)
	}
}

func (c *gatewayCache) ReleaseCanonicalWalletHold(ctx context.Context, platformUserID, authorizationID, stateAfter, classAfter string) (int64, error) {
	if c == nil || c.rdb == nil {
		return 0, errors.New("canonical wallet Redis store unavailable")
	}
	if strings.TrimSpace(platformUserID) == "" || strings.TrimSpace(authorizationID) == "" {
		return 0, errors.New("canonical wallet hold release requires a platform user id and an authorization id")
	}
	if stateAfter != "released" && stateAfter != "abandoned" {
		return 0, errors.New("canonical wallet hold release state_after must be released or abandoned")
	}
	// A script cannot derive a key: the lease hash (KEYS[3]) is computed from
	// the hold's own lease_id, read here first. A hold that vanishes between
	// the read and the script answers {1} — idempotent.
	leaseID, err := c.rdb.HGet(ctx, canonicalWalletHoldKey(platformUserID, authorizationID), "lease_id").Result()
	if err == redis.Nil {
		return 0, service.ErrCanonicalWalletHoldMissing
	}
	if err != nil {
		return 0, err
	}
	result, err := releaseCanonicalWalletHoldScript.Run(ctx, c.rdb,
		[]string{canonicalWalletHoldKey(platformUserID, authorizationID), canonicalWalletHoldSetKey(platformUserID), canonicalWalletLeaseKey(platformUserID, leaseID)},
		authorizationID, stateAfter, classAfter,
	).Slice()
	if err != nil {
		return 0, err
	}
	if len(result) == 0 {
		return 0, errors.New("canonical wallet hold release returned no result")
	}
	code, err := redisResultInt64(result[0])
	if err != nil {
		return 0, err
	}
	switch code {
	case 1:
		return 0, service.ErrCanonicalWalletHoldMissing
	case 7:
		state := fmt.Sprint(result[1])
		eventID := ""
		if len(result) > 2 {
			eventID = fmt.Sprint(result[2])
		}
		return 0, &service.CanonicalWalletHoldNotArmedError{State: state, EventID: eventID}
	case 0:
		if len(result) != 2 {
			return 0, errors.New("canonical wallet hold release returned an invalid reply")
		}
		return redisResultInt64(result[1])
	default:
		return 0, fmt.Errorf("unknown canonical wallet hold release code %d", code)
	}
}

func (c *gatewayCache) ConvertCanonicalWalletHold(ctx context.Context, platformUserID, authorizationID, eventID string, actualUnits int64, now time.Time) (service.CanonicalWalletHoldConversion, error) {
	if c == nil || c.rdb == nil {
		return service.CanonicalWalletHoldConversion{}, errors.New("canonical wallet Redis store unavailable")
	}
	if strings.TrimSpace(platformUserID) == "" || strings.TrimSpace(authorizationID) == "" || strings.TrimSpace(eventID) == "" || actualUnits < 0 {
		return service.CanonicalWalletHoldConversion{}, errors.New("canonical wallet hold convert requires a platform user id, an authorization id, an event id and non-negative units")
	}
	if err := ensureRedisLuaSafeInt64(actualUnits); err != nil {
		return service.CanonicalWalletHoldConversion{}, err
	}
	// The conversion script needs the lease hash as KEYS[3] and the event's
	// reservation marker as KEYS[4]; both derive from the hold's own lease_id.
	leaseID, err := c.rdb.HGet(ctx, canonicalWalletHoldKey(platformUserID, authorizationID), "lease_id").Result()
	if err == redis.Nil {
		return service.CanonicalWalletHoldConversion{Code: 1}, nil
	}
	if err != nil {
		return service.CanonicalWalletHoldConversion{}, err
	}
	result, err := convertCanonicalWalletHoldScript.Run(ctx, c.rdb,
		[]string{
			canonicalWalletHoldKey(platformUserID, authorizationID), canonicalWalletHoldSetKey(platformUserID),
			canonicalWalletLeaseKey(platformUserID, leaseID), canonicalWalletReservationKey(platformUserID, eventID),
		},
		authorizationID, eventID, actualUnits, now.UnixMilli(),
	).Slice()
	if err != nil {
		return service.CanonicalWalletHoldConversion{}, err
	}
	if len(result) == 0 {
		return service.CanonicalWalletHoldConversion{}, errors.New("canonical wallet hold convert returned no result")
	}
	code, err := redisResultInt64(result[0])
	if err != nil {
		return service.CanonicalWalletHoldConversion{}, err
	}
	conv := service.CanonicalWalletHoldConversion{Code: int(code)}
	switch code {
	case 0:
		if len(result) != 2 {
			return service.CanonicalWalletHoldConversion{}, errors.New("canonical wallet hold convert returned an invalid reply")
		}
		conv.LeaseID = fmt.Sprint(result[1])
	case 3, 4:
		// no extras on the wire
	case 7:
		if len(result) != 4 {
			return service.CanonicalWalletHoldConversion{}, errors.New("canonical wallet hold convert returned an invalid not-armed reply")
		}
		conv.State = fmt.Sprint(result[1])
		conv.EventID = fmt.Sprint(result[2])
		conv.LeaseID = fmt.Sprint(result[3])
	case 1:
	default:
		return service.CanonicalWalletHoldConversion{}, fmt.Errorf("unknown canonical wallet hold convert code %d", code)
	}
	return conv, nil
}

func (c *gatewayCache) GetCanonicalWalletHold(ctx context.Context, platformUserID, authorizationID string) (*service.CanonicalWalletHold, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("canonical wallet Redis store unavailable")
	}
	values, err := c.rdb.HGetAll(ctx, canonicalWalletHoldKey(platformUserID, authorizationID)).Result()
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, service.ErrCanonicalWalletHoldMissing
	}
	return parseCanonicalWalletHold(authorizationID, values)
}

func parseCanonicalWalletHold(authorizationID string, values map[string]string) (*service.CanonicalWalletHold, error) {
	held, err := strconv.ParseInt(values["held_units"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse canonical wallet hold units: %w", err)
	}
	armedAtMS, err := strconv.ParseInt(values["armed_at_ms"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse canonical wallet hold armed_at: %w", err)
	}
	return &service.CanonicalWalletHold{
		AuthorizationID: authorizationID, LeaseID: values["lease_id"], HeldUnits: held,
		ArmedAt: time.UnixMilli(armedAtMS).UTC(), Class: values["class"], State: values["state"], EventID: values["event_id"],
	}, nil
}

func (c *gatewayCache) ListCanonicalWalletHolds(ctx context.Context, platformUserID string, limit int) ([]string, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("canonical wallet Redis store unavailable")
	}
	if limit <= 0 {
		limit = 200
	}
	members, err := c.rdb.SMembers(ctx, canonicalWalletHoldSetKey(platformUserID)).Result()
	if err != nil {
		return nil, err
	}
	if len(members) > limit {
		members = members[:limit]
	}
	return members, nil
}

func (c *gatewayCache) ListCanonicalWalletHoldUsers(ctx context.Context, cursor uint64, count int64) ([]string, uint64, error) {
	if c == nil || c.rdb == nil {
		return nil, 0, errors.New("canonical wallet Redis store unavailable")
	}
	if count <= 0 {
		count = 100
	}
	keys, next, err := c.rdb.SScan(ctx, canonicalWalletHoldUsersKey, cursor, "", count).Result()
	if err != nil {
		return nil, 0, err
	}
	return keys, next, nil
}

func (c *gatewayCache) PruneCanonicalWalletHoldUser(ctx context.Context, platformUserID string) error {
	if c == nil || c.rdb == nil {
		return errors.New("canonical wallet Redis store unavailable")
	}
	return c.rdb.SRem(ctx, canonicalWalletHoldUsersKey, strings.TrimSpace(platformUserID)).Err()
}

func (c *gatewayCache) TryCanonicalWalletReaperLease(ctx context.Context, ttl time.Duration) (bool, error) {
	if c == nil || c.rdb == nil {
		return false, errors.New("canonical wallet Redis store unavailable")
	}
	return c.rdb.SetNX(ctx, canonicalWalletReaperTickKey, "1", ttl).Result()
}

func (c *gatewayCache) ForgetCanonicalWalletHold(ctx context.Context, platformUserID, authorizationID string) error {
	if c == nil || c.rdb == nil {
		return errors.New("canonical wallet Redis store unavailable")
	}
	return c.rdb.SRem(ctx, canonicalWalletHoldSetKey(platformUserID), authorizationID).Err()
}

func (c *gatewayCache) MarkCanonicalWalletHoldUserEmpty(ctx context.Context, platformUserID string, ttl time.Duration) (bool, error) {
	if c == nil || c.rdb == nil {
		return false, errors.New("canonical wallet Redis store unavailable")
	}
	ok, err := c.rdb.SetNX(ctx, canonicalWalletHoldUserEmptyKey(platformUserID), "1", ttl).Result()
	if err != nil {
		return false, err
	}
	return !ok, nil // alreadySeen = the key existed (SETNX answered false)
}

func (c *gatewayCache) ClearCanonicalWalletHoldUserEmpty(ctx context.Context, platformUserID string) error {
	if c == nil || c.rdb == nil {
		return errors.New("canonical wallet Redis store unavailable")
	}
	return c.rdb.Del(ctx, canonicalWalletHoldUserEmptyKey(platformUserID)).Err()
}

func (c *gatewayCache) MarkCanonicalWalletHoldClass(ctx context.Context, platformUserID, authorizationID, class string) (*service.CanonicalWalletHold, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("canonical wallet Redis store unavailable")
	}
	if strings.TrimSpace(platformUserID) == "" || strings.TrimSpace(authorizationID) == "" || strings.TrimSpace(class) == "" {
		return nil, errors.New("canonical wallet hold mark-class requires a platform user id, an authorization id and a class")
	}
	result, err := markCanonicalWalletHoldClassScript.Run(ctx, c.rdb,
		[]string{canonicalWalletHoldKey(platformUserID, authorizationID)},
		class,
	).Slice()
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, errors.New("canonical wallet hold mark-class returned no result")
	}
	code, err := redisResultInt64(result[0])
	if err != nil {
		return nil, err
	}
	switch code {
	case 1:
		return nil, service.ErrCanonicalWalletHoldMissing
	case 7:
		state := fmt.Sprint(result[1])
		eventID := ""
		if len(result) > 2 {
			eventID = fmt.Sprint(result[2])
		}
		return nil, &service.CanonicalWalletHoldNotArmedError{State: state, EventID: eventID}
	case 0:
		return c.GetCanonicalWalletHold(ctx, platformUserID, authorizationID)
	default:
		return nil, fmt.Errorf("unknown canonical wallet hold mark-class code %d", code)
	}
}

// ReleaseCanonicalWalletReservation (Phase 3.5, redesign §11.2) releases a
// settlement event's reservation on the lease its marker names. Two callers,
// two dispositions: named_lease_id and the late capture pass dropMarker =
// true (the event was captured elsewhere or the row is unbound — nothing
// more will be captured here, and the DEL is the idempotency); the split
// passes dropMarker = false (the parent redelivers against this lease and
// its marker must survive for the reserve script's {5}) and is gated on the
// per-event release marker. Returns true iff the script ran its write path;
// {1}/{6}/{7} answer (false, nil).
func (c *gatewayCache) ReleaseCanonicalWalletReservation(ctx context.Context, platformUserID, leaseID, eventID string, units int64, dropMarker bool) (bool, error) {
	if c == nil || c.rdb == nil {
		return false, errors.New("canonical wallet Redis store unavailable")
	}
	if strings.TrimSpace(platformUserID) == "" || strings.TrimSpace(leaseID) == "" || strings.TrimSpace(eventID) == "" {
		return false, errors.New("canonical wallet reservation release requires a platform user id, a lease id and an event id")
	}
	if err := ensureRedisLuaSafeInt64(units); err != nil {
		return false, err
	}
	if units <= 0 {
		return false, errors.New("canonical wallet reservation release requires positive units")
	}
	drop := "0"
	if dropMarker {
		drop = "1"
	}
	result, err := releaseCanonicalWalletReservationScript.Run(ctx, c.rdb,
		[]string{
			canonicalWalletLeaseKey(platformUserID, leaseID),
			canonicalWalletReservationKey(platformUserID, eventID),
			canonicalWalletReleaseMarkerKey(platformUserID, eventID),
		},
		leaseID, units, drop,
	).Slice()
	if err != nil {
		return false, err
	}
	if len(result) == 0 {
		return false, errors.New("canonical wallet reservation release returned no result")
	}
	code, err := redisResultInt64(result[0])
	if err != nil {
		return false, err
	}
	switch code {
	case 0:
		return true, nil
	case 1, 6, 7:
		return false, nil
	default:
		return false, fmt.Errorf("unknown canonical wallet reservation release code %d", code)
	}
}

// decodeCanonicalWalletReservationResult turns the reservation Lua script's
// raw reply into a reservation or a typed error. It is deliberately a pure
// function over the raw []any and not a method: every defensive branch here
// guards against a reply shape the shipped script cannot actually produce, so
// the ONLY honest way to test them is to hand the decoder those shapes
// directly. Inlined into the Redis call, they were untestable without faking
// a driver — which is not a thing this repository does.
func decodeCanonicalWalletReservationResult(result []any, platformUserID string) (*service.CanonicalWalletReservation, error) {
	if len(result) == 0 {
		return nil, errors.New("canonical wallet reservation returned no result")
	}
	code, err := redisResultInt64(result[0])
	if err != nil {
		return nil, err
	}
	switch code {
	case 1:
		return nil, service.ErrCanonicalWalletLeaseMissing
	case 2:
		return nil, service.ErrCanonicalWalletLeaseCurrencyMismatch
	case 3:
		return nil, service.ErrCanonicalWalletLeaseExpired
	case 4:
		return nil, service.ErrCanonicalWalletLeaseExhausted
	case 6:
		return nil, service.ErrCanonicalWalletReservationConflict
	case 0, 5:
		if len(result) != 6 {
			return nil, errors.New("canonical wallet reservation returned an invalid snapshot")
		}
		budget, err := redisResultInt64(result[3])
		if err != nil {
			return nil, err
		}
		consumed, err := redisResultInt64(result[4])
		if err != nil {
			return nil, err
		}
		expiresAtMS, err := redisResultInt64(result[5])
		if err != nil {
			return nil, err
		}
		return &service.CanonicalWalletReservation{Lease: service.CanonicalWalletLease{
			LeaseID: fmt.Sprint(result[1]), PlatformUserID: strings.TrimSpace(platformUserID), Currency: fmt.Sprint(result[2]),
			BudgetUnits: budget, ConsumedUnits: consumed, ExpiresAt: time.UnixMilli(expiresAtMS).UTC(),
		}, Duplicate: code == 5}, nil
	default:
		return nil, fmt.Errorf("unknown canonical wallet reservation code %d", code)
	}
}

func parseCanonicalWalletLease(values map[string]string) (*service.CanonicalWalletLease, error) {
	budget, err := strconv.ParseInt(values["budget_units"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse canonical wallet budget: %w", err)
	}
	consumed, err := strconv.ParseInt(values["consumed_units"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse canonical wallet consumption: %w", err)
	}
	expiresAtMS, err := strconv.ParseInt(values["expires_at_ms"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse canonical wallet expiry: %w", err)
	}
	return &service.CanonicalWalletLease{
		LeaseID: values["lease_id"], PlatformUserID: values["platform_user_id"], Currency: values["currency"],
		BudgetUnits: budget, ConsumedUnits: consumed, ExpiresAt: time.UnixMilli(expiresAtMS).UTC(),
	}, nil
}

func redisResultInt64(value any) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	case []byte:
		return strconv.ParseInt(string(v), 10, 64)
	default:
		return 0, fmt.Errorf("unexpected Redis integer type %T", value)
	}
}

var _ service.CanonicalWalletLeaseStore = (*gatewayCache)(nil)
