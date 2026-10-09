package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

var protectMediaHoldScript = redis.NewScript(`
 local restored=0
 local revision=tonumber(ARGV[10]);local returned=tonumber(ARGV[11]);local funded=tonumber(ARGV[12])
 local previous=tonumber(redis.call('HGET',KEYS[4],'return_revision') or redis.call('HGET',KEYS[1],'return_revision') or '0')
 local previous_returned=tonumber(redis.call('HGET',KEYS[4],'returned_units') or redis.call('HGET',KEYS[1],'returned_units') or '0')
 if revision<previous or returned<previous_returned or funded-returned~=tonumber(ARGV[3])
  or ((redis.call('HGET',KEYS[4],'funding_frozen')=='1' or redis.call('HGET',KEYS[1],'funding_frozen')=='1') and ARGV[13]~='1')
 then return redis.error_reply('stale media funding basis') end

 if redis.call('EXISTS',KEYS[1])==0 then
  redis.call('HSET',KEYS[1],'lease_id',ARGV[1],'platform_user_id',ARGV[2],'currency','USD','budget_units',ARGV[3],'consumed_units',ARGV[4],'released_units',ARGV[5],'expires_at_ms',ARGV[6],'sealed','1','media_recovered','1','return_revision',revision,'returned_units',returned,'funded_units',funded,'funding_frozen',ARGV[13],'funding_scope',ARGV[14],'funding_owner_id',ARGV[15],'funding_issuance_key',ARGV[16],'budget_revision',ARGV[17]);restored=1
 end
 if ARGV[13]=='1' then
  if revision>previous then redis.call('HDEL',KEYS[4],'receipt_signature') end
  redis.call('HSET',KEYS[4],'funding_frozen','1','return_revision',revision,'returned_units',returned,'funded_units',funded);redis.call('PERSIST',KEYS[4])
 end
 if redis.call('HGET',KEYS[1],'platform_user_id')~=ARGV[2] then return redis.error_reply('media lease owner mismatch') end
 local old_scope=redis.call('HGET',KEYS[1],'funding_scope')
 if old_scope and old_scope~='' and (old_scope~=ARGV[14] or (redis.call('HGET',KEYS[1],'funding_owner_id') or '')~=ARGV[15]
  or (redis.call('HGET',KEYS[1],'funding_issuance_key') or '')~=ARGV[16]) then return redis.error_reply('media funding identity conflict') end
 if ARGV[13]=='1' then
  local net=math.max(tonumber(redis.call('HGET',KEYS[1],'consumed_units') or '0'),tonumber(ARGV[4]))-tonumber(redis.call('HGET',KEYS[1],'released_units') or '0')
  if net>tonumber(ARGV[3]) then return redis.error_reply('media frozen backing conflict') end
  redis.call('HSET',KEYS[1],'budget_units',ARGV[3],'return_revision',revision,'returned_units',returned,'funded_units',funded,'funding_frozen','1','budget_revision',ARGV[17])
 end
 if redis.call('EXISTS',KEYS[2])==0 then
  redis.call('HSET',KEYS[2],'lease_id',ARGV[1],'held_units',ARGV[7],'armed_at_ms',ARGV[8],'class','indeterminate','state','armed','event_id','')
  if restored==0 then redis.call('HSET',KEYS[1],'sealed','1','media_recovered','1');local current=tonumber(redis.call('HGET',KEYS[1],'consumed_units') or '0');redis.call('HSET',KEYS[1],'consumed_units',math.max(current,tonumber(ARGV[4]))) end
 end
 if redis.call('HGET',KEYS[2],'lease_id')~=ARGV[1] or redis.call('HGET',KEYS[2],'held_units')~=ARGV[7] then return redis.error_reply('media hold mismatch') end
 redis.call('HSET',KEYS[1],'durable_media','1');redis.call('PERSIST',KEYS[1]);redis.call('PERSIST',KEYS[2])
 if redis.call('HGET',KEYS[2],'state')=='armed' then redis.call('SADD',KEYS[3],ARGV[9]) end
 return 1
`)

func (c *canonicalWalletRedisStore) ProtectMediaHold(ctx context.Context, lease service.CanonicalWalletLease, hold service.CanonicalWalletHold) error {
	if c == nil || c.rdb == nil {
		return errors.New("media wallet unavailable")
	}
	if lease.LeaseID == "" || hold.AuthorizationID == "" || hold.HeldUnits <= 0 {
		return errors.New("invalid media protection")
	}
	if err := ensureRedisLuaSafeInt64(lease.BudgetUnits, lease.ConsumedUnits, lease.ReleasedUnits, hold.HeldUnits, lease.FundingPrincipalUnits(), lease.ReturnedUnits, lease.ReturnRevision, lease.BudgetRevision); err != nil {
		return err
	}
	_, err := protectMediaHoldScript.Run(ctx, c.rdb, []string{canonicalWalletLeaseKey(lease.PlatformUserID, lease.LeaseID), canonicalWalletHoldKey(lease.PlatformUserID, hold.AuthorizationID), canonicalWalletHoldSetKey(lease.PlatformUserID), canonicalWalletFundingTombstoneKey(lease.PlatformUserID, lease.LeaseID)}, lease.LeaseID, lease.PlatformUserID, lease.BudgetUnits, lease.ConsumedUnits, lease.ReleasedUnits, lease.ExpiresAt.UnixMilli(), hold.HeldUnits, hold.ArmedAt.UnixMilli(), hold.AuthorizationID, lease.ReturnRevision, lease.ReturnedUnits, lease.FundingPrincipalUnits(), boolWalletFlag(lease.FundingFrozen), lease.FundingScope, lease.FundingOwnerID, lease.FundingIssuanceKey, lease.BudgetRevision).Result()
	if err == nil {
		err = c.rdb.SAdd(ctx, canonicalWalletHoldUsersKey, lease.PlatformUserID).Err()
	}
	return err
}
func (c *canonicalWalletRedisStore) UnprotectMediaLease(ctx context.Context, user, lease, auth, event string, remaining bool) error {
	flag := "0"
	if remaining {
		flag = "1"
	}
	_, err := redis.NewScript(`
 if ARGV[3]=='0' and redis.call('EXISTS',KEYS[1])==1 then redis.call('HDEL',KEYS[1],'durable_media');local expires=tonumber(redis.call('HGET',KEYS[1],'expires_at_ms'));redis.call('PEXPIREAT',KEYS[1],math.max(expires,tonumber(ARGV[1]))+tonumber(ARGV[2])) end
 redis.call('PEXPIRE',KEYS[2],ARGV[2]);redis.call('PEXPIRE',KEYS[3],ARGV[2])
 return 1`).Run(ctx, c.rdb, []string{canonicalWalletLeaseKey(user, lease), canonicalWalletHoldKey(user, auth), canonicalWalletReservationKey(user, event)}, time.Now().UnixMilli(), int64(1800000), flag).Result()
	return err
}

var readMediaWalletStateScript = redis.NewScript(`
 local count=tonumber(ARGV[2]);local out={}; for i=1,count do out[#out+1]=redis.call('HMGET',KEYS[i],'lease_id','budget_units','consumed_units','released_units','expires_at_ms','sealed','media_recovered','funded_units','returned_units','return_revision','budget_revision','funding_frozen') end
 local ids=redis.call('SMEMBERS',KEYS[count+1]);if #ids>1000 then return redis.error_reply('too many holds') end;table.sort(ids)
 local holds={};for _,id in ipairs(ids) do local h=redis.call('HMGET',ARGV[1]..id,'lease_id','held_units','armed_at_ms','class','state','event_id');h[#h+1]=id;holds[#holds+1]=h end
 local reservations={};for i=count+2,#KEYS do reservations[#reservations+1]=redis.call('GET',KEYS[i]) end
 return {out,holds,reservations}
`)

func (c *canonicalWalletRedisStore) ReadMediaWalletState(ctx context.Context, user string, ids, events []string) (service.MediaWalletRawState, error) {
	out := service.MediaWalletRawState{Leases: []service.MediaWalletRawLease{}, Holds: []service.CanonicalWalletHold{}, Reservations: map[string]string{}}
	keys := []string{}
	for _, id := range ids {
		keys = append(keys, canonicalWalletLeaseKey(user, id))
	}
	keys = append(keys, canonicalWalletHoldSetKey(user))
	for _, id := range events {
		keys = append(keys, canonicalWalletReservationKey(user, id))
	}
	value, err := readMediaWalletStateScript.Run(ctx, c.rdb, keys, canonicalWalletHoldKey(user, ""), len(ids)).Slice()
	if err != nil {
		return out, err
	}
	if len(value) != 3 {
		return out, errors.New("invalid wallet state reply")
	}
	leases, ok := value[0].([]any)
	if !ok || len(leases) != len(ids) {
		return out, errors.New("invalid wallet leases reply")
	}
	number := func(v any) (int64, error) {
		if v == nil {
			return 0, nil
		}
		return redisResultInt64(v)
	}
	for i, item := range leases {
		values, ok := item.([]any)
		if !ok || len(values) != 12 {
			return out, errors.New("invalid wallet lease")
		}
		r := service.MediaWalletRawLease{LeaseID: ids[i], Present: values[0] != nil}
		if r.Present {
			var e error
			r.BudgetUnits, e = number(values[1])
			if e != nil {
				return out, e
			}
			r.ConsumedUnits, e = number(values[2])
			if e != nil {
				return out, e
			}
			r.ReleasedUnits, e = number(values[3])
			if e != nil {
				return out, e
			}
			r.ExpiresAtMS, e = number(values[4])
			if e != nil {
				return out, e
			}
			r.Sealed = fmt.Sprint(values[5]) == "1"
			r.Recovered = fmt.Sprint(values[6]) == "1"
			r.FundedUnits, e = number(values[7])
			if e != nil {
				return out, e
			}
			r.ReturnedUnits, e = number(values[8])
			if e != nil {
				return out, e
			}
			r.ReturnRevision, e = number(values[9])
			if e != nil {
				return out, e
			}
			r.BudgetRevision, e = number(values[10])
			if e != nil {
				return out, e
			}
			r.FundingFrozen = fmt.Sprint(values[11]) == "1"
		}
		out.Leases = append(out.Leases, r)
	}
	holds, ok := value[1].([]any)
	if !ok {
		return out, errors.New("invalid wallet holds reply")
	}
	for _, item := range holds {
		v, ok := item.([]any)
		if !ok || len(v) != 7 {
			return out, errors.New("invalid wallet hold")
		}
		if v[0] == nil {
			return out, errors.New("hold set has an absent record")
		}
		units, e := number(v[1])
		if e != nil {
			return out, e
		}
		armed, e := number(v[2])
		if e != nil {
			return out, e
		}
		out.Holds = append(out.Holds, service.CanonicalWalletHold{AuthorizationID: fmt.Sprint(v[6]), LeaseID: fmt.Sprint(v[0]), HeldUnits: units, ArmedAt: time.UnixMilli(armed), Class: fmt.Sprint(v[3]), State: fmt.Sprint(v[4]), EventID: fmt.Sprint(v[5])})
	}
	markers, ok := value[2].([]any)
	if !ok || len(markers) != len(events) {
		return out, errors.New("invalid wallet markers reply")
	}
	for i, v := range markers {
		if v != nil {
			out.Reservations[events[i]] = fmt.Sprint(v)
		}
	}
	raw, _ := json.Marshal(out)
	hash := sha256.Sum256(raw)
	out.Fingerprint = hex.EncodeToString(hash[:])
	return out, nil
}

var _ service.MediaWalletStateStore = (*canonicalWalletRedisStore)(nil)
