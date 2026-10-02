package repository

import (
	"context"
	"errors"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
	"time"
)

// All keys share the existing user hash slot. Validate every share before
// changing any counter; a concurrent LLM/media attempt can win only once.
var armCanonicalWalletPoolScript = redis.NewScript(`
 local count=tonumber(ARGV[1]);local now=tonumber(ARGV[2]);local grace=tonumber(ARGV[3]);local duplicates=0
 for i=1,count do
  local lease=KEYS[2*i-1];local hold=KEYS[2*i];local units=tonumber(ARGV[3+2*i]);local auth=ARGV[2+2*i]
  if redis.call('EXISTS',hold)==1 then
   if redis.call('HGET',hold,'lease_id')~=redis.call('HGET',lease,'lease_id') or redis.call('HGET',hold,'held_units')~=ARGV[3+2*i] or redis.call('HGET',hold,'state')~='armed' then return {6} end
   duplicates=duplicates+1
  else
   if redis.call('EXISTS',lease)==0 then return {1} end
   if redis.call('HGET',lease,'currency')~='USD' then return {2} end
   if redis.call('HGET',lease,'sealed')=='1' or tonumber(redis.call('HGET',lease,'expires_at_ms') or '0')<=now then return {3} end
   local budget=tonumber(redis.call('HGET',lease,'budget_units') or '0');local consumed=tonumber(redis.call('HGET',lease,'consumed_units') or '0');local released=tonumber(redis.call('HGET',lease,'released_units') or '0')
   if units<=0 or consumed-released+units>budget or consumed+units>9007199254740991 then return {4} end
  end
 end
 if duplicates==count then return {0} end
 if duplicates>0 then return {6} end
 for i=1,count do
  local lease=KEYS[2*i-1];local hold=KEYS[2*i];local auth=ARGV[2+2*i];local units=tonumber(ARGV[3+2*i]);local consumed=tonumber(redis.call('HGET',lease,'consumed_units') or '0')
  redis.call('HSET',lease,'consumed_units',consumed+units)
  redis.call('HSET',hold,'lease_id',redis.call('HGET',lease,'lease_id'),'held_units',units,'armed_at_ms',now,'class','','state','armed','event_id','')
  redis.call('PEXPIREAT',hold,tonumber(redis.call('HGET',lease,'expires_at_ms'))+grace)
  redis.call('SADD',KEYS[2*count+1],auth)
 end
 return {0}
`)

func (c *canonicalWalletRedisStore) ArmCanonicalWalletPool(ctx context.Context, user string, segments []service.AuthorizationSegment, grace int64, now time.Time) error {
	if len(segments) == 0 || len(segments) > 1000 || user == "" {
		return errors.New("invalid wallet authorization pool")
	}
	keys := []string{}
	args := []any{len(segments), now.UnixMilli(), grace}
	seen := map[string]bool{}
	for _, s := range segments {
		if s.AuthorizationID == "" || s.LeaseID == "" || seen[s.LeaseID] {
			return errors.New("duplicate wallet pool lease")
		}
		seen[s.LeaseID] = true
		if err := ensureRedisLuaSafeInt64(s.HeldUnits); err != nil {
			return err
		}
		keys = append(keys, canonicalWalletLeaseKey(user, s.LeaseID), canonicalWalletHoldKey(user, s.AuthorizationID))
		args = append(args, s.AuthorizationID, s.HeldUnits)
	}
	keys = append(keys, canonicalWalletHoldSetKey(user))
	value, err := armCanonicalWalletPoolScript.Run(ctx, c.rdb, keys, args...).Int64Slice()
	if err != nil {
		return err
	}
	if len(value) != 1 {
		return errors.New("invalid pool arm reply")
	}
	switch value[0] {
	case 0:
		return c.rdb.SAdd(ctx, canonicalWalletHoldUsersKey, user).Err()
	case 1:
		return service.ErrCanonicalWalletLeaseMissing
	case 2:
		return service.ErrCanonicalWalletLeaseCurrencyMismatch
	case 3:
		return service.ErrCanonicalWalletLeaseExpired
	case 4:
		return service.ErrCanonicalWalletLeaseExhausted
	default:
		return errors.New("wallet authorization pool is not replayable")
	}
}
