package repository

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

func canonicalWalletFundingTombstoneKey(user, lease string) string {
	return canonicalWalletLeaseKey(user, lease) + ":funding"
}
func boolWalletFlag(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

var freezeCanonicalWalletFundingScript = redis.NewScript(`
 if redis.call('EXISTS',KEYS[1])==0 then return redis.error_reply('canonical funding lease missing') end
 local all=redis.call('HGETALL',KEYS[1]);local lease={};for i=1,#all,2 do lease[all[i]]=all[i+1] end
 if lease.platform_user_id~=ARGV[1] or lease.lease_id~=ARGV[2] then return redis.error_reply('canonical funding owner mismatch') end
 local ids=redis.call('SMEMBERS',KEYS[4]);if #ids>1000 then return redis.error_reply('canonical funding holds exceed bound') end
 table.sort(ids);local holds={}
 for _,id in ipairs(ids) do
  local key=ARGV[3]..id
  if redis.call('HGET',key,'lease_id')==ARGV[2] and redis.call('HGET',key,'state')=='armed' then
   holds[#holds+1]={authorization_id=id,held_units=redis.call('HGET',key,'held_units')}
  end
 end
 if #holds>128 then return redis.error_reply('canonical funding lease holds exceed bound') end
 redis.call('HSET',KEYS[1],'funding_frozen','1');redis.call('PERSIST',KEYS[1]);lease.funding_frozen='1'
 redis.call('HSET',KEYS[3],'funding_frozen','1','funded_units',lease.funded_units or lease.budget_units,
  'returned_units',lease.returned_units or '0','return_revision',lease.return_revision or '0')
 redis.call('PERSIST',KEYS[3])
 if redis.call('GET',KEYS[2])==ARGV[2] then redis.call('DEL',KEYS[2]) end
 return cjson.encode({lease=lease,holds=holds})
`)

func (c *canonicalWalletRedisStore) FreezeCanonicalWalletFunding(ctx context.Context, user, leaseID string) (*service.CanonicalWalletFundingBasis, error) {
	raw, err := freezeCanonicalWalletFundingScript.Run(ctx, c.rdb, []string{canonicalWalletLeaseKey(user, leaseID), canonicalWalletCurrentKey(user), canonicalWalletFundingTombstoneKey(user, leaseID), canonicalWalletHoldSetKey(user)}, user, leaseID, canonicalWalletHoldKey(user, "")).Text()
	if err != nil {
		return nil, err
	}
	var wire struct {
		Lease map[string]string `json:"lease"`
		Holds json.RawMessage   `json:"holds"`
	}
	if err = json.Unmarshal([]byte(raw), &wire); err != nil {
		return nil, err
	}
	lease, err := parseCanonicalWalletLease(wire.Lease)
	if err != nil {
		return nil, err
	}
	var holds []struct {
		AuthorizationID string `json:"authorization_id"`
		HeldUnits       string `json:"held_units"`
	}
	// cjson serializes the empty Lua table as {}; preserve an empty hold array.
	if string(wire.Holds) != "{}" {
		if err = json.Unmarshal(wire.Holds, &holds); err != nil {
			return nil, err
		}
	}
	out := &service.CanonicalWalletFundingBasis{Lease: *lease, Holds: []service.CanonicalWalletHold{}}
	for _, hold := range holds {
		units, e := strconv.ParseInt(hold.HeldUnits, 10, 64)
		if e != nil {
			return nil, e
		}
		if units <= 0 {
			return nil, errors.New("invalid frozen hold")
		}
		out.Holds = append(out.Holds, service.CanonicalWalletHold{AuthorizationID: hold.AuthorizationID, LeaseID: leaseID, HeldUnits: units, State: "armed"})
	}
	return out, nil
}

var applyCanonicalWalletFundingReturnScript = redis.NewScript(`
 local revision=tonumber(ARGV[1]);local before=tonumber(ARGV[2]);local after=tonumber(ARGV[3]);local funded=tonumber(ARGV[4])
 local previous=tonumber(redis.call('HGET',KEYS[2],'return_revision') or '0')
 local prior_returned=tonumber(redis.call('HGET',KEYS[2],'returned_units') or '0')
 if previous>revision or prior_returned>after then return redis.error_reply('stale canonical funding receipt') end
 local previous_signature=redis.call('HGET',KEYS[2],'receipt_signature')
 if previous==revision and (prior_returned~=after or (previous_signature and previous_signature~='' and previous_signature~=ARGV[6])) then return redis.error_reply('canonical funding receipt conflict') end
 if previous<revision and redis.call('EXISTS',KEYS[2])==1 and (previous~=revision-1 or prior_returned~=before) then return redis.error_reply('canonical funding revision conflict') end
 if tonumber(redis.call('HGET',KEYS[2],'funded_units') or ARGV[4])~=funded then return redis.error_reply('canonical funding principal conflict') end
 if redis.call('EXISTS',KEYS[1])==1 then
  local consumed=tonumber(redis.call('HGET',KEYS[1],'consumed_units') or '0');local released=tonumber(redis.call('HGET',KEYS[1],'released_units') or '0')
  if consumed-released>funded-after or redis.call('HGET',KEYS[1],'funding_frozen')~='1'
   or tonumber(redis.call('HGET',KEYS[1],'funded_units') or ARGV[4])~=funded
   or tonumber(redis.call('HGET',KEYS[1],'return_revision') or '0')>revision
   or tonumber(redis.call('HGET',KEYS[1],'returned_units') or '0')>after then return redis.error_reply('canonical funding obligation conflict') end
  redis.call('HSET',KEYS[1],'budget_units',funded-after,'funded_units',funded,'returned_units',after,'return_revision',revision,'funding_frozen','1')
  if ARGV[5]=='close' then redis.call('HSET',KEYS[1],'sealed','1','funding_closed','1') end
  redis.call('PERSIST',KEYS[1])
 end
 -- Only an already validated primary-PG signed receipt invokes this method.
 -- Rebuilding a lost tombstone from its latest ACK can skip prior revisions.
 redis.call('HSET',KEYS[2],'funded_units',funded,'returned_units',after,'return_revision',revision,'funding_frozen','1','receipt_signature',ARGV[6])
 if ARGV[5]=='close' then redis.call('HSET',KEYS[2],'funding_closed','1') end
 redis.call('PERSIST',KEYS[2])
 return 1
`)

func (c *canonicalWalletRedisStore) ApplyCanonicalWalletFundingReturn(ctx context.Context, r service.WalletFundingReturnReceipt) error {
	before, err := strconv.ParseInt(r.ReturnedBeforeUnits, 10, 64)
	if err != nil {
		return err
	}
	after, err := strconv.ParseInt(r.ReturnedAfterUnits, 10, 64)
	if err != nil {
		return err
	}
	funded, err := strconv.ParseInt(r.FundedUnits, 10, 64)
	if err != nil {
		return err
	}
	if r.ReturnRevision <= 0 || after < before || after > funded || r.Signature == "" {
		return errors.New("invalid signed funding return")
	}
	if err = ensureRedisLuaSafeInt64(before, after, funded, r.ReturnRevision); err != nil {
		return err
	}
	return applyCanonicalWalletFundingReturnScript.Run(ctx, c.rdb, []string{canonicalWalletLeaseKey(r.PlatformUserID, r.LeaseID), canonicalWalletFundingTombstoneKey(r.PlatformUserID, r.LeaseID)}, r.ReturnRevision, before, after, funded, r.Mode, r.Signature).Err()
}
