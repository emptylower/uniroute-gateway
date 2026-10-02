#!/usr/bin/env python3
"""Clone five cloud-backed groups into independent, official-priced routes.

Plan and check only read PostgreSQL. Apply requires a saved plan; rollback only
disables IDs created by that apply receipt. No users, keys, balances, preferences,
account credentials, original groups, or channel pricing are updated.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys


TARGETS = [
    {"source": group_id, "platform": platform,
     "name": platform + "-official-cloud",
     "marker": "uniroute-official-cloud-v1-source-" + str(group_id)}
    for group_id, platform in [(2, "anthropic"), (3, "openai"),
                               (4, "gemini"), (7, "grok"), (8, "deepseek")]
]
DESCRIPTION = "Official list-price route served by the existing cloud upstream"
SOURCE_IDS = ",".join(str(target["source"]) for target in TARGETS)
TARGET_JSON = json.dumps(TARGETS, separators=(",", ":"))
IGNORED = "ARRAY['id','name','description','rate_multiplier','rate_multiplier_usd','rate_multiplier_cny','peak_rate_enabled','peak_rate_multiplier','is_exclusive','status','subscription_type','fallback_group_id','fallback_group_id_on_invalid_request','duplicate_operation_id','created_at','updated_at','deleted_at']"

# The snapshot exposes group configuration, bindings and credential hashes only.
# JSONB canonicalizes keys; array order is explicit for deterministic baselines.
SNAPSHOT = f"""jsonb_build_object(
 'groups',(SELECT jsonb_agg(to_jsonb(g) ORDER BY g.id) FROM groups g WHERE g.id IN ({SOURCE_IDS})),
 'account_groups',(SELECT jsonb_agg(to_jsonb(ag) ORDER BY ag.group_id,ag.account_id) FROM account_groups ag WHERE ag.group_id IN ({SOURCE_IDS})),
 'accounts',(SELECT jsonb_agg(jsonb_build_object('id',a.id,'platform',a.platform,'type',a.type,'status',a.status,'schedulable',a.schedulable,'deleted_at',a.deleted_at,'expires_at',a.expires_at,'credentials_digest',md5(a.credentials::text)) ORDER BY a.id) FROM accounts a WHERE a.id IN (SELECT account_id FROM account_groups WHERE group_id IN ({SOURCE_IDS}))),
 'channel_groups',(SELECT coalesce(jsonb_agg(to_jsonb(cg) ORDER BY cg.group_id,cg.channel_id),'[]'::jsonb) FROM channel_groups cg WHERE cg.group_id IN ({SOURCE_IDS})),
 'channels',(SELECT coalesce(jsonb_agg(to_jsonb(c) ORDER BY c.id),'[]'::jsonb) FROM channels c WHERE c.id IN (SELECT channel_id FROM channel_groups WHERE group_id IN ({SOURCE_IDS}))),
 'channel_model_pricing',(SELECT coalesce(jsonb_agg(to_jsonb(p) ORDER BY p.id),'[]'::jsonb) FROM channel_model_pricing p WHERE p.channel_id IN (SELECT channel_id FROM channel_groups WHERE group_id IN ({SOURCE_IDS}))))"""


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def psql(args, sql):
    command = ["docker", "exec", "-i", args.container, "sh", "-c",
               'psql -X -q -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At']
    result = subprocess.run(command, input=sql, text=True, capture_output=True)
    if result.returncode:
        # PostgreSQL errors can include a complete row, so do not echo stderr.
        raise RuntimeError("PostgreSQL validation or transaction failed; no successful receipt was written")
    rows = [line for line in result.stdout.splitlines() if line.startswith("{")]
    if len(rows) != 1:
        raise RuntimeError("Expected exactly one database result")
    return json.loads(rows[0])


def event_sql(group_id):
    # Matches schedulerOutboxDedupKey(event, nil, groupID, nil), including NULs.
    key = "scheduler_outbox:" + hashlib.sha256(
        b"group_changed\0\0" + str(group_id).encode() + b"\0").hexdigest()
    return ("INSERT INTO scheduler_outbox(event_type,account_id,group_id,payload,dedup_key) "
            f"VALUES ('group_changed',NULL,{int(group_id)},NULL,{literal(key)}) "
            "ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING;")


def clone_validation(allow_missing):
    missing = "CONTINUE;" if allow_missing else "RAISE EXCEPTION 'required official group missing';"
    return f"""
 FOR t IN SELECT value FROM jsonb_array_elements({literal(TARGET_JSON)}::jsonb) LOOP
   SELECT * INTO src FROM groups WHERE id=(t->>'source')::bigint;
   IF NOT FOUND OR src.deleted_at IS NOT NULL OR src.status<>'active'
      OR src.platform<>t->>'platform' OR src.subscription_type<>'standard'
      OR src.is_exclusive OR src.rate_multiplier<=0 OR src.rate_multiplier>=1 THEN
     RAISE EXCEPTION 'source group is not the approved active discounted group';
   END IF;
   IF EXISTS (SELECT 1 FROM account_groups ag JOIN accounts a ON a.id=ag.account_id
     WHERE ag.group_id=src.id AND (a.deleted_at IS NOT NULL OR
       (src.require_oauth_only AND a.type='apikey'))) THEN
     RAISE EXCEPTION 'source has ineligible account binding';
   END IF;
   IF NOT EXISTS (SELECT 1 FROM account_groups ag JOIN accounts a ON a.id=ag.account_id
     WHERE ag.group_id=src.id AND a.status='active' AND a.schedulable
       AND a.deleted_at IS NULL AND (a.expires_at IS NULL OR a.expires_at>now())) THEN
     RAISE EXCEPTION 'source has no active usable account';
   END IF;
   IF EXISTS (SELECT 1 FROM channel_groups cg JOIN channels c ON c.id=cg.channel_id
       WHERE cg.group_id=src.id AND c.status<>'active') THEN
     RAISE EXCEPTION 'source channel inactive';
   END IF;
   SELECT * INTO dst FROM groups WHERE duplicate_operation_id=t->>'marker' AND deleted_at IS NULL;
   IF NOT FOUND THEN
     IF EXISTS (SELECT 1 FROM groups WHERE name=t->>'name') THEN
       RAISE EXCEPTION 'target group name is already owned by another operation';
     END IF;
     {missing}
   END IF;
   IF dst.name<>t->>'name' OR dst.description IS DISTINCT FROM {literal(DESCRIPTION)}
      OR dst.rate_multiplier IS DISTINCT FROM 1 OR dst.rate_multiplier_usd IS DISTINCT FROM 1
      OR dst.rate_multiplier_cny IS DISTINCT FROM 1 OR dst.peak_rate_enabled OR dst.peak_rate_multiplier IS DISTINCT FROM 1
      OR dst.is_exclusive OR dst.status<>'active' OR dst.subscription_type<>'standard'
      OR dst.fallback_group_id IS NOT NULL OR dst.fallback_group_id_on_invalid_request IS NOT NULL
      OR (to_jsonb(dst)-{IGNORED}) IS DISTINCT FROM (to_jsonb(src)-{IGNORED}) THEN
     RAISE EXCEPTION 'existing target configuration does not match approved clone';
   END IF;
   IF (SELECT coalesce(jsonb_agg(jsonb_build_array(account_id,priority) ORDER BY account_id),'[]'::jsonb) FROM account_groups WHERE group_id=src.id)
      IS DISTINCT FROM
      (SELECT coalesce(jsonb_agg(jsonb_build_array(account_id,priority) ORDER BY account_id),'[]'::jsonb) FROM account_groups WHERE group_id=dst.id) THEN
     RAISE EXCEPTION 'target account bindings differ';
   END IF;
   IF (SELECT coalesce(jsonb_agg(channel_id ORDER BY channel_id),'[]'::jsonb) FROM channel_groups WHERE group_id=src.id)
      IS DISTINCT FROM
      (SELECT coalesce(jsonb_agg(channel_id ORDER BY channel_id),'[]'::jsonb) FROM channel_groups WHERE group_id=dst.id) THEN
     RAISE EXCEPTION 'target channel binding differs';
   END IF;
 END LOOP;
"""


def transaction_sql(mode, expected=None, rollback_ids=None):
    writable = mode in ("apply", "rollback")
    start = "BEGIN;" if writable else "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;"
    locks = ("LOCK TABLE groups,account_groups,channel_groups IN SHARE ROW EXCLUSIVE MODE;\n"
             "LOCK TABLE accounts,channels,channel_model_pricing IN SHARE MODE;\n") if writable else ""
    baseline_assert = (f"IF md5(({SNAPSHOT})::text)<>{literal(expected)} THEN "
                       "RAISE EXCEPTION 'source configuration changed since plan'; END IF;") if expected else ""
    block = f"""DO $operation$
 DECLARE t jsonb; src groups%ROWTYPE; dst groups%ROWTYPE;
   column_names text; expressions text; column_info record; new_id bigint;
   shared_account_id bigint; account_payload text;
   created_ids jsonb := '[]'::jsonb; before_digest text;
 BEGIN
   before_digest:=md5(({SNAPSHOT})::text);
   {baseline_assert}
   {clone_validation(mode in ('plan', 'apply')) if mode != 'rollback' else ''}
"""
    if mode == "apply":
        block += f"""
   FOR t IN SELECT value FROM jsonb_array_elements({literal(TARGET_JSON)}::jsonb) LOOP
     IF EXISTS (SELECT 1 FROM groups WHERE duplicate_operation_id=t->>'marker' AND deleted_at IS NULL) THEN CONTINUE; END IF;
     column_names:=''; expressions:='';
     FOR column_info IN SELECT column_name FROM information_schema.columns
       WHERE table_schema='public' AND table_name='groups' AND column_name<>'id'
       AND is_generated='NEVER' ORDER BY ordinal_position LOOP
       column_names:=concat_ws(',',nullif(column_names,''),quote_ident(column_info.column_name));
       expressions:=concat_ws(',',nullif(expressions,''),CASE column_info.column_name
         WHEN 'name' THEN quote_literal(t->>'name')
         WHEN 'description' THEN {literal(literal(DESCRIPTION))}
         WHEN 'duplicate_operation_id' THEN quote_literal(t->>'marker')
         WHEN 'rate_multiplier' THEN '1' WHEN 'rate_multiplier_usd' THEN '1' WHEN 'rate_multiplier_cny' THEN '1'
         WHEN 'peak_rate_multiplier' THEN '1' WHEN 'peak_rate_enabled' THEN 'false'
         WHEN 'is_exclusive' THEN 'false' WHEN 'status' THEN quote_literal('active')
         WHEN 'subscription_type' THEN quote_literal('standard')
         WHEN 'fallback_group_id' THEN 'NULL' WHEN 'fallback_group_id_on_invalid_request' THEN 'NULL'
         WHEN 'deleted_at' THEN 'NULL' WHEN 'created_at' THEN 'now()' WHEN 'updated_at' THEN 'now()'
         ELSE 's.'||quote_ident(column_info.column_name) END);
     END LOOP;
     EXECUTE format('INSERT INTO groups(%s) SELECT %s FROM groups s WHERE id=$1 RETURNING id',column_names,expressions)
       USING (t->>'source')::bigint INTO new_id;
     INSERT INTO account_groups(account_id,group_id,priority,created_at)
       SELECT account_id,new_id,priority,now() FROM account_groups WHERE group_id=(t->>'source')::bigint;
     INSERT INTO channel_groups(channel_id,group_id,created_at)
       SELECT channel_id,new_id,now() FROM channel_groups WHERE group_id=(t->>'source')::bigint;
     INSERT INTO scheduler_outbox(event_type,account_id,group_id,payload,dedup_key)
       VALUES ('group_changed',NULL,new_id,NULL,'scheduler_outbox:'||encode(sha256(
         convert_to('group_changed','UTF8')||decode('0000','hex')||convert_to(new_id::text,'UTF8')||decode('00','hex')),'hex'))
       ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING;
     created_ids:=created_ids||jsonb_build_array(new_id);
   END LOOP;
   {clone_validation(False)}
   -- Refresh account snapshots explicitly, including platforms outside the
   -- scheduler's canonical platform rebuild list. Also repair existing clones.
   FOR shared_account_id IN SELECT DISTINCT ag.account_id FROM account_groups ag
     WHERE ag.group_id IN ({SOURCE_IDS}) ORDER BY ag.account_id LOOP
     SELECT '{{"group_ids":['||string_agg(ag.group_id::text,',' ORDER BY ag.group_id)||']}}'
       INTO account_payload FROM account_groups ag WHERE ag.account_id=shared_account_id;
     INSERT INTO scheduler_outbox(event_type,account_id,group_id,payload,dedup_key)
       VALUES ('account_changed',shared_account_id,NULL,account_payload::jsonb,
         'scheduler_outbox:'||encode(sha256(convert_to('account_changed','UTF8')||decode('00','hex')||
           convert_to(shared_account_id::text,'UTF8')||decode('0000','hex')||convert_to(account_payload,'UTF8')),'hex'))
       ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING;
   END LOOP;
"""
    elif mode == "rollback":
        for target_id in rollback_ids or []:
            block += f"""
   IF NOT EXISTS (SELECT 1 FROM groups WHERE id={int(target_id)} AND duplicate_operation_id IN
     (SELECT value->>'marker' FROM jsonb_array_elements({literal(TARGET_JSON)}::jsonb)) AND deleted_at IS NULL) THEN
     RAISE EXCEPTION 'rollback ID is not owned by this operation';
   END IF;
   UPDATE groups SET status='disabled',updated_at=now() WHERE id={int(target_id)} AND status IS DISTINCT FROM 'disabled';
   IF FOUND THEN {event_sql(target_id)} END IF;
"""
    block += f"""
   IF md5(({SNAPSHOT})::text)<>before_digest THEN RAISE EXCEPTION 'original configuration changed'; END IF;
   PERFORM set_config('uniroute.created_ids',created_ids::text,true);
 END $operation$;
"""
    return (start + "\nSET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='30s';\n" + locks + block +
            f"SELECT jsonb_build_object('mode',{literal(mode)},'source_digest',md5(({SNAPSHOT})::text)," +
            f"'snapshot',{SNAPSHOT},'created_ids',current_setting('uniroute.created_ids')::jsonb," +
            "'routes',(SELECT coalesce(jsonb_agg(jsonb_build_object('id',g.id,'name',g.name,'platform',g.platform," +
            "'rate_multiplier',g.rate_multiplier,'status',g.status,'marker',g.duplicate_operation_id) ORDER BY g.id),'[]'::jsonb) " +
            f"FROM groups g WHERE duplicate_operation_id IN (SELECT value->>'marker' FROM jsonb_array_elements({literal(TARGET_JSON)}::jsonb)) " +
            "AND deleted_at IS NULL));\nCOMMIT;\n")


def reserve_private(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    stream = os.fdopen(descriptor, "w")
    stream.write('{"mode":"pending"}\n')
    stream.flush()
    os.fsync(stream.fileno())
    return stream


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["plan", "check", "apply", "rollback"])
    parser.add_argument("--container", default="sub2api-postgres")
    parser.add_argument("--baseline", help="private JSON plan required for apply")
    parser.add_argument("--receipt", help="private successful apply receipt required for rollback")
    parser.add_argument("--record", help="new private output file, required for mutation")
    args = parser.parse_args()
    if args.mode in ("apply", "rollback") and not args.record:
        parser.error("apply and rollback require --record")
    if args.record and Path(args.record).exists():
        parser.error("--record must be a new file; existing evidence is never overwritten")
    expected = None
    if args.baseline:
        with open(args.baseline) as stream:
            baseline = json.load(stream)
        if baseline.get("mode") != "plan":
            parser.error("--baseline must be an unmodified plan record")
        expected = baseline["source_digest"]
    if args.mode == "apply" and not expected:
        parser.error("apply requires --baseline")
    rollback_ids = None
    if args.mode == "rollback":
        if not args.receipt:
            parser.error("rollback requires --receipt")
        with open(args.receipt) as stream:
            receipt = json.load(stream)
        if receipt.get("mode") != "apply":
            parser.error("rollback requires a successful apply receipt")
        rollback_ids = receipt["created_ids"]
        if not rollback_ids:
            parser.error("this apply created no groups; there is nothing owned by its receipt to disable")
    # Establish exclusive, writable receipt storage before beginning a mutation.
    record = reserve_private(args.record) if args.record else None
    try:
        result = psql(args, transaction_sql(args.mode, expected, rollback_ids))
        if record:
            record.seek(0)
            json.dump(result, record, indent=2)
            record.write("\n")
            record.truncate()
            record.flush()
            os.fsync(record.fileno())
    except Exception:
        if record:
            record.close()
            # Keep the pending receipt after an uncertain database or disk outcome.
        raise
    finally:
        if record and not record.closed:
            record.close()
    print(json.dumps({"mode": result["mode"], "source_digest": result["source_digest"],
                      "created_ids": result["created_ids"], "routes": result["routes"]}, ensure_ascii=False))


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError, KeyError, ValueError) as error:
        print("ERROR: " + str(error), file=sys.stderr)
        sys.exit(1)
