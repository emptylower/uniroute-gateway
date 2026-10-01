# Gateway-owned Playground media

New image, video and music requests use the signed platform identity API and the
canonical wallet. The browser never receives a provider credential, selects a
gateway API key, supplies a price, or changes a task's billing outcome. ShipAny
authenticates the session and signs an assertion whose subject exactly matches
the user in the path. `gateway:inference:create` grants task creation;
`gateway:inference:read` grants catalog, quote, owned task history and legacy reads.
`wallet:availability` grants only the user's financial read model.

The gateway creates one stable internal projected API key per platform user. Its
random verifier has no recoverable plaintext credential. A disabled managed KIE
account supplies the usage-log foreign key; its credentials are empty and it is
not eligible for ordinary account routing. KIE authentication remains an
environment secret.

## Contract

Base: `/api/internal/v1/users/:platform_user_id/inference`. Responses use the
existing `{code,message,data}` envelope.

| Method/path | Input | Data |
| --- | --- | --- |
| GET `/catalog` | — | `{models,currency:'USD',policy_version:'usd-wallet-v1',enabled}` |
| POST `/quote` | `{model,option}` | `{model,option,currency,quoted_usd,policy_version}` |
| POST `/tasks` | `{model,prompt,option,media_type?}` plus `Idempotency-Key` | owned task |
| GET `/tasks/:task_id` | — | owned task |
| GET `/tasks?limit=20&cursor=...` | limit 1–100, opaque cursor | `{tasks,next_cursor}` |
| POST `/legacy-read` | `{provider_task_id,model,media_type}` | provider status and URLs; no new task, authorization or billing |

Task fields are `task_id`, `model`, `media_type`, `option`, `prompt`, `status`,
`billing`, `urls`, optional `title`, `cover_url`, `error_code`, `error_message`,
and ISO `created_at`/`updated_at`. Public status is `pending`, `processing`,
`success`, `failed`, or `indeterminate`. Billing contains `currency:'USD'`,
`state` (`held`, `settling`, `charged`, `released`, `indeterminate`), decimal
`quoted_usd`, `held_usd`, nullable `charged_usd`/`released_usd`, and policy version.
URLs contain `{kind:'image'|'video'|'audio',url}`. Provider task IDs remain private.

The same user/idempotency key and normalized payload returns the same task. A
different payload under that key returns 409. History reads enforce user ownership
in SQL. For legacy reads ShipAny must first read its existing `ai_task` row and
verify the logged-in owner before signing the request. The gateway performs only
the provider status GET; old refund compatibility remains an idempotent ShipAny
operation.

## Frozen pricing

The allowlist preserves the 11 verified model/parameter combinations. Exact prices
are integer ten-thousandths of USD. Video multiplies the chosen duration; images
and music charge one unit per request, including music responses with multiple
audio URLs. FLUX 2 Pro's 2K option costs USD 0.12; Seedance 2 Mini uses the
480p/no-video rate USD 0.0215/second. No cheapest-tier substitution occurs.

Each accepted request durably freezes `kie-playground-v1` prices, USD wallet
policy, and the 7.2 nominal USD:CNY conversion before authorization. Wire amounts
remain decimal integer strings in `cny-e8-v1`. The gateway arms the exact quoted
amount through `CanonicalWalletAuthorizer`; catalog or FX edits cannot reprice an
already frozen task.

## Durable lifecycle

Migration 217 adds `gateway_media_task`, provider-account linkage and the media
usage field. Migration 218 persists the per-lease authorization plan; 219 adds
shared HTTP/WS/Live attempt states and settlement payloads. A task and immutable billing snapshot commit together before any
hold can arm. The authorization ID is preallocated in that task, allowing the
reaper to find its owner even if the process stops between Redis arm and the
checkpoint that records the lease.

The worker uses `FOR UPDATE SKIP LOCKED`, a new UUID for every claim, an expiry,
and SQL compare-and-set on that claim. It records the authorization/lease basis,
persists the Redis hold and lease keys, and obtains a signed D1 task-pin ACK before
the irreversible `submitting` checkpoint and provider POST. Recovery of a
`submitting` row never repeats that POST. A transport timeout or malformed
acceptance response becomes `indeterminate` and preserves the pin indefinitely.
The task deadline similarly requires review; it is not proof of provider failure.

Explicit provider rejection and terminal `fail` move to durable `releasing` before
the idempotent Redis zero release and signed D1 pin finish. A terminal provider
`success` converts the original hold and commits the usage row, canonical outbox
events for every funding segment and task `settling` transition in one PostgreSQL transaction. A failed
transaction cannot commit only part of that outcome. Missing delivery URLs do not
turn provider success into a refund. The task becomes `charged` and unpins only
after every exact bound segment outbox event is `delivered` and D1 confirms each capture.
Split/dead-letter/mismatched settlement requires review instead of early unpin.

D1 pins atomically block lease close/drain/refund of reserved funds, including
the grace sweep. Redis persistence does not extend `expires_at`: an expired
lease cannot authorize new calls. The signed pool read supplies all owned active leases. Only policy-matching,
undrained, unexpired, unsealed leases with an existing raw Redis cache are eligible.
A missing cache is not reconstructed as free money from D1 captured totals.

The gateway persists a complete segment plan before one Lua script validates and
arms every share atomically. All keys use the same user hash slot. One provider
POST and one total usage row remain, while each share preserves its original
lease/auth/event identity for pinning and capture. If total cached free funds
are insufficient, the exact shortfall can top up one eligible lease monotonically
from unleased grants. The top-up uses an absolute minimum budget, not an additive
retry debit; consumed,
released and existing holds are preserved, and the lease expiry can only shrink.

After Redis loss, the worker reads the idempotent D1 pin's current lease basis and
captured event IDs, restores durable media obligations, and seals the reconstructed
cache. Unknown ordinary reservations cannot be assumed absent. Reconstruction sums
all durable media and generic segments and seals the lease. Availability
therefore reports `unknown` until a complete observation can be proved; it never
exposes unverified recovered cache headroom as spendable.

## Availability

POST `/api/internal/v1/users/:platform_user_id/wallet/availability` accepts policy
versions, canonical lease snapshots and captured event IDs from ShipAny. Every
`*_units` value is a decimal string. It returns `as_of`, `completeness`, raw lease
consumed/released/free/held amounts and mutually exclusive hold/media/Live/outbox
obligations. Captured event IDs suppress ACK-loss duplicates. Held totals include
only uncaptured obligations covered by the same Redis lease. The gateway reads
Redis atomically twice around a repeatable-read PostgreSQL observation; a changed
fingerprint or unexplained counter makes the observation unknown. ShipAny also
checks its before/after grants, leases and pins before displaying spendable funds.

KIE's official [task detail contract](https://docs.kie.ai/market/common/get-task-detail)
documents the common status GET and terminal states. Provider generation is tested
through local HTTP doubles; integration verification must not create paid jobs.

## Shared HTTP, WS and Live protection

Every new fixed-USD authorization uses the same funding pool. Ordinary HTTP/WS
and each Live window persist their segment plan, obtain all D1 pin ACKs, and enter
`held` before allowing a provider write. The HTTP/WS decorator durably marks the
attempt `indeterminate` with its write token before sending. Unknown writes retain
all segments indefinitely; neither the reaper nor the lease grace sweep releases
them. A protected authorization permits only one write; a retry cannot replace
its token or reuse an uncertain submission. Explicit validation/authentication
HTTP rejection and proven TCP dial/DNS failure resolve as zero. Cancellation,
timeouts and absent trace callbacks alone are not zero proof. A stale `prepared`
or tokenless `held` plan can resolve as zero only after a guarded transition
blocks any future send; its deadline uses immutable creation time.
An HTTP correction retry first confirms durable zero for every old share and
receives every zero pin-finish ACK. It then creates a fresh immutable authorization
parent using the same trusted identity and frozen pricing. The original parent
and callbacks retain their identities; settlement accessors resolve the current
attempt. Missing proof or ACK stops the retry before sending.

A LiveWindow.Token is the segment parent; the existing window's first lease stays
compatible while all shares are looked up through that stable parent. Window
pending amounts and total usage remain unchanged. The reconciliation read exposes
new segment identities; old windows without segment rows retain their previous
single-event derivation. Recovery runs independently of the media feature flag.
Advancing a pooled Live window activates all its shares with a logical write
token in the same PostgreSQL transaction as the window CAS. Tokenless rows
already bound to an active provisional window cannot expire as undispatched.
If a zero resolution wins before the advance, the Live controller stops the
session instead of repeatedly trying to consume that resolved authorization.

For generic attempts, the expected event for each share is SHA256 of
`v1|<parent-auth>:<child-auth>|<platform-user>|CNY`, prefixed `gwusg_`. The pin's
GatewayJobID is the parent `auth_` identifier (media continues to use `media_`).
Known actual cost is divided FIFO over the original shares. Unused shares resolve
as zero; all positive events commit together. The settlement payload is persisted
before Redis conversion so a failed outbox transaction can be reconstructed. A
late zero request cannot release a share with a positive durable settlement.
An exact positive durable segment retains its original lease binding through
expiry or recovery sealing. Its already-funded Redis event marker permits
idempotent delivery; fresh reservations still reject expired/sealed leases.
Write, settlement and zero-release transitions lock every share in ordinal order,
so mixed positive/unused groups cannot be partially released by a concurrent
zero callback. Recovery rotates inspected groups behind other pending groups;
old unknown attempts cannot monopolize the bounded recovery batch.
Usage beyond the original estimate retains a separate stable canonical receivable
through the existing outbox workflow; it is never omitted from the total.

The generic recovery worker restores pending protection after Redis loss and
finishes a pin only after its exact capture ACK or confirmed zero release.
A recovered cache remains sealed and availability stays unknown until all raw
counters, durable obligations and canonical captured events can be reconciled.
