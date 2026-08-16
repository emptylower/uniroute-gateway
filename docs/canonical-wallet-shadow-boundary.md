# Canonical Wallet Shadow Boundary

Status: implemented as an opt-in observation boundary; production cutover is not implemented.

## Ownership

ShipAny will be the canonical wallet owner. The gateway keeps its current local balance checks and deductions while this bridge is in `shadow` mode. Redis stores only bounded, short-lived lease state; it is not a new wallet database.

All amounts crossing the boundary are integer CNY micro-units. ShipAny accepts CNY only, with the exact conversion `1 CNY = 1,000,000 micros` and `1 ShipAny credit = 10,000 micros`. Floating-point gateway costs are rounded once at the boundary; non-CNY gateway users are counted and skipped in shadow mode.

## Modes

- `disabled` (default): no control-plane calls, queue allocation, or lease Redis operations.
- `shadow`: after an idempotent local balance deduction is applied, enqueue the corresponding canonical settlement observation. Any timeout, queue overflow, missing identity, lease exhaustion, or control-plane error is logged and counted but never changes the request result.
- `enforce`: rejected during configuration validation in this release because
  admission is not wired to the reservation state machine. This prevents an
  operator from mistaking an inert setting for financial enforcement.

## Control-plane contract

Both calls use `Authorization: Bearer <HS256 assertion>`. Assertions have a unique `jti`, `kid=canonical_wallet.version`, `sub=sub2api-gateway`, an exact audience/issuer, one scope, and a 30-second lifetime. The secret must be independent from panel JWT and the ShipAny-to-gateway identity bridge. Lease acquisition also carries a deterministic `Idempotency-Key` for the user, currency, and lease-time window so concurrent gateway instances cannot allocate duplicate budget accidentally.

### Acquire lease

`POST /api/internal/v1/wallet/leases/acquire`, scope `wallet:lease`:

```json
{
  "platform_user_id": "shipany-user-id",
  "currency": "CNY",
  "requested_micros": 5000000,
  "requested_ttl_seconds": 60
}
```

The response may be the object directly or under `data`:

```json
{
  "lease_id": "lease-id",
  "platform_user_id": "shipany-user-id",
  "currency": "CNY",
  "budget_micros": 5000000,
  "consumed_micros": 0,
  "expires_at": "2026-08-16T12:00:00Z"
}
```

### Submit settlement

`POST /api/internal/v1/wallet/settlements`, scope `wallet:settlement`. `Idempotency-Key` equals `event_id`.

```json
{
  "event_id": "gwusg_<sha256>",
  "gateway_request_id": "stable gateway request id",
  "platform_user_id": "shipany-user-id",
  "lease_id": "lease-id",
  "currency": "CNY",
  "amount_micros": 1234,
  "local_balance_after_micros": 9000000,
  "occurred_at": "2026-08-16T12:00:00Z"
}
```

Response:

```json
{
  "accepted": true,
  "duplicate": false,
  "canonical_balance_micros": 9000000
}
```

The lease acquisition `Idempotency-Key` is the deterministic lease ID used by ShipAny. `event_id` is a stable SHA-256 derivation of version, gateway request ID, platform user ID, currency, and amount. Redis reservation is atomic and idempotent by that event ID and binds the reservation to a specific lease.

## Observability and rollback

`CanonicalWalletBridgeStats()` exposes queue, lease acquisition, reservation, settlement, identity, and balance-difference counters. Structured logs include event and lease IDs but no secrets. Rollback is setting `CANONICAL_WALLET_MODE=disabled` and restarting; the existing balance path remains untouched throughout shadowing.

Before any enforce wiring, ShipAny must implement replay protection for the service assertion, durable idempotent settlement persistence, lease issuance atomic with wallet funds, and a reconciliation dashboard. A sustained zero-loss shadow period is required; queue drops and settlement errors are blockers.
