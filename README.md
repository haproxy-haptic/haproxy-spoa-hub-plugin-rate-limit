# haproxy-spoa-hub-plugin-rate-limit

Shared, cross-fleet **rate limiting + quotas** for [haproxy-spoa-hub](https://gitlab.com/haproxy-haptic/haproxy-spoa-hub),
built for **low latency first**.

Where HAProxy's stick-tables are per-pod (a client hitting N replicas gets N× its
budget), this plugin enforces a **fleet-global** limit — while keeping the shared
store (Valkey/Redis) **off the per-request hot path**.

## Design in one paragraph

Local-first, lease-amortized token bucket. Each sidecar holds a bounded
in-process token bucket per limit-key and decides the default `lease` mode
allow/deny **entirely in memory**. Valkey is a budget allocator: a sidecar
**leases** a slice of the global rate on a time cadence `T` and spends it
locally. Store work is proportional to active keys and refresh cadence, not raw
request RPS; local state is bounded by `max_keys`, idle entries are evicted by
`idle_ttl_ms`, and low-water refreshes are coalesced through a bounded queue.
Global limits are approximate but bounded — instantaneous overshoot is at most
`N_pods × lease_size` plus cold-start optimism, while sustained throughput stays
within the shared store's grants. Under `max_keys` pressure, newly inserted keys
created by capacity eviction start empty instead of optimistic, so
high-cardinality traffic cannot mint fresh local tokens by cycling keys. A
circuit breaker **fails static** (keeps enforcing the last local lease) through a
store outage. An opt-in **exact mode** performs synchronous GCRA per request for
low-volume limits that need fleet-exact accounting while the store is healthy.
`store_failure_mode` selects a bounded per-sidecar fallback, unconditional
allow, or deny when the store can't answer. See
[`specs/001-ratelimit-plugin/design.md`](specs/001-ratelimit-plugin/design.md).

## Status

Under active development, but the shared rate-limit path is implemented:

- Local token bucket, rule parsing, and SPOE `process()` glue.
- Lease manager with bounded local key state, idle eviction, bounded/coalesced
  low-water prefetch, fail-static circuit breaker, and immediate rule-update
  convergence for active keys. Capacity-pressure cold starts require a shared
  lease before admitting traffic.
- Valkey store with atomic single-key Lua reservation and GCRA scripts. Scripts
  use Valkey server time rather than sidecar wall clocks.
- Lazy Valkey connection/reconnection so startup does not permanently fail when
  the store is not ready yet.

**Tested:** in-process correctness tests for windowed safety, sustained rate,
burst, recovery, cost, concurrency/no-double-spend, lease overshoot, bounded key
state, refresh coalescing, rule updates, and store timeout behavior; plus
acceptance tests against a real Valkey (`make acceptance-docker`) for Lua
reservation atomicity, fleet-shared leasing, server-time refill, and exact GCRA.

Next: quota/window algorithms and richer key derivation.

## SPOE contract

Message `check-ratelimit`. Args in: `key`, `limit`, `period`, `burst`, `cost`,
`mode`. Verdict out (`txn.hub.rate_limit.*`): `allowed` (bool), `limit`,
`remaining`, `reset`, `retry_after`, `degraded` (bool), and `accounted` (bool).
`degraded` is true only when store unavailability, rather than quota
exhaustion, selected the verdict. `accounted` is true when authoritative or
local fallback counters produced the verdict and rate-limit headers; it is
false for untracked fail-open and synthetic fail-closed decisions. When it is
false, the plugin omits `limit`, `remaining`, `reset`, and `retry_after`.

## Store failure policy

Set `store_failure_mode` in `[plugins.params]`:

| Value | Store error, timeout, or open breaker |
|---|---|
| `local` | Enforce a per-sidecar emergency token bucket. |
| `open` | Allow the request. |
| `closed` | Deny the request. |

The local emergency budget is shared by lease and exact checks for the same
key. It starts with `burst` tokens on the first failed store decision, refills
at the configured rate, and survives store recovery, so breaker flaps don't
mint new bursts. Its registry is independently bounded by `max_keys`, in
addition to the lease registry's `max_keys` bound; active debt is never evicted
to admit a new key. If the emergency registry is full, `local` allows the
untracked request and marks it degraded. Idle state is removed only after its
budget has fully refilled.

Setting `store_failure_mode` also disables the legacy unbacked optimistic lease
for a new key. The first request either acquires store-backed tokens or resolves
through the selected failure mode. Existing store-issued lease tokens remain
available while the store is down. Omitting `store_failure_mode` retains the
v0.2 cold-start behavior.

`local` preserves per-sidecar service, not fleet-global exactness. During an
outage of duration `D`, each sidecar can admit up to `burst + rate * D` from its
emergency budget. Lease mode can also spend tokens granted before the outage.

`invalid_rule_fail_open` controls malformed rules independently. For backward
compatibility, omitting the new settings keeps the v0.2 behavior: `fail_open`
controls invalid rules and lease borrow failures, while exact store failures
deny. A deployment that wants local store fallback and strict rule parsing can
use:

```toml
[plugins.params]
store_failure_mode = "local"
invalid_rule_fail_open = false
```

`ratelimit_requests_total{outcome=...}` distinguishes authoritative allowance
and exhaustion from `exact_fallback_allowed`, `exact_fallback_limited`,
`exact_fallback_capacity_open`, `exact_fail_open`, `exact_fail_closed`, and the
corresponding `borrow_*` outcomes.

## Build & test

```sh
make test
make lint
make acceptance-docker  # requires Docker; starts disposable Valkey
make build              # -> target/release/librate_limit_plugin.so
```

## License

Apache-2.0.
