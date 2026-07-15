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
store outage. An opt-in **exact mode** (synchronous GCRA per request) is
available for low-volume money/contract limits and fails closed on store errors
or store-operation timeouts. See
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
`remaining`, `reset`, `retry_after`.

## Build & test

```sh
make test
make lint
make acceptance-docker  # requires Docker; starts disposable Valkey
make build              # -> target/release/librate_limit_plugin.so
```

## License

Apache-2.0.
