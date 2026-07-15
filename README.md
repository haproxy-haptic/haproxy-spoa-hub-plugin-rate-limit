# haproxy-spoa-hub-plugin-ratelimit

Shared, cross-fleet **rate limiting + quotas** for [haproxy-spoa-hub](https://gitlab.com/haproxy-haptic/haproxy-spoa-hub),
built for **low latency first**.

Where HAProxy's stick-tables are per-pod (a client hitting N replicas gets N× its
budget), this plugin enforces a **fleet-global** limit — while keeping the shared
store (Valkey/Redis) **off the per-request hot path**.

## Design in one paragraph

Local-first, lease-amortized token bucket. Each sidecar holds an in-process token
bucket per limit-key and decides allow/deny **entirely in memory** (tens of
nanoseconds, zero network). Valkey is only a *budget allocator*: a sidecar
**leases** a slice of the global rate on a time cadence `T` and spends it locally,
so **Valkey QPS ≈ pods/T, independent of request RPS**. Global limits are
*approximate but bounded* — instantaneous overshoot ≤ `N_pods × lease_size`, while
the sustained rate stays ≤ the limit. An in-process negative cache short-circuits
already-throttled abusers with no Valkey contact, and a circuit breaker **fails
static** (keeps enforcing the last lease) through a store outage. An opt-in
**exact mode** (synchronous GCRA per request) is available for money/contract
limits. See [`specs/001-ratelimit-plugin/design.md`](specs/001-ratelimit-plugin/design.md).

## Status

Under active development. Implemented so far:

- **Phase 1 — local enforcement core.** The per-request hot path: a lazy-refill
  token bucket (`src/bucket.rs`), the per-key limiter (`src/limiter.rs`), rule
  parsing (`src/rule.rs`), and the SPOE `process()` glue (`src/plugin.rs`).
- **Circuit breaker** (`src/breaker.rs`, fail-static) + **L1 negative cache**
  (`src/negcache.rs`) — the resilience/short-circuit components.
- **Shared-tier Valkey store** (`src/store/`) — the atomic single-key Lua
  reservation (`reserve.lua`), the budget allocator the lease manager talks to.

**Tested:** 33 in-process tests incl. a thorough rate-limiting correctness suite
(`tests/algorithm_correctness.rs`: windowed safety, sustained rate, burst,
recovery, cost, concurrency/no-double-spend), plus **acceptance tests against a
real Valkey** (`tests/valkey_acceptance.rs`, incl. the Lua reservation's
atomicity under 500-way concurrency).

Next: the lease manager (time-based leasing + async low-water prefetch, wiring the
store into the local buckets), then chart wiring + cross-pod e2e (Phase 3), quotas
+ exact GCRA + high-cardinality CMS (Phase 4).

## SPOE contract

Message `check-ratelimit`. Args in: `key`, `limit`, `period`, `burst`, `cost`,
`mode`. Verdict out (`txn.hub.rate_limit.*`): `allowed` (bool), `limit`,
`remaining`, `reset`, `retry_after`.

## Build & test

```sh
cargo test          # unit tests
cargo clippy --all-targets -- -D warnings
cargo build --release   # -> target/release/librate_limit_plugin.so
```

## License

Apache-2.0.
