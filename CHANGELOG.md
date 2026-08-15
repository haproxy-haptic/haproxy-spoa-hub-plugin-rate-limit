# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.1] - 2026-08-15

### Fixed

- A shared-store connect that never resolved pinned the sidecar in local
  fallback for good: the deferred store held its lock across an unbounded
  `ValkeyStore::connect`, so after a Sentinel failover one lost `SCRIPT LOAD`
  reply left every reserve timing out behind the lock and nothing able to swap
  the dead client out (observed on a live cluster: one of two sidecars degraded
  until its pod was restarted). Connects are now bounded (2s), run outside the
  lock, and fred commands carry a default timeout instead of waiting forever.

## [0.3.0] - 2026-08-14

### Added

- Add `store_failure_mode = "local" | "open" | "closed"` for both lease and
  exact store failures. `local` enforces a bounded per-sidecar emergency
  token bucket; it does not provide fleet-global exactness during an outage.
- Add `invalid_rule_fail_open` so malformed-rule handling is independent of
  shared-store availability policy.
- Add `txn.hub.rate_limit.degraded` and `txn.hub.rate_limit.accounted` verdicts,
  plus distinct request outcomes for local fallback allowance, local fallback
  exhaustion, capacity fail-open, unconditional fail-open, and fail-closed
  decisions.

### Fixed

- Return store-failure verdicts within `store_timeout_ms` with non-blocking,
  generation-safe connection invalidation and one failed store attempt per
  coalesced borrow cohort.
- Prevent explicit store-failure policies from spending an unbacked optimistic
  lease on a new key; legacy configurations retain their prior cold-start behavior.

## [0.2.0] - 2026-07-18

### Fixed

- **Lease (token-bucket) mode now honors `burst`.** The local lease pool held
  only `rate x sync_interval` tokens (4 for 1200/min at 200ms) with the
  configured `burst` never reachable from the request path, so a browser page
  load multiplexing ~60 requests over one connection was mass-denied while
  hundreds of tokens sat unused in the store. An empty pool now escalates to a
  single bounded, per-key-coalesced `reserve` against the authoritative store
  bucket (borrow-on-empty) before denying — one store round-trip serves a whole
  concurrent burst, and `burst` is exactly as real as the store's bucket.
  Steady-state allowed traffic still never touches the store. Disable with
  `borrow_on_empty = false` to restore the previous instant-deny behavior.
- **`fail_open` is honored when the store is unreachable and the pool is
  empty.** Previously a drained pool with an open circuit breaker denied
  indefinitely regardless of `fail_open` ("fail-static" in name, fail-closed in
  effect — observed during Sentinel failovers). The borrow path now resolves
  breaker-open / store-error / timeout via `fail_open`. Background refresh
  failures remain fail-static (the pool keeps serving its last lease).
- **Honest rate-limit headers.** `remaining` now reports the budget the pod
  knows about (local pool + store-side remaining at the last reservation)
  instead of the tiny lease pool (the "frozen 3"), and `reset` counts down to
  full replenishment instead of repeating `ceil(period)`.

### Changed

- Local pool capacity is the rule's `burst` (was one `rate x interval` batch),
  so grants for a real burst fit locally; the optimistic cold-start fill is
  unchanged (one batch), preserving the fleet overshoot bound.
- Background lease refreshes are demand-sized (Doorman-style "wants"): a key's
  observed per-interval demand — not just the static batch — sizes the next
  lease, capped at `burst`, so recurring bursts are served from the local pool
  with no borrow round-trips.
- After a borrow proves the store bucket empty, further borrows for that key
  back off for one sync interval: a sustained over-limit flood costs the store
  at most one round-trip per interval per pod.

### Added

- `borrow_on_empty` config param (default `true`).
- Per-outcome request metrics for the borrow path: `borrowed`,
  `borrow_local_retry`, `borrow_coalesced`, `limited`, `limited_backoff`,
  `borrow_fail_open`, `borrow_fail_closed`.

## [0.1.6] - 2026-07-15

### Fixed

- Keep Valkey/Sentinel warm-reconnect tasks alive after the initial connection
  and mark timed-out shards unhealthy, so exact/GCRA checks recover after a
  Sentinel failover without performing reconnects inside the request timeout.

## [0.1.5] - 2026-07-15

### Fixed

- Drop and recreate cached Valkey/Sentinel clients after store-operation
  failures so exact/GCRA checks recover after Sentinel promotes a new primary.

## [0.1.4] - 2026-07-15

### Added

- Add `store_urls` for stable key sharding across multiple Valkey/Redis
  endpoints, while keeping `store_url` backward-compatible.
- Enable Sentinel URL support in the Valkey client so chart-managed or BYO
  Sentinel-backed stores can fail over automatically.

## [0.1.3] - 2026-07-15

### Fixed

- Warm the shared Valkey connection in the background when `store_url` is
  configured, preventing exact/GCRA mode from repeatedly failing closed while
  paying connection setup on the request path.

## [0.1.2] - 2026-07-15

### Added

- Bound local lease-manager state with `max_keys` and `idle_ttl_ms` to cap
  memory and periodic refresh work under high-cardinality traffic.
- Disable optimistic cold-starts for new keys inserted after capacity eviction,
  preventing high-cardinality key cycling from minting fresh local tokens.
- Bound and coalesce low-water prefetch work with `max_prefetch_queue` so hot
  over-limit keys cannot grow an unbounded refresh backlog.
- Add `store_timeout_ms` and enforce it around Valkey reservation and exact GCRA
  checks.
- Add a local `acceptance-docker` make target that runs the Valkey acceptance
  suite against a disposable real Valkey container.

### Changed

- Valkey Lua scripts now use Valkey server time for refill/GCRA calculations
  instead of trusting each sidecar's wall clock.
- Valkey connections are established lazily and retried after failures, so
  plugin initialization no longer permanently fails when the store Service is
  not ready during pod startup.
- Existing lease entries now apply rule changes immediately instead of keeping
  stale limits until a plugin reload.

### Fixed

- Exact mode now fails closed on slow store operations before the hub/SPOE
  timeout budget is exhausted.

## [0.1.1] - 2026-07-15

### Fixed

- Plugin initialization is reload-safe: hub config reloads now replace the
  plugin state instead of failing after the first generation with
  `manager already initialized`.

## [0.1.0] - 2026-07-15

### Added

- Shared, low-latency rate limiting as a spoa-hub SPOA plugin. The per-request
  path is local and in-memory: a lease-amortized token bucket enforces the limit
  in-process, and the shared store (Valkey) is a background budget *allocator*,
  never on the hot path.
- Lease manager: each pod holds a local bucket topped up in batches from the
  shared store on a periodic tick and on a low-water signal. Fleet overshoot is
  bounded to `N_pods × batch`; sustained throughput stays within the configured
  limit.
- Pluggable `Store` trait with two backends: `ValkeyStore` (atomic lease
  reservation as a preloaded Lua script run via `EVALSHA`) and `InMemoryStore`
  (per-pod / single-instance, and the Valkey-free test substrate).
- Exact mode (opt-in per key, SPOE arg `mode=exact`): a synchronous GCRA check
  in the shared store for money/contract limits where overshoot is
  unacceptable. Fails **closed** on a store error or open circuit breaker.
- Circuit breaker on the store path: fails **static** (serves the last local
  lease) so a store outage never blocks requests in the default lease mode.
- In-process negative cache for short-lived over-limit decisions.
- Configurable `fail_open`, `store_url`, refresh `interval_ms`, `low_water_percent`,
  lease `ttl_ms`, and breaker threshold/cooldown.
- SPOE glue: reads `key,limit,period,burst,cost,mode` and writes
  `txn.hub.rate_limit.{allowed,limit,remaining,reset,retry_after}`.
- Multi-arch release pipeline (amd64/arm64/armv7) with keyless cosign signing
  and Generic Package Registry publication.
