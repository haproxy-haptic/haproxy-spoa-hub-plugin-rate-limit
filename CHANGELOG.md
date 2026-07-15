# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
