# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
