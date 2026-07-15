//! Shared, low-latency rate-limiting plugin for haproxy-spoa-hub.
//!
//! Design: local-first, lease-amortized token bucket. The per-request hot path
//! ([`plugin::process`] → [`limiter::Limiter::check`] → [`bucket::TokenBucket`])
//! decides allow/deny entirely in memory. The shared tier (Valkey) is a budget
//! allocator consulted out-of-band — it is off the per-request path in the
//! default model. See `specs/001-ratelimit-plugin/` for the full design.
//!
//! Phase 1 (this milestone): local enforcement only (single-instance semantics,
//! no Valkey) so the SPOE round-trip and the token-bucket core are proven end to
//! end. Phase 2 layers the Valkey lease manager, circuit breaker, and negative
//! cache on top without changing the hot-path shape.
#![allow(non_camel_case_types, non_local_definitions)]

pub mod breaker;
pub mod bucket;
pub mod config;
pub mod limiter;
pub mod negcache;
pub mod rule;
pub mod store;

mod plugin;
