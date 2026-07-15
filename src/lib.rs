//! Shared, low-latency rate-limiting plugin for haproxy-spoa-hub.
//!
//! Design: local-first, lease-amortized token bucket. The per-request hot path
//! ([`plugin::process`] → [`lease::LeaseManager::check`] →
//! [`bucket::TokenBucket`]) decides allow/deny entirely in memory for the default
//! lease mode. The shared tier (Valkey) is a budget allocator consulted
//! out-of-band — it is off the per-request path except for opt-in exact/GCRA
//! mode. Local key state and refresh work are bounded for high-cardinality
//! traffic. See `specs/001-ratelimit-plugin/` for the full design.
#![allow(non_camel_case_types, non_local_definitions)]

pub mod breaker;
pub mod bucket;
pub mod config;
pub mod lease;
pub mod limiter;
pub mod negcache;
pub mod rule;
pub mod store;

mod plugin;
