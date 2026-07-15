//! Shared-tier store: the budget allocator the lease manager talks to.
//!
//! Reserving a lease is the only per-key operation the lease manager needs, so
//! the trait is deliberately tiny — keying is orthogonal (the key is opaque)
//! and the algorithm lives in the store (the Lua reservation for Valkey). This
//! is consulted only on the *background* lease-refresh path, never per request.

pub mod valkey;

use std::fmt;

/// The outcome of a lease reservation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Reservation {
    /// Tokens granted to this sidecar to spend locally until the next refresh.
    pub granted: f64,
    /// Global tokens remaining after this grant (observability).
    pub remaining: f64,
}

/// A store failure. The lease manager treats any error as "store unavailable"
/// and drives the circuit breaker; the local buckets keep enforcing their last
/// lease (fail-static).
#[derive(Debug)]
pub enum StoreError {
    Connection(String),
    Protocol(String),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::Connection(m) => write!(f, "store connection error: {m}"),
            StoreError::Protocol(m) => write!(f, "store protocol error: {m}"),
        }
    }
}

impl std::error::Error for StoreError {}

/// A shared budget allocator. Implementations reserve up to `requested` tokens
/// from the authoritative global bucket for `key`, atomically.
#[allow(async_fn_in_trait)]
pub trait Store: Send + Sync {
    /// Reserve up to `requested` tokens from the global bucket for `key`,
    /// refilling it at `rate` (tokens/s) capped at `burst`. `now_ms` is the
    /// caller's clock; `ttl_ms` self-expires the key (≈ one refill period).
    async fn reserve(
        &self,
        key: &str,
        rate: f64,
        burst: f64,
        requested: f64,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<Reservation, StoreError>;
}
