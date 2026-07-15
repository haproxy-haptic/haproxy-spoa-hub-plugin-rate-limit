//! Local token bucket — the per-request hot path.
//!
//! This is the local tier of the local-first, lease-amortized design (Envoy
//! `local_ratelimit` primitive): a lazy-refill token bucket that decides
//! allow/deny entirely in memory, with **zero network I/O** on the fast path.
//! Tokens refill continuously at the currently-leased rate, derived from
//! elapsed monotonic time (no background filler thread). The shared tier
//! (Valkey) adjusts `rate`/`burst` out-of-band via [`TokenBucket::set_lease`];
//! between lease refreshes the bucket enforces locally against the lease.
//!
//! Tokens are held as `f64` — a rate limiter's counts are small and the
//! fixed-point precision an integer representation would buy isn't worth the
//! awkwardness. State lives behind a `Mutex`; uncontended lock/unlock is tens
//! of ns, and contention is per-key (keys shard across independent buckets).
// ponytail: Mutex per bucket, not lock-free CAS — swap to atomics only if a
// single hot key is measured to contend. Per-key sharding makes that unlikely.

use std::sync::Mutex;
use std::time::Instant;

/// Outcome of a token acquisition attempt.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Decision {
    /// Whether `cost` tokens were available and consumed.
    pub allowed: bool,
    /// Tokens left after the attempt (never negative).
    pub remaining: f64,
    /// Seconds until `cost` tokens would be available, when denied (else 0).
    pub retry_after: f64,
}

struct Inner {
    tokens: f64,
    last_refill: Instant,
    /// Leased rate in tokens per second.
    rate: f64,
    /// Bucket capacity (max accumulated tokens) — the leased burst.
    burst: f64,
}

impl Inner {
    /// Refill lazily from elapsed monotonic time, capped at `burst`.
    fn refill(&mut self, now: Instant) {
        // `now` should be monotonic and >= last_refill; guard against a
        // non-monotonic clock (or a `now` from before the last refill) so
        // elapsed can never be negative and drain the bucket.
        if now > self.last_refill {
            let elapsed = now.duration_since(self.last_refill).as_secs_f64();
            self.tokens = (self.tokens + elapsed * self.rate).min(self.burst);
            self.last_refill = now;
        }
    }
}

/// A local, lazy-refill token bucket. Cheap to clone the handle via `Arc`.
pub struct TokenBucket {
    inner: Mutex<Inner>,
}

impl TokenBucket {
    /// Create a bucket that starts full (`tokens == burst`).
    pub fn new(rate: f64, burst: f64, now: Instant) -> Self {
        Self::new_with_tokens(rate, burst, burst, now)
    }

    /// Create a bucket with an explicit initial token count, clamped to
    /// `[0, burst]`. Lease mode uses this to disable optimistic cold-starts
    /// when local key cardinality is already at the configured cap.
    pub fn new_with_tokens(rate: f64, burst: f64, tokens: f64, now: Instant) -> Self {
        let burst = burst.max(0.0);
        Self {
            inner: Mutex::new(Inner {
                tokens: tokens.max(0.0).min(burst),
                last_refill: now,
                rate: rate.max(0.0),
                burst,
            }),
        }
    }

    /// Fast path: refill, then consume `cost` tokens if available.
    pub fn try_acquire(&self, cost: f64, now: Instant) -> Decision {
        let cost = cost.max(0.0);
        let mut s = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        s.refill(now);
        if s.tokens >= cost {
            s.tokens -= cost;
            Decision {
                allowed: true,
                remaining: s.tokens,
                retry_after: 0.0,
            }
        } else {
            let deficit = cost - s.tokens;
            let retry_after = if s.rate > 0.0 {
                deficit / s.rate
            } else {
                f64::INFINITY
            };
            Decision {
                allowed: false,
                remaining: s.tokens,
                retry_after,
            }
        }
    }

    /// Apply a lease refresh from the shared tier: set the enforced rate/burst
    /// for the next interval. Refills against the *old* rate up to `now` first
    /// so no accrued tokens are lost, then clamps to the new burst.
    pub fn set_lease(&self, rate: f64, burst: f64, now: Instant) {
        let mut s = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        s.refill(now);
        s.rate = rate.max(0.0);
        s.burst = burst.max(0.0);
        if s.tokens > s.burst {
            s.tokens = s.burst;
        }
    }

    /// Deposit leased tokens into the pool (from a shared-tier reservation),
    /// capped at `burst`. Refills against the current rate up to `now` first.
    /// Returns the resulting available tokens. This is how the lease manager
    /// feeds a batch of globally-reserved tokens into the local bucket.
    pub fn deposit(&self, tokens: f64, now: Instant) -> f64 {
        let mut s = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        s.refill(now);
        s.tokens = (s.tokens + tokens.max(0.0)).min(s.burst);
        s.tokens
    }

    /// Current available tokens (after a refill) — used for the low-water
    /// prefetch decision. Does not consume.
    pub fn available(&self, now: Instant) -> f64 {
        let mut s = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        s.refill(now);
        s.tokens
    }

    /// The leased burst (capacity), for computing the low-water threshold.
    pub fn burst(&self) -> f64 {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).burst
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn at(base: Instant, ms: u64) -> Instant {
        base + Duration::from_millis(ms)
    }

    #[test]
    fn starts_full_and_drains_to_the_limit() {
        let t0 = Instant::now();
        let b = TokenBucket::new(10.0, 5.0, t0); // 10/s, burst 5, starts full
        // 5 immediate allows, then denied (no time elapsed → no refill).
        for i in 0..5 {
            let d = b.try_acquire(1.0, t0);
            assert!(d.allowed, "token {i} should be allowed");
        }
        let d = b.try_acquire(1.0, t0);
        assert!(!d.allowed);
        assert_eq!(d.remaining, 0.0);
        // retry_after ≈ 1 token / 10 per s = 0.1s
        assert!(
            (d.retry_after - 0.1).abs() < 1e-9,
            "retry_after={}",
            d.retry_after
        );
    }

    #[test]
    fn refills_over_time_at_the_leased_rate() {
        let t0 = Instant::now();
        let b = TokenBucket::new(10.0, 5.0, t0);
        for _ in 0..5 {
            assert!(b.try_acquire(1.0, t0).allowed);
        } // drain
        assert!(!b.try_acquire(1.0, t0).allowed);
        // 300ms later → 3 tokens refilled.
        assert!(b.try_acquire(1.0, at(t0, 300)).allowed);
        assert!(b.try_acquire(1.0, at(t0, 300)).allowed);
        assert!(b.try_acquire(1.0, at(t0, 300)).allowed);
        assert!(!b.try_acquire(1.0, at(t0, 300)).allowed);
    }

    #[test]
    fn refill_is_capped_at_burst() {
        let t0 = Instant::now();
        let b = TokenBucket::new(10.0, 5.0, t0);
        for _ in 0..5 {
            assert!(b.try_acquire(1.0, t0).allowed);
        }
        // A long time later, tokens cap at burst (5), not rate*elapsed.
        assert_eq!(b.available(at(t0, 10_000)), 5.0);
    }

    #[test]
    fn cost_greater_than_one() {
        let t0 = Instant::now();
        let b = TokenBucket::new(1.0, 10.0, t0);
        let d = b.try_acquire(4.0, t0);
        assert!(d.allowed);
        assert_eq!(d.remaining, 6.0);
        assert!(!b.try_acquire(7.0, t0).allowed); // only 6 left
    }

    #[test]
    fn non_monotonic_now_does_not_drain() {
        let t0 = Instant::now();
        let b = TokenBucket::new(10.0, 5.0, t0);
        assert!(b.try_acquire(1.0, t0).allowed);
        // A `now` from before the last refill must not produce negative elapsed.
        let before = t0;
        let d = b.try_acquire(1.0, before);
        assert!(d.allowed);
        assert_eq!(d.remaining, 3.0);
    }

    #[test]
    fn set_lease_updates_rate_and_clamps_tokens() {
        let t0 = Instant::now();
        let b = TokenBucket::new(10.0, 100.0, t0); // starts full at 100
        // Shrink the lease to burst 5 → tokens clamp down to 5.
        b.set_lease(5.0, 5.0, t0);
        assert_eq!(b.available(t0), 5.0);
        // New rate 5/s: after 1s, +5 but capped at burst 5.
        assert_eq!(b.available(at(t0, 1000)), 5.0);
    }

    #[test]
    fn zero_rate_bucket_denies_with_infinite_retry() {
        let t0 = Instant::now();
        let b = TokenBucket::new(0.0, 1.0, t0);
        assert!(b.try_acquire(1.0, t0).allowed); // the one starting token
        let d = b.try_acquire(1.0, t0);
        assert!(!d.allowed);
        assert!(d.retry_after.is_infinite());
    }
}
