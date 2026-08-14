//! The local limiter: a registry of per-key token buckets + the verdict shape.
//!
//! Phase 1 enforces at the rule's full configured rate (single-instance
//! semantics). Phase 2 will feed each bucket a *leased* rate from Valkey via
//! [`crate::bucket::TokenBucket::set_lease`], turning this into the local tier
//! of the shared limiter — with no change to [`Limiter::check`]'s hot path.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use crate::bucket::TokenBucket;
use crate::rule::Rule;

/// The rate-limit decision returned to HAProxy (maps to `txn.hub.rate_limit.*`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Verdict {
    pub allowed: bool,
    /// The shared store could not answer and a configured fallback decided.
    pub degraded: bool,
    /// Whether rate-limit counters produced this verdict and its headers.
    pub accounted: bool,
    pub limit: u32,
    pub remaining: u32,
    /// Seconds until the window resets (for `X-RateLimit-Reset`).
    pub reset_s: u32,
    /// Seconds to wait before retrying, when denied (for `Retry-After`).
    pub retry_after_s: u32,
}

/// Per-key token buckets. Lookups take a read lock; only first-sight of a key
/// takes the write lock to insert.
// ponytail: RwLock<HashMap> is fine at per-key granularity; swap to a sharded
// map (dashmap) only if the write lock on cold-key insert is measured to hurt.
pub struct Limiter {
    buckets: RwLock<HashMap<String, Arc<TokenBucket>>>,
}

impl Default for Limiter {
    fn default() -> Self {
        Self::new()
    }
}

impl Limiter {
    pub fn new() -> Self {
        Self {
            buckets: RwLock::new(HashMap::new()),
        }
    }

    /// Enforce `rule` for `key` at monotonic time `now`.
    pub fn check(&self, key: &str, rule: &Rule, now: Instant) -> Verdict {
        let bucket = self.bucket_for(key, rule, now);
        let d = bucket.try_acquire(rule.cost, now);
        Verdict {
            allowed: d.allowed,
            degraded: false,
            accounted: true,
            limit: rule.limit as u32,
            remaining: d.remaining.floor().max(0.0) as u32,
            reset_s: rule.period_s.ceil().max(0.0) as u32,
            retry_after_s: if d.allowed {
                0
            } else if d.retry_after.is_finite() {
                d.retry_after.ceil().max(1.0) as u32
            } else {
                rule.period_s.ceil().max(1.0) as u32
            },
        }
    }

    fn bucket_for(&self, key: &str, rule: &Rule, now: Instant) -> Arc<TokenBucket> {
        if let Some(b) = self
            .buckets
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
        {
            return b.clone();
        }
        self.buckets
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(TokenBucket::new(rule.rate(), rule.burst, now)))
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn allows_up_to_burst_then_denies_same_key() {
        let l = Limiter::new();
        let rule = Rule {
            limit: 5.0,
            period_s: 1.0,
            burst: 5.0,
            cost: 1.0,
        };
        let t0 = Instant::now();
        for _ in 0..5 {
            assert!(l.check("k", &rule, t0).allowed);
        }
        let v = l.check("k", &rule, t0);
        assert!(!v.allowed);
        assert_eq!(v.remaining, 0);
        assert_eq!(v.limit, 5);
        assert!(v.retry_after_s >= 1);
    }

    #[test]
    fn distinct_keys_have_independent_budgets() {
        let l = Limiter::new();
        let rule = Rule {
            limit: 1.0,
            period_s: 1.0,
            burst: 1.0,
            cost: 1.0,
        };
        let t0 = Instant::now();
        assert!(l.check("a", &rule, t0).allowed);
        assert!(l.check("b", &rule, t0).allowed); // b unaffected by a
        assert!(!l.check("a", &rule, t0).allowed);
    }

    #[test]
    fn refills_across_time() {
        let l = Limiter::new();
        let rule = Rule {
            limit: 2.0,
            period_s: 1.0,
            burst: 2.0,
            cost: 1.0,
        };
        let t0 = Instant::now();
        assert!(l.check("k", &rule, t0).allowed);
        assert!(l.check("k", &rule, t0).allowed);
        assert!(!l.check("k", &rule, t0).allowed);
        // +1s → 2 tokens back (capped at burst).
        assert!(l.check("k", &rule, t0 + Duration::from_secs(1)).allowed);
    }
}
