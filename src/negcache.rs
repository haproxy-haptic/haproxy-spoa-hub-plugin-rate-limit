//! L1 negative cache — in-process, over-limit verdicts only.
//!
//! The first check on every request: if a key is flagged over-limit (unexpired
//! TTL), the request is rejected **with zero Valkey contact**. This is what
//! short-circuits a flooding abuser — the high-volume path never reaches the
//! store. Only over-limit keys are cached (never the live counter, which
//! changes every request); the TTL is short (≈ one lease/sync interval).
//!
//! Later (Phase 2) a Valkey RESP3 `CLIENT TRACKING` (BCAST) subscription will
//! push `block:` invalidations into [`NegativeCache::invalidate`], upgrading the
//! cache from pure-TTL to server-pushed invalidation. The API is already shaped
//! for that.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

pub struct NegativeCache {
    /// key -> expiry instant.
    entries: RwLock<HashMap<String, Instant>>,
}

impl Default for NegativeCache {
    fn default() -> Self {
        Self::new()
    }
}

impl NegativeCache {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Hot path: is `key` currently flagged over-limit? Read-lock only; expired
    /// entries read as not-blocked (they are reaped by [`sweep`]).
    pub fn is_blocked(&self, key: &str, now: Instant) -> bool {
        self.entries
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
            .is_some_and(|&expiry| now < expiry)
    }

    /// Flag `key` as over-limit for `ttl`. A later, longer TTL extends; a
    /// shorter one never shrinks an existing block.
    pub fn block(&self, key: &str, ttl: Duration, now: Instant) {
        let expiry = now + ttl;
        let mut e = self.entries.write().unwrap_or_else(|e| e.into_inner());
        e.entry(key.to_string())
            .and_modify(|cur| {
                if expiry > *cur {
                    *cur = expiry;
                }
            })
            .or_insert(expiry);
    }

    /// Explicitly clear a key (RESP3 `CLIENT TRACKING` invalidation, or a lifted
    /// block). Idempotent.
    pub fn invalidate(&self, key: &str) {
        self.entries
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(key);
    }

    /// Reap expired entries to bound memory. Called periodically off the hot
    /// path.
    pub fn sweep(&self, now: Instant) {
        self.entries
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|_, &mut expiry| now < expiry);
    }

    /// Number of retained entries (for tests / metrics).
    pub fn len(&self) -> usize {
        self.entries.read().unwrap_or_else(|e| e.into_inner()).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(base: Instant, ms: u64) -> Instant {
        base + Duration::from_millis(ms)
    }

    #[test]
    fn blocks_until_ttl_then_reads_unblocked() {
        let t0 = Instant::now();
        let c = NegativeCache::new();
        assert!(!c.is_blocked("k", t0));
        c.block("k", Duration::from_millis(200), t0);
        assert!(c.is_blocked("k", at(t0, 100)));
        assert!(!c.is_blocked("k", at(t0, 200))); // expiry is exclusive
        assert!(!c.is_blocked("k", at(t0, 300)));
    }

    #[test]
    fn block_extends_but_never_shrinks() {
        let t0 = Instant::now();
        let c = NegativeCache::new();
        c.block("k", Duration::from_millis(500), t0);
        c.block("k", Duration::from_millis(100), t0); // shorter → no shrink
        assert!(c.is_blocked("k", at(t0, 300)));
        c.block("k", Duration::from_millis(1000), t0); // longer → extends
        assert!(c.is_blocked("k", at(t0, 800)));
    }

    #[test]
    fn invalidate_clears_immediately() {
        let t0 = Instant::now();
        let c = NegativeCache::new();
        c.block("k", Duration::from_secs(10), t0);
        assert!(c.is_blocked("k", t0));
        c.invalidate("k");
        assert!(!c.is_blocked("k", t0));
        c.invalidate("k"); // idempotent
    }

    #[test]
    fn sweep_reaps_expired_only() {
        let t0 = Instant::now();
        let c = NegativeCache::new();
        c.block("expired", Duration::from_millis(100), t0);
        c.block("live", Duration::from_secs(10), t0);
        assert_eq!(c.len(), 2);
        c.sweep(at(t0, 200));
        assert_eq!(c.len(), 1);
        assert!(c.is_blocked("live", at(t0, 200)));
    }
}
