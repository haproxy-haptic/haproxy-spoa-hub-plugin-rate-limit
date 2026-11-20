//! Per-sidecar cache of meter verdicts.
//!
//! A meter read (`mode=meter`, zero cost) runs on every request of a route, so
//! it must not round-trip the store each time. A verdict is reused for one
//! lease interval: store reads are bounded by active keys per interval, like
//! lease refreshes, and a key that went over on another sidecar is seen here
//! within one interval.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::store::MeterVerdict;

pub struct MeterCache {
    entries: Mutex<HashMap<String, (MeterVerdict, Instant)>>,
    max_keys: usize,
}

impl MeterCache {
    pub fn new(max_keys: usize) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            max_keys: max_keys.max(1),
        }
    }

    pub fn get(&self, key: &str, now: Instant) -> Option<MeterVerdict> {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
            .filter(|(_, expiry)| now < *expiry)
            .map(|(verdict, _)| *verdict)
    }

    /// Cache `verdict` for `ttl`, but never past the moment an over verdict
    /// drains. At capacity, expired entries are dropped; if none are, the
    /// verdict is not cached and the next read asks the store.
    pub fn put(&self, key: &str, verdict: MeterVerdict, ttl: Duration, now: Instant) {
        let ttl = if verdict.under {
            ttl
        } else {
            ttl.min(Duration::from_millis(verdict.over_ms.max(1)))
        };
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if entries.len() >= self.max_keys && !entries.contains_key(key) {
            entries.retain(|_, (_, expiry)| now < *expiry);
            if entries.len() >= self.max_keys {
                return;
            }
        }
        entries.insert(key.to_string(), (verdict, now + ttl));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const UNDER: MeterVerdict = MeterVerdict {
        under: true,
        over_ms: 0,
    };

    #[test]
    fn a_verdict_is_served_until_its_ttl() {
        let cache = MeterCache::new(8);
        let t0 = Instant::now();
        assert_eq!(cache.get("k", t0), None);
        cache.put("k", UNDER, Duration::from_millis(100), t0);
        assert_eq!(cache.get("k", t0 + Duration::from_millis(99)), Some(UNDER));
        assert_eq!(cache.get("k", t0 + Duration::from_millis(100)), None);
    }

    #[test]
    fn an_over_verdict_expires_when_the_key_drains() {
        let cache = MeterCache::new(8);
        let t0 = Instant::now();
        let over = MeterVerdict {
            under: false,
            over_ms: 20,
        };
        cache.put("k", over, Duration::from_millis(100), t0);
        assert_eq!(cache.get("k", t0 + Duration::from_millis(19)), Some(over));
        assert_eq!(cache.get("k", t0 + Duration::from_millis(20)), None);
    }

    #[test]
    fn a_full_cache_drops_expired_entries_before_refusing_new_ones() {
        let cache = MeterCache::new(1);
        let t0 = Instant::now();
        cache.put("a", UNDER, Duration::from_millis(10), t0);
        cache.put("b", UNDER, Duration::from_millis(10), t0);
        assert_eq!(cache.get("b", t0), None);
        let later = t0 + Duration::from_millis(10);
        cache.put("b", UNDER, Duration::from_millis(10), later);
        assert_eq!(cache.get("b", later), Some(UNDER));
    }
}
