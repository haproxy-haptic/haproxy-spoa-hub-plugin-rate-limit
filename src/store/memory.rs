//! In-process [`Store`] — the same reservation arithmetic as the Valkey Lua,
//! run against a local map.
//!
//! Two uses: (1) single-instance deployments (no shared store configured — the
//! limit is then per-pod, like the stick-table fallback but through the plugin
//! path); (2) a deterministic, Valkey-free store for unit-testing the lease
//! manager. Sharing ONE `InMemoryStore` across several simulated sidecars models
//! a shared global bucket exactly.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::store::{Reservation, Store, StoreError};

struct GlobalBucket {
    tokens: f64,
    ts_ms: u64,
}

pub struct InMemoryStore {
    buckets: Mutex<HashMap<String, GlobalBucket>>,
}

impl Default for InMemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self {
            buckets: Mutex::new(HashMap::new()),
        }
    }
}

impl Store for InMemoryStore {
    async fn reserve(
        &self,
        key: &str,
        rate: f64,
        burst: f64,
        requested: f64,
        now_ms: u64,
        _ttl_ms: u64,
    ) -> Result<Reservation, StoreError> {
        let mut m = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
        let b = m.entry(key.to_string()).or_insert(GlobalBucket {
            tokens: burst,
            ts_ms: now_ms,
        });
        // Refill by elapsed time (guard non-monotonic), capped at burst.
        let elapsed = now_ms.saturating_sub(b.ts_ms) as f64 / 1000.0;
        b.tokens = (b.tokens + elapsed * rate).min(burst);
        b.ts_ms = now_ms;
        // Grant.
        let granted = requested.max(0.0).min(b.tokens);
        b.tokens -= granted;
        Ok(Reservation {
            granted,
            remaining: b.tokens,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn matches_the_lua_semantics() {
        let s = InMemoryStore::new();
        // burst 100, request 30 => grant 30, remaining 70.
        let r = s
            .reserve("k", 10.0, 100.0, 30.0, 1000, 60_000)
            .await
            .unwrap();
        assert_eq!(r.granted, 30.0);
        assert!((r.remaining - 70.0).abs() < 1e-9);
        // request 200 (only 70 left) => grant 70.
        let r2 = s
            .reserve("k", 10.0, 100.0, 200.0, 1000, 60_000)
            .await
            .unwrap();
        assert!((r2.granted - 70.0).abs() < 1e-9);
        // 1s later => +10 refilled.
        let r3 = s
            .reserve("k", 10.0, 100.0, 200.0, 2000, 60_000)
            .await
            .unwrap();
        assert!((r3.granted - 10.0).abs() < 1e-9);
    }
}
