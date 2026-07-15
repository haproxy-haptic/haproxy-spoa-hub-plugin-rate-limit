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

use crate::store::{GcraVerdict, Reservation, Store, StoreError};

struct GlobalBucket {
    tokens: f64,
    ts_ms: u64,
}

pub struct InMemoryStore {
    buckets: Mutex<HashMap<String, GlobalBucket>>,
    /// GCRA theoretical-arrival-time per key (exact mode).
    tats: Mutex<HashMap<String, f64>>,
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
            tats: Mutex::new(HashMap::new()),
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

    async fn gcra_check(
        &self,
        key: &str,
        rate: f64,
        burst: f64,
        cost: f64,
        now_ms: u64,
        _ttl_ms: u64,
    ) -> Result<GcraVerdict, StoreError> {
        if rate <= 0.0 {
            return Ok(GcraVerdict {
                allowed: false,
                retry_after_ms: _ttl_ms,
            });
        }
        let emission = 1000.0 / rate; // ms per token
        let burst_offset = emission * burst;
        let now = now_ms as f64;
        let mut m = self.tats.lock().unwrap_or_else(|e| e.into_inner());
        let tat = m.get(key).copied().unwrap_or(now).max(now);
        let new_tat = tat + cost * emission;
        let allow_at = new_tat - burst_offset;
        if now < allow_at {
            Ok(GcraVerdict {
                allowed: false,
                retry_after_ms: (allow_at - now).ceil().max(0.0) as u64,
            })
        } else {
            m.insert(key.to_string(), new_tat);
            Ok(GcraVerdict {
                allowed: true,
                retry_after_ms: 0,
            })
        }
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

    #[tokio::test]
    async fn gcra_enforces_the_emission_interval_with_burst() {
        let s = InMemoryStore::new();
        // rate 10/s, burst 1 => one token per 100ms, no bursting past 1.
        let now = 1000u64;
        assert!(
            s.gcra_check("k", 10.0, 1.0, 1.0, now, 60_000)
                .await
                .unwrap()
                .allowed
        );
        let v = s
            .gcra_check("k", 10.0, 1.0, 1.0, now, 60_000)
            .await
            .unwrap();
        assert!(!v.allowed);
        assert!(
            v.retry_after_ms > 0 && v.retry_after_ms <= 100,
            "retry={}",
            v.retry_after_ms
        );
        // Exactly one emission interval later → allowed again.
        assert!(
            s.gcra_check("k", 10.0, 1.0, 1.0, now + 100, 60_000)
                .await
                .unwrap()
                .allowed
        );
    }

    #[tokio::test]
    async fn gcra_never_exceeds_the_rate_over_a_window() {
        let s = InMemoryStore::new();
        // rate 5/s, burst 5. Greedy for 2s → admits <= burst + rate*2 = 15.
        let mut admitted = 0u64;
        for ms in 0..=2000u64 {
            if s.gcra_check("k", 5.0, 5.0, 1.0, 1000 + ms, 60_000)
                .await
                .unwrap()
                .allowed
            {
                admitted += 1;
            }
        }
        assert!(admitted <= 5 + 10 + 1, "gcra admitted {admitted} over rate");
        assert!(admitted >= 10, "gcra starved: {admitted}");
    }
}
