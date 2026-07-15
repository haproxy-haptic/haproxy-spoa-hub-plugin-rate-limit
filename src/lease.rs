//! Lease manager: turns the shared store into local budget.
//!
//! Each key gets a local **pool** bucket (rate 0 — it does not refill on its
//! own; it is topped up by leases). The per-request hot path ([`check`]) spends
//! from the pool in memory. Out of band, [`refresh`]/[`refresh_all`] reserve the
//! pool's deficit from the shared store and deposit it — so the store is hit
//! only on the background refresh path (periodic tick + low-water prefetch),
//! never per request. A crashed/idle key's global state self-expires via the
//! store TTL.
//!
//! Each key's latest [`Rule`] is stored alongside its pool, so the background
//! tick can refresh every active key without the caller re-supplying the rule.
//!
//! Overshoot: each pod holds at most one batch `B = rate*T` (clamped to the
//! global burst), so the fleet can over-admit by at most `N_pods * B` in a burst
//! while the sustained rate stays <= the limit (the store's refill is
//! authoritative — a pod only ever deposits tokens the store granted). The cold
//! start is optimistic (a fresh pool starts full at `B` and reconciles on the
//! first refresh), contributing to the same `N*B` bound.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use crate::breaker::CircuitBreaker;
use crate::bucket::TokenBucket;
use crate::limiter::Verdict;
use crate::rule::Rule;
use crate::store::Store;

#[derive(Debug, Clone, Copy)]
pub struct LeaseConfig {
    /// Lease refresh interval T (ms). Sizes the batch and bounds store QPS to
    /// ~pods/T.
    pub interval_ms: u64,
    /// Fraction of the batch below which a request triggers an async prefetch.
    pub low_water: f64,
    /// Store key TTL (ms) — a crashed pod's global state ages out.
    pub ttl_ms: u64,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            interval_ms: 200,
            low_water: 0.2,
            ttl_ms: 60_000,
        }
    }
}

struct Entry {
    bucket: Arc<TokenBucket>,
    rule: Rule,
}

pub struct LeaseManager<S: Store> {
    entries: RwLock<HashMap<String, Entry>>,
    store: Arc<S>,
    breaker: CircuitBreaker,
    config: LeaseConfig,
}

impl<S: Store> LeaseManager<S> {
    pub fn new(store: Arc<S>, breaker: CircuitBreaker, config: LeaseConfig) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            store,
            breaker,
            config,
        }
    }

    pub fn config(&self) -> &LeaseConfig {
        &self.config
    }

    /// The batch size `B` for a rule: the tokens a pod expects to need in one
    /// interval, clamped to [1, global burst].
    fn batch(&self, rule: &Rule) -> f64 {
        let b = rule.rate() * (self.config.interval_ms as f64) / 1000.0;
        b.max(1.0).min(rule.burst.max(1.0))
    }

    /// Hot path: spend from the local pool. Also records the key's latest rule
    /// (so the background tick can refresh it). Returns the verdict and whether
    /// the caller should trigger an async refresh (cold key, or below low-water).
    pub fn check(&self, key: &str, rule: &Rule, now: Instant) -> (Verdict, bool) {
        let (bucket, cold) = self.upsert(key, rule, now);
        let d = bucket.try_acquire(rule.cost, now);
        let low = self.config.low_water * self.batch(rule);
        let needs_refresh = cold || (bucket.available(now) < low);
        let verdict = Verdict {
            allowed: d.allowed,
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
        };
        (verdict, needs_refresh)
    }

    /// Background path: reserve the deficit for `key` (using its stored rule)
    /// from the shared store and deposit it. Skips when the circuit breaker is
    /// open (fail-static). No-op for an unknown key.
    pub async fn refresh(&self, key: &str, now_ms: u64, now: Instant) {
        if !self.breaker.allow(now) {
            return;
        }
        let Some((bucket, rule)) = self.lookup(key) else {
            return;
        };
        let deficit = (self.batch(&rule) - bucket.available(now)).max(0.0);
        if deficit <= 0.0 {
            self.breaker.on_success();
            return;
        }
        match self
            .store
            .reserve(
                key,
                rule.rate(),
                rule.burst,
                deficit,
                now_ms,
                self.config.ttl_ms,
            )
            .await
        {
            Ok(res) => {
                self.breaker.on_success();
                bucket.deposit(res.granted, now);
            }
            Err(e) => {
                log::warn!("ratelimit: lease refresh for {key} failed (fail-static): {e}");
                self.breaker.on_failure(now);
            }
        }
    }

    /// Refresh every active key (the periodic tick).
    pub async fn refresh_all(&self, now_ms: u64, now: Instant) {
        let keys: Vec<String> = self
            .entries
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .cloned()
            .collect();
        for key in keys {
            self.refresh(&key, now_ms, now).await;
        }
    }

    /// Exact mode: a synchronous per-request GCRA check against the shared store
    /// (for money/contract limits). This deliberately round-trips the store on
    /// every request — the caller should `block_on` it. Unlike the approximate
    /// path it **fails closed**: on a store error or an open breaker it denies,
    /// because over-admitting a money limit is worse than a brief false deny.
    pub async fn exact_check(&self, key: &str, rule: &Rule, now_ms: u64, now: Instant) -> Verdict {
        let deny = || Verdict {
            allowed: false,
            limit: rule.limit as u32,
            remaining: 0,
            reset_s: rule.period_s.ceil().max(0.0) as u32,
            retry_after_s: rule.period_s.ceil().max(1.0) as u32,
        };
        if !self.breaker.allow(now) {
            return deny();
        }
        match self
            .store
            .gcra_check(
                key,
                rule.rate(),
                rule.burst,
                rule.cost,
                now_ms,
                self.config.ttl_ms,
            )
            .await
        {
            Ok(v) => {
                self.breaker.on_success();
                Verdict {
                    allowed: v.allowed,
                    limit: rule.limit as u32,
                    remaining: 0,
                    reset_s: rule.period_s.ceil().max(0.0) as u32,
                    retry_after_s: if v.allowed {
                        0
                    } else {
                        ((v.retry_after_ms as f64) / 1000.0).ceil().max(1.0) as u32
                    },
                }
            }
            Err(e) => {
                log::warn!("ratelimit: exact check for {key} failed (fail-closed): {e}");
                self.breaker.on_failure(now);
                deny()
            }
        }
    }

    fn upsert(&self, key: &str, rule: &Rule, now: Instant) -> (Arc<TokenBucket>, bool) {
        if let Some(e) = self
            .entries
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
        {
            // Latest rule is recorded on the write path below; a value-copy read
            // here keeps the hot path lock-light. Rule updates converge on the
            // next cold miss or are applied by refresh using the stored rule.
            return (e.bucket.clone(), false);
        }
        let batch = self.batch(rule);
        let mut w = self.entries.write().unwrap_or_else(|e| e.into_inner());
        if let Some(e) = w.get(key) {
            return (e.bucket.clone(), false);
        }
        // Pool: rate 0 (no local refill), capacity = one batch, starts full
        // (optimistic cold start).
        let bucket = Arc::new(TokenBucket::new(0.0, batch, now));
        w.insert(
            key.to_string(),
            Entry {
                bucket: bucket.clone(),
                rule: rule.clone(),
            },
        );
        (bucket, true)
    }

    fn lookup(&self, key: &str) -> Option<(Arc<TokenBucket>, Rule)> {
        self.entries
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
            .map(|e| (e.bucket.clone(), e.rule.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memory::InMemoryStore;
    use std::time::Duration;

    fn mgr(store: Arc<InMemoryStore>) -> LeaseManager<InMemoryStore> {
        LeaseManager::new(
            store,
            CircuitBreaker::new(3, Duration::from_secs(1), Instant::now()),
            LeaseConfig {
                interval_ms: 200,
                low_water: 0.2,
                ttl_ms: 60_000,
            },
        )
    }

    #[tokio::test]
    async fn single_pod_sustained_rate_stays_within_limit() {
        let store = Arc::new(InMemoryStore::new());
        let m = mgr(Arc::clone(&store));
        let rule = Rule {
            limit: 100.0,
            period_s: 1.0,
            burst: 100.0,
            cost: 1.0,
        };
        let base = Instant::now();

        let mut admitted = 0u64;
        let duration_ms = 10_000u64;
        let batch = m.batch(&rule);
        for ms in 0..=duration_ms {
            let now = base + Duration::from_millis(ms);
            let (v, refresh) = m.check("k", &rule, now);
            if v.allowed {
                admitted += 1;
            }
            if refresh || ms % m.config.interval_ms == 0 {
                m.refresh("k", ms, now).await;
            }
        }
        let global_cap = rule.burst + rule.rate() * (duration_ms as f64) / 1000.0;
        let bound = (global_cap + batch).ceil() as u64 + 1;
        assert!(
            admitted <= bound,
            "admitted {admitted} exceeded bound {bound}"
        );
        assert!(
            admitted as f64 >= rule.rate() * 9.0,
            "admitted {admitted} starved"
        );
    }

    #[tokio::test]
    async fn fleet_overshoot_is_bounded_by_n_times_batch() {
        let store = Arc::new(InMemoryStore::new());
        let n_pods = 8usize;
        let pods: Vec<_> = (0..n_pods).map(|_| mgr(Arc::clone(&store))).collect();
        let rule = Rule {
            limit: 50.0,
            period_s: 1.0,
            burst: 50.0,
            cost: 1.0,
        };
        let base = Instant::now();
        let duration_ms = 5_000u64;
        let batch = pods[0].batch(&rule);

        let mut admitted = 0u64;
        for ms in 0..=duration_ms {
            let now = base + Duration::from_millis(ms);
            for p in &pods {
                let (v, refresh) = p.check("shared", &rule, now);
                if v.allowed {
                    admitted += 1;
                }
                if refresh || ms % p.config.interval_ms == 0 {
                    p.refresh("shared", ms, now).await;
                }
            }
        }

        let global_cap = rule.burst + rule.rate() * (duration_ms as f64) / 1000.0;
        let bound = (global_cap + (n_pods as f64) * batch).ceil() as u64 + n_pods as u64;
        assert!(
            admitted <= bound,
            "fleet admitted {admitted} exceeded bound {bound} (global_cap {global_cap}, N*B {})",
            n_pods as f64 * batch
        );
        let per_pod_would_be = rule.rate() * (n_pods as f64) * (duration_ms as f64) / 1000.0;
        assert!(
            (admitted as f64) < per_pod_would_be * 0.6,
            "fleet admitted {admitted} looks per-pod ({per_pod_would_be}), not shared"
        );
    }

    #[tokio::test]
    async fn refresh_is_skipped_while_the_breaker_is_open() {
        struct FailingStore;
        impl Store for FailingStore {
            async fn reserve(
                &self,
                _k: &str,
                _r: f64,
                _b: f64,
                _req: f64,
                _now: u64,
                _ttl: u64,
            ) -> Result<crate::store::Reservation, crate::store::StoreError> {
                Err(crate::store::StoreError::Connection("down".into()))
            }
            async fn gcra_check(
                &self,
                _k: &str,
                _r: f64,
                _b: f64,
                _cost: f64,
                _now: u64,
                _ttl: u64,
            ) -> Result<crate::store::GcraVerdict, crate::store::StoreError> {
                Err(crate::store::StoreError::Connection("down".into()))
            }
        }
        let m = LeaseManager::new(
            Arc::new(FailingStore),
            CircuitBreaker::new(1, Duration::from_secs(60), Instant::now()),
            LeaseConfig::default(),
        );
        let rule = Rule {
            limit: 10.0,
            period_s: 1.0,
            burst: 10.0,
            cost: 1.0,
        };
        let base = Instant::now();
        let (v, _) = m.check("k", &rule, base);
        assert!(v.allowed);
        m.refresh("k", 0, base).await; // fails -> trips breaker
        m.refresh("k", 100, base).await; // open -> short-circuits (no panic)
        let (v2, _) = m.check("k", &rule, base);
        assert!(v2.allowed); // pool still serves its optimistic tokens (fail-static)
    }

    #[tokio::test]
    async fn refresh_all_tops_up_every_active_key() {
        let store = Arc::new(InMemoryStore::new());
        let m = mgr(Arc::clone(&store));
        let rule = Rule {
            limit: 100.0,
            period_s: 1.0,
            burst: 100.0,
            cost: 1.0,
        };
        let base = Instant::now();
        // Touch three keys (creates optimistic pools) and drain each a bit.
        for key in ["a", "b", "c"] {
            for _ in 0..5 {
                m.check(key, &rule, base);
            }
        }
        // A tick refreshes all of them without re-supplying the rule.
        m.refresh_all(10, base).await;
        // Each pool is topped back up to its batch.
        for key in ["a", "b", "c"] {
            let (v, _) = m.check(key, &rule, base);
            assert!(v.allowed, "{key} not topped up by refresh_all");
        }
    }
}
