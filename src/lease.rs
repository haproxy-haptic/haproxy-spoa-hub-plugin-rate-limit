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
//! authoritative — a pod only ever deposits tokens the store granted). Normal
//! cold start is optimistic (a fresh pool starts full at `B` and reconciles on
//! the first refresh), contributing to the same `N*B` bound. Under local
//! `max_keys` pressure, new entries created after capacity eviction start empty
//! instead; this prevents high-cardinality traffic from repeatedly evicting and
//! recreating keys to mint fresh optimistic tokens.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::breaker::CircuitBreaker;
use crate::bucket::TokenBucket;
use crate::limiter::Verdict;
use crate::rule::Rule;
use crate::store::Store;

#[derive(Debug, Clone, Copy)]
pub struct LeaseConfig {
    /// Lease refresh interval T (ms). Sizes the batch and bounds store work by
    /// active keys and refresh cadence rather than raw request RPS.
    pub interval_ms: u64,
    /// Fraction of the batch below which a request triggers an async prefetch.
    pub low_water: f64,
    /// Store key TTL (ms) — a crashed pod's global state ages out.
    pub ttl_ms: u64,
    /// Maximum number of local keys retained by one sidecar. Bounds memory and
    /// periodic refresh work under high-cardinality traffic.
    pub max_keys: usize,
    /// Drop local entries that have not been seen for this long. The Valkey key
    /// TTL handles global cleanup; this bounds per-sidecar state.
    pub idle_ttl_ms: u64,
    /// Per-store-operation timeout. Exact mode relies on this being lower than
    /// the hub/SPOE timeout so fail-closed verdicts reach HAProxy before the hub
    /// drops the plugin result.
    pub store_timeout_ms: u64,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            interval_ms: 200,
            low_water: 0.2,
            ttl_ms: 60_000,
            max_keys: 50_000,
            idle_ttl_ms: 120_000,
            store_timeout_ms: 25,
        }
    }
}

struct Entry {
    bucket: Arc<TokenBucket>,
    rule: RwLock<Rule>,
    last_seen: Mutex<Instant>,
    refresh_pending: AtomicBool,
}

impl Entry {
    fn new(rule: &Rule, batch: f64, optimistic: bool, now: Instant) -> Self {
        Self {
            // Pool: rate 0 (no local refill), capacity = one batch. Normal
            // cold-starts begin full for one optimistic batch; capacity-pressure
            // cold-starts begin empty and wait for a shared lease.
            bucket: Arc::new(TokenBucket::new_with_tokens(
                0.0,
                batch,
                if optimistic { batch } else { 0.0 },
                now,
            )),
            rule: RwLock::new(rule.clone()),
            last_seen: Mutex::new(now),
            refresh_pending: AtomicBool::new(false),
        }
    }

    fn touch(&self, now: Instant) {
        *self.last_seen.lock().unwrap_or_else(|e| e.into_inner()) = now;
    }

    fn idle_for(&self, now: Instant) -> Duration {
        now.checked_duration_since(*self.last_seen.lock().unwrap_or_else(|e| e.into_inner()))
            .unwrap_or_default()
    }

    fn rule(&self) -> Rule {
        self.rule.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    fn update_rule(&self, rule: &Rule, batch: f64, now: Instant) -> bool {
        {
            let current = self.rule.read().unwrap_or_else(|e| e.into_inner());
            if *current == *rule {
                return false;
            }
        }
        let mut current = self.rule.write().unwrap_or_else(|e| e.into_inner());
        if *current == *rule {
            return false;
        }
        *current = rule.clone();
        // Local pool capacity is derived from the rule's batch. Reconfigure it
        // immediately so annotation changes converge for already-active keys.
        self.bucket.set_lease(0.0, batch, now);
        true
    }

    fn mark_refresh_pending(&self) -> bool {
        !self.refresh_pending.swap(true, Ordering::AcqRel)
    }

    fn clear_refresh_pending(&self) {
        self.refresh_pending.store(false, Ordering::Release);
    }
}

pub struct LeaseManager<S: Store> {
    entries: RwLock<HashMap<String, Arc<Entry>>>,
    store: Arc<S>,
    breaker: CircuitBreaker,
    config: LeaseConfig,
}

impl<S: Store> LeaseManager<S> {
    pub fn new(store: Arc<S>, breaker: CircuitBreaker, mut config: LeaseConfig) -> Self {
        config.interval_ms = config.interval_ms.max(1);
        config.ttl_ms = config.ttl_ms.max(1);
        config.max_keys = config.max_keys.max(1);
        config.idle_ttl_ms = config.idle_ttl_ms.max(config.interval_ms);
        config.store_timeout_ms = config.store_timeout_ms.max(1);
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
        let (entry, cold, rule_changed) = self.upsert(key, rule, now);
        let d = entry.bucket.try_acquire(rule.cost, now);
        let low = self.config.low_water * self.batch(rule);
        let wants_refresh = cold || rule_changed || (entry.bucket.available(now) < low);
        let needs_refresh = wants_refresh && entry.mark_refresh_pending();
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
        let Some(entry) = self.lookup(key) else {
            return;
        };
        let rule = entry.rule();
        let deficit = (self.batch(&rule) - entry.bucket.available(now)).max(0.0);
        if deficit <= 0.0 {
            entry.clear_refresh_pending();
            return;
        }
        match tokio::time::timeout(
            Duration::from_millis(self.config.store_timeout_ms),
            self.store.reserve(
                key,
                rule.rate(),
                rule.burst,
                deficit,
                now_ms,
                self.config.ttl_ms,
            ),
        )
        .await
        {
            Ok(Ok(res)) => {
                self.breaker.on_success();
                entry.bucket.deposit(res.granted, now);
                entry.clear_refresh_pending();
            }
            Ok(Err(e)) => {
                log::warn!("ratelimit: lease refresh for {key} failed (fail-static): {e}");
                self.breaker.on_failure(now);
            }
            Err(_) => {
                self.store.mark_unhealthy(key).await;
                log::warn!(
                    "ratelimit: lease refresh for {key} timed out after {}ms (fail-static)",
                    self.config.store_timeout_ms
                );
                self.breaker.on_failure(now);
            }
        }
    }

    /// Refresh every active key (the periodic tick).
    pub async fn refresh_all(&self, now_ms: u64, now: Instant) {
        self.sweep_idle(now);
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
        match tokio::time::timeout(
            Duration::from_millis(self.config.store_timeout_ms),
            self.store.gcra_check(
                key,
                rule.rate(),
                rule.burst,
                rule.cost,
                now_ms,
                self.config.ttl_ms,
            ),
        )
        .await
        {
            Ok(Ok(v)) => {
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
            Ok(Err(e)) => {
                log::warn!("ratelimit: exact check for {key} failed (fail-closed): {e}");
                self.breaker.on_failure(now);
                deny()
            }
            Err(_) => {
                self.store.mark_unhealthy(key).await;
                log::warn!(
                    "ratelimit: exact check for {key} timed out after {}ms (fail-closed)",
                    self.config.store_timeout_ms
                );
                self.breaker.on_failure(now);
                deny()
            }
        }
    }

    pub fn active_keys(&self) -> usize {
        self.entries.read().unwrap_or_else(|e| e.into_inner()).len()
    }

    fn upsert(&self, key: &str, rule: &Rule, now: Instant) -> (Arc<Entry>, bool, bool) {
        let batch = self.batch(rule);
        if let Some(e) = self
            .entries
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
        {
            e.touch(now);
            let rule_changed = e.update_rule(rule, batch, now);
            return (e.clone(), false, rule_changed);
        }
        let mut w = self.entries.write().unwrap_or_else(|e| e.into_inner());
        if let Some(e) = w.get(key) {
            e.touch(now);
            let rule_changed = e.update_rule(rule, batch, now);
            return (e.clone(), false, rule_changed);
        }

        let evicted_for_capacity = self.evict_if_needed(&mut w, now);
        let entry = Arc::new(Entry::new(rule, batch, !evicted_for_capacity, now));
        w.insert(key.to_string(), entry.clone());
        (entry, true, false)
    }

    fn lookup(&self, key: &str) -> Option<Arc<Entry>> {
        self.entries
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
            .cloned()
    }

    fn sweep_idle(&self, now: Instant) {
        let idle_ttl = Duration::from_millis(self.config.idle_ttl_ms);
        self.entries
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|_, entry| entry.idle_for(now) <= idle_ttl);
    }

    fn evict_if_needed(&self, entries: &mut HashMap<String, Arc<Entry>>, now: Instant) -> bool {
        let before = entries.len();
        let idle_ttl = Duration::from_millis(self.config.idle_ttl_ms);
        entries.retain(|_, entry| entry.idle_for(now) <= idle_ttl);
        let mut capacity_evicted = false;
        while entries.len() >= self.config.max_keys {
            let Some(oldest) = entries
                .iter()
                .min_by_key(|(_, entry)| *entry.last_seen.lock().unwrap_or_else(|e| e.into_inner()))
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            entries.remove(&oldest);
            capacity_evicted = true;
        }
        capacity_evicted && before > entries.len()
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
                ..LeaseConfig::default()
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

    #[test]
    fn low_water_refreshes_are_coalesced_per_key() {
        let store = Arc::new(InMemoryStore::new());
        let m = mgr(store);
        let rule = Rule {
            limit: 2.0,
            period_s: 1.0,
            burst: 2.0,
            cost: 1.0,
        };
        let base = Instant::now();

        let (_, first) = m.check("k", &rule, base);
        assert!(first, "cold key should request one refresh");
        let (_, second) = m.check("k", &rule, base);
        assert!(
            !second,
            "same key must not enqueue duplicate refresh work while one is pending"
        );
        let (_, third) = m.check("k", &rule, base);
        assert!(
            !third,
            "over-limit hot key must not grow the prefetch queue"
        );
    }

    #[tokio::test]
    async fn local_key_state_is_bounded_and_idle_entries_are_swept() {
        let store = Arc::new(InMemoryStore::new());
        let m = LeaseManager::new(
            store,
            CircuitBreaker::new(3, Duration::from_secs(1), Instant::now()),
            LeaseConfig {
                max_keys: 2,
                idle_ttl_ms: 1_000,
                ..LeaseConfig::default()
            },
        );
        let rule = Rule {
            limit: 10.0,
            period_s: 1.0,
            burst: 10.0,
            cost: 1.0,
        };
        let base = Instant::now();

        m.check("a", &rule, base);
        m.check("b", &rule, base + Duration::from_millis(1));
        m.check("c", &rule, base + Duration::from_millis(2));
        assert_eq!(m.active_keys(), 2, "max_keys must cap local state");

        m.refresh_all(2_000, base + Duration::from_millis(2_000))
            .await;
        assert_eq!(m.active_keys(), 0, "idle keys must be swept");
    }

    #[test]
    fn capacity_pressure_cold_starts_do_not_mint_optimistic_tokens() {
        let store = Arc::new(InMemoryStore::new());
        let m = LeaseManager::new(
            store,
            CircuitBreaker::new(3, Duration::from_secs(1), Instant::now()),
            LeaseConfig {
                max_keys: 1,
                idle_ttl_ms: 60_000,
                ..LeaseConfig::default()
            },
        );
        let rule = Rule {
            limit: 10.0,
            period_s: 1.0,
            burst: 10.0,
            cost: 1.0,
        };
        let base = Instant::now();

        let (first, _) = m.check("a", &rule, base);
        assert!(
            first.allowed,
            "normal cold-start keeps one optimistic batch"
        );

        let (second, refresh) = m.check("b", &rule, base + Duration::from_millis(1));
        assert!(
            !second.allowed,
            "new keys created by capacity eviction must wait for shared lease"
        );
        assert!(
            refresh,
            "empty capacity-pressure entry must request refresh"
        );
        assert_eq!(m.active_keys(), 1, "max_keys still caps local state");
    }

    #[tokio::test]
    async fn rule_updates_reconfigure_existing_local_bucket() {
        let store = Arc::new(InMemoryStore::new());
        let m = mgr(store);
        let initial = Rule {
            limit: 100.0,
            period_s: 1.0,
            burst: 100.0,
            cost: 1.0,
        };
        let tightened = Rule {
            limit: 5.0,
            period_s: 1.0,
            burst: 5.0,
            cost: 1.0,
        };
        let base = Instant::now();

        let (v, _) = m.check("k", &initial, base);
        assert!(v.allowed);
        m.refresh("k", 0, base).await;

        let (v, refresh) = m.check("k", &tightened, base);
        assert!(v.allowed, "one clamped token remains after tightening");
        assert!(refresh, "rule change should request a shared refresh");

        let (v, _) = m.check("k", &tightened, base);
        assert!(
            !v.allowed,
            "tightened rule should take effect for already-active keys"
        );
    }

    struct SlowStore;

    impl Store for SlowStore {
        async fn reserve(
            &self,
            _k: &str,
            _r: f64,
            _b: f64,
            _req: f64,
            _now: u64,
            _ttl: u64,
        ) -> Result<crate::store::Reservation, crate::store::StoreError> {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(crate::store::Reservation {
                granted: 1.0,
                remaining: 0.0,
            })
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
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(crate::store::GcraVerdict {
                allowed: true,
                retry_after_ms: 0,
            })
        }
    }

    #[tokio::test]
    async fn store_timeouts_fail_static_for_lease_mode() {
        let m = LeaseManager::new(
            Arc::new(SlowStore),
            CircuitBreaker::new(1, Duration::from_secs(1), Instant::now()),
            LeaseConfig {
                store_timeout_ms: 1,
                ..LeaseConfig::default()
            },
        );
        let rule = Rule {
            limit: 10.0,
            period_s: 1.0,
            burst: 10.0,
            cost: 1.0,
        };
        let base = Instant::now();

        let (v, refresh) = m.check("k", &rule, base);
        assert!(v.allowed);
        assert!(refresh);
        m.refresh("k", 0, base).await;
        let (v, _) = m.check("k", &rule, base);
        assert!(v.allowed, "lease mode keeps serving local budget");
    }

    #[tokio::test]
    async fn store_timeouts_fail_closed_for_exact_mode() {
        let m = LeaseManager::new(
            Arc::new(SlowStore),
            CircuitBreaker::new(1, Duration::from_secs(1), Instant::now()),
            LeaseConfig {
                store_timeout_ms: 1,
                ..LeaseConfig::default()
            },
        );
        let rule = Rule {
            limit: 10.0,
            period_s: 1.0,
            burst: 10.0,
            cost: 1.0,
        };

        let v = m.exact_check("k", &rule, 0, Instant::now()).await;
        assert!(!v.allowed, "exact mode must fail closed on store timeout");
    }
}
