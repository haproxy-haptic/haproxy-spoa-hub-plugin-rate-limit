//! Lease manager: turns the shared store into local budget.
//!
//! Each key gets a local **pool** bucket (rate 0 — it does not refill on its
//! own; it is topped up by leases) with capacity equal to the rule's `burst`,
//! so store grants for a real burst fit locally. The per-request hot path
//! ([`check`]) spends from the pool in memory. Out of band,
//! [`refresh`]/[`refresh_all`] reserve the pool's deficit from the shared store
//! and deposit it, sized to the larger of one batch `B = rate*T` and the key's
//! observed per-interval demand (Doorman-style "wants"), capped at `burst` —
//! so recurring bursts are pre-stocked and the store is hit only on the
//! background refresh path for steady traffic. A crashed/idle key's global
//! state self-expires via the store TTL.
//!
//! When the pool cannot cover a request, [`borrow_check`] escalates: one
//! bounded, per-key-coalesced synchronous `reserve` against the authoritative
//! store bucket before denying. An empty lease pool is not evidence the caller
//! is over limit — `burst` lives in the store — so this is the path that makes
//! `burst` real for concurrent bursts (the Envoy local-absorb/global-
//! authoritative layering). A borrow that finds the store bucket empty is an
//! honest over-limit deny and arms a one-interval backoff, bounding a flood's
//! store cost to the refresh cadence. Store-unreachable borrows resolve via
//! the configured local, open, or closed failure mode.
//!
//! Each key's latest [`Rule`] is stored alongside its pool, so the background
//! tick can refresh every active key without the caller re-supplying the rule.
//!
//! Overshoot: tokens only enter a pool via store grants (already debited from
//! the authoritative bucket) — except the optimistic cold-start fill of one
//! batch `B`, so the fleet can over-admit by at most `N_pods * B` in a burst
//! while the sustained rate stays <= the limit. Normal cold start is
//! optimistic (a fresh pool starts with `B` tokens and reconciles on the first
//! refresh). Under local `max_keys` pressure, new entries created after
//! capacity eviction start empty instead; this prevents high-cardinality
//! traffic from repeatedly evicting and recreating keys to mint fresh
//! optimistic tokens.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::breaker::CircuitBreaker;
use crate::bucket::TokenBucket;
use crate::config::StoreFailureMode;
use crate::fallback::{FallbackDecision, FallbackLimiter};
use crate::limiter::Verdict;
use crate::rule::Rule;
use crate::store::Store;

/// How a [`LeaseManager::borrow_check`] resolved — drives per-outcome metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BorrowOutcome {
    /// A racing deposit covered the request before any coordination.
    LocalRetry,
    /// The coalescing leader's deposit covered this waiter.
    CoalescedRetry,
    /// This request led a store borrow and was admitted from the grant.
    Borrowed,
    /// The store's authoritative bucket is empty: honest over-limit deny.
    Exhausted,
    /// Within the post-exhaustion backoff window: denied without store I/O.
    Backoff,
    /// Store unreachable, `fail_open=true`: admitted degraded.
    FailOpen,
    /// Store unreachable, `fail_open=false`: denied.
    FailClosed,
    /// Store unreachable: the per-sidecar fallback admitted the request.
    FallbackAllowed,
    /// Store unreachable: the per-sidecar fallback exhausted its budget.
    FallbackLimited,
    /// Store unreachable and the bounded local registry is full: admitted.
    FallbackCapacityOpen,
}

impl BorrowOutcome {
    /// Stable label for the metrics `outcome` dimension.
    pub fn as_str(self) -> &'static str {
        match self {
            BorrowOutcome::LocalRetry => "borrow_local_retry",
            BorrowOutcome::CoalescedRetry => "borrow_coalesced",
            BorrowOutcome::Borrowed => "borrowed",
            BorrowOutcome::Exhausted => "limited",
            BorrowOutcome::Backoff => "limited_backoff",
            BorrowOutcome::FailOpen => "borrow_fail_open",
            BorrowOutcome::FailClosed => "borrow_fail_closed",
            BorrowOutcome::FallbackAllowed => "borrow_fallback_allowed",
            BorrowOutcome::FallbackLimited => "borrow_fallback_limited",
            BorrowOutcome::FallbackCapacityOpen => "borrow_fallback_capacity_open",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExactOutcome {
    Allowed,
    Limited,
    FailOpen,
    FailClosed,
    FallbackAllowed,
    FallbackLimited,
    FallbackCapacityOpen,
}

impl ExactOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Allowed => "exact_allowed",
            Self::Limited => "exact_limited",
            Self::FailOpen => "exact_fail_open",
            Self::FailClosed => "exact_fail_closed",
            Self::FallbackAllowed => "exact_fallback_allowed",
            Self::FallbackLimited => "exact_fallback_limited",
            Self::FallbackCapacityOpen => "exact_fallback_capacity_open",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StoreFailureResolution {
    Open,
    Closed,
    FallbackAllowed,
    FallbackLimited,
    FallbackCapacityOpen,
}

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
    /// Per-store-operation timeout. This must be lower than the hub/SPOE
    /// timeout so the selected fallback verdict reaches HAProxy.
    pub store_timeout_ms: u64,
    /// When the pool is empty, allow one bounded synchronous borrow from the
    /// shared store before denying (see [`LeaseManager::borrow_check`]). This is
    /// what makes the rule's `burst` real for concurrent bursts; disable to
    /// restore the pre-0.2 instant-deny behavior.
    pub borrow_on_empty: bool,
    /// Preserve the pre-0.3 unbacked batch for configurations that do not set
    /// an explicit store failure policy.
    pub optimistic_cold_start: bool,
    /// Store-unavailability policy after the local lease pool is empty.
    pub borrow_failure_mode: StoreFailureMode,
    /// Store-unavailability policy for synchronous exact checks.
    pub exact_failure_mode: StoreFailureMode,
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
            borrow_on_empty: true,
            optimistic_cold_start: true,
            borrow_failure_mode: StoreFailureMode::Open,
            exact_failure_mode: StoreFailureMode::Closed,
        }
    }
}

struct Entry {
    bucket: Arc<TokenBucket>,
    rule: RwLock<Rule>,
    last_seen: Mutex<Instant>,
    refresh_pending: AtomicBool,
    /// Cost attempted since the last demand harvest — the Doorman-style
    /// "wants" signal that sizes refresh/borrow asks to observed demand
    /// instead of the static `rate x interval` batch.
    demand: Mutex<f64>,
    /// Serializes synchronous borrows per key so a concurrent burst costs one
    /// store round-trip, not one per request; waiters re-check the pool after
    /// the leader's deposit.
    borrow_gate: tokio::sync::Mutex<()>,
    /// After a borrow found the global bucket empty, skip further store
    /// borrows for this key until this instant — a sustained flood costs the
    /// store at most one round-trip per refresh interval, preserving the
    /// amortization invariant even while over limit.
    borrow_backoff_until: Mutex<Option<Instant>>,
    /// A failed borrow covers the whole coalesced waiter cohort. Waiters use
    /// the configured fallback until this short backoff expires.
    borrow_failure_until: Mutex<Option<Instant>>,
    /// Global tokens remaining as reported by the store at the last
    /// reservation (f64 bits). Feeds honest `remaining` headers instead of
    /// surfacing the tiny local pool.
    global_remaining: std::sync::atomic::AtomicU64,
}

impl Entry {
    fn new(rule: &Rule, batch: f64, optimistic: bool, now: Instant) -> Self {
        // Pool capacity is the rule's burst (so borrowed/leased tokens for a
        // real burst FIT locally); it never refills on its own (rate 0) — all
        // tokens are store-granted, except the optimistic cold-start batch.
        let capacity = rule.burst.max(batch);
        Self {
            bucket: Arc::new(TokenBucket::new_with_tokens(
                0.0,
                capacity,
                if optimistic { batch } else { 0.0 },
                now,
            )),
            rule: RwLock::new(rule.clone()),
            last_seen: Mutex::new(now),
            refresh_pending: AtomicBool::new(false),
            demand: Mutex::new(0.0),
            borrow_gate: tokio::sync::Mutex::new(()),
            borrow_backoff_until: Mutex::new(None),
            borrow_failure_until: Mutex::new(None),
            // Best guess before the first store contact: the full budget minus
            // the optimistic local fill.
            global_remaining: std::sync::atomic::AtomicU64::new(
                (rule.burst.max(0.0) - if optimistic { batch } else { 0.0 })
                    .max(0.0)
                    .to_bits(),
            ),
        }
    }

    fn add_demand(&self, cost: f64) {
        *self.demand.lock().unwrap_or_else(|e| e.into_inner()) += cost.max(0.0);
    }

    /// Read-and-reset the accumulated demand (refresh path).
    fn take_demand(&self) -> f64 {
        let mut d = self.demand.lock().unwrap_or_else(|e| e.into_inner());
        std::mem::take(&mut *d)
    }

    /// Read without resetting (borrow path — refresh keeps its window signal).
    fn peek_demand(&self) -> f64 {
        *self.demand.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn set_global_remaining(&self, remaining: f64) {
        self.global_remaining.store(
            remaining.max(0.0).to_bits(),
            std::sync::atomic::Ordering::Release,
        );
    }

    fn global_remaining(&self) -> f64 {
        f64::from_bits(
            self.global_remaining
                .load(std::sync::atomic::Ordering::Acquire),
        )
    }

    fn in_borrow_backoff(&self, now: Instant) -> bool {
        self.borrow_backoff_until
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some_and(|until| now < until)
    }

    fn set_borrow_backoff(&self, until: Instant) {
        *self
            .borrow_backoff_until
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(until);
    }

    fn in_borrow_failure_backoff(&self, now: Instant) -> bool {
        self.borrow_failure_until
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some_and(|until| now < until)
    }

    fn set_borrow_failure_backoff(&self, until: Instant) {
        *self
            .borrow_failure_until
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(until);
    }

    fn clear_borrow_failure_backoff(&self) {
        *self
            .borrow_failure_until
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = None;
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
        // Local pool capacity tracks the rule's burst (floored at one batch).
        // Reconfigure immediately so annotation changes converge for
        // already-active keys.
        self.bucket.set_lease(0.0, rule.burst.max(batch), now);
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
    fallback: FallbackLimiter,
}

impl<S: Store> LeaseManager<S> {
    pub fn new(store: Arc<S>, breaker: CircuitBreaker, mut config: LeaseConfig) -> Self {
        config.interval_ms = config.interval_ms.max(1);
        config.ttl_ms = config.ttl_ms.max(1);
        config.max_keys = config.max_keys.max(1);
        config.idle_ttl_ms = config.idle_ttl_ms.max(config.interval_ms);
        config.store_timeout_ms = config.store_timeout_ms.max(1);
        let fallback =
            FallbackLimiter::new(config.max_keys, Duration::from_millis(config.idle_ttl_ms));
        Self {
            entries: RwLock::new(HashMap::new()),
            store,
            breaker,
            config,
            fallback,
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
        entry.add_demand(rule.cost);
        let d = entry.bucket.try_acquire(rule.cost, now);
        let low = self.config.low_water * self.batch(rule);
        let wants_refresh = cold || rule_changed || (entry.bucket.available(now) < low);
        let needs_refresh = wants_refresh && entry.mark_refresh_pending();
        let verdict = Self::verdict_from_decision(rule, &d, entry.global_remaining());
        (verdict, needs_refresh)
    }

    /// Build a Verdict from a local bucket decision plus the last-known global
    /// remaining. `remaining` reports the budget this pod knows about (local
    /// pool + store-side remaining at the last reservation) instead of the
    /// tiny local lease pool; `reset` is the time until that budget is fully
    /// replenished (counts down as tokens refill) rather than a static period.
    fn verdict_from_decision(
        rule: &Rule,
        d: &crate::bucket::Decision,
        global_remaining: f64,
    ) -> Verdict {
        let known_remaining = (d.remaining + global_remaining).max(0.0);
        let reset_s = if rule.rate() > 0.0 {
            ((rule.burst - known_remaining).max(0.0) / rule.rate()).ceil()
        } else {
            rule.period_s.ceil()
        };
        Verdict {
            allowed: d.allowed,
            degraded: false,
            accounted: true,
            limit: rule.limit as u32,
            remaining: known_remaining.floor().min(u32::MAX as f64) as u32,
            reset_s: reset_s.clamp(0.0, rule.period_s.ceil().max(1.0)) as u32,
            retry_after_s: if d.allowed {
                0
            } else if d.retry_after.is_finite() {
                d.retry_after.ceil().max(1.0) as u32
            } else {
                rule.period_s.ceil().max(1.0) as u32
            },
        }
    }

    fn fallback_verdict(rule: &Rule, decision: &crate::bucket::Decision) -> Verdict {
        let reset_s = if rule.rate() > 0.0 {
            ((rule.burst - decision.remaining).max(0.0) / rule.rate()).ceil()
        } else {
            rule.period_s.ceil()
        };
        Verdict {
            allowed: decision.allowed,
            degraded: true,
            accounted: true,
            limit: rule.limit as u32,
            remaining: decision.remaining.floor().min(u32::MAX as f64) as u32,
            reset_s: reset_s.clamp(0.0, rule.period_s.ceil().max(1.0)) as u32,
            retry_after_s: if decision.allowed {
                0
            } else if decision.retry_after.is_finite() {
                decision.retry_after.ceil().max(1.0) as u32
            } else {
                rule.period_s.ceil().max(1.0) as u32
            },
        }
    }

    fn resolve_store_failure(
        &self,
        mode: StoreFailureMode,
        key: &str,
        rule: &Rule,
        now: Instant,
        mut closed: Verdict,
    ) -> (Verdict, StoreFailureResolution) {
        closed.allowed = false;
        closed.degraded = true;
        closed.accounted = false;
        closed.retry_after_s = 1;
        match mode {
            StoreFailureMode::Open => {
                closed.allowed = true;
                closed.retry_after_s = 0;
                (closed, StoreFailureResolution::Open)
            }
            StoreFailureMode::Closed => (closed, StoreFailureResolution::Closed),
            StoreFailureMode::Local => match self.fallback.check(key, rule, now) {
                FallbackDecision::Available(decision) => (
                    Self::fallback_verdict(rule, &decision),
                    if decision.allowed {
                        StoreFailureResolution::FallbackAllowed
                    } else {
                        StoreFailureResolution::FallbackLimited
                    },
                ),
                FallbackDecision::Unavailable(_) => {
                    closed.allowed = true;
                    closed.retry_after_s = 0;
                    (closed, StoreFailureResolution::FallbackCapacityOpen)
                }
            },
        }
    }

    fn borrow_store_failure(
        &self,
        key: &str,
        rule: &Rule,
        now: Instant,
        closed: Verdict,
    ) -> (Verdict, BorrowOutcome) {
        let (verdict, resolution) =
            self.resolve_store_failure(self.config.borrow_failure_mode, key, rule, now, closed);
        let outcome = match resolution {
            StoreFailureResolution::Open => BorrowOutcome::FailOpen,
            StoreFailureResolution::Closed => BorrowOutcome::FailClosed,
            StoreFailureResolution::FallbackAllowed => BorrowOutcome::FallbackAllowed,
            StoreFailureResolution::FallbackLimited => BorrowOutcome::FallbackLimited,
            StoreFailureResolution::FallbackCapacityOpen => BorrowOutcome::FallbackCapacityOpen,
        };
        (verdict, outcome)
    }

    fn exact_store_failure(
        &self,
        key: &str,
        rule: &Rule,
        now: Instant,
        closed: Verdict,
    ) -> (Verdict, ExactOutcome) {
        let (verdict, resolution) =
            self.resolve_store_failure(self.config.exact_failure_mode, key, rule, now, closed);
        let outcome = match resolution {
            StoreFailureResolution::Open => ExactOutcome::FailOpen,
            StoreFailureResolution::Closed => ExactOutcome::FailClosed,
            StoreFailureResolution::FallbackAllowed => ExactOutcome::FallbackAllowed,
            StoreFailureResolution::FallbackLimited => ExactOutcome::FallbackLimited,
            StoreFailureResolution::FallbackCapacityOpen => ExactOutcome::FallbackCapacityOpen,
        };
        (verdict, outcome)
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
        // Demand-driven lease sizing (Doorman-style "wants"): a key whose
        // observed demand exceeds the static batch gets its pool stocked to
        // that demand (capped at burst), so recurring bursts are served from
        // the local pool without borrow round-trips.
        let target = self
            .batch(&rule)
            .max(entry.take_demand())
            .min(rule.burst.max(1.0));
        let deficit = (target - entry.bucket.available(now)).max(0.0);
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
                entry.set_global_remaining(res.remaining);
                entry.bucket.deposit(res.granted, now);
                entry.clear_borrow_failure_backoff();
                entry.clear_refresh_pending();
            }
            Ok(Err(e)) => {
                log::warn!("ratelimit: lease refresh for {key} failed (fail-static): {e}");
                self.breaker.on_failure(Instant::now());
            }
            Err(_) => {
                log::warn!(
                    "ratelimit: lease refresh for {key} timed out after {}ms (fail-static)",
                    self.config.store_timeout_ms
                );
                self.breaker.on_failure(Instant::now());
            }
        }
    }

    /// Refresh every active key (the periodic tick).
    pub async fn refresh_all(&self, now_ms: u64, now: Instant) {
        self.sweep_idle(now);
        self.fallback.sweep_idle(now);
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

    /// Borrow-on-empty: the burst-correctness escalation. Called by the plugin
    /// when [`check`] denied — i.e. the local pool is exhausted — and only
    /// then, so the steady-state hot path stays store-free. Coalesced per key
    /// (one store round-trip serves a whole concurrent burst), bounded by the
    /// store timeout, and rate-limited by a per-key backoff so a sustained
    /// over-limit flood costs the store at most one round-trip per refresh
    /// interval.
    ///
    /// Semantics:
    ///  - store grants tokens → allow (the rule's `burst` is exactly as real
    ///    as the store's authoritative bucket),
    ///  - store answers "empty" → an honest deny (the caller IS over limit),
    ///  - store unreachable (breaker open / error / timeout) → the configured
    ///    local, open, or closed failure mode decides.
    pub async fn borrow_check(
        &self,
        key: &str,
        rule: &Rule,
        now_ms: u64,
        now: Instant,
    ) -> (Verdict, BorrowOutcome) {
        let (entry, _, _) = self.upsert(key, rule, now);

        // A racing borrower (or the background refresh) may have already
        // topped the pool up — retry locally before any coordination.
        let d = entry.bucket.try_acquire(rule.cost, now);
        if d.allowed {
            return (
                Self::verdict_from_decision(rule, &d, entry.global_remaining()),
                BorrowOutcome::LocalRetry,
            );
        }

        // Over-limit flood guard: a recent borrow proved the global bucket
        // empty; don't ask again until the backoff lapses.
        if entry.in_borrow_backoff(now) {
            return (
                Self::verdict_from_decision(rule, &d, entry.global_remaining()),
                BorrowOutcome::Backoff,
            );
        }
        if entry.in_borrow_failure_backoff(now) {
            let closed = Self::verdict_from_decision(rule, &d, entry.global_remaining());
            return self.borrow_store_failure(key, rule, now, closed);
        }

        // Coalesce: one leader per key does the store round-trip; waiters
        // re-check the pool the leader filled.
        let _gate = entry.borrow_gate.lock().await;
        let now = Instant::now();
        let d = entry.bucket.try_acquire(rule.cost, now);
        if d.allowed {
            return (
                Self::verdict_from_decision(rule, &d, entry.global_remaining()),
                BorrowOutcome::CoalescedRetry,
            );
        }
        if entry.in_borrow_backoff(now) {
            return (
                Self::verdict_from_decision(rule, &d, entry.global_remaining()),
                BorrowOutcome::Backoff,
            );
        }
        if entry.in_borrow_failure_backoff(now) {
            let closed = Self::verdict_from_decision(rule, &d, entry.global_remaining());
            return self.borrow_store_failure(key, rule, now, closed);
        }

        let closed = Self::verdict_from_decision(rule, &d, entry.global_remaining());

        if !self.breaker.allow(now) {
            return self.borrow_store_failure(key, rule, now, closed);
        }

        // Ask for observed demand (at least the cost, at least one batch),
        // capped at the burst: a 60-request page load sizes its own lease.
        let ask = self
            .batch(rule)
            .max(entry.peek_demand())
            .max(rule.cost)
            .min(rule.burst.max(rule.cost));
        match tokio::time::timeout(
            Duration::from_millis(self.config.store_timeout_ms),
            self.store.reserve(
                key,
                rule.rate(),
                rule.burst,
                ask,
                now_ms,
                self.config.ttl_ms,
            ),
        )
        .await
        {
            Ok(Ok(res)) => {
                self.breaker.on_success();
                entry.clear_borrow_failure_backoff();
                entry.set_global_remaining(res.remaining);
                entry.bucket.deposit(res.granted, now);
                let d = entry.bucket.try_acquire(rule.cost, now);
                if d.allowed {
                    (
                        Self::verdict_from_decision(rule, &d, entry.global_remaining()),
                        BorrowOutcome::Borrowed,
                    )
                } else {
                    // The authoritative bucket is (nearly) empty: an honest
                    // over-limit deny. Back off further borrows for one
                    // refresh interval so floods cost the store one
                    // round-trip per interval, not one per request.
                    entry.set_borrow_backoff(now + Duration::from_millis(self.config.interval_ms));
                    (
                        Self::verdict_from_decision(rule, &d, entry.global_remaining()),
                        BorrowOutcome::Exhausted,
                    )
                }
            }
            Ok(Err(e)) => {
                log::warn!(
                    "ratelimit: borrow for {key} failed ({}): {e}",
                    self.config.borrow_failure_mode.as_str()
                );
                let failure_now = Instant::now();
                entry.set_borrow_failure_backoff(
                    failure_now + Duration::from_millis(self.config.interval_ms),
                );
                self.breaker.on_failure(failure_now);
                self.borrow_store_failure(key, rule, failure_now, closed)
            }
            Err(_) => {
                log::warn!(
                    "ratelimit: borrow for {key} timed out after {}ms ({})",
                    self.config.store_timeout_ms,
                    self.config.borrow_failure_mode.as_str()
                );
                let failure_now = Instant::now();
                entry.set_borrow_failure_backoff(
                    failure_now + Duration::from_millis(self.config.interval_ms),
                );
                self.breaker.on_failure(failure_now);
                self.borrow_store_failure(key, rule, failure_now, closed)
            }
        }
    }

    /// Exact mode: a synchronous per-request GCRA check against the shared
    /// store. Store failures resolve through the configured failure mode;
    /// local mode is a per-sidecar emergency budget, not fleet-exact GCRA.
    pub async fn exact_check(
        &self,
        key: &str,
        rule: &Rule,
        now_ms: u64,
        now: Instant,
    ) -> (Verdict, ExactOutcome) {
        let deny = || Verdict {
            allowed: false,
            degraded: false,
            accounted: false,
            limit: rule.limit as u32,
            remaining: 0,
            reset_s: rule.period_s.ceil().max(0.0) as u32,
            retry_after_s: rule.period_s.ceil().max(1.0) as u32,
        };
        if !self.breaker.allow(now) {
            return self.exact_store_failure(key, rule, now, deny());
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
                let verdict = Verdict {
                    allowed: v.allowed,
                    degraded: false,
                    accounted: true,
                    limit: rule.limit as u32,
                    remaining: 0,
                    reset_s: rule.period_s.ceil().max(0.0) as u32,
                    retry_after_s: if v.allowed {
                        0
                    } else {
                        ((v.retry_after_ms as f64) / 1000.0).ceil().max(1.0) as u32
                    },
                };
                let outcome = if verdict.allowed {
                    ExactOutcome::Allowed
                } else {
                    ExactOutcome::Limited
                };
                (verdict, outcome)
            }
            Ok(Err(e)) => {
                log::warn!(
                    "ratelimit: exact check for {key} failed ({}): {e}",
                    self.config.exact_failure_mode.as_str()
                );
                let failure_now = Instant::now();
                self.breaker.on_failure(failure_now);
                self.exact_store_failure(key, rule, failure_now, deny())
            }
            Err(_) => {
                log::warn!(
                    "ratelimit: exact check for {key} timed out after {}ms ({})",
                    self.config.store_timeout_ms,
                    self.config.exact_failure_mode.as_str()
                );
                let failure_now = Instant::now();
                self.breaker.on_failure(failure_now);
                self.exact_store_failure(key, rule, failure_now, deny())
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
        let entry = Arc::new(Entry::new(
            rule,
            batch,
            self.config.optimistic_cold_start && !evicted_for_capacity,
            now,
        ));
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

        // Tightening reconfigures the live pool: capacity drops from the old
        // burst (100) to the new one (5), clamping the leased tokens with it.
        // The new burst itself remains spendable — burst is the real local
        // capacity since 0.2 — and the very next request over it is denied.
        for i in 0..5 {
            let (v, _) = m.check("k", &tightened, base);
            assert!(v.allowed, "token {i} within the tightened burst");
        }
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

    struct DownStore;

    impl Store for DownStore {
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

    #[tokio::test]
    async fn explicit_closed_policy_denies_a_cold_key_when_the_store_is_down() {
        let m = LeaseManager::new(
            Arc::new(DownStore),
            CircuitBreaker::new(100, Duration::from_secs(1), Instant::now()),
            LeaseConfig {
                optimistic_cold_start: false,
                borrow_failure_mode: StoreFailureMode::Closed,
                ..LeaseConfig::default()
            },
        );
        let rule = Rule {
            limit: 10.0,
            period_s: 1.0,
            burst: 10.0,
            cost: 1.0,
        };

        let (verdict, outcome) = check_then_borrow(&m, "cold", &rule, 0, Instant::now()).await;
        assert!(!verdict.allowed);
        assert!(verdict.degraded);
        assert!(!verdict.accounted);
        assert_eq!(outcome, Some(BorrowOutcome::FailClosed));
    }

    #[tokio::test]
    async fn explicit_local_policy_uses_fallback_for_a_cold_key() {
        let m = LeaseManager::new(
            Arc::new(DownStore),
            CircuitBreaker::new(100, Duration::from_secs(1), Instant::now()),
            LeaseConfig {
                optimistic_cold_start: false,
                borrow_failure_mode: StoreFailureMode::Local,
                ..LeaseConfig::default()
            },
        );
        let rule = Rule {
            limit: 2.0,
            period_s: 1.0,
            burst: 2.0,
            cost: 1.0,
        };

        let (verdict, outcome) = check_then_borrow(&m, "cold", &rule, 0, Instant::now()).await;
        assert!(verdict.allowed);
        assert!(verdict.degraded);
        assert!(verdict.accounted);
        assert_eq!(outcome, Some(BorrowOutcome::FallbackAllowed));
    }

    #[tokio::test]
    async fn explicit_closed_policy_keeps_spending_store_backed_lease_tokens() {
        use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

        struct ToggleLeaseStore {
            down: AtomicBool,
            inner: InMemoryStore,
        }

        impl Store for ToggleLeaseStore {
            async fn reserve(
                &self,
                key: &str,
                rate: f64,
                burst: f64,
                requested: f64,
                now_ms: u64,
                ttl_ms: u64,
            ) -> Result<crate::store::Reservation, crate::store::StoreError> {
                if self.down.load(AtomicOrdering::Acquire) {
                    Err(crate::store::StoreError::Connection("down".into()))
                } else {
                    self.inner
                        .reserve(key, rate, burst, requested, now_ms, ttl_ms)
                        .await
                }
            }

            async fn gcra_check(
                &self,
                _key: &str,
                _rate: f64,
                _burst: f64,
                _cost: f64,
                _now_ms: u64,
                _ttl_ms: u64,
            ) -> Result<crate::store::GcraVerdict, crate::store::StoreError> {
                unreachable!()
            }
        }

        let store = Arc::new(ToggleLeaseStore {
            down: AtomicBool::new(false),
            inner: InMemoryStore::new(),
        });
        let m = LeaseManager::new(
            Arc::clone(&store),
            CircuitBreaker::new(100, Duration::from_secs(1), Instant::now()),
            LeaseConfig {
                optimistic_cold_start: false,
                borrow_failure_mode: StoreFailureMode::Closed,
                ..LeaseConfig::default()
            },
        );
        let rule = Rule {
            limit: 10.0,
            period_s: 1.0,
            burst: 10.0,
            cost: 1.0,
        };
        let now = Instant::now();

        let (cold, _) = m.check("backed", &rule, now);
        assert!(
            !cold.allowed,
            "explicit policy starts without unbacked tokens"
        );
        m.refresh("backed", 0, now).await;
        store.down.store(true, AtomicOrdering::Release);

        for _ in 0..2 {
            let (leased, _) = m.check("backed", &rule, now);
            assert!(leased.allowed, "store-issued lease remains spendable");
            assert!(!leased.degraded);
            assert!(leased.accounted);
        }
        let (closed, outcome) = check_then_borrow(&m, "backed", &rule, 0, now).await;
        assert!(!closed.allowed);
        assert_eq!(outcome, Some(BorrowOutcome::FailClosed));
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

        let (v, outcome) = m.exact_check("k", &rule, 0, Instant::now()).await;
        assert!(!v.allowed, "exact mode must fail closed on store timeout");
        assert!(v.degraded);
        assert_eq!(outcome, ExactOutcome::FailClosed);
    }

    #[tokio::test]
    async fn exact_store_failure_uses_bounded_local_budget() {
        let m = LeaseManager::new(
            Arc::new(DownStore),
            CircuitBreaker::new(100, Duration::from_secs(1), Instant::now()),
            LeaseConfig {
                exact_failure_mode: StoreFailureMode::Local,
                ..LeaseConfig::default()
            },
        );
        let rule = Rule {
            limit: 2.0,
            period_s: 1.0,
            burst: 2.0,
            cost: 1.0,
        };
        let now = Instant::now();

        for _ in 0..2 {
            let (verdict, outcome) = m.exact_check("k", &rule, 0, now).await;
            assert!(verdict.allowed);
            assert!(verdict.degraded);
            assert!(verdict.accounted);
            assert_eq!(outcome, ExactOutcome::FallbackAllowed);
        }
        let (verdict, outcome) = m.exact_check("k", &rule, 0, now).await;
        assert!(!verdict.allowed);
        assert!(verdict.degraded);
        assert!(verdict.accounted);
        assert_eq!(verdict.retry_after_s, 1);
        assert_eq!(outcome, ExactOutcome::FallbackLimited);
    }

    #[tokio::test]
    async fn fallback_capacity_fails_open_without_evicting_existing_budget() {
        let m = LeaseManager::new(
            Arc::new(DownStore),
            CircuitBreaker::new(100, Duration::from_secs(1), Instant::now()),
            LeaseConfig {
                max_keys: 1,
                exact_failure_mode: StoreFailureMode::Local,
                ..LeaseConfig::default()
            },
        );
        let rule = Rule {
            limit: 1.0,
            period_s: 60.0,
            burst: 1.0,
            cost: 1.0,
        };
        let now = Instant::now();

        let (_, first) = m.exact_check("tracked", &rule, 0, now).await;
        assert_eq!(first, ExactOutcome::FallbackAllowed);
        let (capacity_open, outcome) = m.exact_check("untracked", &rule, 0, now).await;
        assert!(capacity_open.allowed);
        assert!(capacity_open.degraded);
        assert!(!capacity_open.accounted);
        assert_eq!(outcome, ExactOutcome::FallbackCapacityOpen);
        let (limited, outcome) = m.exact_check("tracked", &rule, 0, now).await;
        assert!(!limited.allowed);
        assert_eq!(outcome, ExactOutcome::FallbackLimited);
    }

    #[tokio::test]
    async fn successful_store_checks_do_not_reset_emergency_budget() {
        use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

        struct ToggleStore(AtomicBool);
        impl Store for ToggleStore {
            async fn reserve(
                &self,
                _k: &str,
                _r: f64,
                _b: f64,
                _req: f64,
                _now: u64,
                _ttl: u64,
            ) -> Result<crate::store::Reservation, crate::store::StoreError> {
                unreachable!()
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
                if self.0.load(AtomicOrdering::Acquire) {
                    Ok(crate::store::GcraVerdict {
                        allowed: true,
                        retry_after_ms: 0,
                    })
                } else {
                    Err(crate::store::StoreError::Connection("down".into()))
                }
            }
        }

        let store = Arc::new(ToggleStore(AtomicBool::new(false)));
        let m = LeaseManager::new(
            Arc::clone(&store),
            CircuitBreaker::new(100, Duration::from_secs(1), Instant::now()),
            LeaseConfig {
                exact_failure_mode: StoreFailureMode::Local,
                ..LeaseConfig::default()
            },
        );
        let rule = Rule {
            limit: 1.0,
            period_s: 60.0,
            burst: 1.0,
            cost: 1.0,
        };
        let now = Instant::now();

        let (_, first) = m.exact_check("k", &rule, 0, now).await;
        assert_eq!(first, ExactOutcome::FallbackAllowed);
        store.0.store(true, AtomicOrdering::Release);
        let (_, healthy) = m.exact_check("k", &rule, 0, now).await;
        assert_eq!(healthy, ExactOutcome::Allowed);
        store.0.store(false, AtomicOrdering::Release);
        let (limited, second_failure) = m.exact_check("k", &rule, 0, now).await;
        assert!(!limited.allowed);
        assert_eq!(second_failure, ExactOutcome::FallbackLimited);
    }

    #[tokio::test]
    async fn request_fallback_stays_within_timeout_and_coalesces_waiters() {
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

        struct TimeoutStore {
            reserve_calls: AtomicUsize,
        }
        impl Store for TimeoutStore {
            async fn reserve(
                &self,
                _k: &str,
                _r: f64,
                _b: f64,
                _req: f64,
                _now: u64,
                _ttl: u64,
            ) -> Result<crate::store::Reservation, crate::store::StoreError> {
                self.reserve_calls.fetch_add(1, AtomicOrdering::Relaxed);
                tokio::time::sleep(Duration::from_millis(100)).await;
                unreachable!()
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
                tokio::time::sleep(Duration::from_millis(100)).await;
                unreachable!()
            }
        }

        let store = Arc::new(TimeoutStore {
            reserve_calls: AtomicUsize::new(0),
        });
        let m = Arc::new(LeaseManager::new(
            Arc::clone(&store),
            CircuitBreaker::new(100, Duration::from_secs(1), Instant::now()),
            LeaseConfig {
                store_timeout_ms: 10,
                borrow_failure_mode: StoreFailureMode::Local,
                exact_failure_mode: StoreFailureMode::Local,
                ..LeaseConfig::default()
            },
        ));
        let rule = Rule {
            limit: 1.0,
            period_s: 1.0,
            burst: 1.0,
            cost: 1.0,
        };

        let started = Instant::now();
        let (exact, outcome) = m.exact_check("exact", &rule, 0, started).await;
        assert!(exact.allowed);
        assert_eq!(outcome, ExactOutcome::FallbackAllowed);
        assert!(
            started.elapsed() < Duration::from_millis(50),
            "exact fallback exceeded the request deadline"
        );

        let now = Instant::now();
        let (optimistic, _) = m.check("lease", &rule, now);
        assert!(optimistic.allowed);
        let started = Instant::now();
        let (borrow, outcome) = m.borrow_check("lease", &rule, 0, started).await;
        assert!(borrow.allowed);
        assert_eq!(outcome, BorrowOutcome::FallbackAllowed);
        assert!(
            started.elapsed() < Duration::from_millis(50),
            "borrow fallback exceeded the request deadline"
        );

        let concurrent_rule = Rule {
            limit: 20.0,
            period_s: 1.0,
            burst: 20.0,
            cost: 1.0,
        };
        let now = Instant::now();
        for _ in 0..m.batch(&concurrent_rule) as usize {
            let (verdict, _) = m.check("concurrent", &concurrent_rule, now);
            assert!(verdict.allowed);
        }
        let barrier = Arc::new(tokio::sync::Barrier::new(11));
        let mut tasks = Vec::new();
        for _ in 0..10 {
            let m = Arc::clone(&m);
            let barrier = Arc::clone(&barrier);
            let rule = concurrent_rule.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                let started = Instant::now();
                let (verdict, outcome) = m.borrow_check("concurrent", &rule, 0, started).await;
                (verdict, outcome, started.elapsed())
            }));
        }
        barrier.wait().await;
        for task in tasks {
            let (verdict, outcome, elapsed) = task.await.unwrap();
            assert!(verdict.allowed);
            assert_eq!(outcome, BorrowOutcome::FallbackAllowed);
            assert!(
                elapsed < Duration::from_millis(50),
                "coalesced waiter retried the slow store for {elapsed:?}"
            );
        }
        assert_eq!(
            store.reserve_calls.load(AtomicOrdering::Relaxed),
            2,
            "one sequential and one concurrent-cohort borrow may reach the store"
        );
    }

    #[tokio::test]
    async fn breaker_cooldown_starts_when_a_store_timeout_finishes() {
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

        struct CountingTimeoutStore(AtomicUsize);
        impl Store for CountingTimeoutStore {
            async fn reserve(
                &self,
                _k: &str,
                _r: f64,
                _b: f64,
                _req: f64,
                _now: u64,
                _ttl: u64,
            ) -> Result<crate::store::Reservation, crate::store::StoreError> {
                unreachable!()
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
                self.0.fetch_add(1, AtomicOrdering::Relaxed);
                tokio::time::sleep(Duration::from_millis(100)).await;
                unreachable!()
            }
        }

        let store = Arc::new(CountingTimeoutStore(AtomicUsize::new(0)));
        let m = LeaseManager::new(
            Arc::clone(&store),
            CircuitBreaker::new(1, Duration::from_millis(5), Instant::now()),
            LeaseConfig {
                store_timeout_ms: 20,
                exact_failure_mode: StoreFailureMode::Local,
                ..LeaseConfig::default()
            },
        );
        let rule = Rule {
            limit: 2.0,
            period_s: 1.0,
            burst: 2.0,
            cost: 1.0,
        };

        let (_, first) = m.exact_check("k", &rule, 0, Instant::now()).await;
        assert_eq!(first, ExactOutcome::FallbackAllowed);
        let started = Instant::now();
        let (_, second) = m.exact_check("k", &rule, 0, started).await;
        assert_eq!(second, ExactOutcome::FallbackAllowed);
        assert!(started.elapsed() < Duration::from_millis(15));
        assert_eq!(store.0.load(AtomicOrdering::Relaxed), 1);
    }

    // ------------------------------------------------------------------
    // Burst-correctness regression suite (v0.2.0, borrow-on-empty).
    // The first test replays the production incident that motivated the
    // change: a browser page load multiplexing ~60 requests over one
    // connection against `1200/min, burst 300` was mass-429'd because the
    // local lease pool held only rate x interval = 4 tokens and `burst`
    // never reached the hot path.
    // ------------------------------------------------------------------

    /// The plugin-level flow under test everywhere below: local check first,
    /// borrow from the authoritative store only when the pool is empty.
    async fn check_then_borrow(
        m: &LeaseManager<impl Store>,
        key: &str,
        rule: &Rule,
        now_ms: u64,
        now: Instant,
    ) -> (Verdict, Option<BorrowOutcome>) {
        let (v, _) = m.check(key, rule, now);
        if v.allowed {
            return (v, None);
        }
        let (v, outcome) = m.borrow_check(key, rule, now_ms, now).await;
        (v, Some(outcome))
    }

    fn unifi_rule() -> Rule {
        Rule {
            limit: 1200.0,
            period_s: 60.0,
            burst: 300.0,
            cost: 1.0,
        }
    }

    #[tokio::test]
    async fn browser_burst_on_one_key_is_fully_admitted() {
        // 60 same-instant requests on one cold key: every one must pass —
        // the burst budget (300) trivially covers them. Pre-0.2 behavior
        // admitted ~4 (the lease batch) and denied the rest.
        let store = Arc::new(InMemoryStore::new());
        let m = mgr(store);
        let rule = unifi_rule();
        let base = Instant::now();

        let mut admitted = 0;
        for _ in 0..60 {
            let (v, _) = check_then_borrow(&m, "unifi", &rule, 0, base).await;
            if v.allowed {
                admitted += 1;
            }
        }
        assert_eq!(
            admitted, 60,
            "a 60-request page load must sail through burst 300"
        );
    }

    #[tokio::test]
    async fn concurrent_burst_is_admitted_and_borrows_are_coalesced() {
        // The same shape with real concurrency: 60 tasks racing on one key.
        // All must be admitted; the coalescing gate means the store sees only
        // a handful of reserve calls, not one per request.
        use std::sync::atomic::{AtomicU32, Ordering as AOrd};

        struct CountingStore {
            inner: InMemoryStore,
            reserves: AtomicU32,
        }
        impl Store for CountingStore {
            async fn reserve(
                &self,
                k: &str,
                r: f64,
                b: f64,
                req: f64,
                now: u64,
                ttl: u64,
            ) -> Result<crate::store::Reservation, crate::store::StoreError> {
                self.reserves.fetch_add(1, AOrd::SeqCst);
                self.inner.reserve(k, r, b, req, now, ttl).await
            }
            async fn gcra_check(
                &self,
                k: &str,
                r: f64,
                b: f64,
                c: f64,
                now: u64,
                ttl: u64,
            ) -> Result<crate::store::GcraVerdict, crate::store::StoreError> {
                self.inner.gcra_check(k, r, b, c, now, ttl).await
            }
        }

        let store = Arc::new(CountingStore {
            inner: InMemoryStore::new(),
            reserves: AtomicU32::new(0),
        });
        let m = Arc::new(LeaseManager::new(
            Arc::clone(&store),
            CircuitBreaker::new(3, Duration::from_secs(1), Instant::now()),
            LeaseConfig::default(),
        ));
        let rule = unifi_rule();

        let mut handles = Vec::new();
        for _ in 0..60 {
            let m = Arc::clone(&m);
            let rule = rule.clone();
            handles.push(tokio::spawn(async move {
                let (v, _) = check_then_borrow(m.as_ref(), "unifi", &rule, 0, Instant::now()).await;
                v.allowed
            }));
        }
        let mut admitted = 0;
        for h in handles {
            if h.await.unwrap() {
                admitted += 1;
            }
        }
        assert_eq!(
            admitted, 60,
            "all concurrent requests within burst must pass"
        );
        let reserves = store.reserves.load(AOrd::SeqCst);
        assert!(
            reserves <= 60,
            "borrows must be coalesced, not one per request (saw {reserves})"
        );
    }

    #[tokio::test]
    async fn burst_admission_is_bounded_by_the_global_burst() {
        // 400 same-instant requests against burst 300: admission is bounded
        // by the authoritative budget (plus the optimistic cold-start batch),
        // and the overflow is denied honestly.
        let store = Arc::new(InMemoryStore::new());
        let m = mgr(store);
        let rule = unifi_rule();
        let base = Instant::now();
        let batch = m.batch(&rule);

        let mut admitted = 0u32;
        for _ in 0..400 {
            let (v, _) = check_then_borrow(&m, "unifi", &rule, 0, base).await;
            if v.allowed {
                admitted += 1;
            }
        }
        let upper = (rule.burst + batch).ceil() as u32;
        assert!(
            admitted >= rule.burst as u32 && admitted <= upper,
            "admitted {admitted}, expected within [{}, {upper}]",
            rule.burst as u32
        );
    }

    #[tokio::test]
    async fn fleet_burst_stays_bounded_with_shared_store() {
        // Two pods bursting on the same key split one authoritative budget:
        // combined admission <= burst + 2 x optimistic batch.
        let store = Arc::new(InMemoryStore::new());
        let a = mgr(Arc::clone(&store));
        let b = mgr(Arc::clone(&store));
        let rule = unifi_rule();
        let base = Instant::now();

        let mut admitted = 0u32;
        for _ in 0..300 {
            let (v, _) = check_then_borrow(&a, "k", &rule, 0, base).await;
            if v.allowed {
                admitted += 1;
            }
            let (v, _) = check_then_borrow(&b, "k", &rule, 0, base).await;
            if v.allowed {
                admitted += 1;
            }
        }
        let bound = (rule.burst + 2.0 * a.batch(&rule)).ceil() as u32;
        assert!(
            admitted <= bound,
            "fleet admitted {admitted}, bound {bound}"
        );
        assert!(
            admitted >= rule.burst as u32,
            "the shared budget itself must be admittable (got {admitted})"
        );
    }

    #[tokio::test]
    async fn borrow_honors_open_and_closed_modes_when_store_is_down() {
        // Open mode: once the optimistic pool is drained, requests are
        // admitted degraded instead of silently fail-closing (the pre-0.2
        // "fail-static in name, fail-closed in effect" defect).
        let m = LeaseManager::new(
            Arc::new(DownStore),
            CircuitBreaker::new(1, Duration::from_secs(60), Instant::now()),
            LeaseConfig {
                borrow_failure_mode: StoreFailureMode::Open,
                ..LeaseConfig::default()
            },
        );
        let rule = unifi_rule();
        let base = Instant::now();
        let batch = m.batch(&rule) as u32;
        for _ in 0..batch {
            let (v, _) = check_then_borrow(&m, "k", &rule, 0, base).await;
            assert!(v.allowed, "optimistic pool serves the first batch");
        }
        let (v, outcome) = check_then_borrow(&m, "k", &rule, 0, base).await;
        assert!(v.allowed, "open mode must admit when the store is down");
        assert!(v.degraded);
        assert!(!v.accounted);
        assert_eq!(outcome, Some(BorrowOutcome::FailOpen));
        // The breaker is now open (threshold 1): the next escalation resolves
        // without a store call, still honoring the failure mode.
        let (v, outcome) = check_then_borrow(&m, "k", &rule, 0, base).await;
        assert!(v.allowed);
        assert!(!v.accounted);
        assert_eq!(outcome, Some(BorrowOutcome::FailOpen));

        // Closed mode: the same situation denies.
        let m = LeaseManager::new(
            Arc::new(DownStore),
            CircuitBreaker::new(1, Duration::from_secs(60), Instant::now()),
            LeaseConfig {
                borrow_failure_mode: StoreFailureMode::Closed,
                ..LeaseConfig::default()
            },
        );
        for _ in 0..batch {
            let (v, _) = check_then_borrow(&m, "k", &rule, 0, base).await;
            assert!(v.allowed);
        }
        let (v, outcome) = check_then_borrow(&m, "k", &rule, 0, base).await;
        assert!(!v.allowed, "closed mode must deny when the store is down");
        assert!(v.degraded);
        assert!(!v.accounted);
        assert_eq!(outcome, Some(BorrowOutcome::FailClosed));
    }

    #[tokio::test]
    async fn borrow_store_failure_uses_bounded_local_budget() {
        let m = LeaseManager::new(
            Arc::new(DownStore),
            CircuitBreaker::new(100, Duration::from_secs(60), Instant::now()),
            LeaseConfig {
                borrow_failure_mode: StoreFailureMode::Local,
                ..LeaseConfig::default()
            },
        );
        let rule = Rule {
            limit: 2.0,
            period_s: 1.0,
            burst: 2.0,
            cost: 1.0,
        };
        let now = Instant::now();

        let optimistic = m.batch(&rule) as usize;
        for _ in 0..optimistic {
            let (verdict, outcome) = check_then_borrow(&m, "k", &rule, 0, now).await;
            assert!(verdict.allowed);
            assert_eq!(outcome, None);
        }
        for _ in 0..2 {
            let (verdict, outcome) = check_then_borrow(&m, "k", &rule, 0, now).await;
            assert!(verdict.allowed);
            assert!(verdict.degraded);
            assert!(verdict.accounted);
            assert_eq!(outcome, Some(BorrowOutcome::FallbackAllowed));
        }
        let (limited, outcome) = check_then_borrow(&m, "k", &rule, 0, now).await;
        assert!(!limited.allowed);
        assert!(limited.degraded);
        assert!(limited.accounted);
        assert_eq!(limited.retry_after_s, 1);
        assert_eq!(outcome, Some(BorrowOutcome::FallbackLimited));
    }

    #[tokio::test]
    async fn flood_backoff_bounds_store_roundtrips() {
        use std::sync::atomic::{AtomicU32, Ordering as AOrd};
        struct CountingStore {
            inner: InMemoryStore,
            reserves: AtomicU32,
        }
        impl Store for CountingStore {
            async fn reserve(
                &self,
                k: &str,
                r: f64,
                b: f64,
                req: f64,
                now: u64,
                ttl: u64,
            ) -> Result<crate::store::Reservation, crate::store::StoreError> {
                self.reserves.fetch_add(1, AOrd::SeqCst);
                self.inner.reserve(k, r, b, req, now, ttl).await
            }
            async fn gcra_check(
                &self,
                k: &str,
                r: f64,
                b: f64,
                c: f64,
                now: u64,
                ttl: u64,
            ) -> Result<crate::store::GcraVerdict, crate::store::StoreError> {
                self.inner.gcra_check(k, r, b, c, now, ttl).await
            }
        }

        // Tiny budget: burst 5 at 1/s. A 200-request flood at one instant
        // must not turn into 200 store round-trips: after the borrow that
        // proves the bucket empty, further borrows back off for one interval.
        let store = Arc::new(CountingStore {
            inner: InMemoryStore::new(),
            reserves: AtomicU32::new(0),
        });
        let m = LeaseManager::new(
            Arc::clone(&store),
            CircuitBreaker::new(3, Duration::from_secs(1), Instant::now()),
            LeaseConfig::default(),
        );
        let rule = Rule {
            limit: 60.0,
            period_s: 60.0,
            burst: 5.0,
            cost: 1.0,
        };
        let base = Instant::now();

        let mut admitted = 0u32;
        for _ in 0..200 {
            let (v, _) = check_then_borrow(&m, "flood", &rule, 0, base).await;
            if v.allowed {
                admitted += 1;
            }
        }
        // Optimistic batch (1) + burst (5) admitted at most.
        assert!(admitted <= 6, "admitted {admitted} of a 200-request flood");
        let reserves = store.reserves.load(AOrd::SeqCst);
        assert!(
            reserves <= 4,
            "flood must not hammer the store: {reserves} reserve calls in one interval"
        );
    }

    #[tokio::test]
    async fn headers_report_global_budget_not_local_pool() {
        // Pre-0.2, `remaining` surfaced the 0-4 token lease pool ("frozen 3")
        // and `reset` was a static ceil(period). Now the first request on a
        // cold key reports the real budget: burst 300 minus what this pod
        // holds/spent.
        let store = Arc::new(InMemoryStore::new());
        let m = mgr(store);
        let rule = unifi_rule();
        let base = Instant::now();

        let (v, _) = m.check("k", &rule, base);
        assert!(v.allowed);
        assert!(
            v.remaining >= 290,
            "remaining must reflect the ~300 budget, not the local pool (got {})",
            v.remaining
        );
        assert!(
            v.reset_s <= rule.period_s as u32,
            "reset must be a replenishment countdown, capped at the period"
        );
    }
}
