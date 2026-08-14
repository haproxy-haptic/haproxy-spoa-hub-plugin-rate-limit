//! Bounded per-sidecar budget used only when the shared store cannot answer.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::bucket::{Decision, TokenBucket};
use crate::rule::Rule;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FallbackDecision {
    Available(Decision),
    Unavailable(FallbackUnavailable),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FallbackUnavailable {
    Capacity,
}

struct Entry {
    bucket: TokenBucket,
    rule: RwLock<Rule>,
    last_seen: Mutex<Instant>,
}

impl Entry {
    fn new(rule: &Rule, now: Instant) -> Self {
        Self {
            bucket: TokenBucket::new(rule.rate(), rule.burst, now),
            rule: RwLock::new(rule.clone()),
            last_seen: Mutex::new(now),
        }
    }

    fn check(&self, rule: &Rule, now: Instant) -> Decision {
        let mut current = self.rule.write().unwrap_or_else(|e| e.into_inner());
        if *current != *rule {
            self.bucket.set_lease(rule.rate(), rule.burst, now);
            *current = rule.clone();
        }
        *self.last_seen.lock().unwrap_or_else(|e| e.into_inner()) = now;
        self.bucket.try_acquire(rule.cost, now)
    }

    fn idle_for(&self, now: Instant) -> Duration {
        now.checked_duration_since(*self.last_seen.lock().unwrap_or_else(|e| e.into_inner()))
            .unwrap_or_default()
    }

    fn is_full(&self, now: Instant) -> bool {
        self.bucket.available(now) >= self.bucket.burst()
    }
}

pub struct FallbackLimiter {
    entries: RwLock<HashMap<String, Arc<Entry>>>,
    max_keys: usize,
    idle_ttl: Duration,
    next_sweep: Mutex<Option<Instant>>,
}

impl FallbackLimiter {
    pub fn new(max_keys: usize, idle_ttl: Duration) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            max_keys: max_keys.max(1),
            idle_ttl,
            next_sweep: Mutex::new(None),
        }
    }

    pub fn check(&self, key: &str, rule: &Rule, now: Instant) -> FallbackDecision {
        {
            let entries = self.entries.read().unwrap_or_else(|e| e.into_inner());
            if let Some(entry) = entries.get(key) {
                return FallbackDecision::Available(entry.check(rule, now));
            }
        }

        let mut entries = self.entries.write().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = entries.get(key) {
            return FallbackDecision::Available(entry.check(rule, now));
        }
        if entries.len() >= self.max_keys {
            return FallbackDecision::Unavailable(FallbackUnavailable::Capacity);
        }
        let entry = Arc::new(Entry::new(rule, now));
        let decision = entry.check(rule, now);
        entries.insert(key.to_string(), entry);
        FallbackDecision::Available(decision)
    }

    pub fn sweep_idle(&self, now: Instant) {
        let mut next_sweep = self.next_sweep.lock().unwrap_or_else(|e| e.into_inner());
        if next_sweep.is_some_and(|next| now < next) {
            return;
        }
        *next_sweep = now.checked_add(self.idle_ttl);
        drop(next_sweep);
        let idle_ttl = self.idle_ttl;
        self.entries
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|_, entry| entry.idle_for(now) <= idle_ttl || !entry.is_full(now));
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.read().unwrap_or_else(|e| e.into_inner()).len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule() -> Rule {
        Rule {
            limit: 2.0,
            period_s: 1.0,
            burst: 2.0,
            cost: 1.0,
        }
    }

    #[test]
    fn fallback_enforces_a_local_budget_with_retry_after() {
        let base = Instant::now();
        let limiter = FallbackLimiter::new(10, Duration::from_secs(60));
        for _ in 0..2 {
            let FallbackDecision::Available(decision) = limiter.check("k", &rule(), base) else {
                panic!("expected local fallback state");
            };
            assert!(decision.allowed);
        }
        let FallbackDecision::Available(limited) = limiter.check("k", &rule(), base) else {
            panic!("expected local fallback state");
        };
        assert!(!limited.allowed);
        assert!(limited.retry_after >= 0.49 && limited.retry_after <= 0.51);

        let FallbackDecision::Available(allowed) =
            limiter.check("k", &rule(), base + Duration::from_millis(500))
        else {
            panic!("expected local fallback state");
        };
        assert!(allowed.allowed);
    }

    #[test]
    fn exact_and_lease_failures_share_one_emergency_budget() {
        let base = Instant::now();
        let limiter = FallbackLimiter::new(1, Duration::from_secs(60));
        for _ in 0..2 {
            assert!(matches!(
                limiter.check("same-key", &rule(), base),
                FallbackDecision::Available(Decision { allowed: true, .. })
            ));
        }
        assert!(matches!(
            limiter.check("same-key", &rule(), base),
            FallbackDecision::Available(Decision { allowed: false, .. })
        ));
        assert_eq!(limiter.len(), 1);
    }

    #[test]
    fn concurrent_same_key_admission_never_exceeds_burst() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let base = Instant::now();
        let limiter = Arc::new(FallbackLimiter::new(10, Duration::from_secs(60)));
        let rule = Rule {
            limit: 100.0,
            period_s: 1.0,
            burst: 100.0,
            cost: 1.0,
        };
        let admitted = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for _ in 0..200 {
                let limiter = Arc::clone(&limiter);
                let rule = rule.clone();
                let admitted = &admitted;
                scope.spawn(move || {
                    if matches!(
                        limiter.check("k", &rule, base),
                        FallbackDecision::Available(Decision { allowed: true, .. })
                    ) {
                        admitted.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
        });
        assert_eq!(admitted.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn rule_changes_preserve_and_clamp_existing_state() {
        let base = Instant::now();
        let limiter = FallbackLimiter::new(10, Duration::from_secs(60));
        let initial = Rule {
            limit: 10.0,
            period_s: 1.0,
            burst: 10.0,
            cost: 1.0,
        };
        assert!(matches!(
            limiter.check("k", &initial, base),
            FallbackDecision::Available(Decision { allowed: true, .. })
        ));
        let tightened = Rule {
            limit: 1.0,
            period_s: 1.0,
            burst: 1.0,
            cost: 1.0,
        };
        assert!(matches!(
            limiter.check("k", &tightened, base),
            FallbackDecision::Available(Decision { allowed: true, .. })
        ));
        assert!(matches!(
            limiter.check("k", &tightened, base),
            FallbackDecision::Available(Decision { allowed: false, .. })
        ));
    }

    #[test]
    fn concurrent_rule_transitions_apply_each_rule_atomically() {
        let base = Instant::now();
        let limiter = Arc::new(FallbackLimiter::new(10, Duration::from_secs(60)));
        let narrow = Rule {
            limit: 1.0,
            period_s: 60.0,
            burst: 1.0,
            cost: 1.0,
        };
        let wide = Rule {
            limit: 10.0,
            period_s: 60.0,
            burst: 10.0,
            cost: 1.0,
        };

        std::thread::scope(|scope| {
            for rule in [narrow, wide] {
                let limiter = Arc::clone(&limiter);
                scope.spawn(move || {
                    for _ in 0..1_000 {
                        let FallbackDecision::Available(decision) = limiter.check("k", &rule, base)
                        else {
                            panic!("existing key must remain tracked");
                        };
                        assert!(decision.remaining <= rule.burst);
                    }
                });
            }
        });
    }

    #[test]
    fn capacity_does_not_evict_active_debt_or_mint_budget() {
        let base = Instant::now();
        let limiter = FallbackLimiter::new(1, Duration::from_secs(60));
        for _ in 0..2 {
            let _ = limiter.check("kept", &rule(), base);
        }
        assert_eq!(
            limiter.check("excluded", &rule(), base),
            FallbackDecision::Unavailable(FallbackUnavailable::Capacity)
        );
        assert_eq!(limiter.len(), 1);
        assert!(matches!(
            limiter.check("kept", &rule(), base),
            FallbackDecision::Available(Decision { allowed: false, .. })
        ));
    }

    #[test]
    fn idle_state_is_evicted_only_after_its_budget_refills() {
        let base = Instant::now();
        let limiter = FallbackLimiter::new(1, Duration::from_millis(100));
        let _ = limiter.check("k", &rule(), base);
        limiter.sweep_idle(base + Duration::from_millis(200));
        assert_eq!(limiter.len(), 1, "partially spent state must survive");
        limiter.sweep_idle(base + Duration::from_millis(1_200));
        assert_eq!(limiter.len(), 0, "full idle state can be discarded");
    }

    #[test]
    fn concurrent_idle_sweep_cannot_detach_active_budget() {
        use std::sync::Barrier;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let rule = Rule {
            limit: 1_000.0,
            period_s: 1.0,
            burst: 1.0,
            cost: 1.0,
        };
        for _ in 0..100 {
            let base = Instant::now();
            let race_now = base + Duration::from_millis(10);
            let limiter = Arc::new(FallbackLimiter::new(1, Duration::from_millis(1)));
            let _ = limiter.check("k", &rule, base);
            let barrier = Arc::new(Barrier::new(5));
            let admitted = AtomicUsize::new(0);
            std::thread::scope(|scope| {
                for _ in 0..3 {
                    let limiter = Arc::clone(&limiter);
                    let barrier = Arc::clone(&barrier);
                    let rule = rule.clone();
                    let admitted = &admitted;
                    scope.spawn(move || {
                        barrier.wait();
                        if matches!(
                            limiter.check("k", &rule, race_now),
                            FallbackDecision::Available(Decision { allowed: true, .. })
                        ) {
                            admitted.fetch_add(1, Ordering::Relaxed);
                        }
                    });
                }
                let limiter = Arc::clone(&limiter);
                let sweep_barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    sweep_barrier.wait();
                    limiter.sweep_idle(race_now);
                });
                barrier.wait();
            });
            assert_eq!(admitted.load(Ordering::Relaxed), 1);
        }
    }
}
