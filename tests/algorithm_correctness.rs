//! Thorough correctness tests for the rate-limiting algorithm (the local token
//! bucket — the default enforcement primitive).
//!
//! These assert the *guarantees* a rate limiter must uphold, not just example
//! transitions:
//!
//!   1. Windowed safety — over ANY window of the period, grants never exceed
//!      `burst + limit` (the fundamental "never over-admit" property).
//!   2. Sustained throughput converges to the configured rate.
//!   3. Liveness — a client at or under the rate is never falsely denied.
//!   4. Burst is honoured, then throughput throttles to the rate.
//!   5. Recovery — a throttled client recovers at exactly the rate, and its
//!      advertised `retry_after` is honest.
//!   6. Cost weighting is exact.
//!   7. Concurrency — no double-spend: under N threads the bucket grants exactly
//!      its capacity, never more (safety) and never fewer (liveness).
//!
//! The bucket takes an explicit monotonic `now`, so every timing test runs on a
//! deterministic virtual clock — no sleeps, no flakiness.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use rate_limit_plugin::bucket::TokenBucket;
use rate_limit_plugin::limiter::Limiter;
use rate_limit_plugin::rule::Rule;

/// A deterministic virtual clock rooted at a fixed `Instant`.
struct Clock {
    base: Instant,
}
impl Clock {
    fn new() -> Self {
        Self {
            base: Instant::now(),
        }
    }
    fn at_ms(&self, ms: u64) -> Instant {
        self.base + Duration::from_millis(ms)
    }
}

/// Assert that in every sliding window of `window_ms`, the number of grant
/// timestamps (ms, ascending) never exceeds `max`. This is the core rate-limit
/// safety property, checked exhaustively with a two-pointer sweep.
fn assert_windowed_safety(grants_ms: &[u64], window_ms: u64, max: usize) {
    let mut start = 0usize;
    for end in 0..grants_ms.len() {
        // Shrink [start, end] so its span is < window_ms (half-open window).
        while grants_ms[end] - grants_ms[start] >= window_ms {
            start += 1;
        }
        let count = end - start + 1;
        assert!(
            count <= max,
            "windowed safety violated: {count} grants within a {window_ms}ms window \
             (max {max}); window [{}..={}]ms",
            grants_ms[start],
            grants_ms[end],
        );
    }
}

/// Drive a greedy client (tries every `step_ms`) over `duration_ms`, returning
/// the ms timestamps at which requests were granted.
fn greedy_grants(
    bucket: &TokenBucket,
    cost: f64,
    step_ms: u64,
    duration_ms: u64,
    clock: &Clock,
) -> Vec<u64> {
    let mut grants = Vec::new();
    let mut t = 0;
    while t <= duration_ms {
        if bucket.try_acquire(cost, clock.at_ms(t)).allowed {
            grants.push(t);
        }
        t += step_ms;
    }
    grants
}

#[test]
fn windowed_safety_under_a_flood() {
    // 100 req / 10s => rate 10/s, burst 100. A greedy flood must never exceed
    // burst + rate*window in ANY window of the period.
    let clock = Clock::new();
    let rate = 10.0;
    let burst = 100.0;
    let period_ms = 10_000;
    let bucket = TokenBucket::new(rate, burst, clock.at_ms(0));

    // Flood: a request every 5ms for 60s.
    let grants = greedy_grants(&bucket, 1.0, 5, 60_000, &clock);

    // Over any `period` window, max grants = burst + rate*period = 100 + 100 = 200.
    let max_per_window = (burst + rate * (period_ms as f64) / 1000.0).ceil() as usize;
    assert_windowed_safety(&grants, period_ms, max_per_window);

    // And over any 1s window (steady state, small burst contribution): grants
    // can't exceed burst + rate*1s in the worst case, but sustained is ~rate.
    assert_windowed_safety(&grants, 1000, (burst + rate).ceil() as usize);
}

#[test]
fn sustained_throughput_converges_to_the_rate() {
    // Over a long flood the average admit rate → the configured rate; the total
    // is bounded by the refilled tokens plus the initial burst.
    let clock = Clock::new();
    let rate = 50.0; // tokens/s
    let burst = 50.0;
    let bucket = TokenBucket::new(rate, burst, clock.at_ms(0));

    let duration_ms = 20_000;
    let grants = greedy_grants(&bucket, 1.0, 1, duration_ms, &clock);

    let expected_refill = rate * (duration_ms as f64) / 1000.0; // 1000
    let lo = expected_refill as usize; // at least the refilled tokens
    let hi = (expected_refill + burst).ceil() as usize + 1; // + initial burst
    assert!(
        grants.len() >= lo && grants.len() <= hi,
        "sustained throughput {} outside [{lo}, {hi}]",
        grants.len()
    );
}

#[test]
fn a_client_exactly_at_the_rate_is_never_denied() {
    // One request every 1/rate seconds must always be admitted (liveness).
    let clock = Clock::new();
    let rate = 20.0; // 20/s => one every 50ms
    let burst = 1.0; // minimal burst; still must pass at exactly the rate
    let bucket = TokenBucket::new(rate, burst, clock.at_ms(0));

    let step_ms = 50; // exactly 1/rate
    let mut denied = 0;
    for i in 0..1000u64 {
        if !bucket.try_acquire(1.0, clock.at_ms(i * step_ms)).allowed {
            denied += 1;
        }
    }
    assert_eq!(
        denied, 0,
        "a client at exactly the rate was denied {denied} times"
    );
}

#[test]
fn burst_is_honoured_then_throughput_throttles() {
    let clock = Clock::new();
    let rate = 10.0;
    let burst = 25.0;
    let bucket = TokenBucket::new(rate, burst, clock.at_ms(0));

    // The full burst is admitted instantly at t=0.
    let mut admitted_at_zero = 0;
    for _ in 0..1000 {
        if bucket.try_acquire(1.0, clock.at_ms(0)).allowed {
            admitted_at_zero += 1;
        }
    }
    assert_eq!(
        admitted_at_zero, burst as usize,
        "burst not honoured exactly"
    );

    // Immediately after, denied (no tokens, no time passed).
    assert!(!bucket.try_acquire(1.0, clock.at_ms(0)).allowed);

    // After 1s, exactly `rate` tokens have refilled.
    let mut refilled = 0;
    for _ in 0..1000 {
        if bucket.try_acquire(1.0, clock.at_ms(1000)).allowed {
            refilled += 1;
        }
    }
    assert_eq!(refilled, rate as usize, "refill over 1s != rate");
}

#[test]
fn retry_after_is_honest() {
    // When denied, waiting exactly `retry_after` must yield an allow.
    let clock = Clock::new();
    let rate = 4.0; // one token every 250ms
    let burst = 1.0;
    let bucket = TokenBucket::new(rate, burst, clock.at_ms(0));

    assert!(bucket.try_acquire(1.0, clock.at_ms(0)).allowed); // drains the 1 token
    let d = bucket.try_acquire(1.0, clock.at_ms(0));
    assert!(!d.allowed);
    // retry_after ~= 0.25s. Waiting that long (in ms) must allow.
    let wait_ms = (d.retry_after * 1000.0).ceil() as u64;
    assert!(
        bucket.try_acquire(1.0, clock.at_ms(wait_ms)).allowed,
        "retry_after under-promised"
    );
    // Waiting just under it must still deny.
    let bucket2 = TokenBucket::new(rate, burst, clock.at_ms(0));
    assert!(bucket2.try_acquire(1.0, clock.at_ms(0)).allowed);
    let d2 = bucket2.try_acquire(1.0, clock.at_ms(0));
    let just_under = ((d2.retry_after * 1000.0).floor() as u64).saturating_sub(1);
    assert!(
        !bucket2.try_acquire(1.0, clock.at_ms(just_under)).allowed,
        "retry_after over-promised"
    );
}

#[test]
fn cost_weighting_is_exact() {
    let clock = Clock::new();
    let bucket = TokenBucket::new(1.0, 10.0, clock.at_ms(0)); // burst 10, slow refill

    // cost 3 four times = 12 > 10: three succeed (9), the fourth (needs 3, has 1) fails.
    assert!(bucket.try_acquire(3.0, clock.at_ms(0)).allowed); // 7 left
    assert!(bucket.try_acquire(3.0, clock.at_ms(0)).allowed); // 4 left
    assert!(bucket.try_acquire(3.0, clock.at_ms(0)).allowed); // 1 left
    assert!(!bucket.try_acquire(3.0, clock.at_ms(0)).allowed); // needs 3, has 1
    // A cost-1 still fits.
    assert!(bucket.try_acquire(1.0, clock.at_ms(0)).allowed); // 0 left
    assert!(!bucket.try_acquire(1.0, clock.at_ms(0)).allowed);
}

#[test]
fn concurrent_access_never_double_spends() {
    // The atomic decrement must be exact under contention: with the clock frozen
    // (no refill), a bucket of capacity B hit by N threads × M attempts admits
    // EXACTLY B — never B+1 (safety), never B-1 (liveness, since N*M >> B).
    let clock = Clock::new();
    let now = clock.at_ms(0);
    let burst = 500.0;
    let bucket = Arc::new(TokenBucket::new(0.0, burst, now)); // rate 0 => no refill ever
    let admitted = Arc::new(AtomicUsize::new(0));

    let threads: Vec<_> = (0..16)
        .map(|_| {
            let b = Arc::clone(&bucket);
            let a = Arc::clone(&admitted);
            std::thread::spawn(move || {
                for _ in 0..1000 {
                    if b.try_acquire(1.0, now).allowed {
                        a.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }

    assert_eq!(
        admitted.load(Ordering::Relaxed),
        burst as usize,
        "concurrent admits != capacity (double-spend or lost grant)"
    );
}

#[test]
fn concurrent_windowed_safety_with_refill() {
    // Even with refill and concurrent load, total admits over the whole run stay
    // within burst + rate*duration (no thread can conjure extra tokens).
    let clock = Clock::new();
    let rate = 100.0;
    let burst = 100.0;
    let duration_ms = 2000u64;
    let bucket = Arc::new(TokenBucket::new(rate, burst, clock.at_ms(0)));
    let admitted = Arc::new(AtomicUsize::new(0));
    let base = clock.base;

    let threads: Vec<_> = (0..8)
        .map(|_| {
            let b = Arc::clone(&bucket);
            let a = Arc::clone(&admitted);
            std::thread::spawn(move || {
                // Each thread sweeps the full timeline in 1ms steps.
                for ms in 0..=duration_ms {
                    if b.try_acquire(1.0, base + Duration::from_millis(ms)).allowed {
                        a.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }

    let max = (burst + rate * (duration_ms as f64) / 1000.0).ceil() as usize + 1;
    let total = admitted.load(Ordering::Relaxed);
    assert!(
        total <= max,
        "concurrent total admits {total} exceeded safety bound {max}"
    );
}

// ---- Limiter-level (end-to-end verdict) correctness -------------------------

#[test]
fn limiter_verdict_fields_are_correct() {
    let l = Limiter::new();
    let rule = Rule {
        limit: 10.0,
        period_s: 5.0,
        burst: 10.0,
        cost: 1.0,
    };
    let clock = Clock::new();

    let first = l.check("k", &rule, clock.at_ms(0));
    assert!(first.allowed);
    assert_eq!(first.limit, 10);
    assert_eq!(first.remaining, 9);
    assert_eq!(first.retry_after_s, 0);

    // Drain the rest.
    for _ in 0..9 {
        assert!(l.check("k", &rule, clock.at_ms(0)).allowed);
    }
    let denied = l.check("k", &rule, clock.at_ms(0));
    assert!(!denied.allowed);
    assert_eq!(denied.remaining, 0);
    assert!(
        denied.retry_after_s >= 1,
        "denied verdict must advertise a retry_after"
    );
}

#[test]
fn limiter_keys_are_isolated_and_fair() {
    let l = Limiter::new();
    let rule = Rule {
        limit: 3.0,
        period_s: 1.0,
        burst: 3.0,
        cost: 1.0,
    };
    let clock = Clock::new();
    let t0 = clock.at_ms(0);

    // Exhaust key "a".
    for _ in 0..3 {
        assert!(l.check("a", &rule, t0).allowed);
    }
    assert!(!l.check("a", &rule, t0).allowed);

    // "b" and "c" are entirely unaffected.
    for key in ["b", "c"] {
        for _ in 0..3 {
            assert!(
                l.check(key, &rule, t0).allowed,
                "{key} wrongly limited by a's usage"
            );
        }
        assert!(!l.check(key, &rule, t0).allowed);
    }
}

#[test]
fn quota_shape_allows_limit_per_long_window() {
    // A quota is a token bucket with burst == limit over a long period: `limit`
    // admits up front, then a slow refill over the window.
    let clock = Clock::new();
    let limit = 1000.0;
    let period_s = 3600.0; // 1h
    let rule = Rule {
        limit,
        period_s,
        burst: limit,
        cost: 1.0,
    };
    let l = Limiter::new();
    let t0 = clock.at_ms(0);

    let mut admitted = 0;
    for _ in 0..2000 {
        if l.check("quota", &rule, t0).allowed {
            admitted += 1;
        }
    }
    assert_eq!(
        admitted, limit as usize,
        "quota admitted != limit in the burst"
    );

    // Half the window later, ~half the quota has refilled.
    let half_ms = (period_s * 1000.0 / 2.0) as u64;
    let mut refilled = 0;
    for _ in 0..2000 {
        if l.check("quota", &rule, clock.at_ms(half_ms)).allowed {
            refilled += 1;
        }
    }
    let expected = (limit / 2.0) as usize;
    assert!(
        (refilled as i64 - expected as i64).abs() <= 1,
        "half-window refill {refilled} != ~{expected}"
    );
}
