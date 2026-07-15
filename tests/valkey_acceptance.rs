//! Acceptance tests against a **real Valkey** instance.
//!
//! These exercise the shared-tier Lua reservation ([`ValkeyStore`]) end to end,
//! including its atomicity under concurrent load — the property that makes the
//! global limit safe. They are gated on the `VALKEY_URL` env var: unset, they
//! skip (so a plain `cargo test` needs no Valkey); set, they run for real.
//!
//! Run locally:
//!   docker run -d -p 6379:6379 valkey/valkey:8
//!   VALKEY_URL=redis://127.0.0.1:6379 cargo test --test valkey_acceptance
//!
//! In CI a `services: valkey/valkey` provides the instance (see .gitlab-ci.yml).
//!
//! Each test uses a unique key (nanosecond suffix) so runs are isolated without
//! flushing the database.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use std::time::{Duration, Instant};

use rate_limit_plugin::breaker::CircuitBreaker;
use rate_limit_plugin::lease::{LeaseConfig, LeaseManager};
use rate_limit_plugin::rule::Rule;
use rate_limit_plugin::store::Store;
use rate_limit_plugin::store::valkey::ValkeyStore;

/// Connect to `VALKEY_URL`, or return `None` (and print) when it is unset so the
/// test skips. When it IS set, a connection failure is a hard error — an
/// acceptance test must run against a live store.
async fn store_or_skip() -> Option<ValkeyStore> {
    match std::env::var("VALKEY_URL") {
        Ok(url) => Some(
            ValkeyStore::connect(&url)
                .await
                .expect("connect to VALKEY_URL"),
        ),
        Err(_) => {
            eprintln!(
                "SKIP: set VALKEY_URL (e.g. redis://127.0.0.1:6379) to run the Valkey acceptance tests"
            );
            None
        }
    }
}

fn unique_key(name: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("rl:acc:{name}:{nanos}")
}

const TTL_MS: u64 = 60_000;

#[tokio::test]
async fn reserve_grants_then_drains_the_global_bucket() {
    let Some(s) = store_or_skip().await else {
        return;
    };
    let key = unique_key("drain");
    let now = 1_000_000u64;

    // Fresh key: rate 10/s, burst 100. Request 30 → grant 30, remaining 70.
    let r = s
        .reserve(&key, 10.0, 100.0, 30.0, now, TTL_MS)
        .await
        .unwrap();
    assert_eq!(r.granted, 30.0);
    assert!(
        (r.remaining - 70.0).abs() < 1e-6,
        "remaining={}",
        r.remaining
    );

    // Request 80 with no time passed → only 70 left → grant 70, remaining 0.
    let r2 = s
        .reserve(&key, 10.0, 100.0, 80.0, now, TTL_MS)
        .await
        .unwrap();
    assert!((r2.granted - 70.0).abs() < 1e-6, "granted={}", r2.granted);
    assert!(r2.remaining.abs() < 1e-6);

    // Empty → grant 0.
    let r3 = s
        .reserve(&key, 10.0, 100.0, 5.0, now, TTL_MS)
        .await
        .unwrap();
    assert_eq!(r3.granted, 0.0);
}

#[tokio::test]
async fn grant_is_min_of_requested_and_available() {
    let Some(s) = store_or_skip().await else {
        return;
    };
    let key = unique_key("min");
    let now = 2_000_000u64;
    // burst 50, request 20 → grant 20 (requested < available).
    let r = s.reserve(&key, 1.0, 50.0, 20.0, now, TTL_MS).await.unwrap();
    assert_eq!(r.granted, 20.0);
    // request 100 → grant 30 (available < requested).
    let r2 = s
        .reserve(&key, 1.0, 50.0, 100.0, now, TTL_MS)
        .await
        .unwrap();
    assert_eq!(r2.granted, 30.0);
}

#[tokio::test]
async fn refills_over_time_capped_at_burst() {
    let Some(s) = store_or_skip().await else {
        return;
    };
    let key = unique_key("refill");
    let now = 5_000_000u64;
    // burst 10, rate 10/s. Drain it.
    assert_eq!(
        s.reserve(&key, 10.0, 10.0, 10.0, now, TTL_MS)
            .await
            .unwrap()
            .granted,
        10.0
    );
    assert_eq!(
        s.reserve(&key, 10.0, 10.0, 5.0, now, TTL_MS)
            .await
            .unwrap()
            .granted,
        0.0
    );
    // 500ms later → 5 tokens refilled (10/s × 0.5s).
    let r = s
        .reserve(&key, 10.0, 10.0, 5.0, now + 500, TTL_MS)
        .await
        .unwrap();
    assert!((r.granted - 5.0).abs() < 1e-6, "granted={}", r.granted);
    // A long time later → capped at burst 10, not rate×elapsed.
    let r2 = s
        .reserve(&key, 10.0, 10.0, 1000.0, now + 100_000, TTL_MS)
        .await
        .unwrap();
    assert_eq!(r2.granted, 10.0);
}

#[tokio::test]
async fn concurrent_reservations_are_atomic_and_never_over_grant() {
    // THE acceptance property: the single-key Lua reservation runs atomically in
    // Valkey, so N concurrent reservations of the whole fleet can never grant
    // more than the bucket holds. rate=0 (no refill) + burst B + N>B concurrent
    // 1-token requests must grant EXACTLY B — never B+1 (safety, the over-admit
    // bug), never B-1 (liveness, no lost grant).
    let Some(s) = store_or_skip().await else {
        return;
    };
    let s = Arc::new(s);
    let key = unique_key("atomic");
    let now = 9_000_000u64;
    let burst = 200.0;
    let concurrency = 500u64;

    let granted = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for _ in 0..concurrency {
        let s = Arc::clone(&s);
        let key = key.clone();
        let g = Arc::clone(&granted);
        handles.push(tokio::spawn(async move {
            let r = s.reserve(&key, 0.0, burst, 1.0, now, TTL_MS).await.unwrap();
            if r.granted >= 1.0 {
                g.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    assert_eq!(
        granted.load(Ordering::Relaxed),
        burst as u64,
        "atomic reservation over- or under-granted under {concurrency}-way concurrency"
    );
}

#[tokio::test]
async fn distinct_keys_have_independent_global_buckets() {
    let Some(s) = store_or_skip().await else {
        return;
    };
    let a = unique_key("iso-a");
    let b = unique_key("iso-b");
    let now = 3_000_000u64;
    // Drain a.
    assert_eq!(
        s.reserve(&a, 0.0, 5.0, 5.0, now, TTL_MS)
            .await
            .unwrap()
            .granted,
        5.0
    );
    assert_eq!(
        s.reserve(&a, 0.0, 5.0, 1.0, now, TTL_MS)
            .await
            .unwrap()
            .granted,
        0.0
    );
    // b is full and unaffected.
    assert_eq!(
        s.reserve(&b, 0.0, 5.0, 5.0, now, TTL_MS)
            .await
            .unwrap()
            .granted,
        5.0
    );
}

#[tokio::test]
async fn fleet_of_lease_managers_shares_one_valkey_bucket() {
    // The whole distributed model against REAL Valkey: N independent lease
    // managers (each its own local pool) sharing ONE Valkey global bucket for the
    // same key under a fleet-wide flood. Total admits must stay within the global
    // capacity + N*batch (the fleet-shared bound), and be far below N*limit (the
    // per-pod behaviour this replaces).
    let Some(store) = store_or_skip().await else {
        return;
    };
    let store = Arc::new(store);
    let key = unique_key("fleet");
    let n_pods = 6usize;
    let interval_ms = 200u64;
    let cfg = LeaseConfig {
        interval_ms,
        low_water: 0.2,
        ttl_ms: TTL_MS,
    };
    let pods: Vec<_> = (0..n_pods)
        .map(|_| {
            LeaseManager::new(
                Arc::clone(&store),
                CircuitBreaker::new(3, Duration::from_secs(1), Instant::now()),
                cfg,
            )
        })
        .collect();

    let rule = Rule {
        limit: 50.0,
        period_s: 1.0,
        burst: 50.0,
        cost: 1.0,
    };
    let base = Instant::now();
    let duration_ms = 3_000u64;
    // batch = rate*interval clamped to [1, burst] = 50*0.2 = 10.
    let batch = (rule.limit / rule.period_s * (interval_ms as f64) / 1000.0)
        .max(1.0)
        .min(rule.burst);

    let mut admitted = 0u64;
    for ms in 0..=duration_ms {
        let now = base + Duration::from_millis(ms);
        for p in &pods {
            let (v, refresh) = p.check(&key, &rule, now);
            if v.allowed {
                admitted += 1;
            }
            if refresh || ms % interval_ms == 0 {
                p.refresh(&key, ms, now).await;
            }
        }
    }

    let global_cap = rule.burst + rule.limit / rule.period_s * (duration_ms as f64) / 1000.0;
    let bound = (global_cap + (n_pods as f64) * batch).ceil() as u64 + n_pods as u64;
    assert!(
        admitted <= bound,
        "fleet admitted {admitted} exceeded shared bound {bound} (global_cap {global_cap}, N*B {})",
        n_pods as f64 * batch
    );
    let per_pod_would_be =
        rule.limit / rule.period_s * (n_pods as f64) * (duration_ms as f64) / 1000.0;
    assert!(
        (admitted as f64) < per_pod_would_be * 0.6,
        "fleet admitted {admitted} looks per-pod ({per_pod_would_be}), not shared via Valkey"
    );
}
