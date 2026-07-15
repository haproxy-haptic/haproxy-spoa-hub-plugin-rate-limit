//! SPOE glue + Phase-2 wiring: parse the `check-ratelimit` message, enforce
//! locally via the lease manager, write the verdict, and signal an async lease
//! refresh when needed.
//!
//! `process(&self, ...)` is synchronous and in-memory (the lease manager's local
//! pool). A dedicated tokio runtime, spawned in `init`, runs the background
//! refresh loop — a periodic tick (`refresh_all`) plus a prefetch channel drained
//! on the low-water signal. The shared store (Valkey) is therefore touched only
//! on that background path, never per request.

use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use haproxy_spoa_hub_plugin_api::{
    MetricRecorder, ProcessingResult, RVec, SpoeValue, TxnVariable, define_plugin,
};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::time::MissedTickBehavior;

use crate::breaker::CircuitBreaker;
use crate::config::PluginConfig;
use crate::lease::{LeaseConfig, LeaseManager};
use crate::limiter::Verdict;
use crate::rule::Rule;
use crate::store::memory::InMemoryStore;
use crate::store::valkey::ValkeyStore;
use crate::store::{Reservation, Store, StoreError};

static CONFIG: OnceLock<PluginConfig> = OnceLock::new();
static RUNTIME: OnceLock<Runtime> = OnceLock::new();
static MANAGER: OnceLock<std::sync::Arc<LeaseManager<AnyStore>>> = OnceLock::new();
static PREFETCH: OnceLock<UnboundedSender<PrefetchReq>> = OnceLock::new();
static METRICS: MetricRecorder = MetricRecorder::new();

/// Runtime store selection: shared Valkey, or per-pod in-process.
enum AnyStore {
    Valkey(ValkeyStore),
    Memory(InMemoryStore),
}

impl Store for AnyStore {
    async fn reserve(
        &self,
        key: &str,
        rate: f64,
        burst: f64,
        requested: f64,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<Reservation, StoreError> {
        match self {
            AnyStore::Valkey(s) => s.reserve(key, rate, burst, requested, now_ms, ttl_ms).await,
            AnyStore::Memory(s) => s.reserve(key, rate, burst, requested, now_ms, ttl_ms).await,
        }
    }

    async fn gcra_check(
        &self,
        key: &str,
        rate: f64,
        burst: f64,
        cost: f64,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<crate::store::GcraVerdict, StoreError> {
        match self {
            AnyStore::Valkey(s) => s.gcra_check(key, rate, burst, cost, now_ms, ttl_ms).await,
            AnyStore::Memory(s) => s.gcra_check(key, rate, burst, cost, now_ms, ttl_ms).await,
        }
    }
}

struct PrefetchReq {
    key: String,
    now_ms: u64,
}

fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Background refresh loop: periodic tick + on-demand prefetch. Runs on the
/// plugin's tokio runtime for the process lifetime.
async fn background(
    manager: std::sync::Arc<LeaseManager<AnyStore>>,
    mut rx: UnboundedReceiver<PrefetchReq>,
    interval_ms: u64,
) {
    let mut tick = tokio::time::interval(Duration::from_millis(interval_ms.max(1)));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = tick.tick() => {
                manager.refresh_all(wall_ms(), Instant::now()).await;
            }
            maybe = rx.recv() => {
                match maybe {
                    Some(req) => manager.refresh(&req.key, req.now_ms, Instant::now()).await,
                    None => break, // sender dropped
                }
            }
        }
    }
}

#[derive(Debug)]
struct RateLimitPlugin;

define_plugin!(RateLimitPlugin, {
    fn new() -> Self {
        RateLimitPlugin
    }

    fn init(
        &mut self,
        context: &PluginContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let config = PluginConfig::from_context(context)?;

        // A small dedicated runtime drives the background lease refresh + the
        // async Valkey connection. The per-request path never touches it.
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("ratelimit-lease")
            .enable_all()
            .build()?;

        let store = match &config.store_url {
            Some(url) => AnyStore::Valkey(
                runtime
                    .block_on(ValkeyStore::connect(url))
                    .map_err(|e| format!("connect to store {url}: {e}"))?,
            ),
            None => {
                log::info!("ratelimit: no store_url configured — using per-pod in-process store");
                AnyStore::Memory(InMemoryStore::new())
            }
        };

        let breaker = CircuitBreaker::new(
            config.breaker_threshold,
            Duration::from_millis(config.breaker_cooldown_ms),
            Instant::now(),
        );
        let lease_cfg = LeaseConfig {
            interval_ms: config.interval_ms,
            low_water: (config.low_water_percent as f64) / 100.0,
            ttl_ms: config.ttl_ms,
        };
        let manager = std::sync::Arc::new(LeaseManager::new(
            std::sync::Arc::new(store),
            breaker,
            lease_cfg,
        ));

        let (tx, rx) = mpsc::unbounded_channel();
        runtime.spawn(background(manager.clone(), rx, config.interval_ms));

        MANAGER
            .set(manager)
            .map_err(|_| "manager already initialized")?;
        PREFETCH
            .set(tx)
            .map_err(|_| "prefetch already initialized")?;
        RUNTIME
            .set(runtime)
            .map_err(|_| "runtime already initialized")?;
        CONFIG
            .set(config)
            .map_err(|_| "config already initialized")?;
        Ok(())
    }

    fn name(&self) -> &str {
        "rate-limit"
    }

    fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    fn process(
        &self,
        message: &SpoeMessage,
    ) -> Result<ProcessingResult, Box<dyn std::error::Error + Send + Sync>> {
        let config = CONFIG
            .get()
            .ok_or("plugin not initialized: config missing")?;
        let manager = MANAGER
            .get()
            .ok_or("plugin not initialized: manager missing")?;

        let key = match message.get_string("key").filter(|s| !s.is_empty()) {
            Some(k) => k,
            None => {
                METRICS.counter("ratelimit_requests_total", &[("outcome", "no_key")], 1);
                return Ok(build_result(&Verdict::allow()));
            }
        };

        let rule = Rule::parse(
            message.get_string("limit"),
            message.get_string("period"),
            message.get_string("burst"),
            message.get_string("cost"),
        );
        let rule = match rule {
            Some(r) => r,
            None => {
                log::warn!(
                    "ratelimit: key={key} has no valid limit/period; failing {}",
                    if config.fail_open { "open" } else { "closed" }
                );
                METRICS.counter("ratelimit_requests_total", &[("outcome", "bad_rule")], 1);
                let v = if config.fail_open {
                    Verdict::allow()
                } else {
                    Verdict::deny()
                };
                return Ok(build_result(&v));
            }
        };

        // Exact mode (money/contract limits): a synchronous GCRA round-trip per
        // request — the deliberate exactness/latency trade, scoped to opted-in
        // keys, never the fleet default.
        if message
            .get_string("mode")
            .is_some_and(|m| m.eq_ignore_ascii_case("exact"))
        {
            let runtime = RUNTIME
                .get()
                .ok_or("plugin not initialized: runtime missing")?;
            let verdict =
                runtime.block_on(manager.exact_check(key, &rule, wall_ms(), Instant::now()));
            METRICS.counter(
                "ratelimit_requests_total",
                &[(
                    "outcome",
                    if verdict.allowed {
                        "exact_allowed"
                    } else {
                        "exact_limited"
                    },
                )],
                1,
            );
            return Ok(build_result(&verdict));
        }

        let (verdict, needs_refresh) = manager.check(key, &rule, Instant::now());
        if needs_refresh {
            if let Some(tx) = PREFETCH.get() {
                // Unbounded, non-blocking: signal the background loop and move on.
                let _ = tx.send(PrefetchReq {
                    key: key.to_string(),
                    now_ms: wall_ms(),
                });
            }
        }
        METRICS.counter(
            "ratelimit_requests_total",
            &[(
                "outcome",
                if verdict.allowed {
                    "allowed"
                } else {
                    "limited"
                },
            )],
            1,
        );
        Ok(build_result(&verdict))
    }

    fn config_schema(&self) -> Option<&str> {
        Some(PluginConfig::config_schema())
    }

    metrics_static = METRICS;
});

impl Verdict {
    /// Allow with no rate-limit accounting (no key / fail-open).
    fn allow() -> Self {
        Verdict {
            allowed: true,
            limit: 0,
            remaining: 0,
            reset_s: 0,
            retry_after_s: 0,
        }
    }

    /// Deny with no accounting (fail-closed on a bad rule).
    fn deny() -> Self {
        Verdict {
            allowed: false,
            limit: 0,
            remaining: 0,
            reset_s: 0,
            retry_after_s: 1,
        }
    }
}

/// Build the `txn.hub.rate_limit.*` variables from a verdict.
fn build_result(v: &Verdict) -> ProcessingResult {
    let mut vars: RVec<TxnVariable> = RVec::with_capacity(5);
    vars.push(TxnVariable::transaction(
        "allowed",
        SpoeValue::Bool(v.allowed),
    ));
    vars.push(TxnVariable::transaction(
        "limit",
        SpoeValue::Uint32(v.limit),
    ));
    vars.push(TxnVariable::transaction(
        "remaining",
        SpoeValue::Uint32(v.remaining),
    ));
    vars.push(TxnVariable::transaction(
        "reset",
        SpoeValue::Uint32(v.reset_s),
    ));
    vars.push(TxnVariable::transaction(
        "retry_after",
        SpoeValue::Uint32(v.retry_after_s),
    ));
    ProcessingResult { variables: vars }
}
