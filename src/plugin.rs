//! SPOE glue + Phase-2 wiring: parse the `check-ratelimit` message, enforce
//! locally via the lease manager, write the verdict, and signal an async lease
//! refresh when needed.
//!
//! `process(&self, ...)` is synchronous and in-memory for the default lease mode
//! (the lease manager's local pool). A dedicated tokio runtime, spawned in
//! `init`, runs the background refresh loop — a periodic tick (`refresh_all`)
//! plus a prefetch channel drained on the low-water signal. The shared store can
//! be sharded across multiple Valkey/Redis/Sentinel endpoints; a stable hash
//! maps each rate-limit key to one shard. Exact/GCRA mode is the deliberate
//! exception: it synchronously checks the selected shared-store shard per
//! request, so all configured Valkey connections are warmed in the background at
//! startup.

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use haproxy_spoa_hub_plugin_api::{
    MetricRecorder, ProcessingResult, RVec, SpoeValue, TxnVariable, define_plugin,
};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::time::MissedTickBehavior;

use crate::breaker::CircuitBreaker;
use crate::config::PluginConfig;
use crate::lease::{LeaseConfig, LeaseManager};
use crate::limiter::Verdict;
use crate::rule::Rule;
use crate::store::memory::InMemoryStore;
use crate::store::valkey::ValkeyStore;
use crate::store::{Reservation, Store, StoreError};

static METRICS: MetricRecorder = MetricRecorder::new();
static STATE: RwLock<Option<PluginState>> = RwLock::new(None);

/// Runtime store selection: shared Valkey, or per-pod in-process.
enum AnyStore {
    Valkey(Arc<ShardedValkeyStore>),
    Memory(InMemoryStore),
}

/// Connects to Valkey on demand and reconnects after startup/DNS/store outages.
/// Plugin initialization must not fail permanently just because the chart-owned
/// store Service has not been applied yet. A background warm-connect loop hides
/// normal startup ordering from the exact-mode request path once the store is
/// reachable.
struct DeferredValkeyStore {
    url: String,
    inner: AsyncMutex<Option<Arc<ValkeyStore>>>,
}

impl DeferredValkeyStore {
    fn new(url: String) -> Self {
        Self {
            url,
            inner: AsyncMutex::new(None),
        }
    }

    async fn get(&self) -> Result<Arc<ValkeyStore>, StoreError> {
        let mut guard = self.inner.lock().await;
        if let Some(store) = guard.as_ref().cloned() {
            return Ok(store);
        }
        let store = Arc::new(ValkeyStore::connect(&self.url).await?);
        *guard = Some(Arc::clone(&store));
        Ok(store)
    }
}

impl Store for DeferredValkeyStore {
    async fn reserve(
        &self,
        key: &str,
        rate: f64,
        burst: f64,
        requested: f64,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<Reservation, StoreError> {
        self.get()
            .await?
            .reserve(key, rate, burst, requested, now_ms, ttl_ms)
            .await
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
        self.get()
            .await?
            .gcra_check(key, rate, burst, cost, now_ms, ttl_ms)
            .await
    }
}

struct ShardedValkeyStore {
    shards: Vec<Arc<DeferredValkeyStore>>,
}

impl ShardedValkeyStore {
    fn new(urls: &[String]) -> Self {
        Self {
            shards: urls
                .iter()
                .cloned()
                .map(DeferredValkeyStore::new)
                .map(Arc::new)
                .collect(),
        }
    }

    fn shard(&self, key: &str) -> &DeferredValkeyStore {
        let idx = (stable_hash64(key.as_bytes()) as usize) % self.shards.len();
        &self.shards[idx]
    }

    fn shards(&self) -> impl Iterator<Item = Arc<DeferredValkeyStore>> + '_ {
        self.shards.iter().cloned()
    }
}

impl Store for ShardedValkeyStore {
    async fn reserve(
        &self,
        key: &str,
        rate: f64,
        burst: f64,
        requested: f64,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<Reservation, StoreError> {
        self.shard(key)
            .reserve(key, rate, burst, requested, now_ms, ttl_ms)
            .await
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
        self.shard(key)
            .gcra_check(key, rate, burst, cost, now_ms, ttl_ms)
            .await
    }
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

fn stable_hash64(bytes: &[u8]) -> u64 {
    // FNV-1a: small, deterministic across processes/languages, and sufficient
    // for spreading arbitrary limiter keys across a small set of store shards.
    let mut h = 0xcbf29ce484222325u64;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

struct PrefetchReq {
    key: String,
    now_ms: u64,
}

async fn warm_valkey_connection(store: Arc<DeferredValkeyStore>) {
    let mut ticker = tokio::time::interval(Duration::from_millis(200));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        match store.get().await {
            Ok(_) => {
                log::info!("ratelimit: shared store connection ready");
                return;
            }
            Err(e) => {
                log::debug!("ratelimit: shared store connection not ready yet: {e}");
            }
        }
        ticker.tick().await;
    }
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
    mut rx: Receiver<PrefetchReq>,
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

struct PluginState {
    config: PluginConfig,
    runtime: Option<Runtime>,
    manager: Arc<LeaseManager<AnyStore>>,
    prefetch: Option<Sender<PrefetchReq>>,
}

impl Drop for PluginState {
    fn drop(&mut self) {
        // Close the prefetch channel first so the background loop can observe
        // EOF and exit cleanly before the runtime is torn down. A plugin reload
        // replaces this state under STATE's write lock after in-flight process()
        // calls release their read locks.
        self.prefetch.take();
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_timeout(Duration::from_millis(100));
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

        let store = if config.store_urls.is_empty() {
            log::info!("ratelimit: no store_url(s) configured — using per-pod in-process store");
            AnyStore::Memory(InMemoryStore::new())
        } else {
            log::info!(
                "ratelimit: {} shared store shard(s) configured; warming connections",
                config.store_urls.len()
            );
            let store = Arc::new(ShardedValkeyStore::new(&config.store_urls));
            for shard in store.shards() {
                runtime.spawn(warm_valkey_connection(shard));
            }
            AnyStore::Valkey(store)
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
            max_keys: config.max_keys,
            idle_ttl_ms: config.idle_ttl_ms,
            store_timeout_ms: config.store_timeout_ms,
        };
        let manager = Arc::new(LeaseManager::new(Arc::new(store), breaker, lease_cfg));

        let (tx, rx) = mpsc::channel(config.max_prefetch_queue);
        runtime.spawn(background(manager.clone(), rx, config.interval_ms));

        let mut state = STATE
            .write()
            .map_err(|_| "plugin state lock poisoned during init")?;
        *state = Some(PluginState {
            config,
            runtime: Some(runtime),
            manager,
            prefetch: Some(tx),
        });
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
        let state = STATE
            .read()
            .map_err(|_| "plugin state lock poisoned during process")?;
        let state = state.as_ref().ok_or("plugin not initialized")?;
        let config = &state.config;
        let manager = &state.manager;

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
            let runtime = state
                .runtime
                .as_ref()
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
            if let Some(tx) = &state.prefetch {
                // Bounded, non-blocking: signal the background loop and move on.
                // The lease manager coalesces per-key refreshes, so a full queue
                // can safely drop this hint; the periodic refresh loop remains.
                let req = PrefetchReq {
                    key: key.to_string(),
                    now_ms: wall_ms(),
                };
                if let Err(e) = tx.try_send(req) {
                    let reason = match e {
                        TrySendError::Full(_) => "full",
                        TrySendError::Closed(_) => "closed",
                    };
                    METRICS.counter("ratelimit_prefetch_dropped_total", &[("reason", reason)], 1);
                }
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

#[cfg(test)]
mod tests {
    use super::*;
    use haproxy_spoa_hub_plugin_api::{PluginContext, RHashMap, RString};

    fn test_context() -> PluginContext {
        PluginContext {
            name: RString::from("rate_limit"),
            config: RHashMap::new(),
        }
    }

    #[test]
    fn init_can_run_for_multiple_plugin_generations_in_one_process() {
        let ctx = test_context();

        let mut first = RateLimitPlugin::__new();
        first.__init(&ctx).expect("first plugin generation init");

        let mut second = RateLimitPlugin::__new();
        second.__init(&ctx).expect("second plugin generation init");
    }

    #[test]
    fn stable_hash_has_known_value() {
        assert_eq!(
            stable_hash64(b"default/ingress|127.0.0.1"),
            7262911014780214683
        );
    }
}
