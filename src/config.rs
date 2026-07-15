//! Plugin configuration, parsed from `[plugins.params]` in the hub TOML.

use haproxy_spoa_hub_plugin_api::PluginContext;

#[derive(Debug, Clone)]
pub struct PluginConfig {
    /// Allow the request when a rule can't be evaluated (bad rule). Default true
    /// — a rate limiter must not become an outage. Money limits fail closed
    /// per-rule (Phase 4).
    pub fail_open: bool,
    /// Valkey/Redis URLs for the shared budget allocator. A single URL uses
    /// one store; multiple URLs shard keys across independent stores with a
    /// stable hash. URLs may point at plain Redis/Valkey, Sentinel, or any
    /// topology supported by fred's URL parser. When unset, the plugin uses an
    /// in-process store — limits are then per-pod, like the stick-table
    /// fallback but through the plugin path.
    pub store_urls: Vec<String>,
    /// Lease refresh interval T (ms). Store work is bounded by active keys and
    /// refresh cadence rather than raw request RPS.
    pub interval_ms: u64,
    /// Low-water threshold as a percent of the batch (a request below it
    /// triggers an async prefetch). Default 20.
    pub low_water_percent: u64,
    /// Store key TTL (ms).
    pub ttl_ms: u64,
    /// Circuit-breaker: consecutive failures before opening.
    pub breaker_threshold: u32,
    /// Circuit-breaker: open-state cooldown (ms) before a half-open probe.
    pub breaker_cooldown_ms: u64,
    /// Maximum local keys retained by one sidecar.
    pub max_keys: usize,
    /// Milliseconds after which an inactive local key is evicted.
    pub idle_ttl_ms: u64,
    /// Timeout for one shared-store operation.
    pub store_timeout_ms: u64,
    /// Bounded low-water prefetch queue size.
    pub max_prefetch_queue: usize,
}

impl PluginConfig {
    pub fn from_context(
        context: &PluginContext,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let get_bool =
            |k: &str, d: bool| context.get_config(k).and_then(|v| v.as_bool()).unwrap_or(d);
        let get_u64 = |k: &str, d: u64, lo: i64, hi: i64| {
            context
                .get_config(k)
                .and_then(|v| v.as_integer())
                .map(|n| n.clamp(lo, hi) as u64)
                .unwrap_or(d)
        };

        let mut store_urls: Vec<String> = context
            .get_config("store_urls")
            .and_then(|v| v.as_array())
            .into_iter()
            .flatten()
            .filter_map(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect();

        if store_urls.is_empty() {
            store_urls = context
                .get_config("store_url")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| vec![String::from(s)])
                .unwrap_or_default();
        }

        Ok(PluginConfig {
            fail_open: get_bool("fail_open", true),
            store_urls,
            interval_ms: get_u64("sync_interval_ms", 200, 1, 60_000),
            low_water_percent: get_u64("low_water_percent", 20, 0, 100),
            ttl_ms: get_u64("ttl_ms", 60_000, 1, 3_600_000),
            breaker_threshold: get_u64("breaker_threshold", 5, 1, 1_000) as u32,
            breaker_cooldown_ms: get_u64("breaker_cooldown_ms", 5_000, 1, 600_000),
            max_keys: get_u64("max_keys", 50_000, 1, 10_000_000) as usize,
            idle_ttl_ms: get_u64("idle_ttl_ms", 120_000, 1_000, 86_400_000),
            store_timeout_ms: get_u64("store_timeout_ms", 25, 1, 60_000),
            max_prefetch_queue: get_u64("max_prefetch_queue", 8_192, 1, 1_000_000) as usize,
        })
    }

    pub fn config_schema() -> &'static str {
        r#"{
  "type": "object",
  "properties": {
    "fail_open": { "type": "boolean" },
    "store_url": { "type": "string", "description": "Single Valkey/Redis URL; unset with no store_urls = per-pod in-process store" },
    "store_urls": { "type": "array", "items": { "type": "string" }, "description": "Valkey/Redis URLs; keys are sharded across entries with a stable hash" },
    "sync_interval_ms": { "type": "integer", "minimum": 1 },
    "low_water_percent": { "type": "integer", "minimum": 0, "maximum": 100 },
    "ttl_ms": { "type": "integer", "minimum": 1 },
    "breaker_threshold": { "type": "integer", "minimum": 1 },
    "breaker_cooldown_ms": { "type": "integer", "minimum": 1 },
    "max_keys": { "type": "integer", "minimum": 1 },
    "idle_ttl_ms": { "type": "integer", "minimum": 1000 },
    "store_timeout_ms": { "type": "integer", "minimum": 1 },
    "max_prefetch_queue": { "type": "integer", "minimum": 1 }
  }
}"#
    }
}
