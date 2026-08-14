//! Plugin configuration, parsed from `[plugins.params]` in the hub TOML.

use std::io;

use haproxy_spoa_hub_plugin_api::PluginContext;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreFailureMode {
    Local,
    Open,
    Closed,
}

impl StoreFailureMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Open => "open",
            Self::Closed => "closed",
        }
    }

    fn parse(value: &str) -> Result<Self, io::Error> {
        match value {
            "local" => Ok(Self::Local),
            "open" => Ok(Self::Open),
            "closed" => Ok(Self::Closed),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("store_failure_mode must be local, open, or closed; got {value}"),
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PluginConfig {
    /// Legacy failure switch. New deployments should set the store and invalid
    /// rule policies independently.
    pub fail_open: bool,
    /// Explicit policy for shared-store errors, timeouts, and an open breaker.
    /// When absent, lease mode follows `fail_open` and exact mode fails closed.
    pub store_failure_mode: Option<StoreFailureMode>,
    /// Explicit malformed-rule policy. When absent, follows `fail_open`.
    pub invalid_rule_fail_open: Option<bool>,
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
    /// When the local lease pool is empty, do one bounded, coalesced borrow
    /// from the shared store before denying (makes the rule's `burst` real for
    /// concurrent bursts). Default true; disable to restore pre-0.2
    /// instant-deny behavior.
    pub borrow_on_empty: bool,
}

impl PluginConfig {
    pub fn lease_store_failure_mode(&self) -> StoreFailureMode {
        self.store_failure_mode.unwrap_or(if self.fail_open {
            StoreFailureMode::Open
        } else {
            StoreFailureMode::Closed
        })
    }

    pub fn exact_store_failure_mode(&self) -> StoreFailureMode {
        self.store_failure_mode.unwrap_or(StoreFailureMode::Closed)
    }

    pub fn effective_invalid_rule_fail_open(&self) -> bool {
        self.invalid_rule_fail_open.unwrap_or(self.fail_open)
    }

    pub fn use_legacy_optimistic_cold_start(&self) -> bool {
        self.store_failure_mode.is_none()
    }

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

        let store_failure_mode = context
            .get_config("store_failure_mode")
            .and_then(|v| v.as_str())
            .map(StoreFailureMode::parse)
            .transpose()?;

        Ok(PluginConfig {
            fail_open: get_bool("fail_open", true),
            store_failure_mode,
            invalid_rule_fail_open: context
                .get_config("invalid_rule_fail_open")
                .and_then(|v| v.as_bool()),
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
            borrow_on_empty: get_bool("borrow_on_empty", true),
        })
    }

    pub fn config_schema() -> &'static str {
        r#"{
  "type": "object",
  "properties": {
    "fail_open": { "type": "boolean", "description": "Legacy fallback for invalid rules and lease store failures when their explicit policies are absent" },
    "store_failure_mode": { "type": "string", "enum": ["local", "open", "closed"], "description": "Store outage policy: bounded per-sidecar fallback, allow, or deny" },
    "invalid_rule_fail_open": { "type": "boolean", "description": "Allow malformed rules independently of the store outage policy" },
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
    "max_prefetch_queue": { "type": "integer", "minimum": 1 },
    "borrow_on_empty": { "type": "boolean", "description": "Empty local pool escalates to one bounded store borrow before denying (burst correctness); default true" }
  }
}"#
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use haproxy_spoa_hub_plugin_api::{ConfigValue, RHashMap, RString};

    fn context(values: &[(&str, ConfigValue)]) -> PluginContext {
        let mut config = RHashMap::new();
        for (key, value) in values {
            config.insert(RString::from(*key), value.clone());
        }
        PluginContext {
            name: RString::from("rate_limit"),
            config,
        }
    }

    #[test]
    fn absent_new_policies_preserve_legacy_behavior() {
        let default = PluginConfig::from_context(&context(&[])).unwrap();
        assert_eq!(default.lease_store_failure_mode(), StoreFailureMode::Open);
        assert_eq!(default.exact_store_failure_mode(), StoreFailureMode::Closed);
        assert!(default.effective_invalid_rule_fail_open());
        assert!(default.use_legacy_optimistic_cold_start());

        let closed =
            PluginConfig::from_context(&context(&[("fail_open", ConfigValue::Bool(false))]))
                .unwrap();
        assert_eq!(closed.lease_store_failure_mode(), StoreFailureMode::Closed);
        assert_eq!(closed.exact_store_failure_mode(), StoreFailureMode::Closed);
        assert!(!closed.effective_invalid_rule_fail_open());
        assert!(closed.use_legacy_optimistic_cold_start());
    }

    #[test]
    fn explicit_policies_are_independent() {
        let config = PluginConfig::from_context(&context(&[
            (
                "store_failure_mode",
                ConfigValue::String(RString::from("local")),
            ),
            ("invalid_rule_fail_open", ConfigValue::Bool(false)),
        ]))
        .unwrap();
        assert_eq!(config.lease_store_failure_mode(), StoreFailureMode::Local);
        assert_eq!(config.exact_store_failure_mode(), StoreFailureMode::Local);
        assert!(!config.effective_invalid_rule_fail_open());
        assert!(!config.use_legacy_optimistic_cold_start());
    }

    #[test]
    fn rejects_unknown_store_failure_mode() {
        let error = PluginConfig::from_context(&context(&[(
            "store_failure_mode",
            ConfigValue::String(RString::from("sometimes")),
        )]))
        .unwrap_err();
        assert!(error.to_string().contains("local, open, or closed"));
    }
}
