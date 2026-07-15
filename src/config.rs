//! Plugin configuration, parsed from `[plugins.params]` in the hub TOML.
//!
//! Phase 1 exposes only `fail_open` (the store-unavailable policy). Phase 2 adds
//! the Valkey address, the lease interval `T`, the per-pod batch `B`, and the
//! per-key fail policy; the schema grows without breaking this surface.

use haproxy_spoa_hub_plugin_api::PluginContext;

#[derive(Debug, Clone)]
pub struct PluginConfig {
    /// When a rule can't be evaluated (Phase 2: the shared store is
    /// unavailable), allow the request rather than block it. Default `true` —
    /// a rate limiter must not become an outage. Money/quota limits override
    /// to fail closed per-rule (Phase 4).
    pub fail_open: bool,
}

impl PluginConfig {
    pub fn from_context(
        context: &PluginContext,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let fail_open = context
            .get_config("fail_open")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        Ok(PluginConfig { fail_open })
    }

    pub fn config_schema() -> &'static str {
        r#"{
  "type": "object",
  "properties": {
    "fail_open": {
      "type": "boolean",
      "description": "Allow requests when a rule cannot be evaluated (default true)"
    }
  }
}"#
    }
}
