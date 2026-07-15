//! SPOE glue: parse the `check-ratelimit` message, enforce, write the verdict.
//!
//! The plugin is instantiated once and `process(&self, ...)` is called
//! concurrently, so all mutable state lives in `static`s behind interior
//! mutability (the limiter's per-key locks). The hot path here is a couple of
//! string parses plus one [`Limiter::check`] — no network I/O.

use std::sync::OnceLock;
use std::time::Instant;

use haproxy_spoa_hub_plugin_api::{
    MetricRecorder, ProcessingResult, RVec, SpoeValue, TxnVariable, define_plugin,
};

use crate::config::PluginConfig;
use crate::limiter::{Limiter, Verdict};
use crate::rule::Rule;

static CONFIG: OnceLock<PluginConfig> = OnceLock::new();
static LIMITER: OnceLock<Limiter> = OnceLock::new();
static METRICS: MetricRecorder = MetricRecorder::new();

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
        CONFIG
            .set(config)
            .map_err(|_| "config already initialized")?;
        LIMITER
            .set(Limiter::new())
            .map_err(|_| "limiter already initialized")?;
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
        let limiter = LIMITER
            .get()
            .ok_or("plugin not initialized: limiter missing")?;

        // No key ⇒ the route carries no shared rate-limit; allow (nothing to
        // enforce). This is not a failure, so it does not consult fail_open.
        let key = match message.get_string("key").filter(|s| !s.is_empty()) {
            Some(k) => k,
            None => {
                METRICS.counter("ratelimit_requests_total", &[("outcome", "no_key")], 1);
                return Ok(build_result(&Verdict::allow()));
            }
        };

        // A key with a missing/invalid limit or period is a misconfiguration;
        // honour fail_open rather than silently enforcing a nonsense rule.
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

        let verdict = limiter.check(key, &rule, Instant::now());
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
