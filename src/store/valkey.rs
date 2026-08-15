//! Valkey-backed store: the atomic lease reservation + the exact GCRA check.

use std::time::Duration;

use fred::prelude::*;
use fred::types::scripts::Script;

use crate::store::{GcraVerdict, Reservation, Store, StoreError};

/// Budget for connecting (Sentinel discovery + handshake + script preload).
///
/// A connect that never resolves used to pin the deferred store forever: one
/// `SCRIPT LOAD` reply lost across a Sentinel failover left every later reserve
/// timing out behind it. Bounding the whole connect turns that into one failed
/// attempt that the warm loop simply retries.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// The reservation script (see `reserve.lua`), loaded once and run via EVALSHA.
const RESERVE_LUA: &str = include_str!("reserve.lua");
/// The exact GCRA script (see `gcra.lua`), for `exact` mode keys.
const GCRA_LUA: &str = include_str!("gcra.lua");

pub struct ValkeyStore {
    client: Client,
    reserve: Script,
    gcra: Script,
}

impl ValkeyStore {
    /// Connect to Valkey/Redis at `url` (e.g. `redis://127.0.0.1:6379`) and
    /// preload the scripts.
    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        match tokio::time::timeout(CONNECT_TIMEOUT, Self::connect_unbounded(url)).await {
            Ok(result) => result,
            Err(_) => Err(StoreError::Connection(format!(
                "connect timed out after {}ms",
                CONNECT_TIMEOUT.as_millis()
            ))),
        }
    }

    async fn connect_unbounded(url: &str) -> Result<Self, StoreError> {
        let config = Config::from_url(url).map_err(|e| StoreError::Connection(e.to_string()))?;
        // fred's default command timeout is unbounded; a reply that never comes
        // (connection replaced mid-failover) must surface as an error the caller
        // can invalidate on, not hang the shard's client generation.
        let perf = PerformanceConfig {
            default_command_timeout: CONNECT_TIMEOUT,
            ..PerformanceConfig::default()
        };
        let client = Client::new(config, Some(perf), None, None);
        client
            .init()
            .await
            .map_err(|e| StoreError::Connection(e.to_string()))?;
        let reserve = Script::from_lua(RESERVE_LUA);
        reserve
            .load(&client)
            .await
            .map_err(|e| StoreError::Protocol(e.to_string()))?;
        let gcra = Script::from_lua(GCRA_LUA);
        gcra.load(&client)
            .await
            .map_err(|e| StoreError::Protocol(e.to_string()))?;
        Ok(Self {
            client,
            reserve,
            gcra,
        })
    }

    /// Close the connection (best-effort).
    pub async fn shutdown(&self) {
        let _ = self.client.quit().await;
    }
}

impl Store for ValkeyStore {
    async fn reserve(
        &self,
        key: &str,
        rate: f64,
        burst: f64,
        requested: f64,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<Reservation, StoreError> {
        // Floats are passed (and returned) as strings so Valkey doesn't truncate
        // them to integers.
        let args: Vec<String> = vec![
            format!("{rate}"),
            format!("{burst}"),
            format!("{requested}"),
            now_ms.to_string(),
            ttl_ms.to_string(),
        ];
        let (granted, remaining): (String, String) = self
            .reserve
            .evalsha(&self.client, vec![key], args)
            .await
            .map_err(|e| StoreError::Protocol(e.to_string()))?;
        Ok(Reservation {
            granted: granted.parse().unwrap_or(0.0),
            remaining: remaining.parse().unwrap_or(0.0),
        })
    }

    async fn gcra_check(
        &self,
        key: &str,
        rate: f64,
        burst: f64,
        cost: f64,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<GcraVerdict, StoreError> {
        let args: Vec<String> = vec![
            format!("{rate}"),
            format!("{burst}"),
            now_ms.to_string(),
            format!("{cost}"),
            ttl_ms.to_string(),
        ];
        let (allowed, retry_after): (i64, String) = self
            .gcra
            .evalsha(&self.client, vec![key], args)
            .await
            .map_err(|e| StoreError::Protocol(e.to_string()))?;
        Ok(GcraVerdict {
            allowed: allowed == 1,
            retry_after_ms: retry_after.parse::<f64>().unwrap_or(0.0).ceil().max(0.0) as u64,
        })
    }
}
