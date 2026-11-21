//! Per-sidecar cache of meter verdicts.
//!
//! A meter read (`mode=meter`, zero cost) runs on every request of a route, so
//! it must not round-trip the store each time. A verdict is fresh for one
//! lease interval: store reads are bounded by active keys per interval, like
//! lease refreshes, and a key that went over on another sidecar is seen here
//! within one interval.
//!
//! After that a verdict stays usable for a bounded stale window
//! (`meter_stale_ms`): a read inside it gets the cached verdict at once, and
//! the first such read of the key asks for one background refresh. Only a key
//! with no usable entry reads the store synchronously. An over verdict is
//! never served past the moment its score drains below `burst`.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::store::MeterVerdict;

struct Entry {
    verdict: MeterVerdict,
    stored: Instant,
    /// Fresh until here.
    expiry: Instant,
    /// Usable, but stale, until here.
    stale_until: Instant,
    /// A background refresh of this entry is queued or running.
    refresh_pending: bool,
}

/// A cached verdict, with whether it is past its fresh interval.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Cached {
    Fresh(MeterVerdict),
    /// Past its interval, inside the stale window. `refresh` is true for the
    /// one read that should queue the background refresh.
    Stale {
        verdict: MeterVerdict,
        refresh: bool,
    },
}

pub struct MeterCache {
    entries: Mutex<HashMap<String, Entry>>,
    max_keys: usize,
    stale: Duration,
}

impl MeterCache {
    pub fn new(max_keys: usize, stale: Duration) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            max_keys: max_keys.max(1),
            stale,
        }
    }

    /// The key's verdict, fresh or stale, or `None` when it has no usable
    /// entry. `over_ms` counts down from when the verdict was stored. The
    /// first stale read marks the entry's refresh pending and is told to
    /// queue it; later stale reads are not, until the refresh lands or
    /// [`MeterCache::refresh_done`] clears the mark.
    pub fn get(&self, key: &str, now: Instant) -> Option<Cached> {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let entry = entries
            .get_mut(key)
            .filter(|entry| now < entry.stale_until)?;
        let verdict = MeterVerdict {
            under: entry.verdict.under,
            over_ms: entry
                .verdict
                .over_ms
                .saturating_sub(now.duration_since(entry.stored).as_millis() as u64),
        };
        if now < entry.expiry {
            return Some(Cached::Fresh(verdict));
        }
        let refresh = !entry.refresh_pending;
        entry.refresh_pending = true;
        Some(Cached::Stale { verdict, refresh })
    }

    /// A background refresh ended without storing a verdict (store failure,
    /// dropped from a full queue): a later stale read may queue another.
    pub fn refresh_done(&self, key: &str) {
        if let Some(entry) = self
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(key)
        {
            entry.refresh_pending = false;
        }
    }

    /// Cache `verdict`: fresh for `ttl`, then stale for the stale window, but
    /// an over verdict never past the moment its score drains. At capacity,
    /// unusable entries are dropped first, then stale ones; if every entry is
    /// still fresh, the verdict is not cached and the next read asks the store.
    pub fn put(&self, key: &str, verdict: MeterVerdict, ttl: Duration, now: Instant) {
        let (expiry, stale_until) = if verdict.under {
            (now + ttl, now + ttl + self.stale)
        } else {
            let drained = now + Duration::from_millis(verdict.over_ms.max(1));
            (
                (now + ttl).min(drained),
                (now + ttl + self.stale).min(drained),
            )
        };
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        if entries.len() >= self.max_keys && !entries.contains_key(key) {
            entries.retain(|_, entry| now < entry.stale_until);
            if entries.len() >= self.max_keys {
                entries.retain(|_, entry| now < entry.expiry);
            }
            if entries.len() >= self.max_keys {
                return;
            }
        }
        entries.insert(
            key.to_string(),
            Entry {
                verdict,
                stored: now,
                expiry,
                stale_until,
                refresh_pending: false,
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const UNDER: MeterVerdict = MeterVerdict {
        under: true,
        over_ms: 0,
    };

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn a_verdict_is_fresh_until_its_ttl() {
        let cache = MeterCache::new(8, Duration::ZERO);
        let t0 = Instant::now();
        assert_eq!(cache.get("k", t0), None);
        cache.put("k", UNDER, ms(100), t0);
        assert_eq!(cache.get("k", t0 + ms(99)), Some(Cached::Fresh(UNDER)));
        assert_eq!(cache.get("k", t0 + ms(100)), None);
    }

    #[test]
    fn a_verdict_past_its_ttl_is_served_stale_and_asks_for_one_refresh() {
        let cache = MeterCache::new(8, ms(1000));
        let t0 = Instant::now();
        cache.put("k", UNDER, ms(100), t0);
        assert_eq!(
            cache.get("k", t0 + ms(150)),
            Some(Cached::Stale {
                verdict: UNDER,
                refresh: true
            })
        );
        // later stale reads don't queue a second refresh
        assert_eq!(
            cache.get("k", t0 + ms(160)),
            Some(Cached::Stale {
                verdict: UNDER,
                refresh: false
            })
        );
        // the stale window ends: no usable entry, the read is synchronous again
        assert_eq!(cache.get("k", t0 + ms(1100)), None);
    }

    #[test]
    fn a_refresh_replaces_the_stale_verdict() {
        let cache = MeterCache::new(8, ms(1000));
        let t0 = Instant::now();
        cache.put("k", UNDER, ms(100), t0);
        assert!(matches!(
            cache.get("k", t0 + ms(150)),
            Some(Cached::Stale { refresh: true, .. })
        ));
        let over = MeterVerdict {
            under: false,
            over_ms: 5000,
        };
        cache.put("k", over, ms(100), t0 + ms(160));
        assert_eq!(
            cache.get("k", t0 + ms(170)),
            Some(Cached::Fresh(MeterVerdict {
                under: false,
                over_ms: 4990,
            }))
        );
    }

    #[test]
    fn a_failed_refresh_lets_a_later_read_queue_another() {
        let cache = MeterCache::new(8, ms(1000));
        let t0 = Instant::now();
        cache.put("k", UNDER, ms(100), t0);
        assert!(matches!(
            cache.get("k", t0 + ms(150)),
            Some(Cached::Stale { refresh: true, .. })
        ));
        cache.refresh_done("k");
        assert!(matches!(
            cache.get("k", t0 + ms(160)),
            Some(Cached::Stale { refresh: true, .. })
        ));
    }

    #[test]
    fn an_over_verdict_expires_when_the_key_drains_even_inside_the_stale_window() {
        let cache = MeterCache::new(8, ms(60_000));
        let t0 = Instant::now();
        let over = MeterVerdict {
            under: false,
            over_ms: 20,
        };
        cache.put("k", over, ms(100), t0);
        assert_eq!(cache.get("k", t0), Some(Cached::Fresh(over)));
        assert_eq!(cache.get("k", t0 + ms(20)), None);
    }

    #[test]
    fn an_over_verdict_is_served_stale_only_until_it_drains() {
        let cache = MeterCache::new(8, ms(60_000));
        let t0 = Instant::now();
        let over = MeterVerdict {
            under: false,
            over_ms: 300,
        };
        cache.put("k", over, ms(100), t0);
        assert!(matches!(
            cache.get("k", t0 + ms(150)),
            Some(Cached::Stale {
                verdict: MeterVerdict {
                    under: false,
                    over_ms: 150
                },
                ..
            })
        ));
        assert_eq!(cache.get("k", t0 + ms(300)), None);
    }

    #[test]
    fn a_cached_over_verdict_counts_its_remaining_time_down() {
        let cache = MeterCache::new(8, Duration::ZERO);
        let t0 = Instant::now();
        let over = MeterVerdict {
            under: false,
            over_ms: 5000,
        };
        cache.put("k", over, ms(200), t0);
        let Some(Cached::Fresh(hit)) = cache.get("k", t0 + ms(150)) else {
            panic!("expected a fresh hit");
        };
        assert_eq!(hit.over_ms, 4850);
    }

    #[test]
    fn a_full_cache_drops_unusable_then_stale_entries_before_refusing_new_ones() {
        let cache = MeterCache::new(1, ms(100));
        let t0 = Instant::now();
        cache.put("a", UNDER, ms(10), t0);
        cache.put("b", UNDER, ms(10), t0);
        assert_eq!(cache.get("b", t0), None);
        // "a" is stale now; making room drops it
        let later = t0 + ms(10);
        cache.put("b", UNDER, ms(10), later);
        assert_eq!(cache.get("b", later), Some(Cached::Fresh(UNDER)));
        assert_eq!(cache.get("a", later), None);
    }
}
