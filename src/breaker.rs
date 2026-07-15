//! Circuit breaker around the shared store (Valkey).
//!
//! The breaker is consulted by the **background** lease-refresh path, never by
//! the per-request hot path. When Valkey starts failing it opens, so the lease
//! manager stops calling a down store and the local buckets keep enforcing
//! their last lease — this is the **fail-static** degradation. After a cooldown
//! it half-opens and lets a single probe through; success closes it, failure
//! re-opens it.
//!
//! Classic three states:
//! - **Closed** — healthy; lease refreshes proceed. Trips to Open after
//!   `failure_threshold` consecutive failures.
//! - **Open** — the store is skipped entirely until `cooldown` elapses.
//! - **HalfOpen** — exactly one probe is permitted; its outcome decides.

use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Closed,
    Open,
    HalfOpen,
}

struct Inner {
    state: State,
    consecutive_failures: u32,
    /// When Open, the instant at which we may transition to HalfOpen.
    open_until: Instant,
    /// Whether a HalfOpen probe is currently outstanding (only one allowed).
    probe_in_flight: bool,
}

pub struct CircuitBreaker {
    inner: Mutex<Inner>,
    failure_threshold: u32,
    cooldown: Duration,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, cooldown: Duration, now: Instant) -> Self {
        Self {
            inner: Mutex::new(Inner {
                state: State::Closed,
                consecutive_failures: 0,
                open_until: now,
                probe_in_flight: false,
            }),
            failure_threshold: failure_threshold.max(1),
            cooldown,
        }
    }

    /// Should the caller attempt a store operation now? `true` in Closed, and
    /// once per cooldown in Open (the HalfOpen probe). `false` while Open and
    /// within cooldown, or while a probe is already outstanding.
    pub fn allow(&self, now: Instant) -> bool {
        let mut s = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        match s.state {
            State::Closed => true,
            State::HalfOpen => {
                // Only one probe at a time.
                if s.probe_in_flight {
                    false
                } else {
                    s.probe_in_flight = true;
                    true
                }
            }
            State::Open => {
                if now >= s.open_until {
                    s.state = State::HalfOpen;
                    s.probe_in_flight = true;
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Record a successful store operation: reset failures and close.
    pub fn on_success(&self) {
        let mut s = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        s.state = State::Closed;
        s.consecutive_failures = 0;
        s.probe_in_flight = false;
    }

    /// Record a failed store operation. In Closed, trip to Open at the
    /// threshold. In HalfOpen (a failed probe), re-open immediately.
    pub fn on_failure(&self, now: Instant) {
        let mut s = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        s.probe_in_flight = false;
        match s.state {
            State::HalfOpen => {
                s.state = State::Open;
                s.open_until = now + self.cooldown;
            }
            State::Closed | State::Open => {
                s.consecutive_failures = s.consecutive_failures.saturating_add(1);
                if s.consecutive_failures >= self.failure_threshold {
                    s.state = State::Open;
                    s.open_until = now + self.cooldown;
                }
            }
        }
    }

    pub fn state(&self) -> State {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(base: Instant, ms: u64) -> Instant {
        base + Duration::from_millis(ms)
    }

    #[test]
    fn closed_allows_and_stays_closed_on_success() {
        let t0 = Instant::now();
        let cb = CircuitBreaker::new(3, Duration::from_secs(1), t0);
        assert!(cb.allow(t0));
        cb.on_success();
        assert_eq!(cb.state(), State::Closed);
    }

    #[test]
    fn opens_after_threshold_consecutive_failures() {
        let t0 = Instant::now();
        let cb = CircuitBreaker::new(3, Duration::from_secs(1), t0);
        cb.on_failure(t0);
        cb.on_failure(t0);
        assert_eq!(cb.state(), State::Closed); // 2 < 3
        cb.on_failure(t0);
        assert_eq!(cb.state(), State::Open); // 3rd trips it
        assert!(!cb.allow(t0)); // within cooldown → skip
    }

    #[test]
    fn success_resets_the_failure_count() {
        let t0 = Instant::now();
        let cb = CircuitBreaker::new(3, Duration::from_secs(1), t0);
        cb.on_failure(t0);
        cb.on_failure(t0);
        cb.on_success();
        cb.on_failure(t0);
        cb.on_failure(t0);
        assert_eq!(cb.state(), State::Closed); // count reset, only 2 since
    }

    #[test]
    fn half_opens_after_cooldown_then_closes_on_probe_success() {
        let t0 = Instant::now();
        let cb = CircuitBreaker::new(1, Duration::from_secs(1), t0);
        cb.on_failure(t0); // open
        assert_eq!(cb.state(), State::Open);
        assert!(!cb.allow(at(t0, 500))); // still cooling
        assert!(cb.allow(at(t0, 1000))); // cooldown elapsed → probe allowed
        assert_eq!(cb.state(), State::HalfOpen);
        assert!(!cb.allow(at(t0, 1000))); // second probe blocked (one at a time)
        cb.on_success();
        assert_eq!(cb.state(), State::Closed);
    }

    #[test]
    fn half_open_probe_failure_reopens() {
        let t0 = Instant::now();
        let cb = CircuitBreaker::new(1, Duration::from_secs(1), t0);
        cb.on_failure(t0);
        assert!(cb.allow(at(t0, 1000))); // half-open probe
        cb.on_failure(at(t0, 1000)); // probe fails
        assert_eq!(cb.state(), State::Open);
        assert!(!cb.allow(at(t0, 1500))); // cooldown restarts from the probe failure
        assert!(cb.allow(at(t0, 2000)));
    }
}
