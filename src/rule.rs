//! The per-request rate-limit rule, parsed from the SPOE message args.
//!
//! HAPTIC resolves the opaque limit key and the numeric rule chart-side (from
//! annotations + per-app maps) and ships them as SPOE args. The plugin reads
//! every arg as a string and parses it, so it doesn't depend on the SPOE arg
//! typing the hub happens to emit.

/// A rate-limit rule for one request.
#[derive(Debug, Clone, PartialEq)]
pub struct Rule {
    /// Requests allowed per `period_s`.
    pub limit: f64,
    /// Window length in seconds.
    pub period_s: f64,
    /// Bucket capacity (max burst). Defaults to `limit` when unset.
    pub burst: f64,
    /// Weight of this request (default 1).
    pub cost: f64,
}

impl Rule {
    /// Sustained token refill rate (tokens per second).
    pub fn rate(&self) -> f64 {
        if self.period_s > 0.0 {
            self.limit / self.period_s
        } else {
            0.0
        }
    }

    /// Parse a rule from `(limit, period, burst, cost)` string args. Returns
    /// `None` when a required field (`limit`, `period`) is missing or invalid,
    /// so the caller can fail open rather than enforce a nonsensical rule.
    pub fn parse(
        limit: Option<&str>,
        period: Option<&str>,
        burst: Option<&str>,
        cost: Option<&str>,
    ) -> Option<Rule> {
        let limit = parse_pos_f64(limit?)?;
        let period_s = parse_pos_f64(period?)?;
        let burst = burst
            .and_then(parse_pos_f64)
            .filter(|b| *b > 0.0)
            .unwrap_or(limit);
        let cost = cost
            .and_then(|s| s.parse::<f64>().ok())
            .filter(|c| *c > 0.0)
            .unwrap_or(1.0);
        Some(Rule {
            limit,
            period_s,
            burst,
            cost,
        })
    }
}

/// Parse a strictly-positive, finite f64.
fn parse_pos_f64(s: &str) -> Option<f64> {
    s.trim()
        .parse::<f64>()
        .ok()
        .filter(|v| v.is_finite() && *v > 0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_rule() {
        let r = Rule::parse(Some("100"), Some("60"), Some("120"), Some("2")).unwrap();
        assert_eq!(
            r,
            Rule {
                limit: 100.0,
                period_s: 60.0,
                burst: 120.0,
                cost: 2.0
            }
        );
        assert!((r.rate() - 100.0 / 60.0).abs() < 1e-9);
    }

    #[test]
    fn burst_defaults_to_limit_and_cost_to_one() {
        let r = Rule::parse(Some("10"), Some("1"), None, None).unwrap();
        assert_eq!(r.burst, 10.0);
        assert_eq!(r.cost, 1.0);
    }

    #[test]
    fn missing_or_invalid_required_fields_yield_none() {
        assert!(Rule::parse(None, Some("1"), None, None).is_none());
        assert!(Rule::parse(Some("10"), None, None, None).is_none());
        assert!(Rule::parse(Some("0"), Some("1"), None, None).is_none());
        assert!(Rule::parse(Some("-5"), Some("1"), None, None).is_none());
        assert!(Rule::parse(Some("nan"), Some("1"), None, None).is_none());
        assert!(Rule::parse(Some("abc"), Some("1"), None, None).is_none());
    }
}
