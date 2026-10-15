# haproxy-spoa-hub-plugin-rate-limit — development context

Shared, low-latency rate-limiting plugin for haproxy-spoa-hub. Rust cdylib
against `haproxy-spoa-hub-plugin-api` (the `define_plugin!` vtable), edition 2024.

## Design invariant (do not regress)

**Low latency is paramount: the shared store (Valkey) stays off the steady-state
hot path.** `plugin::process` → `lease::check` → `bucket::TokenBucket::try_acquire`
must remain in-memory only for admitted traffic. Valkey is a background budget
allocator (lease refresh timer + low-water prefetch) plus exactly two synchronous
paths: the opt-in `exact` mode, and the bounded, per-key-coalesced
**borrow-on-empty** escalation (v0.2.0) that runs ONLY when the local pool cannot
cover a request — i.e. for requests that would otherwise be *wrongly denied*
(an empty lease pool is not evidence the caller is over limit; `burst` lives in
the store). Never add store I/O to the allowed-request path, and keep borrows
coalesced + backoff-guarded so floods cannot turn into per-request store load.
Full design + overshoot bounds: `specs/001-ratelimit-plugin/design.md`.

## Layout

- `src/bucket.rs` — local lazy-refill token bucket (the hot path).
- `src/rule.rs` — per-request rule parsed from SPOE string args.
- `src/limiter.rs` — per-key bucket registry + the `Verdict` shape.
- `src/config.rs` — `[plugins.params]` parsing.
- `src/plugin.rs` — `define_plugin!` + `process()`; verdict → `txn.hub.rate_limit.*`.
- (Phase 2+) `src/lease.rs`, `src/store/`, `src/breaker.rs`, `src/negcache.rs`.

## Commands

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt -- --check
cargo build --release   # target/release/librate_limit_plugin.so
```

CI (`.github/workflows/ci.yml`) runs fmt + clippy (`-D warnings`) + test + acceptance + a release build.
Keep all four green.

## Conventions

- Plugin key is kebab (`rate-limit`); the `.so` is `librate_limit_plugin.so`; the
  hub var namespace is `rate_limit` (→ `txn.hub.rate_limit.*`). Keep these in sync
  with the chart's `prep-spoa-plugins.sh` `LIB_NAMES` and `haptic.spoaHub.libName`.
- `process(&self, ...)` is called concurrently — all state behind interior
  mutability (per-key locks / atomics), no `&mut self` on the hot path.
- Read SPOE args as strings and parse; don't depend on the hub's arg typing.
- Every non-trivial module carries unit tests (`#[cfg(test)]`).
