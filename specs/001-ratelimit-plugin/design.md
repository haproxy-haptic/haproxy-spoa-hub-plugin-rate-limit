# Design: `haproxy-spoa-hub-plugin-rate-limit`

Shared, cross-fleet rate limiting + quotas for HAPTIC, as a new Rust SPOA plugin.
Status: **design for review** — no repo created yet. Phase 4 of the API-gateway plan.
**Design priority (product owner): low latency is paramount — keep the shared store off as many
request hot paths as possible.** Global limits are therefore *approximate but bounded*.

## 1. Why a plugin (not more HAProxy config)

The existing `haproxy-haptic.org/rate-limit-*` annotations
(`charts/haptic/charts/haptic-annotations/24-rate-limiting.yaml`) are **stick-table +
`peers localinstance`** — reload-surviving but **per-pod**: a client hitting N replicas gets N× its
budget. A true cluster-global limit needs shared state, which HAProxy config can't reach. So: a
spoa-hub plugin that enforces locally and reconciles to a shared store out-of-band. The per-pod
annotations **stay** as the plugin-free fallback; the plugin is opt-in for fleet-global limits.

## 2. Repo layout (modeled on `plugin-external-auth`)

Same shape as the existing plugins (spec-kit workflow, `define_plugin!` API):

```
haproxy-spoa-hub-plugin-rate-limit/
  Cargo.toml            # cdylib+rlib; edition 2024; LTO release
  src/lib.rs            # define_plugin!(RateLimitPlugin, {...}); init/process
  src/plugin.rs         # SPOE glue: SpoeMessage args -> Verdict -> ProcessingResult
  src/config.rs         # [plugins.params]: store addr, sync interval T, defaults, fail policy
  src/bucket.rs         # local lazy-refill token bucket (atomic; Envoy local_ratelimit design)
  src/lease.rs          # lease manager: time-based refresh timer + async low-water prefetch
  src/store/{mod,redis,memory}.rs   # Store trait; Valkey reservation (Lua/FUNCTION); in-proc
  src/breaker.rs        # circuit breaker + fail-static safe-capacity
  src/negcache.rs       # L1 over-limit cache (+ RESP3 CLIENT TRACKING invalidation)
  src/keyspec.rs        # opaque key builder from SPOE args
  benches/ tests/ tests/e2e/ examples/{haproxy.cfg,hub.toml,spoe-ratelimit.conf}
  Makefile CLAUDE.md README.md RELEASING.md renovate.json .gitlab-ci.yml
  specs/001-ratelimit-plugin/{spec,plan,data-model,research,quickstart,tasks}.md
```

**Cargo deps:** `haproxy-spoa-hub-plugin-api = "0.7"` (vtable), `fred` (async Valkey: pooling,
cluster, `EVALSHA`/`FUNCTION`, RESP3 `CLIENT TRACKING`), `governor`/hand-rolled atomic token bucket
(L1), Lua reservation script via `include_str!`, `log`. A small tokio runtime backs `fred`; the
per-request `process()` path is fully synchronous in-process (it never awaits the runtime).

## 3. SPOE interface (the chart contract)

Message **`check-ratelimit`**; the plugin writes a verdict to `txn.hub.rate_limit.*`.
Args HAPTIC sends (from per-app maps keyed on `txn.resource_id`): `key`, `limit`, `period`, `algo`,
`burst`, `cost`, `mode` (approximate|exact). Verdict written back:

| var | HAProxy use |
|-----|-------------|
| `txn.hub.rate_limit.allowed` (bool) | `http-request deny deny_status 429 unless {...allowed -m bool}` |
| `txn.hub.rate_limit.degraded` (bool) | dependency-failure logging and alerting |
| `txn.hub.rate_limit.accounted` (bool) | budget fields are present only for authoritative or local counter verdicts |
| `txn.hub.rate_limit.limit` / `.remaining` / `.reset` | `X-RateLimit-Limit`/`-Remaining`/`-Reset` |
| `txn.hub.rate_limit.retry_after` | `Retry-After` on 429 |

The dispatcher denies 429 + emits headers; a `frontend-spoe-set-ratelimit-headers-*` extension point
sets `X-RateLimit-*` on allowed responses too.

## 4. The engine — low-latency model

*(This section is SOTA-grounded: Envoy `local_ratelimit`, Google Doorman leases, Cloudflare
sliding-window, Kong `sync_rate` batching, Count-Min Sketch.)*

### Model: local-first, lease-amortized token bucket

Each sidecar is the authoritative enforcer for the requests it sees. It holds an in-process token
bucket per limit-key and decides allow/deny entirely in memory. Valkey holds the *authoritative
global* bucket per key and serves only as a **budget allocator**: the sidecar leases a slice of the
global rate on a time cadence and spends it locally.

- **Local tier (per request):** lazy-refill token bucket (Envoy `local_ratelimit` design — atomic
  process-shared counter, refill from elapsed **monotonic** time, no filler thread). Tokens are an
  additive count, so per-pod consumption reconciles by summation and a batch is naturally leasable.
- **Shared tier (Valkey):** a single-key atomic **Lua reservation** script. The global bucket is a
  `HASH {tokens, last_refill}`; the script refills by elapsed time, grants `min(requested,
  available)`, decrements, returns the lease + expiry. Single-key ⇒ Cluster-safe, shards by key.
  Allocation is **demand-weighted proportional share** (Doorman) so skewed load never starves a hot
  pod. Per-key TTL ≈ one refill period reclaims a crashed pod's lease.

Leasing is **time-based** (refresh every `T`, e.g. 100 ms–1 s), so **Valkey QPS ≈ (active_keys ×
N_pods)/T — independent of request RPS.** An **async low-water prefetch** tops up a hot key's lease
before it drains, so no request blocks on the reservation RTT.

### Per-request flow (the fast path never touches Valkey)

1. Derive the opaque limit-key from the descriptor.
2. **L1 negative cache:** key flagged over-limit (unexpired TTL) → reject now. Zero Valkey.
3. Lazy-refill the local bucket from elapsed time against the leased rate.
4. **Fast path:** tokens ≥ cost → atomic decrement, **allow**. No network I/O. *(the common case)*
5. If the lease crossed low-water, fire an async lease refresh; serve this request from remaining tokens.
6. Lease drained → reject + stamp L1 negative cache (short TTL; step 2 absorbs the burst). *(exact
   mode instead: one synchronous Valkey GCRA op — see below.)*

Valkey is touched **only** by: the lease-refresh timer, low-water prefetch, cold-key first sight,
`CLIENT TRACKING` invalidation pushes, and exact-mode keys. Every allowed-under-budget request and
every already-throttled-key rejection is served with **zero Valkey contact.**

### Bounded overshoot (the deliberate trade)

Two additive terms — kept separate in the docs:

- **Sync-lag / burst:** global admission ≤ `limit + N·B`, where N = sidecars holding the key's lease
  and B = per-pod lease size — equivalently `limit + N·r_pod·T` over one interval. **Sustained rate
  stays ≤ limit** because Valkey's refill is authoritative (a pod can only spend tokens the shared
  bucket issued). *Kong measured <0.5% real-world error at B=10; Doorman ~102% avg / ~106% peak
  during rebalancing.*
- **Capacity-shrink transient:** lowering a limit while leases are outstanding admits ≤ `(old−new)`
  allocation for ≤ one lease/TTL, then converges.

Single tuning dial: **B (or T)** — halving it halves the overshoot at ~2× Valkey QPS. Static-partition
(`limit/N` per pod) has zero breach but under-utilizes on skew, which is why the default leases
demand-weighted rather than dividing by N.

### Exact mode (opt-in, per key — for money/contract limits)

`rate-limit-mode: exact` sets `B=1` and enforces synchronously: one **GCRA** check per request as a
Valkey **FUNCTION** (portable `CL.THROTTLE` equivalent; avoids loadable C modules managed Valkey
often forbids). Cost: a ~0.3 ms Valkey RTT on **every** request to that key — the exact latency the
default eliminates — so exact mode is scoped to the specific keys that need it, never fleet-wide.
Store errors, timeouts, and an open breaker resolve through `store_failure_mode`.

### L1 negative cache + circuit breaker

- **L1 negative cache** holds only over-limit/blocked keys (Envoy over-limit + Cloudflare mitigation
  cache). First check on every request; short-circuits abusers with no Valkey hit. RESP3 `CLIENT
  TRACKING` (BCAST) push-invalidates `cfg:`/`block:` prefixes — server-pushed, not per-request GET.
  Never cache the live counter — only slow-changing config and window-stable verdicts.
- **Circuit breaker** wraps every Valkey call. On outage it opens and lease mode keeps enforcing its
  last local grant. An empty lease pool or exact check then uses `store_failure_mode`: `local`
  enforces a bounded per-sidecar emergency token bucket, `open` allows, and `closed` denies. The
  emergency registry never evicts active debt; at capacity, `local` allows untracked traffic.
  **No request path starts depending on Valkey when the breaker is open.**

The emergency bucket is shared by lease and exact checks for one opaque key. It starts full on the
first failed store decision, refills monotonically at the configured rate, and is not reset when the
store recovers. Over an outage of duration `D`, one sidecar can admit at most `burst + rate * D`
through this fallback; the fleet can admit that amount per sidecar, so exact mode is not fleet-exact
while degraded. Lease mode can additionally spend grants obtained before the outage. Emergency
state has its own `max_keys` bound alongside the lease registry and removes only idle, fully-refilled
entries. If that bound prevents local accounting, `local` admits and reports degraded rather than
turning state pressure into an outage.

An explicit store failure policy disables the legacy optimistic cold lease. A new key must first
obtain a store grant or resolve through that policy; already-issued lease tokens remain spendable.
Omitting the new policy keeps the v0.2 cold-start behavior for compatibility.

### High-cardinality keyspaces (per-IP / per-API-key, millions of keys)

Put a **Count-Min Sketch** under the counter: linear (per-pod sketches merge into Valkey by
summation), one-sided over-count error (`≤ eps·N`) → can only reject early, never breach. Lease only
the hot keys a pod sees; the cold tail gets optimistic-allow + async sketch increment. Use exact
counters when key count is small. **Avoid on the hot path:** global-RLS-per-request (Envoy topology)
and `redis-cell`/GCRA as the per-request primitive (both round-trip on every *allowed* request);
avoid sliding-window-log entirely (per-request write, O(n) memory, un-mergeable).

## 5. Annotations (extend the existing family)

New (shared, via plugin): `rate-limit-key` (`ip` | `consumer` | `header:<h>` | `jwt:<claim>` |
composite), `rate-limit-requests`, `rate-limit-period` (reuse), `rate-limit-algorithm`,
`rate-limit-burst`, **`rate-limit-mode`** (`approximate` default | `exact`). Quotas (fixed-window,
long periods): `quota-limit`, `quota-period` (`1h`/`1d`/`30d`), `quota-key`. Fleet tuning (chart
values, not per-route): lease interval `T`, per-pod batch `B`. The existing per-pod keys
(`rate-limit-rps/rpm/connections/…`) **stay** as the plugin-free fallback; `rate-limit-requests` +
plugin-enabled selects the shared path. `rate-limit-key = consumer` reuses `txn.haptic_consumer`
from the merged JWT/API-key work.

## 6. Chart integration (files to touch — from the wiring trace)

- **Build/bundle:** `versions-spoa.env` (+`SPOA_PLUGIN_RATE_LIMIT_VERSION`), `prep-spoa-plugins.sh`
  (`PLUGINS`+`LIB_NAMES`: `librate_limit_plugin`), `render-spoa-bundle.sh` (table row).
- **Values:** `spoaHub.plugins.rate-limit` — gate on an explicit `controller.rateLimit.shared.enabled`
  (native capability; don't force the sidecar onto every default install, same reasoning that kept
  hapticAnnotations out of the coraza OR). `messages:[check-ratelimit]`, `params` (store addr, T, B,
  fail policy).
- **Generic loops unchanged** (config-content `[[plugins]]` loop + haproxytemplateconfig extraContext).
- **SPOE wiring:** `spoa-hub/20-config-content.yaml` add `else if msg=="check-ratelimit"` +
  `spoe-message-check-ratelimit-body`; `spoa-hub/30-frontend-filters.yaml` add
  `frontend-spoe-filters-NNN-rate-limit`; `spoa-hub/_index.yaml` add `map-ratelimit-*`.
- **Native annotations:** extend `24-rate-limiting.yaml` with `map-ratelimit-*` emitters keyed by
  `<ns>/<name>` + **fail-loud** when a `rate-limit-requests` route has the plugin disabled.
  `90-migration-coverage.yaml` entries per new key.
- **Docs/tests:** `operations/spoa-hub.md` bullet, `haptic-annotations.md` rows,
  `spoa-hub/tests/*.yaml` + `haptic-annotations/tests/` validationTests, e2e.

## 7. Datastore deployment

Opt-in, same `k8sResources` mechanism as the Varnish tier: `controller.rateLimit.store` deploys a
**Valkey** (Redis-compatible, permissively licensed) Deployment + Service, gated on
`.enabled`; or **BYO** via a configured address. Because Valkey is only a budget allocator hit at
`~pods/T`, a single instance easily suffices; HA via Valkey Sentinel/Cluster is a follow-up. The
circuit breaker's fail-static keeps enforcement running through a store blip.

## 8. Verification

- **Rust unit:** local token bucket (lazy refill, boundary, burst, cost>1); lease manager
  (proportional-share, low-water prefetch, TTL reclaim); breaker (open/half-open/fail-static);
  negcache TTL + push-invalidation.
- **Rust integration (testcontainers Valkey):** Lua reservation atomicity under concurrency;
  measured **overshoot ≤ N·B** across simulated pods; QPS to Valkey ≈ pods/T independent of RPS.
- **e2e (the cross-instance win):** kind, Valkey + hub + **2 HAProxy pods**; a burst across both pods
  trips **one shared** budget (proves fleet-global vs per-pod stick-table); overshoot within the
  documented bound; quota exhaustion over a long window; `X-RateLimit-*`/`Retry-After` present;
  **kill Valkey → breaker opens, requests keep flowing at fail-static capacity** (latency unchanged);
  exact-mode key hard-denies at the exact limit.

## 9. Risk register

| Risk | Mitigation |
|------|-----------|
| Skewed load → false rejects on a hot pod under `limit/N` | demand-weighted proportional-share leasing, not divide-by-N |
| Crashed/idle pod strands its lease → under-utilization | short per-key TTL (≈ one refresh) + lease-return on graceful shutdown; smaller B |
| Overshoot scales with N; scale-up/synchronized deploy spikes N·B | cap B; source pod_count from a maintained value; jitter lease sizes |
| Synchronized lease-expiry thundering herd on a hot key | jitter T per pod/key |
| Stale pod_count corrupts fail-static safe-capacity during a scale event | conservative pod_count source; bias safe-capacity low |
| Clock skew corrupts window-aligned accounting | monotonic elapsed time for refill; server-authoritative window timestamps |
| High-cardinality blows sidecar/Valkey memory | lease only hot keys seen; CMS + optimistic-allow for the cold tail; approx top-k |
| Store outage posture differs by operator | `store_failure_mode` selects local, open, or closed independently of algorithm |
| L1 neg-cache TTL too long keeps rejecting after abuse stops | TTL ≈ one sync interval, refreshed by CLIENT TRACKING push |

## 10. Phasing

1. Repo scaffold + local token bucket + `InMemoryStore` + SPOE round-trip green (hub e2e, no store).
2. Valkey Lua reservation + time-based leasing + low-water prefetch + L1 negcache + circuit breaker
   (fail-static). This is the low-latency core.
3. Chart wiring + native annotations + Valkey `k8sResources` + **cross-pod overshoot e2e**.
4. Quotas + composite keys + `X-RateLimit-*` headers + exact mode (GCRA FUNCTION).
5. High-cardinality CMS substrate (only if per-IP scale demands it). Then transform + validate
   plugins = Phase 5.

## Open decisions for you

1. **Default lease interval `T` and per-pod batch `B`** — sets the overshoot bound `N·B` vs Valkey QPS
   `pods/T`. Proposed: `T=200ms`, `B` demand-weighted (start ~ `limit·T/period` per pod). OK?
2. **Datastore: Valkey** (recommended, license-clean) vs Redis vs allow Memcached (fixed-window only).
3. **Enable gate:** explicit `controller.rateLimit.shared.enabled` (recommended) vs auto-on when a
   `rate-limit-requests` route exists.
4. **Exact mode in v1** or defer to a later phase (approximate-only first)?
5. **High-cardinality CMS**: build in v1 or defer until per-IP scale needs it (start with exact
   per-key counters)?
