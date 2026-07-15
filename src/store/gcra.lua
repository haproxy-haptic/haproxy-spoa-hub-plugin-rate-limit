-- Exact per-request rate limit via GCRA (Generic Cell Rate Algorithm), the
-- portable equivalent of redis-cell's CL.THROTTLE. Used only for `exact` mode
-- keys (money/contract limits) — it round-trips Valkey on EVERY request, so it
-- is never the fleet default.
--
-- The key stores a single "theoretical arrival time" (TAT). A request of `cost`
-- is allowed iff `now >= TAT + cost*emission_interval - burst_offset`. Exact,
-- with a configurable burst, one atomic op, self-expiring — no leasing/merging.
--
-- KEYS[1] = key
-- ARGV[1] = rate   (tokens per second, float)
-- ARGV[2] = burst  (max burst tokens, float)
-- ARGV[3] = now_ms (integer)
-- ARGV[4] = cost   (float)
-- ARGV[5] = ttl_ms (integer)
--
-- Returns {allowed (0|1), retry_after_ms (string)}.

local rate  = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local now   = tonumber(ARGV[3])
local cost  = tonumber(ARGV[4])
local ttl   = tonumber(ARGV[5])

if rate <= 0 then
  -- A zero rate can never admit (avoid division by zero).
  return { 0, tostring(ttl) }
end

local emission = 1000.0 / rate          -- ms per token
local burst_offset = emission * burst   -- how far ahead of `now` a burst may run

local tat = tonumber(redis.call('GET', KEYS[1]))
if tat == nil or tat < now then
  tat = now
end

local new_tat = tat + cost * emission
local allow_at = new_tat - burst_offset

if now < allow_at then
  -- Denied: how long until it would be allowed.
  return { 0, tostring(allow_at - now) }
else
  redis.call('SET', KEYS[1], tostring(new_tat), 'PX', ttl)
  return { 1, '0' }
end
