-- Atomic lease reservation against the global token bucket for one key.
--
-- The global bucket is a HASH {t = tokens, ts = last_refill_ms}. This script
-- refills it by elapsed time, grants min(requested, available) tokens to the
-- calling sidecar, decrements the global tokens, and self-expires the key. It
-- runs atomically in Valkey (single-threaded), so concurrent reservations from
-- the whole fleet can never over-grant beyond `burst + rate*elapsed`.
--
-- KEYS[1] = bucket key
-- ARGV[1] = rate   (tokens per second, float)
-- ARGV[2] = burst  (capacity, float)
-- ARGV[3] = requested (tokens to reserve, float)
-- ARGV[4] = now_ms (legacy caller clock, ignored; Valkey TIME is authoritative)
-- ARGV[5] = ttl_ms (integer, key expiry ~ one refill period)
--
-- Returns {granted, remaining} as strings (Redis truncates Lua numbers to
-- integers, so floats are returned as strings and parsed by the caller).

local rate      = tonumber(ARGV[1])
local burst     = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
local now_arg   = tonumber(ARGV[4])
local ttl       = tonumber(ARGV[5])

local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
if now == nil then now = now_arg end

local data   = redis.call('HMGET', KEYS[1], 't', 'ts')
local tokens = tonumber(data[1])
local ts     = tonumber(data[2])

if tokens == nil or ts == nil then
  tokens = burst
  ts = now
end

-- Refill by elapsed time (guard against a non-monotonic caller clock), capped
-- at burst.
local elapsed = now - ts
if elapsed < 0 then elapsed = 0 end
tokens = math.min(burst, tokens + (elapsed / 1000.0) * rate)

-- Grant.
local granted = math.min(requested, tokens)
if granted < 0 then granted = 0 end
tokens = tokens - granted

-- Persist + self-expire so a crashed pod's unspent global state ages out.
redis.call('HSET', KEYS[1], 't', tostring(tokens), 'ts', tostring(now))
if ttl > 0 then
  redis.call('PEXPIRE', KEYS[1], ttl)
end

return { tostring(granted), tostring(tokens) }
