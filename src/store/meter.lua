-- Meter: a per-key score that always records its charges, for `meter` mode.
--
-- The key stores a GCRA "theoretical arrival time" (TAT); `(TAT - now) /
-- emission` is the score still outstanding, and it drains at `rate`. Unlike
-- gcra.lua a charge is recorded even when it puts the key over `burst`, so the
-- key stays over until the excess has drained. A zero cost only reads.
--
-- KEYS[1] = key
-- ARGV[1] = rate  (score drained per second, float)
-- ARGV[2] = burst (score at which the key is over, float)
-- ARGV[3] = cost  (float, >= 0)
--
-- Returns {under (0|1), over_ms (string)}: whether the score is below `burst`,
-- and how long until it drains below when it is not.

local rate  = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local cost  = tonumber(ARGV[3])

if rate <= 0 then
  return { 1, '0' }
end

local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)

local emission = 1000.0 / rate
local burst_offset = emission * burst

local tat = tonumber(redis.call('GET', KEYS[1]))
if tat == nil or tat < now then
  tat = now
end

if cost > 0 then
  -- Twice `burst` bounds the debt, so one key is never over for longer than
  -- `burst / rate`.
  tat = math.min(tat + cost * emission, now + 2 * burst_offset)
  redis.call('SET', KEYS[1], tostring(tat), 'PX', math.ceil(tat - now) + 1)
end

local over = tat - now - burst_offset
if over >= 0 then
  return { 0, tostring(over) }
end
return { 1, '0' }
