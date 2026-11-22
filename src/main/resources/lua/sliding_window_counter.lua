-- Sliding Window Counter, atomic version for Redis.
--
-- Time is cut into fixed windows. Each window has its own counter key, and the
-- estimate for "requests in the last window" blends the previous window's
-- count (weighted by how much of it still overlaps) with the current count.
--
-- KEYS[1] = counter for the CURRENT window   (e.g. "rl:swc:{user-1}:1234")
-- KEYS[2] = counter for the PREVIOUS window  (e.g. "rl:swc:{user-1}:1233")
-- ARGV[1] = limit (max requests per window)
-- ARGV[2] = window size in milliseconds
-- ARGV[3] = now, in milliseconds (passed in by the caller)
--
-- Returns 1 if allowed, 0 if rejected.

local limit  = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local now    = tonumber(ARGV[3])

-- A missing key means "no requests in that window": GET returns false, so default to 0.
local current  = tonumber(redis.call('GET', KEYS[1]) or '0')
local previous = tonumber(redis.call('GET', KEYS[2]) or '0')

-- How far into the current window are we? 0 = just started, window = about to end.
local elapsed = now % window
local overlap = 1.0 - (elapsed / window)

local estimate = previous * overlap + current

if estimate < limit then
  redis.call('INCR', KEYS[1])
  -- Keep this counter for 2 windows: one as "current", one more as "previous".
  redis.call('PEXPIRE', KEYS[1], window * 2)
  return 1
end

return 0
