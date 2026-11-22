-- Sliding Window Log, atomic version for Redis.
--
-- A sorted set holds one entry per request in the window; the score is the
-- request's timestamp. Old entries are trimmed, the rest are counted.
--
-- KEYS[1] = the sorted set for this client (e.g. "rl:swl:user-1")
-- ARGV[1] = limit (max requests in the window)
-- ARGV[2] = window size in milliseconds
-- ARGV[3] = now, in milliseconds (passed in by the caller)
-- ARGV[4] = a unique id for this request (sorted-set members must be unique,
--           otherwise two requests in the same millisecond would overwrite each other)
--
-- Returns 1 if allowed, 0 if rejected.

local limit  = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local now    = tonumber(ARGV[3])

local windowStart = now - window

-- 1. Drop every entry at or before the start of the trailing window
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', windowStart)

-- 2. Count what's left
local count = redis.call('ZCARD', KEYS[1])

if count < limit then
  redis.call('ZADD', KEYS[1], now, ARGV[4])
  -- The newest entry leaves the window after `window` ms, so the whole key can expire then.
  redis.call('PEXPIRE', KEYS[1], window)
  return 1
end

return 0
