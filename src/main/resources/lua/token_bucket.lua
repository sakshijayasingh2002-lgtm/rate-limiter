-- Token Bucket, atomic version for Redis.
--
-- KEYS[1] = the bucket's key, e.g. "rl:tb:user-1"
-- ARGV[1] = capacity (max tokens)
-- ARGV[2] = refill rate (tokens per second)
-- ARGV[3] = now, in milliseconds (passed in by the caller)
-- ARGV[4] = tokens requested (normally 1)
--
-- Returns 1 if the request is allowed, 0 if it is rejected.

local capacity  = tonumber(ARGV[1])
local rate      = tonumber(ARGV[2])
local now       = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

-- 1. Read the bucket's saved state (both fields in one call)
local data   = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts     = tonumber(data[2])

-- 2. First time we see this key: start with a full bucket
if tokens == nil then
  tokens = capacity
  ts = now
end

-- 3. Lazy refill, same idea as the Java version.
--    If elapsed <= 0 (no time passed, or the clock went backwards) we do nothing.
local elapsed = now - ts
if elapsed > 0 then
  tokens = math.min(capacity, tokens + (elapsed / 1000.0) * rate)
  ts = now
end

-- 4. Spend a token if there is one
local allowed = 0
if tokens >= requested then
  tokens = tokens - requested
  allowed = 1
end

-- 5. Save state back
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', ts)

-- 6. Idle buckets clean themselves up: expire after the time a full refill takes, plus 1s
redis.call('EXPIRE', KEYS[1], math.ceil(capacity / rate) + 1)

return allowed
