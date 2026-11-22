-- Leaky Bucket (queue-style), atomic version for Redis.
--
-- The bucket's "level" is how many requests are waiting in the queue. It
-- drains at a constant rate. A new request is rejected if it would overflow.
--
-- KEYS[1] = the bucket's key, e.g. "rl:lb:user-1"
-- ARGV[1] = capacity (max requests waiting at once)
-- ARGV[2] = leak rate (requests drained per second)
-- ARGV[3] = now, in milliseconds (passed in by the caller)
--
-- Returns -1 if the request is rejected (bucket full), otherwise the number
-- of milliseconds this request has to wait in the queue (0 = goes straight through).

local capacity = tonumber(ARGV[1])
local rate     = tonumber(ARGV[2])
local now      = tonumber(ARGV[3])

-- 1. Read saved state
local data  = redis.call('HMGET', KEYS[1], 'level', 'ts')
local level = tonumber(data[1])
local ts    = tonumber(data[2])

-- 2. First time we see this key: start with an empty bucket
if level == nil then
  level = 0
  ts = now
end

-- 3. Lazy leak. If elapsed <= 0 (no time passed, or the clock went backwards) do nothing.
--    Never let the level go below empty.
local elapsed = now - ts
if elapsed > 0 then
  level = math.max(0, level - (elapsed / 1000.0) * rate)
  ts = now
end

local ttl = math.ceil(capacity / rate) + 1

-- 4. Full? Reject (but still save the drained level and timestamp).
if level + 1 > capacity then
  redis.call('HSET', KEYS[1], 'level', level, 'ts', ts)
  redis.call('EXPIRE', KEYS[1], ttl)
  return -1
end

-- 5. Accept: everything already in the bucket is ahead of us in line.
local delay = math.ceil((level / rate) * 1000.0)
level = level + 1

redis.call('HSET', KEYS[1], 'level', level, 'ts', ts)
redis.call('EXPIRE', KEYS[1], ttl)

return delay
