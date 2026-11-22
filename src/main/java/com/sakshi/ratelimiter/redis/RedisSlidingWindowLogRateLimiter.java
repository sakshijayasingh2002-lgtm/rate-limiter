package com.sakshi.ratelimiter.redis;

import com.sakshi.ratelimiter.core.RateLimiter;
import com.sakshi.ratelimiter.core.TimeSource;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.UUID;

/**
 * Sliding Window Log whose request log lives in a Redis sorted set.
 *
 * <h2>How the state is stored</h2>
 * One sorted set per client. Each request is an entry whose score is its
 * timestamp. The Lua script trims entries older than the window
 * (ZREMRANGEBYSCORE), counts what's left (ZCARD) and, if there's room, adds
 * the new request (ZADD). Redis keeps the set sorted by score for us, which
 * is the job the Deque did in the in-memory version.
 *
 * <h2>Why every request gets a unique id</h2>
 * A sorted set can't hold two identical members: adding the same member again
 * only updates its score. If we used the timestamp as the member, two
 * requests in the same millisecond would count as ONE and the limiter would
 * be too generous. So each request is stored as "timestamp:random-uuid".
 *
 * <h2>The cost</h2>
 * Same trade-off as in memory: one entry per request in the window, per
 * client. That's exact, but a busy client with a large window means a large
 * set in Redis. The Counter variant stores two numbers instead.
 */
public class RedisSlidingWindowLogRateLimiter implements RateLimiter {

    private final int limit;
    private final long windowSizeMillis;
    private final TimeSource timeSource;
    private final String keyPrefix;
    private final RedisScriptExecutor executor;

    public RedisSlidingWindowLogRateLimiter(JedisPool pool, int limit, long windowSizeMillis,
                                            TimeSource timeSource, String keyPrefix) {
        this(pool, limit, windowSizeMillis, timeSource, keyPrefix, true);
    }

    public RedisSlidingWindowLogRateLimiter(JedisPool pool, int limit, long windowSizeMillis,
                                            TimeSource timeSource, String keyPrefix, boolean failOpen) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be > 0");
        }
        if (windowSizeMillis <= 0) {
            throw new IllegalArgumentException("windowSizeMillis must be > 0");
        }
        this.limit = limit;
        this.windowSizeMillis = windowSizeMillis;
        this.timeSource = timeSource;
        this.keyPrefix = keyPrefix;
        this.executor = new RedisScriptExecutor(pool, "/lua/sliding_window_log.lua", failOpen);
    }

    @Override
    public boolean allowRequest(String key) {
        long now = timeSource.currentTimeMillis();

        List<String> keys = List.of(keyPrefix + key);
        List<String> args = List.of(
                String.valueOf(limit),
                String.valueOf(windowSizeMillis),
                String.valueOf(now),
                now + ":" + UUID.randomUUID());
        return executor.isAllowed(keys, args);
    }
}
