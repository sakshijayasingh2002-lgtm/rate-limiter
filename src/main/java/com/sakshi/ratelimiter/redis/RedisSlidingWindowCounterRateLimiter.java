package com.sakshi.ratelimiter.redis;

import com.sakshi.ratelimiter.core.RateLimiter;
import com.sakshi.ratelimiter.core.TimeSource;
import redis.clients.jedis.JedisPool;

import java.util.List;

/**
 * Sliding Window Counter whose counters live in Redis.
 *
 * <h2>How the state is stored</h2>
 * One small counter key per fixed window, e.g. {@code rl:swc:{user-1}:1234}
 * where 1234 is the window number (now / windowSize). To decide a request,
 * the Lua script reads this window's counter and the previous window's
 * counter and blends them, exactly like the in-memory version. Counters
 * expire by themselves after two windows, so idle clients cost no memory.
 *
 * <h2>Why the {user-1} curly braces in the key</h2>
 * The script touches two keys (current + previous window). On a Redis
 * Cluster, a script's keys must live in the same hash slot, and Redis only
 * hashes the part inside {braces}. Wrapping the client id in braces forces
 * both window keys onto the same slot, so this works on a cluster too, not
 * just on a single Redis.
 *
 * <h2>Why the caller computes the window numbers</h2>
 * Redis scripts must be given every key they touch up front (in KEYS), so
 * Java works out "current window" and "previous window" and passes both.
 */
public class RedisSlidingWindowCounterRateLimiter implements RateLimiter {

    private final int limit;
    private final long windowSizeMillis;
    private final TimeSource timeSource;
    private final String keyPrefix;
    private final RedisScriptExecutor executor;

    public RedisSlidingWindowCounterRateLimiter(JedisPool pool, int limit, long windowSizeMillis,
                                                TimeSource timeSource, String keyPrefix) {
        this(pool, limit, windowSizeMillis, timeSource, keyPrefix, true);
    }

    public RedisSlidingWindowCounterRateLimiter(JedisPool pool, int limit, long windowSizeMillis,
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
        this.executor = new RedisScriptExecutor(pool, "/lua/sliding_window_counter.lua", failOpen);
    }

    @Override
    public boolean allowRequest(String key) {
        long now = timeSource.currentTimeMillis();
        long currentWindowId = now / windowSizeMillis;

        String base = keyPrefix + "{" + key + "}:";
        List<String> keys = List.of(base + currentWindowId, base + (currentWindowId - 1));
        List<String> args = List.of(
                String.valueOf(limit),
                String.valueOf(windowSizeMillis),
                String.valueOf(now));
        return executor.isAllowed(keys, args);
    }
}