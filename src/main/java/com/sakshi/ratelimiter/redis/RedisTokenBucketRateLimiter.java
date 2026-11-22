package com.sakshi.ratelimiter.redis;

import com.sakshi.ratelimiter.core.RateLimiter;
import com.sakshi.ratelimiter.core.TimeSource;
import redis.clients.jedis.JedisPool;

import java.util.List;

/**
 * Token Bucket rate limiter whose state lives in Redis, so many service
 * instances share one set of buckets.
 *
 * <p>The algorithm is identical to the in-memory version, but the refill +
 * spend logic runs inside Redis as a Lua script ({@code /lua/token_bucket.lua}).
 * Redis runs one script at a time, start to finish, so two instances can never
 * both read "1 token left" and both spend it. This class only builds the keys
 * and arguments; {@link RedisScriptExecutor} does the talking to Redis.
 */
public class RedisTokenBucketRateLimiter implements RateLimiter {

    private final long capacity;
    private final double refillTokensPerSecond;
    private final TimeSource timeSource;
    private final String keyPrefix;
    private final RedisScriptExecutor executor;

    public RedisTokenBucketRateLimiter(JedisPool pool, long capacity, double refillTokensPerSecond,
                                       TimeSource timeSource, String keyPrefix) {
        this(pool, capacity, refillTokensPerSecond, timeSource, keyPrefix, true);
    }

    /**
     * @param pool                  Redis connection pool
     * @param capacity              max tokens per bucket (also the max burst)
     * @param refillTokensPerSecond tokens added back per second
     * @param timeSource            clock, pass a fake one in tests
     * @param keyPrefix             prepended to every key, e.g. "rl:tb:"
     * @param failOpen              true: allow requests when Redis errors; false: reject them
     */
    public RedisTokenBucketRateLimiter(JedisPool pool, long capacity, double refillTokensPerSecond,
                                       TimeSource timeSource, String keyPrefix, boolean failOpen) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        if (refillTokensPerSecond <= 0) {
            throw new IllegalArgumentException("refillTokensPerSecond must be > 0");
        }
        this.capacity = capacity;
        this.refillTokensPerSecond = refillTokensPerSecond;
        this.timeSource = timeSource;
        this.keyPrefix = keyPrefix;
        this.executor = new RedisScriptExecutor(pool, "/lua/token_bucket.lua", failOpen);
    }

    @Override
    public boolean allowRequest(String key) {
        List<String> keys = List.of(keyPrefix + key);
        List<String> args = List.of(
                String.valueOf(capacity),
                String.valueOf(refillTokensPerSecond),
                String.valueOf(timeSource.currentTimeMillis()),
                "1");
        return executor.isAllowed(keys, args);
    }
}
