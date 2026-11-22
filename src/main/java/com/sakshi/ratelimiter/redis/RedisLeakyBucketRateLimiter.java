package com.sakshi.ratelimiter.redis;

import com.sakshi.ratelimiter.core.RateLimiter;
import com.sakshi.ratelimiter.core.TimeSource;
import redis.clients.jedis.JedisPool;

import java.util.List;

/**
 * Leaky Bucket (queue-style) whose state lives in Redis.
 *
 * <p>Same idea as the in-memory version: the bucket's "level" is the number
 * of requests waiting in the queue, it drains at a fixed rate, and a request
 * that would overflow is rejected. The drain-and-add logic runs as one Lua
 * script ({@code /lua/leaky_bucket.lua}), so many service instances can share
 * one bucket per client without two of them claiming the last slot.
 *
 * <p>State is a small hash per client with two fields: {@code level} (how full
 * the bucket is) and {@code ts} (when we last drained it). The key expires on
 * its own once the bucket would have fully drained, so idle clients cost
 * nothing.
 *
 * <p>{@link #acceptAndGetDelayMillis(String)} returns how long the request
 * would wait in the queue, for callers that want real traffic shaping.
 * {@link #allowRequest(String)} is the plain allow/reject view of the same thing.
 */
public class RedisLeakyBucketRateLimiter implements RateLimiter {

    private final long capacity;
    private final double leakRatePerSecond;
    private final TimeSource timeSource;
    private final String keyPrefix;
    private final RedisScriptExecutor executor;

    public RedisLeakyBucketRateLimiter(JedisPool pool, long capacity, double leakRatePerSecond,
                                       TimeSource timeSource, String keyPrefix) {
        this(pool, capacity, leakRatePerSecond, timeSource, keyPrefix, true);
    }

    /**
     * @param pool              Redis connection pool
     * @param capacity          max requests waiting in the bucket at once
     * @param leakRatePerSecond requests drained per second (the fixed output rate)
     * @param timeSource        clock, pass a fake one in tests
     * @param keyPrefix         prepended to every key, e.g. "rl:lb:"
     * @param failOpen          true: allow requests when Redis errors; false: reject them
     */
    public RedisLeakyBucketRateLimiter(JedisPool pool, long capacity, double leakRatePerSecond,
                                       TimeSource timeSource, String keyPrefix, boolean failOpen) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        if (leakRatePerSecond <= 0) {
            throw new IllegalArgumentException("leakRatePerSecond must be > 0");
        }
        this.capacity = capacity;
        this.leakRatePerSecond = leakRatePerSecond;
        this.timeSource = timeSource;
        this.keyPrefix = keyPrefix;
        this.executor = new RedisScriptExecutor(pool, "/lua/leaky_bucket.lua", failOpen);
    }

    @Override
    public boolean allowRequest(String key) {
        return acceptAndGetDelayMillis(key) >= 0;
    }

    /**
     * Try to put one request into the bucket.
     *
     * @return -1 if the bucket is full and the request is rejected; otherwise the number of
     *         milliseconds this request waits before it would be processed (0 = straight through).
     *         If Redis is unreachable: 0 when failing open, -1 when failing closed.
     */
    public long acceptAndGetDelayMillis(String key) {
        List<String> keys = List.of(keyPrefix + key);
        List<String> args = List.of(
                String.valueOf(capacity),
                String.valueOf(leakRatePerSecond),
                String.valueOf(timeSource.currentTimeMillis()));

        Long result = executor.evalLong(keys, args);
        if (result == null) {
            return executor.isFailOpen() ? 0 : -1;
        }
        return result;
    }
}
