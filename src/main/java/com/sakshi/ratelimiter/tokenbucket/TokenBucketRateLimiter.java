package com.sakshi.ratelimiter.tokenbucket;

import com.sakshi.ratelimiter.core.RateLimiter;
import com.sakshi.ratelimiter.core.TimeSource;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * In-memory Token Bucket rate limiter.
 *
 * <h2>How it works</h2>
 * Each key gets its own bucket that holds up to {@code capacity} tokens.
 * Tokens refill continuously at {@code refillTokensPerSecond}. Each allowed
 * request consumes one token. If the bucket is empty, the request is
 * rejected.
 *
 * <h2>Why "lazy" refill instead of a background thread</h2>
 * A naive implementation would run a scheduled task every N ms to add
 * tokens to every bucket. That doesn't scale — you'd be doing work for
 * buckets nobody is using, and it needs its own thread pool + shutdown
 * hooks. Instead, we compute "how many tokens would have accumulated since
 * last time we looked" only when a request actually arrives. Idle buckets
 * cost nothing. This is the standard approach used in real implementations
 * (e.g. Guava's RateLimiter, Stripe's public rate limiter design).
 *
 * <h2>Concurrency</h2>
 * State per key lives in a small mutable {@link Bucket} object. We
 * synchronize on that object during refill+consume so two threads hitting
 * the same key can't both read stale token counts and both get allowed
 * when only one should be. Different keys never contend with each other.
 */
public class TokenBucketRateLimiter implements RateLimiter {

    private final long capacity;
    private final double refillTokensPerSecond;
    private final TimeSource timeSource;
    private final ConcurrentMap<String, Bucket> buckets = new ConcurrentHashMap<>();

    /**
     * @param capacity              max tokens a bucket can hold (also the max burst size)
     * @param refillTokensPerSecond how many tokens are added back per second
     * @param timeSource            clock to use — pass a fake one in tests
     */
    public TokenBucketRateLimiter(long capacity, double refillTokensPerSecond, TimeSource timeSource) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        if (refillTokensPerSecond <= 0) {
            throw new IllegalArgumentException("refillTokensPerSecond must be > 0");
        }
        this.capacity = capacity;
        this.refillTokensPerSecond = refillTokensPerSecond;
        this.timeSource = timeSource;
    }

    @Override
    public boolean allowRequest(String key) {
        Bucket bucket = buckets.computeIfAbsent(key, k -> new Bucket(capacity, timeSource.currentTimeMillis()));

        synchronized (bucket) {
            refill(bucket);
            if (bucket.tokens >= 1.0) {
                bucket.tokens -= 1.0;
                return true;
            }
            return false;
        }
    }

    private void refill(Bucket bucket) {
        long now = timeSource.currentTimeMillis();
        long elapsedMillis = now - bucket.lastRefillTimestampMillis;
        if (elapsedMillis <= 0) {
            return;
        }
        double tokensToAdd = (elapsedMillis / 1000.0) * refillTokensPerSecond;
        bucket.tokens = Math.min(capacity, bucket.tokens + tokensToAdd);
        bucket.lastRefillTimestampMillis = now;
    }

    /** Mutable per-key state. Package-private, not thread-safe on its own — callers must synchronize. */
    private static final class Bucket {
        double tokens;
        long lastRefillTimestampMillis;

        Bucket(long initialTokens, long now) {
            this.tokens = initialTokens; // buckets start full, standard behavior
            this.lastRefillTimestampMillis = now;
        }
    }
}
