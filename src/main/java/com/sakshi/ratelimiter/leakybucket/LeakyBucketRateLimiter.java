package com.sakshi.ratelimiter.leakybucket;

import com.sakshi.ratelimiter.core.RateLimiter;
import com.sakshi.ratelimiter.core.TimeSource;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * In-memory Leaky Bucket rate limiter (queue-style).
 *
 * <h2>How it works</h2>
 * Picture a bucket with a small hole. Requests are water poured in, and the
 * water drains out the hole at a constant rate ({@code leakRatePerSecond}).
 * The bucket holds at most {@code capacity} units. If a new request would
 * overflow it, the request is rejected.
 *
 * <p>In practice the "water level" is the number of requests waiting in a
 * queue to be processed. The queue is served at a fixed pace no matter how
 * bursty the input is, so the output is always smooth.
 *
 * <h2>How it differs from Token Bucket</h2>
 * Token Bucket lets a burst through immediately (up to capacity), then
 * throttles. Leaky Bucket never lets a burst through: it accepts it into
 * the queue (if there's room) but serves it at the fixed rate. That is why
 * {@link #acceptAndGetDelayMillis(String)} exists: it tells the caller how
 * long this request would have to wait in the queue. A caller that wants
 * true traffic shaping sleeps for that long. A caller that only wants
 * allow/reject uses {@link #allowRequest(String)}.
 *
 * <h2>Lazy leak, same idea as lazy refill</h2>
 * We don't run a background thread draining buckets. We compute how much
 * would have leaked since the last request only when a request arrives.
 */
public class LeakyBucketRateLimiter implements RateLimiter {

    private final long capacity;
    private final double leakRatePerSecond;
    private final TimeSource timeSource;
    private final ConcurrentMap<String, Bucket> buckets = new ConcurrentHashMap<>();

    /**
     * @param capacity          max requests that can be waiting in the bucket at once
     * @param leakRatePerSecond how many requests drain out per second (the fixed output rate)
     * @param timeSource        clock to use, pass a fake one in tests
     */
    public LeakyBucketRateLimiter(long capacity, double leakRatePerSecond, TimeSource timeSource) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        if (leakRatePerSecond <= 0) {
            throw new IllegalArgumentException("leakRatePerSecond must be > 0");
        }
        this.capacity = capacity;
        this.leakRatePerSecond = leakRatePerSecond;
        this.timeSource = timeSource;
    }

    @Override
    public boolean allowRequest(String key) {
        return acceptAndGetDelayMillis(key) >= 0;
    }

    /**
     * Try to put one request into the bucket.
     *
     * @return -1 if the bucket is full and the request is rejected; otherwise the number of
     *         milliseconds this request must wait before it would be processed (0 if the
     *         bucket was empty and it can go straight through)
     */
    public long acceptAndGetDelayMillis(String key) {
        Bucket bucket = buckets.computeIfAbsent(key, k -> new Bucket(timeSource.currentTimeMillis()));

        synchronized (bucket) {
            leak(bucket);

            if (bucket.level + 1.0 > capacity) {
                return -1;
            }

            // Everything already in the bucket is ahead of us in line.
            long delayMillis = (long) Math.ceil((bucket.level / leakRatePerSecond) * 1000.0);
            bucket.level += 1.0;
            return delayMillis;
        }
    }

    private void leak(Bucket bucket) {
        long now = timeSource.currentTimeMillis();
        long elapsedMillis = now - bucket.lastLeakTimestampMillis;
        if (elapsedMillis <= 0) {
            return; // no time passed, or the clock went backwards: do nothing
        }
        double leaked = (elapsedMillis / 1000.0) * leakRatePerSecond;
        bucket.level = Math.max(0.0, bucket.level - leaked);
        bucket.lastLeakTimestampMillis = now;
    }

    /** Mutable per-key state. Not thread-safe on its own, callers must synchronize. */
    private static final class Bucket {
        double level = 0.0; // buckets start empty
        long lastLeakTimestampMillis;

        Bucket(long now) {
            this.lastLeakTimestampMillis = now;
        }
    }
}