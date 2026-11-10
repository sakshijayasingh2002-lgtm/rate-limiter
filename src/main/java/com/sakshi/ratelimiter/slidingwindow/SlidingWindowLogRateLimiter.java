package com.sakshi.ratelimiter.slidingwindow;

import com.sakshi.ratelimiter.core.RateLimiter;
import com.sakshi.ratelimiter.core.TimeSource;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * In-memory Sliding Window Log rate limiter.
 *
 * <h2>How it works</h2>
 * For each key, we keep the exact timestamp of every request that's still
 * inside the trailing window (e.g. the last 60 seconds). On a new request:
 * 1. Drop all stored timestamps older than {@code now - windowSizeMillis}
 * 2. If what's left is under the limit, record `now` and allow
 * 3. Otherwise reject
 *
 * <h2>Why this exists alongside Token Bucket</h2>
 * Token Bucket allows bursts up to its capacity, then smooths out — it
 * doesn't care exactly *when* within a second those requests landed.
 * Sliding Window Log gives an exact count of "how many requests in the last
 * N seconds," with no boundary artifacts. It's the most accurate algorithm
 * of the three we're building, at the cost of memory (one timestamp stored
 * per request in the window, per key).
 *
 * <h2>Why "Log" and not just "Counter"</h2>
 * This variant keeps every individual timestamp, which is exact but doesn't
 * scale well — a high-traffic key with a big window can store thousands of
 * timestamps. {@link SlidingWindowCounterRateLimiter} trades a small amount
 * of accuracy for O(1) memory per key. Both are implemented here so the
 * trade-off can actually be demonstrated, not just described.
 */
public class SlidingWindowLogRateLimiter implements RateLimiter {

    private final int limit;
    private final long windowSizeMillis;
    private final TimeSource timeSource;
    private final ConcurrentMap<String, Deque<Long>> requestLogs = new ConcurrentHashMap<>();

    /**
     * @param limit            max requests allowed within the window
     * @param windowSizeMillis size of the trailing window, in milliseconds
     * @param timeSource       clock to use — pass a fake one in tests
     */
    public SlidingWindowLogRateLimiter(int limit, long windowSizeMillis, TimeSource timeSource) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be > 0");
        }
        if (windowSizeMillis <= 0) {
            throw new IllegalArgumentException("windowSizeMillis must be > 0");
        }
        this.limit = limit;
        this.windowSizeMillis = windowSizeMillis;
        this.timeSource = timeSource;
    }

    @Override
    public boolean allowRequest(String key) {
        Deque<Long> log = requestLogs.computeIfAbsent(key, k -> new ArrayDeque<>());

        synchronized (log) {
            long now = timeSource.currentTimeMillis();
            long windowStart = now - windowSizeMillis;

            // Evict anything that's fallen out of the trailing window.
            // Timestamps are added in increasing order, so the oldest is
            // always at the front — no need to scan the whole deque.
            while (!log.isEmpty() && log.peekFirst() <= windowStart) {
                log.pollFirst();
            }

            if (log.size() < limit) {
                log.addLast(now);
                return true;
            }
            return false;
        }
    }
}