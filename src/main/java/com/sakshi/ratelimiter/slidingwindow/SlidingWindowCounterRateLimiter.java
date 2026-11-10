package com.sakshi.ratelimiter.slidingwindow;

import com.sakshi.ratelimiter.core.RateLimiter;
import com.sakshi.ratelimiter.core.TimeSource;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * In-memory Sliding Window Counter rate limiter.
 *
 * <h2>How it works</h2>
 * Time is divided into fixed-size buckets ("windows") of {@code windowSizeMillis}.
 * Each key tracks just two numbers: the request count in the *current*
 * window, and the count from the *previous* window. To estimate "how many
 * requests happened in the last windowSizeMillis", we take the current
 * count plus a weighted slice of the previous window's count — weighted by
 * how much of the previous window still overlaps the trailing look-back.
 *
 * <p>Formula: {@code estimate = previousCount * overlapFraction + currentCount}
 * where {@code overlapFraction} shrinks from 1.0 (just entered the new
 * window) to 0.0 (about to enter the next one).
 *
 * <h2>Why this instead of Sliding Window Log</h2>
 * Sliding Window Log stores one timestamp per request — memory scales with
 * traffic. This stores exactly two integers per key, always, regardless of
 * how many requests come in. That O(1) memory is why this is the variant
 * companies actually run at scale (e.g. this is the approach behind
 * Cloudflare's public rate limiter design). The cost: it's an
 * *approximation*, not exact — it assumes requests are evenly distributed
 * within each window, which is close enough in practice but can be
 * slightly generous or strict at the edges compared to the Log variant.
 *
 * <h2>Why not Fixed Window (no previous-window blending)</h2>
 * The naive version — just reset a counter every windowSizeMillis with no
 * blending — allows a burst of 2x the limit right at a window boundary
 * (limit requests at the very end of one window, then limit more at the
 * very start of the next, all within a much shorter real time span). The
 * previous-window weighting is exactly what fixes that boundary problem.
 */
public class SlidingWindowCounterRateLimiter implements RateLimiter {

    private final int limit;
    private final long windowSizeMillis;
    private final TimeSource timeSource;
    private final ConcurrentMap<String, WindowState> states = new ConcurrentHashMap<>();

    /**
     * @param limit            max requests allowed within the window
     * @param windowSizeMillis size of each fixed window, in milliseconds
     * @param timeSource       clock to use — pass a fake one in tests
     */
    public SlidingWindowCounterRateLimiter(int limit, long windowSizeMillis, TimeSource timeSource) {
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
        WindowState state = states.computeIfAbsent(key, k -> new WindowState());
        long now = timeSource.currentTimeMillis();
        long currentWindowId = now / windowSizeMillis;

        synchronized (state) {
            rollWindowIfNeeded(state, currentWindowId);

            long elapsedInCurrentWindow = now - (currentWindowId * windowSizeMillis);
            double overlapFraction = 1.0 - ((double) elapsedInCurrentWindow / windowSizeMillis);
            double estimate = (state.previousCount * overlapFraction) + state.currentCount;

            if (estimate < limit) {
                state.currentCount++;
                return true;
            }
            return false;
        }
    }

    private void rollWindowIfNeeded(WindowState state, long currentWindowId) {
        if (state.windowId == currentWindowId) {
            return; // still in the same window, nothing to roll
        }
        if (state.windowId == currentWindowId - 1) {
            // Rolled forward exactly one window — the count we had becomes "previous".
            state.previousCount = state.currentCount;
        } else {
            // We skipped one or more windows entirely (key was idle) — no
            // meaningful "previous" data exists, or the clock moved
            // backward past a window boundary. Either way, don't carry
            // stale/nonsensical counts forward.
            state.previousCount = 0;
        }
        state.currentCount = 0;
        state.windowId = currentWindowId;
    }

    /** Mutable per-key state. Not thread-safe on its own — callers must synchronize. */
    private static final class WindowState {
        long windowId = Long.MIN_VALUE; // sentinel: "never seen a request yet"
        int currentCount = 0;
        int previousCount = 0;
    }
}