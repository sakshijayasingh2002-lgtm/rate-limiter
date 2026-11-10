package com.sakshi.ratelimiter.slidingwindow;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SlidingWindowCounterRateLimiterTest {

    private FakeTimeSource clock;

    @BeforeEach
    void setUp() {
        clock = new FakeTimeSource(0L);
    }

    @Test
    void allowsUpToLimitWithinFirstWindow() {
        // limit 4 per 1000ms window
        SlidingWindowCounterRateLimiter limiter = new SlidingWindowCounterRateLimiter(4, 1000, clock);

        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1"));
    }

    @Test
    void smoothsOutTheWindowBoundaryBurst() {
        // limit 4 per 1000ms window. Use up all 4 near the end of window 0.
        SlidingWindowCounterRateLimiter limiter = new SlidingWindowCounterRateLimiter(4, 1000, clock);

        clock.advance(800);
        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1")); // 4 used, all within window 0

        // Very start of window 1. A naive Fixed Window would reset to 0 here
        // and allow 4 more immediately (a burst of 8 in a tiny real span).
        // Sliding Window Counter must not allow that.
        clock.advance(200); // t=1000, window 1, elapsed=0 -> full overlap with window 0
        assertFalse(limiter.allowRequest("user-1"));

        // Halfway through window 1, the weighted estimate has decayed
        // enough that some room opens back up.
        clock.advance(500); // t=1500
        assertTrue(limiter.allowRequest("user-1"));
    }

    @Test
    void idleKeyDoesNotCarryStaleCountAcrossSkippedWindows() {
        SlidingWindowCounterRateLimiter limiter = new SlidingWindowCounterRateLimiter(2, 1000, clock);

        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1"));

        // Key goes idle for several windows, not just one.
        clock.advance(5000);

        // No "previous window" data should be blended in — fresh allowance.
        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1"));
    }

    @Test
    void differentKeysAreIndependent() {
        SlidingWindowCounterRateLimiter limiter = new SlidingWindowCounterRateLimiter(1, 1000, clock);

        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1"));

        assertTrue(limiter.allowRequest("user-2"));
    }

    @Test
    void constructorRejectsInvalidArguments() {
        assertThrows(IllegalArgumentException.class, () -> new SlidingWindowCounterRateLimiter(0, 1000, clock));
        assertThrows(IllegalArgumentException.class, () -> new SlidingWindowCounterRateLimiter(5, 0, clock));
        assertThrows(IllegalArgumentException.class, () -> new SlidingWindowCounterRateLimiter(5, -100, clock));
    }
}