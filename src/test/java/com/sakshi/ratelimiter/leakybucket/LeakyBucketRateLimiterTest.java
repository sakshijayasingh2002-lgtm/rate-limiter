package com.sakshi.ratelimiter.leakybucket;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LeakyBucketRateLimiterTest {

    private FakeTimeSource clock;

    @BeforeEach
    void setUp() {
        clock = new FakeTimeSource(0L);
    }

    @Test
    void acceptsUpToCapacity_thenRejects() {
        // capacity 3, leaks 1/sec, no time passes
        LeakyBucketRateLimiter limiter = new LeakyBucketRateLimiter(3, 1.0, clock);

        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1")); // bucket full
    }

    @Test
    void drainsOverTime_makingRoomAgain() {
        LeakyBucketRateLimiter limiter = new LeakyBucketRateLimiter(2, 1.0, clock);

        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1")); // full

        clock.advance(1000); // 1 unit leaks out
        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1")); // full again
    }

    @Test
    void delayGrowsForRequestsQueuedBehindOthers() {
        // leak 2/sec -> each queued request adds 500ms of waiting for the next one
        LeakyBucketRateLimiter limiter = new LeakyBucketRateLimiter(5, 2.0, clock);

        assertEquals(0, limiter.acceptAndGetDelayMillis("user-1"));    // empty bucket, goes straight through
        assertEquals(500, limiter.acceptAndGetDelayMillis("user-1"));  // 1 ahead of it
        assertEquals(1000, limiter.acceptAndGetDelayMillis("user-1")); // 2 ahead of it
    }

    @Test
    void rejectedRequestReturnsMinusOne() {
        LeakyBucketRateLimiter limiter = new LeakyBucketRateLimiter(1, 1.0, clock);

        assertEquals(0, limiter.acceptAndGetDelayMillis("user-1"));
        assertEquals(-1, limiter.acceptAndGetDelayMillis("user-1"));
    }

    @Test
    void neverDrainsBelowEmpty() {
        LeakyBucketRateLimiter limiter = new LeakyBucketRateLimiter(2, 10.0, clock);

        assertTrue(limiter.allowRequest("user-1"));
        clock.advance(60_000); // far more than enough to drain; level must clamp at 0, not go negative

        // If the level had gone negative, we'd get extra free room. We should get exactly capacity.
        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1"));
    }

    @Test
    void clockGoingBackwardsDoesNotDrainOrBreak() {
        LeakyBucketRateLimiter limiter = new LeakyBucketRateLimiter(1, 1.0, clock);

        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1")); // full

        clock.advance(-5000); // clock jumps backward
        assertFalse(limiter.allowRequest("user-1")); // still full, no free drain

        clock.advance(5000); // back to the original time
        clock.advance(1000); // then 1s of real drain
        assertTrue(limiter.allowRequest("user-1"));
    }

    @Test
    void differentKeysAreIndependent() {
        LeakyBucketRateLimiter limiter = new LeakyBucketRateLimiter(1, 1.0, clock);

        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1"));

        assertTrue(limiter.allowRequest("user-2"));
    }

    @Test
    void constructorRejectsInvalidArguments() {
        assertThrows(IllegalArgumentException.class, () -> new LeakyBucketRateLimiter(0, 1.0, clock));
        assertThrows(IllegalArgumentException.class, () -> new LeakyBucketRateLimiter(5, 0.0, clock));
        assertThrows(IllegalArgumentException.class, () -> new LeakyBucketRateLimiter(5, -1.0, clock));
    }
}