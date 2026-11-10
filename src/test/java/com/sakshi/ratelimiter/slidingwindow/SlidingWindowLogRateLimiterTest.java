package com.sakshi.ratelimiter.slidingwindow;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SlidingWindowLogRateLimiterTest {

    private FakeTimeSource clock;

    @BeforeEach
    void setUp() {
        clock = new FakeTimeSource(0L);
    }

    @Test
    void allowsUpToLimitWithinWindow() {
        // limit 3 requests per 1000ms window
        SlidingWindowLogRateLimiter limiter = new SlidingWindowLogRateLimiter(3, 1000, clock);

        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1"));
    }

    @Test
    void oldRequestsExpireOutOfTheWindow() {
        SlidingWindowLogRateLimiter limiter = new SlidingWindowLogRateLimiter(2, 1000, clock);

        assertTrue(limiter.allowRequest("user-1")); // t=0
        clock.advance(600);
        assertTrue(limiter.allowRequest("user-1")); // t=600
        assertFalse(limiter.allowRequest("user-1")); // t=600, both slots used

        clock.advance(500); // t=1100 -> the t=0 request is now outside the 1000ms window
        assertTrue(limiter.allowRequest("user-1")); // one slot freed up
        assertFalse(limiter.allowRequest("user-1")); // t=600 request still counts, slot used again
    }

    @Test
    void noBoundaryBurst_exactCountOverAnySlidingInterval() {
        // Classic Fixed-Window failure case: requests at the end of one window
        // plus requests at the start of the next, all within a short real span.
        // A fixed window would reset its counter at t=1000 and allow more.
        // Sliding Window Log must still remember the t=900 requests at t=1050.
        SlidingWindowLogRateLimiter limiter = new SlidingWindowLogRateLimiter(2, 1000, clock);

        clock.advance(900);
        assertTrue(limiter.allowRequest("user-1")); // t=900
        assertTrue(limiter.allowRequest("user-1")); // t=900, limit reached

        clock.advance(150); // t=1050 -> trailing window is (50, 1050], t=900 still inside
        assertFalse(limiter.allowRequest("user-1"));
    }

    @Test
    void differentKeysAreIndependent() {
        SlidingWindowLogRateLimiter limiter = new SlidingWindowLogRateLimiter(1, 1000, clock);

        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1"));

        assertTrue(limiter.allowRequest("user-2"));
    }

    @Test
    void constructorRejectsInvalidArguments() {
        assertThrows(IllegalArgumentException.class, () -> new SlidingWindowLogRateLimiter(0, 1000, clock));
        assertThrows(IllegalArgumentException.class, () -> new SlidingWindowLogRateLimiter(5, 0, clock));
        assertThrows(IllegalArgumentException.class, () -> new SlidingWindowLogRateLimiter(5, -100, clock));
    }
}