package com.sakshi.ratelimiter.tokenbucket;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TokenBucketRateLimiterTest {

    private FakeTimeSource clock;

    @BeforeEach
    void setUp() {
        clock = new FakeTimeSource(0L);
    }

    @Test
    void allowsRequestsUpToCapacity_thenRejects() {
        // capacity 3, refill rate irrelevant here since no time passes
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(3, 1.0, clock);

        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        // bucket started full with 3 tokens, all three consumed above
        assertFalse(limiter.allowRequest("user-1"));
    }

    @Test
    void refillsTokensOverTime() {
        // capacity 2, refills 1 token/sec
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(2, 1.0, clock);

        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1")); // empty

        clock.advance(1000); // 1 second passes -> +1 token
        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1")); // empty again
    }

    @Test
    void neverRefillsPastCapacity() {
        // capacity 2, refills fast (10 tokens/sec)
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(2, 10.0, clock);

        clock.advance(10_000); // huge gap — would be 100 tokens if uncapped
        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1")); // still capped at 2
    }

    @Test
    void differentKeysAreIndependent() {
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(1, 1.0, clock);

        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1")); // user-1 is out

        // user-2 has never been touched — full bucket of its own
        assertTrue(limiter.allowRequest("user-2"));
    }

    @Test
    void partialRefillIsFractional_notRoundedUp() {
        // capacity 5, refill 1 token/sec -> 500ms should add 0.5 tokens, not enough for a request
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(5, 1.0, clock);

        for (int i = 0; i < 5; i++) {
            assertTrue(limiter.allowRequest("user-1"));
        }
        assertFalse(limiter.allowRequest("user-1")); // empty

        clock.advance(500); // half a token
        assertFalse(limiter.allowRequest("user-1")); // still not enough for 1 full token

        clock.advance(500); // now a full second has passed total -> 1 token
        assertTrue(limiter.allowRequest("user-1"));
    }

    @Test
    void constructorRejectsInvalidArguments() {
        assertThrows(IllegalArgumentException.class, () -> new TokenBucketRateLimiter(0, 1.0, clock));
        assertThrows(IllegalArgumentException.class, () -> new TokenBucketRateLimiter(5, 0.0, clock));
        assertThrows(IllegalArgumentException.class, () -> new TokenBucketRateLimiter(5, -1.0, clock));
    }

    @Test
    void clockGoingBackwardsDoesNotGrantFreeTokens() {
        // e.g. NTP adjustment or a container clock jump. If we naively did
        // (now - lastRefill) * rate with a negative elapsed time, we'd
        // *subtract* tokens or overflow weirdly. Must be a safe no-op instead.
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(2, 1.0, clock);

        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1")); // empty

        clock.advance(-5000); // clock jumps backward 5 seconds
        assertFalse(limiter.allowRequest("user-1")); // still empty, not broken

        clock.advance(5000); // back to where we were, then +1s normally
        clock.advance(1000);
        assertTrue(limiter.allowRequest("user-1")); // refill resumes normally
    }

    @Test
    void concurrentRequestsNeverExceedCapacity() throws InterruptedException {
        // capacity 10, no refill during the test (rate is tiny, no time advances)
        // 50 threads race for the same key -> exactly 10 should win, never more
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(10, 0.0001, clock);

        int threadCount = 50;
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(threadCount);
        java.util.concurrent.atomic.AtomicInteger allowedCount = new java.util.concurrent.atomic.AtomicInteger(0);
        java.util.concurrent.CountDownLatch ready = new java.util.concurrent.CountDownLatch(threadCount);
        java.util.concurrent.CountDownLatch go = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            pool.submit(() -> {
                ready.countDown();
                try {
                    go.await(); // all threads fire as close to simultaneously as possible
                    if (limiter.allowRequest("shared-key")) {
                        allowedCount.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        ready.await();
        go.countDown();
        done.await();
        pool.shutdown();

        assertEquals(10, allowedCount.get(), "exactly capacity requests should be allowed under concurrent access");
    }
}