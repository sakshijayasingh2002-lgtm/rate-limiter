package com.sakshi.ratelimiter;

import com.sakshi.ratelimiter.core.RateLimiter;
import com.sakshi.ratelimiter.core.TimeSource;
import com.sakshi.ratelimiter.leakybucket.LeakyBucketRateLimiter;
import com.sakshi.ratelimiter.slidingwindow.SlidingWindowCounterRateLimiter;
import com.sakshi.ratelimiter.slidingwindow.SlidingWindowLogRateLimiter;
import com.sakshi.ratelimiter.tokenbucket.TokenBucketRateLimiter;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * One race, run against every algorithm: 50 threads hit the same key at once,
 * and exactly LIMIT requests must get through, never more.
 *
 * The clock is frozen (always returns 0) so no tokens refill, no windows roll
 * and nothing leaks mid-test. That isolates what we want to prove: the
 * per-key locking stops two threads from both claiming the last slot.
 */
class ConcurrencyAcrossAlgorithmsTest {

    private static final int LIMIT = 10;
    private static final int THREADS = 50;
    private static final TimeSource FROZEN_CLOCK = () -> 0L;

    @Test
    void tokenBucket() throws InterruptedException {
        assertExactlyLimitAllowed(new TokenBucketRateLimiter(LIMIT, 1.0, FROZEN_CLOCK));
    }

    @Test
    void slidingWindowLog() throws InterruptedException {
        assertExactlyLimitAllowed(new SlidingWindowLogRateLimiter(LIMIT, 1000, FROZEN_CLOCK));
    }

    @Test
    void slidingWindowCounter() throws InterruptedException {
        assertExactlyLimitAllowed(new SlidingWindowCounterRateLimiter(LIMIT, 1000, FROZEN_CLOCK));
    }

    @Test
    void leakyBucket() throws InterruptedException {
        assertExactlyLimitAllowed(new LeakyBucketRateLimiter(LIMIT, 1.0, FROZEN_CLOCK));
    }

    private void assertExactlyLimitAllowed(RateLimiter limiter) throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        AtomicInteger allowed = new AtomicInteger();
        CountDownLatch ready = new CountDownLatch(THREADS);
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(THREADS);

        for (int i = 0; i < THREADS; i++) {
            pool.submit(() -> {
                ready.countDown();
                try {
                    go.await(); // release every thread at the same moment
                    if (limiter.allowRequest("shared-key")) {
                        allowed.incrementAndGet();
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

        assertEquals(LIMIT, allowed.get());
    }
}