package com.sakshi.ratelimiter.redis;

import com.sakshi.ratelimiter.core.RateLimiter;
import com.sakshi.ratelimiter.core.TimeSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/** Integration tests (need Redis on localhost:6379; skipped if it isn't running). */
class RedisSlidingWindowLogRateLimiterTest {

    private static JedisPool pool;

    private String prefix;
    private AtomicLong now;
    private TimeSource clock;

    @BeforeAll
    static void connect() {
        pool = RedisTestSupport.connectOrSkip();
    }

    @AfterAll
    static void close() {
        if (pool != null) {
            pool.close();
        }
    }

    @BeforeEach
    void setUp() {
        prefix = "test:" + UUID.randomUUID() + ":";
        now = new AtomicLong(0L);
        clock = now::get;
    }

    @AfterEach
    void cleanUp() {
        RedisTestSupport.deleteKeys(pool, prefix);
    }

    @Test
    void allowsUpToLimitWithinWindow() {
        RedisSlidingWindowLogRateLimiter limiter =
                new RedisSlidingWindowLogRateLimiter(pool, 3, 1000, clock, prefix);

        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1"));
    }

    @Test
    void oldRequestsExpireOutOfTheWindow() {
        RedisSlidingWindowLogRateLimiter limiter =
                new RedisSlidingWindowLogRateLimiter(pool, 2, 1000, clock, prefix);

        assertTrue(limiter.allowRequest("user-1")); // t=0
        now.addAndGet(600);
        assertTrue(limiter.allowRequest("user-1")); // t=600
        assertFalse(limiter.allowRequest("user-1")); // both slots used

        now.addAndGet(500); // t=1100: the t=0 request has left the 1000ms window
        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1")); // t=600 and t=1100 still count
    }

    @Test
    void noBoundaryBurst_exactCountOverAnySlidingInterval() {
        RedisSlidingWindowLogRateLimiter limiter =
                new RedisSlidingWindowLogRateLimiter(pool, 2, 1000, clock, prefix);

        now.addAndGet(900);
        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));

        now.addAndGet(150); // t=1050: the t=900 requests are still inside the trailing window
        assertFalse(limiter.allowRequest("user-1"));
    }

    @Test
    void requestsInTheSameMillisecondAreAllCounted() {
        // If the sorted-set member were just the timestamp, these 3 would collapse
        // into 1 entry and the limiter would wrongly allow far more than 3.
        RedisSlidingWindowLogRateLimiter limiter =
                new RedisSlidingWindowLogRateLimiter(pool, 3, 1000, clock, prefix);

        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1")); // all at t=0
    }

    @Test
    void differentKeysAreIndependent() {
        RedisSlidingWindowLogRateLimiter limiter =
                new RedisSlidingWindowLogRateLimiter(pool, 1, 1000, clock, prefix);

        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-2"));
    }

    @Test
    void twoServiceInstancesShareOneLog() {
        JedisPool poolB = RedisTestSupport.newPool();
        try {
            RedisSlidingWindowLogRateLimiter instanceA =
                    new RedisSlidingWindowLogRateLimiter(pool, 5, 60_000, clock, prefix);
            RedisSlidingWindowLogRateLimiter instanceB =
                    new RedisSlidingWindowLogRateLimiter(poolB, 5, 60_000, clock, prefix);

            assertTrue(instanceA.allowRequest("user-1"));
            assertTrue(instanceA.allowRequest("user-1"));
            assertTrue(instanceA.allowRequest("user-1"));
            assertTrue(instanceB.allowRequest("user-1"));
            assertTrue(instanceB.allowRequest("user-1"));

            assertFalse(instanceA.allowRequest("user-1"));
            assertFalse(instanceB.allowRequest("user-1"));
        } finally {
            poolB.close();
        }
    }

    @Test
    void concurrentRequestsAcrossTwoInstancesNeverExceedLimit() throws InterruptedException {
        TimeSource frozen = () -> 0L;
        JedisPool poolB = RedisTestSupport.newPool();
        try {
            RateLimiter instanceA = new RedisSlidingWindowLogRateLimiter(pool, 10, 60_000, frozen, prefix);
            RateLimiter instanceB = new RedisSlidingWindowLogRateLimiter(poolB, 10, 60_000, frozen, prefix);

            int allowed = RedisTestSupport.raceAcrossInstances(List.of(instanceA, instanceB), "shared-key", 50);

            assertEquals(10, allowed);
        } finally {
            poolB.close();
        }
    }

    @Test
    void constructorRejectsInvalidArguments() {
        assertThrows(IllegalArgumentException.class,
                () -> new RedisSlidingWindowLogRateLimiter(pool, 0, 1000, clock, prefix));
        assertThrows(IllegalArgumentException.class,
                () -> new RedisSlidingWindowLogRateLimiter(pool, 5, 0, clock, prefix));
        assertThrows(IllegalArgumentException.class,
                () -> new RedisSlidingWindowLogRateLimiter(pool, 5, -100, clock, prefix));
    }
}
