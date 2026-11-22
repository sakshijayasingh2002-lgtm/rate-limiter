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
class RedisSlidingWindowCounterRateLimiterTest {

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
    void allowsUpToLimitWithinFirstWindow() {
        RedisSlidingWindowCounterRateLimiter limiter =
                new RedisSlidingWindowCounterRateLimiter(pool, 4, 1000, clock, prefix);

        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1"));
    }

    @Test
    void smoothsOutTheWindowBoundaryBurst() {
        RedisSlidingWindowCounterRateLimiter limiter =
                new RedisSlidingWindowCounterRateLimiter(pool, 4, 1000, clock, prefix);

        now.addAndGet(800);
        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1")); // 4 used, all in window 0

        now.addAndGet(200); // t=1000: start of window 1, full overlap with window 0
        assertFalse(limiter.allowRequest("user-1")); // a plain fixed window would allow 4 more here

        now.addAndGet(500); // t=1500: halfway through window 1
        assertTrue(limiter.allowRequest("user-1"));
    }

    @Test
    void idleKeyDoesNotCarryStaleCountAcrossSkippedWindows() {
        RedisSlidingWindowCounterRateLimiter limiter =
                new RedisSlidingWindowCounterRateLimiter(pool, 2, 1000, clock, prefix);

        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1"));

        now.addAndGet(5000); // several windows of silence

        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1"));
    }

    @Test
    void differentKeysAreIndependent() {
        RedisSlidingWindowCounterRateLimiter limiter =
                new RedisSlidingWindowCounterRateLimiter(pool, 1, 1000, clock, prefix);

        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-2"));
    }

    @Test
    void twoServiceInstancesShareOneWindow() {
        JedisPool poolB = RedisTestSupport.newPool();
        try {
            RedisSlidingWindowCounterRateLimiter instanceA =
                    new RedisSlidingWindowCounterRateLimiter(pool, 5, 60_000, clock, prefix);
            RedisSlidingWindowCounterRateLimiter instanceB =
                    new RedisSlidingWindowCounterRateLimiter(poolB, 5, 60_000, clock, prefix);

            assertTrue(instanceA.allowRequest("user-1"));
            assertTrue(instanceA.allowRequest("user-1"));
            assertTrue(instanceA.allowRequest("user-1"));
            assertTrue(instanceB.allowRequest("user-1")); // B sees the 3 that A already used
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
            RateLimiter instanceA = new RedisSlidingWindowCounterRateLimiter(pool, 10, 60_000, frozen, prefix);
            RateLimiter instanceB = new RedisSlidingWindowCounterRateLimiter(poolB, 10, 60_000, frozen, prefix);

            int allowed = RedisTestSupport.raceAcrossInstances(List.of(instanceA, instanceB), "shared-key", 50);

            assertEquals(10, allowed);
        } finally {
            poolB.close();
        }
    }

    @Test
    void constructorRejectsInvalidArguments() {
        assertThrows(IllegalArgumentException.class,
                () -> new RedisSlidingWindowCounterRateLimiter(pool, 0, 1000, clock, prefix));
        assertThrows(IllegalArgumentException.class,
                () -> new RedisSlidingWindowCounterRateLimiter(pool, 5, 0, clock, prefix));
        assertThrows(IllegalArgumentException.class,
                () -> new RedisSlidingWindowCounterRateLimiter(pool, 5, -100, clock, prefix));
    }
}