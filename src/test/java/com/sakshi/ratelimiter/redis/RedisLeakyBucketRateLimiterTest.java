package com.sakshi.ratelimiter.redis;

import com.sakshi.ratelimiter.core.RateLimiter;
import com.sakshi.ratelimiter.core.TimeSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/** Integration tests (need Redis on localhost:6379; skipped if it isn't running). */
class RedisLeakyBucketRateLimiterTest {

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
    void acceptsUpToCapacity_thenRejects() {
        RedisLeakyBucketRateLimiter limiter = new RedisLeakyBucketRateLimiter(pool, 3, 1.0, clock, prefix);

        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1")); // bucket full
    }

    @Test
    void drainsOverTime_makingRoomAgain() {
        RedisLeakyBucketRateLimiter limiter = new RedisLeakyBucketRateLimiter(pool, 2, 1.0, clock, prefix);

        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1"));

        now.addAndGet(1000); // 1 unit leaks out
        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1"));
    }

    @Test
    void delayGrowsForRequestsQueuedBehindOthers() {
        // leak 2/sec -> each request already waiting adds 500ms for the next one
        RedisLeakyBucketRateLimiter limiter = new RedisLeakyBucketRateLimiter(pool, 5, 2.0, clock, prefix);

        assertEquals(0, limiter.acceptAndGetDelayMillis("user-1"));
        assertEquals(500, limiter.acceptAndGetDelayMillis("user-1"));
        assertEquals(1000, limiter.acceptAndGetDelayMillis("user-1"));
    }

    @Test
    void rejectedRequestReturnsMinusOne() {
        RedisLeakyBucketRateLimiter limiter = new RedisLeakyBucketRateLimiter(pool, 1, 1.0, clock, prefix);

        assertEquals(0, limiter.acceptAndGetDelayMillis("user-1"));
        assertEquals(-1, limiter.acceptAndGetDelayMillis("user-1"));
    }

    @Test
    void neverDrainsBelowEmpty() {
        RedisLeakyBucketRateLimiter limiter = new RedisLeakyBucketRateLimiter(pool, 2, 10.0, clock, prefix);

        assertTrue(limiter.allowRequest("user-1"));
        now.addAndGet(60_000); // far more than enough to drain; the level must stop at 0, not go negative

        // A negative level would give extra free room. We should get exactly capacity.
        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1"));
    }

    @Test
    void clockGoingBackwardsDoesNotDrainOrBreak() {
        RedisLeakyBucketRateLimiter limiter = new RedisLeakyBucketRateLimiter(pool, 1, 1.0, clock, prefix);
        now.set(10_000);

        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1"));

        now.addAndGet(-5000);
        assertFalse(limiter.allowRequest("user-1")); // still full, no free drain

        now.addAndGet(6000); // back to the original time, then 1s of real drain
        assertTrue(limiter.allowRequest("user-1"));
    }

    @Test
    void differentKeysAreIndependent() {
        RedisLeakyBucketRateLimiter limiter = new RedisLeakyBucketRateLimiter(pool, 1, 1.0, clock, prefix);

        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-2"));
    }

    @Test
    void twoServiceInstancesShareOneBucket() {
        JedisPool poolB = RedisTestSupport.newPool();
        try {
            RedisLeakyBucketRateLimiter instanceA = new RedisLeakyBucketRateLimiter(pool, 5, 0.0001, clock, prefix);
            RedisLeakyBucketRateLimiter instanceB = new RedisLeakyBucketRateLimiter(poolB, 5, 0.0001, clock, prefix);

            assertTrue(instanceA.allowRequest("user-1"));
            assertTrue(instanceA.allowRequest("user-1"));
            assertTrue(instanceA.allowRequest("user-1"));
            assertTrue(instanceB.allowRequest("user-1")); // B sees the 3 requests A already queued
            assertTrue(instanceB.allowRequest("user-1"));

            assertFalse(instanceA.allowRequest("user-1"));
            assertFalse(instanceB.allowRequest("user-1"));
        } finally {
            poolB.close();
        }
    }

    @Test
    void concurrentRequestsAcrossTwoInstancesNeverExceedCapacity() throws InterruptedException {
        TimeSource frozen = () -> 0L;
        JedisPool poolB = RedisTestSupport.newPool();
        try {
            RateLimiter instanceA = new RedisLeakyBucketRateLimiter(pool, 10, 0.0001, frozen, prefix);
            RateLimiter instanceB = new RedisLeakyBucketRateLimiter(poolB, 10, 0.0001, frozen, prefix);

            int allowed = RedisTestSupport.raceAcrossInstances(List.of(instanceA, instanceB), "shared-key", 50);

            assertEquals(10, allowed);
        } finally {
            poolB.close();
        }
    }

    @Test
    void whenRedisIsDown_failOpenAllows_failClosedRejects() {
        // Nothing listens on port 6390, so every call fails to connect.
        JedisPool deadPool = new JedisPool(new JedisPoolConfig(), RedisTestSupport.HOST, 6390, 200);
        try {
            RedisLeakyBucketRateLimiter failOpen = new RedisLeakyBucketRateLimiter(deadPool, 1, 1.0, clock, prefix, true);
            RedisLeakyBucketRateLimiter failClosed = new RedisLeakyBucketRateLimiter(deadPool, 1, 1.0, clock, prefix, false);

            assertTrue(failOpen.allowRequest("user-1"));
            assertEquals(0, failOpen.acceptAndGetDelayMillis("user-1"));
            assertFalse(failClosed.allowRequest("user-1"));
            assertEquals(-1, failClosed.acceptAndGetDelayMillis("user-1"));
        } finally {
            deadPool.close();
        }
    }

    @Test
    void constructorRejectsInvalidArguments() {
        assertThrows(IllegalArgumentException.class,
                () -> new RedisLeakyBucketRateLimiter(pool, 0, 1.0, clock, prefix));
        assertThrows(IllegalArgumentException.class,
                () -> new RedisLeakyBucketRateLimiter(pool, 5, 0.0, clock, prefix));
        assertThrows(IllegalArgumentException.class,
                () -> new RedisLeakyBucketRateLimiter(pool, 5, -1.0, clock, prefix));
    }
}
