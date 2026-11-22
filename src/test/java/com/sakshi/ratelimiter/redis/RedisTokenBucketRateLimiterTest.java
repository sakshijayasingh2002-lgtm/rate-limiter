package com.sakshi.ratelimiter.redis;

import com.sakshi.ratelimiter.core.TimeSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration tests: these talk to a real Redis on localhost:6379.
 * If Redis isn't running, the whole class is SKIPPED (not failed), so the
 * project still builds on a machine without Redis.
 *
 * Start Redis with:  docker start redis-rl
 */
class RedisTokenBucketRateLimiterTest {

    private static final String HOST = "localhost";
    private static final int PORT = 6379;

    private static JedisPool pool;

    private String prefix;
    private AtomicLong now;
    private TimeSource clock;

    @BeforeAll
    static void connectOrSkip() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(64);
        JedisPool candidate = new JedisPool(config, HOST, PORT);
        try (Jedis jedis = candidate.getResource()) {
            jedis.ping();
        } catch (Exception e) {
            candidate.close();
            assumeTrue(false, "Redis is not running on localhost:6379 - start it with: docker start redis-rl");
        }
        pool = candidate;
    }

    @AfterAll
    static void closePool() {
        if (pool != null) {
            pool.close();
        }
    }

    @BeforeEach
    void setUp() {
        // A fresh key prefix per test, so tests never see each other's buckets.
        prefix = "test:" + UUID.randomUUID() + ":";
        now = new AtomicLong(0L);
        clock = now::get;
    }

    @AfterEach
    void cleanUpKeys() {
        try (Jedis jedis = pool.getResource()) {
            Set<String> leftovers = jedis.keys(prefix + "*");
            if (!leftovers.isEmpty()) {
                jedis.del(leftovers.toArray(new String[0]));
            }
        }
    }

    @Test
    void allowsUpToCapacity_thenRejects() {
        RedisTokenBucketRateLimiter limiter = new RedisTokenBucketRateLimiter(pool, 3, 1.0, clock, prefix);

        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1"));
    }

    @Test
    void refillsTokensOverTime() {
        RedisTokenBucketRateLimiter limiter = new RedisTokenBucketRateLimiter(pool, 2, 1.0, clock, prefix);

        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1"));

        now.addAndGet(1000); // 1 second passes -> +1 token
        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1"));
    }

    @Test
    void neverRefillsPastCapacity() {
        RedisTokenBucketRateLimiter limiter = new RedisTokenBucketRateLimiter(pool, 2, 10.0, clock, prefix);

        assertTrue(limiter.allowRequest("user-1")); // creates the bucket at t=0
        now.addAndGet(10_000); // would be 100 tokens if uncapped

        assertTrue(limiter.allowRequest("user-1"));
        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1")); // capped at 2
    }

    @Test
    void differentKeysAreIndependent() {
        RedisTokenBucketRateLimiter limiter = new RedisTokenBucketRateLimiter(pool, 1, 1.0, clock, prefix);

        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1"));

        assertTrue(limiter.allowRequest("user-2"));
    }

    @Test
    void clockGoingBackwardsDoesNotGrantFreeTokens() {
        RedisTokenBucketRateLimiter limiter = new RedisTokenBucketRateLimiter(pool, 1, 1.0, clock, prefix);
        now.set(10_000);

        assertTrue(limiter.allowRequest("user-1"));
        assertFalse(limiter.allowRequest("user-1"));

        now.addAndGet(-5000);
        assertFalse(limiter.allowRequest("user-1")); // still empty

        now.addAndGet(6000); // back to the original time, then 1s of real refill
        assertTrue(limiter.allowRequest("user-1"));
    }

    @Test
    void twoServiceInstancesShareOneBucket() {
        // Two separate pools = two separate "service instances" that only share Redis.
        JedisPool poolB = new JedisPool(new JedisPoolConfig(), HOST, PORT);
        try {
            RedisTokenBucketRateLimiter instanceA = new RedisTokenBucketRateLimiter(pool, 5, 0.0001, clock, prefix);
            RedisTokenBucketRateLimiter instanceB = new RedisTokenBucketRateLimiter(poolB, 5, 0.0001, clock, prefix);

            assertTrue(instanceA.allowRequest("user-1"));
            assertTrue(instanceA.allowRequest("user-1"));
            assertTrue(instanceA.allowRequest("user-1"));
            assertTrue(instanceB.allowRequest("user-1")); // B sees the 3 tokens A already used
            assertTrue(instanceB.allowRequest("user-1"));

            assertFalse(instanceA.allowRequest("user-1")); // 5 used in total across both
            assertFalse(instanceB.allowRequest("user-1"));
        } finally {
            poolB.close();
        }
    }

    @Test
    void concurrentRequestsAcrossTwoInstancesNeverExceedCapacity() throws InterruptedException {
        TimeSource frozen = () -> 0L; // no refill during the race
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(64);
        JedisPool poolB = new JedisPool(config, HOST, PORT);

        try {
            RedisTokenBucketRateLimiter instanceA = new RedisTokenBucketRateLimiter(pool, 10, 0.0001, frozen, prefix);
            RedisTokenBucketRateLimiter instanceB = new RedisTokenBucketRateLimiter(poolB, 10, 0.0001, frozen, prefix);

            int threads = 50;
            ExecutorService executor = Executors.newFixedThreadPool(threads);
            AtomicInteger allowed = new AtomicInteger();
            CountDownLatch ready = new CountDownLatch(threads);
            CountDownLatch go = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);

            for (int i = 0; i < threads; i++) {
                final RedisTokenBucketRateLimiter limiter = (i % 2 == 0) ? instanceA : instanceB;
                executor.submit(() -> {
                    ready.countDown();
                    try {
                        go.await();
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
            executor.shutdown();

            assertEquals(10, allowed.get(), "exactly capacity requests allowed across both instances");
        } finally {
            poolB.close();
        }
    }

    @Test
    void whenRedisIsDown_failOpenAllows_failClosedRejects() {
        // Nothing listens on port 6390, so every call fails to connect.
        JedisPool deadPool = new JedisPool(new JedisPoolConfig(), HOST, 6390, 200);
        try {
            RedisTokenBucketRateLimiter failOpen =
                    new RedisTokenBucketRateLimiter(deadPool, 1, 1.0, clock, prefix, true);
            RedisTokenBucketRateLimiter failClosed =
                    new RedisTokenBucketRateLimiter(deadPool, 1, 1.0, clock, prefix, false);

            assertTrue(failOpen.allowRequest("user-1"));
            assertFalse(failClosed.allowRequest("user-1"));
        } finally {
            deadPool.close();
        }
    }

    @Test
    void constructorRejectsInvalidArguments() {
        assertThrows(IllegalArgumentException.class,
                () -> new RedisTokenBucketRateLimiter(pool, 0, 1.0, clock, prefix));
        assertThrows(IllegalArgumentException.class,
                () -> new RedisTokenBucketRateLimiter(pool, 5, 0.0, clock, prefix));
        assertThrows(IllegalArgumentException.class,
                () -> new RedisTokenBucketRateLimiter(pool, 5, -1.0, clock, prefix));
    }
}