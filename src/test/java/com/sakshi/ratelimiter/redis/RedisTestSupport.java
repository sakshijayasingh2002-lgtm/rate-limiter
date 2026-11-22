package com.sakshi.ratelimiter.redis;

import com.sakshi.ratelimiter.core.RateLimiter;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Helpers shared by the Redis integration tests. */
final class RedisTestSupport {

    static final String HOST = "localhost";
    static final int PORT = 6379;

    private RedisTestSupport() {
    }

    /** A new connection pool, standing in for one "service instance". */
    static JedisPool newPool() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(64);
        return new JedisPool(config, HOST, PORT);
    }

    /** Connects to Redis, or SKIPS the calling test class if Redis isn't running. */
    static JedisPool connectOrSkip() {
        JedisPool pool = newPool();
        try (Jedis jedis = pool.getResource()) {
            jedis.ping();
        } catch (Exception e) {
            pool.close();
            assumeTrue(false, "Redis is not running on localhost:6379 - start it with: docker start redis-rl");
        }
        return pool;
    }

    static void deleteKeys(JedisPool pool, String prefix) {
        try (Jedis jedis = pool.getResource()) {
            Set<String> leftovers = jedis.keys(prefix + "*");
            if (!leftovers.isEmpty()) {
                jedis.del(leftovers.toArray(new String[0]));
            }
        }
    }

    /**
     * Fires `threads` requests for the same key at the same instant, spread
     * round-robin over the given limiters (each limiter = one service instance),
     * and returns how many were allowed.
     */
    static int raceAcrossInstances(List<RateLimiter> limiters, String key, int threads) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        AtomicInteger allowed = new AtomicInteger();
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            final RateLimiter limiter = limiters.get(i % limiters.size());
            executor.submit(() -> {
                ready.countDown();
                try {
                    go.await(); // release every thread at the same moment
                    if (limiter.allowRequest(key)) {
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
        return allowed.get();
    }
}