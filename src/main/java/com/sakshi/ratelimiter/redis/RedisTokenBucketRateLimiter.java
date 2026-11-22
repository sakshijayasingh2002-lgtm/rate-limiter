package com.sakshi.ratelimiter.redis;

import com.sakshi.ratelimiter.core.RateLimiter;
import com.sakshi.ratelimiter.core.TimeSource;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.exceptions.JedisNoScriptException;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Token Bucket rate limiter whose state lives in Redis, so many service
 * instances share one set of buckets.
 *
 * <h2>How it differs from the in-memory version</h2>
 * The algorithm is identical, but the refill + spend logic now runs inside
 * Redis as a Lua script ({@code /lua/token_bucket.lua}). Redis runs one
 * script at a time, start to finish, so two instances can never both read
 * "1 token left" and both spend it. This class is just the thin Java side:
 * it sends the script the numbers and reads back 1 (allowed) or 0 (rejected).
 *
 * <h2>EVALSHA instead of EVAL</h2>
 * Sending the whole script text on every request wastes bandwidth. Instead we
 * upload it once with SCRIPT LOAD (Redis returns a SHA1 fingerprint) and then
 * call it by fingerprint with EVALSHA. If Redis forgets the script (restart,
 * SCRIPT FLUSH), it answers NOSCRIPT and we simply load it again and retry.
 *
 * <h2>When Redis is down: fail open or fail closed</h2>
 * A rate limiter protects a service, it shouldn't take the service down.
 * With {@code failOpen = true} (the default) a Redis error lets the request
 * through. With {@code false}, a Redis error rejects it, which is safer when
 * the thing behind the limiter must never be overloaded. It is a deliberate
 * choice per use case, so it is a constructor flag.
 *
 * <h2>Connection pool</h2>
 * A {@link Jedis} connection is not thread-safe, so each call borrows one
 * from a {@link JedisPool} and returns it when done.
 */
public class RedisTokenBucketRateLimiter implements RateLimiter {

    private static final String SCRIPT_PATH = "/lua/token_bucket.lua";

    private final JedisPool pool;
    private final long capacity;
    private final double refillTokensPerSecond;
    private final TimeSource timeSource;
    private final String keyPrefix;
    private final boolean failOpen;
    private final String script;

    /** SHA1 of the loaded script, null until first use. Safe to race on: loading twice is harmless. */
    private volatile String scriptSha;

    public RedisTokenBucketRateLimiter(JedisPool pool, long capacity, double refillTokensPerSecond,
                                       TimeSource timeSource, String keyPrefix) {
        this(pool, capacity, refillTokensPerSecond, timeSource, keyPrefix, true);
    }

    /**
     * @param pool                  Redis connection pool
     * @param capacity              max tokens per bucket (also the max burst)
     * @param refillTokensPerSecond tokens added back per second
     * @param timeSource            clock, pass a fake one in tests
     * @param keyPrefix             prepended to every key, e.g. "rl:tb:" so our keys don't collide with other data
     * @param failOpen              true: allow requests when Redis errors; false: reject them
     */
    public RedisTokenBucketRateLimiter(JedisPool pool, long capacity, double refillTokensPerSecond,
                                       TimeSource timeSource, String keyPrefix, boolean failOpen) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        if (refillTokensPerSecond <= 0) {
            throw new IllegalArgumentException("refillTokensPerSecond must be > 0");
        }
        this.pool = pool;
        this.capacity = capacity;
        this.refillTokensPerSecond = refillTokensPerSecond;
        this.timeSource = timeSource;
        this.keyPrefix = keyPrefix;
        this.failOpen = failOpen;
        this.script = loadScriptFromClasspath();
    }

    @Override
    public boolean allowRequest(String key) {
        List<String> keys = List.of(keyPrefix + key);
        List<String> args = List.of(
                String.valueOf(capacity),
                String.valueOf(refillTokensPerSecond),
                String.valueOf(timeSource.currentTimeMillis()),
                "1");

        try (Jedis jedis = pool.getResource()) {
            Object result = runScript(jedis, keys, args);
            // A Lua integer comes back from Redis as a Long.
            return result instanceof Long && ((Long) result) == 1L;
        } catch (JedisException e) {
            return failOpen;
        }
    }

    private Object runScript(Jedis jedis, List<String> keys, List<String> args) {
        String sha = scriptSha;
        if (sha == null) {
            sha = jedis.scriptLoad(script);
            scriptSha = sha;
        }
        try {
            return jedis.evalsha(sha, keys, args);
        } catch (JedisNoScriptException e) {
            // Redis lost its script cache (restart or SCRIPT FLUSH): load again and retry once.
            sha = jedis.scriptLoad(script);
            scriptSha = sha;
            return jedis.evalsha(sha, keys, args);
        }
    }

    private static String loadScriptFromClasspath() {
        try (InputStream in = RedisTokenBucketRateLimiter.class.getResourceAsStream(SCRIPT_PATH)) {
            if (in == null) {
                throw new IllegalStateException("Lua script not found on the classpath: " + SCRIPT_PATH
                        + " (is src/main/resources a source folder? Try Maven > Update Project)");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}