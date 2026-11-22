package com.sakshi.ratelimiter.redis;

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
 * Shared plumbing for every Redis-backed limiter: loads a Lua script from the
 * classpath, runs it with EVALSHA, and decides what to do when Redis fails.
 *
 * <h2>Why this class exists</h2>
 * Day 6 had this logic inside the Token Bucket class. With three Redis
 * limiters, copying it three times means a bug fix has to be made three
 * times. Each limiter now only worries about its own algorithm (which keys
 * and arguments to send), and this class handles the "talk to Redis" part.
 *
 * <h2>EVALSHA with a NOSCRIPT fallback</h2>
 * The script is uploaded once with SCRIPT LOAD and then called by its SHA1
 * fingerprint. If Redis forgets the script (restart, SCRIPT FLUSH) it answers
 * NOSCRIPT, and we load it again and retry once.
 *
 * <h2>Fail open or fail closed</h2>
 * If Redis errors (down, timeout), {@code failOpen = true} lets the request
 * through and {@code false} rejects it.
 */
final class RedisScriptExecutor {

    private final JedisPool pool;
    private final String script;
    private final boolean failOpen;

    /** SHA1 of the loaded script, null until first use. Safe to race on: loading twice is harmless. */
    private volatile String scriptSha;

    RedisScriptExecutor(JedisPool pool, String scriptPath, boolean failOpen) {
        this.pool = pool;
        this.script = loadFromClasspath(scriptPath);
        this.failOpen = failOpen;
    }

    /**
     * Runs the script and returns true if it answered 1 ("allowed").
     * On any Redis error, returns the fail-open/fail-closed choice instead.
     */
    boolean isAllowed(List<String> keys, List<String> args) {
        try (Jedis jedis = pool.getResource()) {
            Object result = run(jedis, keys, args);
            // A Lua integer comes back from Redis as a Long.
            return result instanceof Long && ((Long) result) == 1L;
        } catch (JedisException e) {
            return failOpen;
        }
    }

    private Object run(Jedis jedis, List<String> keys, List<String> args) {
        String sha = scriptSha;
        if (sha == null) {
            sha = jedis.scriptLoad(script);
            scriptSha = sha;
        }
        try {
            return jedis.evalsha(sha, keys, args);
        } catch (JedisNoScriptException e) {
            sha = jedis.scriptLoad(script);
            scriptSha = sha;
            return jedis.evalsha(sha, keys, args);
        }
    }

    private static String loadFromClasspath(String path) {
        try (InputStream in = RedisScriptExecutor.class.getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("Lua script not found on the classpath: " + path
                        + " (is src/main/resources a source folder? Try Maven > Update Project)");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}