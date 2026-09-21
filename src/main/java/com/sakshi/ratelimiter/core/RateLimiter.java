package com.sakshi.ratelimiter.core;

/**
 * Common contract every rate-limiting algorithm implements.
 *
 * <p>Kept deliberately tiny: one method. This is what lets the demo service
 * swap Token Bucket / Sliding Window / Leaky Bucket in and out via config,
 * without the caller (a servlet filter, an interceptor, whatever) knowing
 * or caring which algorithm is behind it.
 *
 * <p>{@code key} identifies "who" is being limited — a user ID, an API key,
 * an IP address, whatever the caller wants to partition by. Each algorithm
 * is responsible for keeping separate state per key.
 */
public interface RateLimiter {

    /**
     * Attempt to consume one unit of capacity for the given key.
     *
     * @param key identifies the client/bucket being rate-limited
     * @return true if the request is allowed, false if it should be rejected
     */
    boolean allowRequest(String key);
}
