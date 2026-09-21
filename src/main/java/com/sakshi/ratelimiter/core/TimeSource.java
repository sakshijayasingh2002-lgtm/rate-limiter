package com.sakshi.ratelimiter.core;

/**
 * Supplies the current time in milliseconds.
 *
 * <p>Every algorithm depends on "how much time has passed" (to refill tokens,
 * expire windows, drain the leaky bucket). If they called
 * {@code System.currentTimeMillis()} directly, testing refill/expiry logic
 * would mean literally sleeping in the test — slow and flaky.
 *
 * <p>Instead, algorithms take a {@code TimeSource} in their constructor.
 * Production code uses {@link #systemTimeSource()}. Tests use a fake that
 * advances time under the test's control.
 */
@FunctionalInterface
public interface TimeSource {

    long currentTimeMillis();

    static TimeSource systemTimeSource() {
        return System::currentTimeMillis;
    }
}
