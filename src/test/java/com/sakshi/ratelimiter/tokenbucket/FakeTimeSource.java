package com.sakshi.ratelimiter.tokenbucket;

import com.sakshi.ratelimiter.core.TimeSource;

/** A clock the test controls explicitly, so refill logic can be tested without sleeping. */
class FakeTimeSource implements TimeSource {

    private long currentMillis;

    FakeTimeSource(long startMillis) {
        this.currentMillis = startMillis;
    }

    void advance(long millis) {
        currentMillis += millis;
    }

    @Override
    public long currentTimeMillis() {
        return currentMillis;
    }
}
