package com.github.lisama569.cachestorageenginejava.tests;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicLong;

/** 测试用可推进时钟（线程安全）。 */
public final class MutableClock extends Clock {

    private final AtomicLong millis;

    public MutableClock(long initialMillis) {
        this.millis = new AtomicLong(initialMillis);
    }

    public static MutableClock startingAtEpoch() {
        return new MutableClock(1_000_000L);
    }

    public void advanceMillis(long delta) {
        millis.addAndGet(delta);
    }

    public void advance(Duration d) {
        millis.addAndGet(d.toMillis());
    }

    @Override
    public long millis() {
        return millis.get();
    }

    @Override
    public Instant instant() {
        return Instant.ofEpochMilli(millis.get());
    }

    @Override
    public ZoneId getZone() {
        return ZoneId.of("UTC");
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return this;
    }
}
