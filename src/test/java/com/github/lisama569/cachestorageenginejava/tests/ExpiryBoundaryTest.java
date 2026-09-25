package com.github.lisama569.cachestorageenginejava.tests;

import com.github.lisama569.cachestorageenginejava.core.CacheEngine;
import com.github.lisama569.cachestorageenginejava.core.CacheResult;
import com.github.lisama569.cachestorageenginejava.core.CacheStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 过期边界测试：判定依据为 now >= expireAtMillis（到期瞬间即过期）。
 * 每个用例打印输入、判定依据与结果。
 */
class ExpiryBoundaryTest {

    private static final Logger log = LoggerFactory.getLogger(ExpiryBoundaryTest.class);

    @TempDir
    Path dir;

    private CacheEngine newEngine(int capacity, MutableClock clock, String name) {
        return new CacheEngine(capacity, dir.resolve(name), clock);
    }

    @Test
    void exactExpiryInstantIsExpired() {
        MutableClock clock = MutableClock.startingAtEpoch();
        try (CacheEngine engine = newEngine(8, clock, "a.wal")) {
            long t0 = clock.millis();
            engine.put("k", "v", 1000);
            log.info("[输入] put(k,v,ttl=1000) @now={} -> expireAt={}", t0, t0 + 1000);

            clock.advanceMillis(999);
            CacheResult<String> before = engine.get("k");
            log.info("[判定] now={} < expireAt={} -> 应命中; 实际={}", clock.millis(), t0 + 1000, before.status());
            assertThat(before.status()).isEqualTo(CacheStatus.OK);
            assertThat(before.value()).isEqualTo("v");

            clock.advanceMillis(1); // now == expireAt，边界：到期瞬间即过期
            CacheResult<String> at = engine.get("k");
            log.info("[判定] now={} >= expireAt={} -> 应过期; 实际={}", clock.millis(), t0 + 1000, at.status());
            assertThat(at.status()).isEqualTo(CacheStatus.EXPIRED);
        }
    }

    @Test
    void missingVsExpiredAreDistinguishable() {
        MutableClock clock = MutableClock.startingAtEpoch();
        try (CacheEngine engine = newEngine(8, clock, "b.wal")) {
            engine.put("k", "v", 100);
            clock.advanceMillis(200);

            CacheResult<String> expired = engine.get("k");
            CacheResult<String> missing = engine.get("absent");
            log.info("[判定] 已过期键 -> {}; 从未写入键 -> {}（两者必须可区分）",
                    expired.status(), missing.status());
            assertThat(expired.status()).isEqualTo(CacheStatus.EXPIRED);
            assertThat(missing.status()).isEqualTo(CacheStatus.NOT_FOUND);
        }
    }

    @Test
    void expiredSlotIsReclaimedBeforeCapacityCheck() {
        MutableClock clock = MutableClock.startingAtEpoch();
        try (CacheEngine engine = newEngine(1, clock, "c.wal")) {
            engine.put("k1", "v1", 500);
            clock.advanceMillis(600); // k1 已过期

            CacheStatus put = engine.put("k2", "v2", 0);
            log.info("[输入] 容量=1, k1 已过期; put(k2) -> {}（应先回收过期槽位而非拒绝）", put);
            assertThat(put).isEqualTo(CacheStatus.OK);
            // k1 已在 put 前被主动回收，物理上不存在 -> NOT_FOUND
            CacheResult<String> k1 = engine.get("k1");
            log.info("[判定] k1 已被回收 -> {}", k1.status());
            assertThat(k1.status()).isEqualTo(CacheStatus.NOT_FOUND);
            assertThat(engine.get("k2").status()).isEqualTo(CacheStatus.OK);
            assertThat(engine.size()).isEqualTo(1);
        }
    }

    @Test
    void deleteDistinguishesStates() {
        MutableClock clock = MutableClock.startingAtEpoch();
        try (CacheEngine engine = newEngine(8, clock, "d.wal")) {
            engine.put("live", "v", 0);
            engine.put("dying", "v", 100);
            clock.advanceMillis(200);

            CacheStatus delLive = engine.delete("live");
            CacheStatus delExpired = engine.delete("dying");
            CacheStatus delMissing = engine.delete("nope");
            log.info("[判定] delete(存活)={} delete(已过期)={} delete(不存在)={}",
                    delLive, delExpired, delMissing);
            assertThat(delLive).isEqualTo(CacheStatus.OK);
            assertThat(delExpired).isEqualTo(CacheStatus.EXPIRED);
            assertThat(delMissing).isEqualTo(CacheStatus.NOT_FOUND);
        }
    }

    @Test
    void purgeExpiredRemovesOnlyExpired() {
        MutableClock clock = MutableClock.startingAtEpoch();
        try (CacheEngine engine = newEngine(8, clock, "e.wal")) {
            engine.put("a", "1", 100);
            engine.put("b", "2", 0);
            engine.put("c", "3", 100);
            clock.advanceMillis(150);

            int purged = engine.purgeExpired();
            log.info("[判定] 过期键 a,c; 存活键 b -> purge 应回收 2 条; 实际={}", purged);
            assertThat(purged).isEqualTo(2);
            assertThat(engine.get("b").status()).isEqualTo(CacheStatus.OK);
            assertThat(engine.size()).isEqualTo(1);
        }
    }

    @Test
    void expiredEntriesAreAbsentAfterRestart() {
        MutableClock clock = MutableClock.startingAtEpoch();
        Path wal = dir.resolve("f.wal");
        try (CacheEngine engine = new CacheEngine(8, wal, clock)) {
            engine.put("short", "v", 100);
            engine.put("long", "v", 0);
        }
        clock.advanceMillis(500); // 重启发生在 short 过期之后
        try (CacheEngine reopened = new CacheEngine(8, wal, clock)) {
            CacheResult<String> shortKey = reopened.get("short");
            CacheResult<String> longKey = reopened.get("long");
            log.info("[恢复结果] 重启后 short(已过期)={} long(存活)={}", shortKey.status(), longKey.status());
            assertThat(shortKey.status()).isEqualTo(CacheStatus.NOT_FOUND);
            assertThat(longKey.status()).isEqualTo(CacheStatus.OK);
        }
    }
}
