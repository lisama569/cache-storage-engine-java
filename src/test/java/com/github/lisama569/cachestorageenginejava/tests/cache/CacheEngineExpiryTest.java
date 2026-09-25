package com.github.lisama569.cachestorageenginejava.tests.cache;

import com.github.lisama569.cachestorageenginejava.cache.CacheEngine;
import com.github.lisama569.cachestorageenginejava.cache.CacheResult;
import com.github.lisama569.cachestorageenginejava.cache.CacheStatus;
import com.github.lisama569.cachestorageenginejava.config.CacheProperties;
import com.github.lisama569.cachestorageenginejava.persistence.WriteAheadLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class CacheEngineExpiryTest {

    private static final Logger log = LoggerFactory.getLogger(CacheEngineExpiryTest.class);

    @TempDir
    Path dir;

    private MutableClock clock;
    private CacheEngine engine;

    @BeforeEach
    void setUp() throws Exception {
        clock = new MutableClock(1_000_000L);
        CacheProperties props = new CacheProperties();
        engine = new CacheEngine(props, new WriteAheadLog(dir.resolve("cache.wal")), clock);
        engine.recover();
    }

    @AfterEach
    void tearDown() throws Exception {
        engine.close();
    }

    @Test
    void expiredEntryReturnsExpiredNotNotFound() {
        log.info("输入: put(k1, v1, ttl=1000ms), 时钟推进 1000ms 后 get(k1)");
        engine.put("k1", "v1".getBytes(StandardCharsets.UTF_8), 1000);
        clock.advance(1000); // 到达过期边界：now >= expireAt 视为过期
        CacheResult<byte[]> result = engine.get("k1");
        log.info("判定依据: now={} >= expireAt=1001000, 应判定为 EXPIRED 而非 NOT_FOUND; 实际={}",
                clock.millis(), result.status());
        assertEquals(CacheStatus.EXPIRED, result.status());

        CacheResult<byte[]> missing = engine.get("never-existed");
        log.info("输入: get(never-existed); 判定依据: 键从未写入, 应为 NOT_FOUND; 实际={}", missing.status());
        assertEquals(CacheStatus.NOT_FOUND, missing.status());
    }

    @Test
    void entryAliveJustBeforeBoundary() {
        log.info("输入: put(k2, v2, ttl=1000ms), 时钟推进 999ms 后 get(k2)");
        engine.put("k2", "v2".getBytes(StandardCharsets.UTF_8), 1000);
        clock.advance(999); // 边界前一毫秒
        CacheResult<byte[]> result = engine.get("k2");
        log.info("判定依据: now={} < expireAt=1001000, 应命中 OK; 实际={}", clock.millis(), result.status());
        assertTrue(result.isOk());
        assertArrayEquals("v2".getBytes(StandardCharsets.UTF_8), result.value());
    }

    @Test
    void removeExpiredReturnsExpired() {
        log.info("输入: put(k3, ttl=500ms), 推进 500ms 后 remove(k3)");
        engine.put("k3", "v3".getBytes(StandardCharsets.UTF_8), 500);
        clock.advance(500);
        CacheResult<Void> result = engine.remove("k3");
        log.info("判定依据: 键已过期, remove 应返回 EXPIRED; 实际={}", result.status());
        assertEquals(CacheStatus.EXPIRED, result.status());
        assertEquals(0, engine.size());
    }

    @Test
    void noTtlNeverExpires() {
        log.info("输入: put(k4, ttl=0 表示永不过期), 推进很长时间后 get(k4)");
        engine.put("k4", "v4".getBytes(StandardCharsets.UTF_8), 0);
        clock.advance(10_000_000L);
        CacheResult<byte[]> result = engine.get("k4");
        log.info("判定依据: ttl<=0 不过期, 应为 OK; 实际={}", result.status());
        assertTrue(result.isOk());
    }
}
