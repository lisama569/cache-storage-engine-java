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
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CacheEngineEvictionTest {

    private static final Logger log = LoggerFactory.getLogger(CacheEngineEvictionTest.class);

    @TempDir
    Path dir;

    private MutableClock clock;
    private CacheProperties props;
    private CacheEngine engine;

    @BeforeEach
    void setUp() throws Exception {
        clock = new MutableClock(0L);
        props = new CacheProperties();
        props.setMaxEntries(3);
        props.setMaxBytes(0); // 本条测试只按条目数限制
        engine = newEngine();
    }

    @AfterEach
    void tearDown() throws Exception {
        engine.close();
    }

    private CacheEngine newEngine() throws Exception {
        CacheEngine e = new CacheEngine(props, new WriteAheadLog(dir.resolve("cache.wal")), clock);
        e.recover();
        return e;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void lruEvictsLeastRecentlyUsed() {
        log.info("输入: maxEntries=3, 依次 put a,b,c; 访问 a 使其成为 MRU; 再 put d");
        engine.put("a", bytes("1"), 0);
        engine.put("b", bytes("2"), 0);
        engine.put("c", bytes("3"), 0);
        engine.get("a"); // a 变为最近使用
        engine.put("d", bytes("4"), 0);
        log.info("判定依据: LRU 应淘汰最久未使用的 b; a={}, b={}, c={}, d={}",
                engine.get("a").status(), engine.get("b").status(),
                engine.get("c").status(), engine.get("d").status());
        assertEquals(CacheStatus.OK, engine.get("a").status());
        assertEquals(CacheStatus.NOT_FOUND, engine.get("b").status());
        assertEquals(CacheStatus.OK, engine.get("c").status());
        assertEquals(CacheStatus.OK, engine.get("d").status());
        assertEquals(3, engine.size());
    }

    @Test
    void expiredEntriesEvictedBeforeLru() {
        log.info("输入: maxEntries=3, put a(ttl=100),b,c; 推进 100ms 使 a 过期; 再 put d");
        engine.put("a", bytes("1"), 100);
        engine.put("b", bytes("2"), 0);
        engine.put("c", bytes("3"), 0);
        clock.advance(100);
        engine.put("d", bytes("4"), 0);
        log.info("判定依据: 淘汰优先选已过期条目 a, 而非 LRU 的 b; b={}, d={}",
                engine.get("b").status(), engine.get("d").status());
        assertEquals(CacheStatus.OK, engine.get("b").status());
        assertEquals(CacheStatus.OK, engine.get("d").status());
        assertEquals(3, engine.size());
    }

    @Test
    void oversizedEntryRejectedWithoutDroppingData() throws Exception {
        engine.close();
        props.setMaxEntries(0);
        props.setMaxBytes(10); // 总共只允许 10 字节
        engine = newEngine();
        log.info("输入: maxBytes=10, put small(5字节) 成功; 再 put big(20字节)");
        assertTrue(engine.put("small", bytes("12345"), 0).isOk());
        CacheResult<Void> big = engine.put("big", bytes("01234567890123456789"), 0);
        log.info("判定依据: 单条 20 字节 > maxBytes=10, 应返回 CAPACITY_EXCEEDED 且已有数据不受影响; "
                + "实际={}, small 状态={}", big.status(), engine.get("small").status());
        assertEquals(CacheStatus.CAPACITY_EXCEEDED, big.status());
        assertEquals(CacheStatus.OK, engine.get("small").status());
        assertEquals(1, engine.size());
    }

    @Test
    void putAllIsAtomicOnCapacityFailure() throws Exception {
        engine.close();
        props.setMaxEntries(0);
        props.setMaxBytes(10);
        engine = newEngine();
        log.info("输入: maxBytes=10, 已有 keep(4字节); 批量 put {x=8字节, y=8字节} 自身共 16 字节");
        engine.put("keep", bytes("keep"), 0);
        Map<String, byte[]> batch = new LinkedHashMap<>();
        batch.put("x", bytes("xxxxxxxx"));
        batch.put("y", bytes("yyyyyyyy"));
        CacheResult<Void> result = engine.putAll(batch, 0);
        log.info("判定依据: 批量自身 16 字节 > maxBytes=10, 即使淘汰全部也放不下, 应整体失败 CAPACITY_EXCEEDED, x/y 均不生效, keep 不丢失; "
                + "实际={}, x={}, y={}, keep={}",
                result.status(), engine.get("x").status(), engine.get("y").status(), engine.get("keep").status());
        assertEquals(CacheStatus.CAPACITY_EXCEEDED, result.status());
        assertEquals(CacheStatus.NOT_FOUND, engine.get("x").status());
        assertEquals(CacheStatus.NOT_FOUND, engine.get("y").status());
        assertEquals(CacheStatus.OK, engine.get("keep").status());
    }

    @Test
    void putAllSuccess() {
        log.info("输入: maxEntries=3, 批量 put {p,q}");
        Map<String, byte[]> batch = new LinkedHashMap<>();
        batch.put("p", bytes("1"));
        batch.put("q", bytes("2"));
        CacheResult<Void> result = engine.putAll(batch, 0);
        Map<String, CacheResult<byte[]>> got = engine.getAll(batch.keySet());
        log.info("判定依据: 批量成功则 getAll 全部 OK; putAll={}, getAll={}", result.status(), got);
        assertTrue(result.isOk());
        got.values().forEach(r -> assertEquals(CacheStatus.OK, r.status()));
    }
}
