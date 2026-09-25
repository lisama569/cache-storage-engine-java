package com.github.lisama569.cachestorageenginejava.tests.cache;

import com.github.lisama569.cachestorageenginejava.cache.CacheEngine;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class CacheEngineConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(CacheEngineConcurrencyTest.class);

    @TempDir
    Path dir;

    private CacheProperties props;
    private CacheEngine engine;

    @BeforeEach
    void setUp() {
        props = new CacheProperties();
        props.setMaxEntries(0);
        props.setMaxBytes(0);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (engine != null) {
            engine.close();
        }
    }

    private CacheEngine newEngine() throws Exception {
        CacheEngine e = new CacheEngine(props, new WriteAheadLog(dir.resolve("cache.wal")), new MutableClock(0));
        e.recover();
        return e;
    }

    @Test
    void concurrentPutsNoLostUpdatesNoCountDrift() throws Exception {
        engine = newEngine();
        int threads = 8, perThread = 200;
        log.info("输入: {} 个线程各 put {} 个互不相同的键, 然后逐个 get 校验", threads, perThread);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int id = t;
            futures.add(pool.submit(() -> {
                start.await();
                for (int i = 0; i < perThread; i++) {
                    assertTrue(engine.put("k-" + id + "-" + i,
                            ("v" + i).getBytes(StandardCharsets.UTF_8), 0).isOk());
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS); // 超时即视为死锁
        }
        pool.shutdown();

        int ok = 0;
        for (int t = 0; t < threads; t++) {
            for (int i = 0; i < perThread; i++) {
                if (engine.get("k-" + t + "-" + i).status() == CacheStatus.OK) {
                    ok++;
                }
            }
        }
        log.info("判定依据: 无丢失更新则命中数==总写入数=={}, 且 size()=={}; 实际命中={}, size={}",
                threads * perThread, threads * perThread, ok, engine.size());
        assertEquals(threads * perThread, ok);
        assertEquals(threads * perThread, engine.size());
    }

    @Test
    void concurrentPutRemoveSameKeysTerminatesConsistently() throws Exception {
        engine = newEngine();
        int threads = 8, rounds = 500, keySpace = 50;
        log.info("输入: {} 个线程对 {} 个共享键并发 put/remove 各 {} 轮", threads, keySpace, rounds);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int id = t;
            futures.add(pool.submit(() -> {
                start.await();
                for (int i = 0; i < rounds; i++) {
                    String key = "shared-" + (i % keySpace);
                    if ((id + i) % 2 == 0) {
                        engine.put(key, ("t" + id).getBytes(StandardCharsets.UTF_8), 0);
                    } else {
                        engine.remove(key);
                    }
                }
                return null;
            }));
        }
        start.countDown();
        assertTimeoutPreemptively(Duration.ofSeconds(60), () -> {
            for (Future<?> f : futures) {
                f.get();
            }
        }, "并发 put/remove 未在限定时间内完成, 疑似死锁");
        pool.shutdown();

        // 线性一致性验证: size() 必须等于实际可命中的键数（计数不漂移）
        int live = 0;
        for (int i = 0; i < keySpace; i++) {
            if (engine.get("shared-" + i).status() == CacheStatus.OK) {
                live++;
            }
        }
        log.info("判定依据: size() 应等于实际命中键数; size={}, 实际命中={}", engine.size(), live);
        assertEquals(live, engine.size());
    }

    @Test
    void concurrentWritesUnderCapacityNeverExceedLimit() throws Exception {
        props.setMaxEntries(100);
        engine = newEngine();
        int threads = 8, perThread = 300;
        log.info("输入: maxEntries=100, {} 个线程各 put {} 个键, 触发并发淘汰", threads, perThread);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int id = t;
            futures.add(pool.submit(() -> {
                start.await();
                for (int i = 0; i < perThread; i++) {
                    engine.put("cap-" + id + "-" + i, new byte[8], 0);
                }
                return null;
            }));
        }
        start.countDown();
        assertTimeoutPreemptively(Duration.ofSeconds(60), () -> {
            for (Future<?> f : futures) {
                f.get();
            }
        }, "并发淘汰未在限定时间内完成, 疑似死锁");
        pool.shutdown();
        log.info("判定依据: 淘汰后 size() 不得超过 maxEntries=100; 实际 size={}", engine.size());
        assertTrue(engine.size() <= 100, "size 超过容量上限: " + engine.size());
    }
}
