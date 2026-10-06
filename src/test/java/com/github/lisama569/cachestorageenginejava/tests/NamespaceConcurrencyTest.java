package com.github.lisama569.cachestorageenginejava.tests;

import com.github.lisama569.cachestorageenginejava.core.CacheEngine;
import com.github.lisama569.cachestorageenginejava.core.CacheStatus;
import com.github.lisama569.cachestorageenginejava.core.EngineStats;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 跨空间并发测试：多线程在多个空间同时读写，不丢更新、计数不错乱、不死锁。
 */
class NamespaceConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(NamespaceConcurrencyTest.class);

    @TempDir
    Path dir;

    @Test
    void concurrentWritesAcrossNamespacesLoseNothing() throws Exception {
        int namespaces = 4, threadsPerNs = 4, keysPerThread = 50;
        int total = namespaces * threadsPerNs * keysPerThread;
        try (CacheEngine engine = new CacheEngine(total, dir.resolve("ncc1.wal"), Clock.systemUTC())) {
            ExecutorService pool = Executors.newFixedThreadPool(namespaces * threadsPerNs);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            for (int ns = 0; ns < namespaces; ns++) {
                String namespace = "ns-" + ns;
                for (int t = 0; t < threadsPerNs; t++) {
                    int threadId = t;
                    futures.add(pool.submit(() -> {
                        try {
                            start.await();
                            for (int i = 0; i < keysPerThread; i++) {
                                String key = "t" + threadId + "-k" + i;
                                CacheStatus s = engine.put(namespace, key, "v" + i, 0);
                                assertThat(s).isEqualTo(CacheStatus.OK);
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }));
                }
            }
            log.info("[输入] {} 个空间 x {} 线程 x {} 键 = {} 次并发写入，同一起跑线放行",
                    namespaces, threadsPerNs, keysPerThread, total);
            start.countDown();
            for (Future<?> f : futures) {
                f.get(60, TimeUnit.SECONDS); // 超时即视为死锁/挂起
            }
            pool.shutdown();

            for (int ns = 0; ns < namespaces; ns++) {
                int expected = threadsPerNs * keysPerThread;
                int actual = engine.size("ns-" + ns);
                log.info("[判定] 空间 ns-{}: 存活 {}/{}（应精确相等，无计数漂移）", ns, actual, expected);
                assertThat(actual).isEqualTo(expected);
            }
            assertThat(engine.size()).isEqualTo(total);
            // 抽查每个空间的数据完整可读
            for (int ns = 0; ns < namespaces; ns++) {
                assertThat(engine.get("ns-" + ns, "t0-k0").value()).isEqualTo("v0");
            }
        }
    }

    @Test
    void concurrentCapacityRacePerNamespaceIsExact() throws Exception {
        int capacityPerNs = 20, namespaces = 3, threads = 12, attemptsPerThread = 10;
        int attempts = threads * attemptsPerThread;
        try (CacheEngine engine = new CacheEngine(capacityPerNs, dir.resolve("ncc2.wal"),
                Clock.systemUTC())) {
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger ok = new AtomicInteger();
            AtomicInteger full = new AtomicInteger();
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int threadId = t;
                futures.add(pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < attemptsPerThread; i++) {
                            // 每个线程轮询打到不同空间，同一批键名跨空间复用
                            String ns = "race-" + (i % namespaces);
                            CacheStatus s = engine.put(ns, "t" + threadId + "-k" + i, "v", 0);
                            if (s == CacheStatus.OK) {
                                ok.incrementAndGet();
                            } else if (s == CacheStatus.CAPACITY_FULL) {
                                full.incrementAndGet();
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }));
            }
            log.info("[输入] {} 线程 x {} 次写入，轮询 {} 个空间（每空间容量 {}），同一起跑线放行",
                    threads, attemptsPerThread, namespaces, capacityPerNs);
            start.countDown();
            for (Future<?> f : futures) {
                f.get(60, TimeUnit.SECONDS);
            }
            pool.shutdown();

            // 每个线程给每个空间写 attemptsPerThread/namespaces 个不同键名（键名含 i，跨空间复用）
            int distinctKeysPerNs = threads * (attemptsPerThread / namespaces);
            int expectedOkPerNs = Math.min(capacityPerNs, distinctKeysPerNs);
            log.info("[判定] 全局 ok={} full={}（每空间恰好放满 {} 条）",
                    ok.get(), full.get(), expectedOkPerNs);
            assertThat(ok.get()).isEqualTo(expectedOkPerNs * namespaces);
            assertThat(full.get()).isEqualTo(attempts - ok.get());
            for (int ns = 0; ns < namespaces; ns++) {
                EngineStats s = engine.stats("race-" + ns);
                log.info("[判定] 空间 race-{}: size={}/{} evictions={}",
                        ns, s.size(), s.capacity(), s.evictions());
                assertThat(s.size()).isEqualTo(expectedOkPerNs);
            }
            assertThat(engine.size()).isEqualTo(expectedOkPerNs * namespaces);
        }
    }

    @Test
    void mixedCrossNamespaceLoadStaysConsistent() throws Exception {
        int namespaces = 3, threads = 9, opsPerThread = 300;
        try (CacheEngine engine = new CacheEngine(64, dir.resolve("ncc3.wal"), Clock.systemUTC())) {
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int threadId = t;
                futures.add(pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < opsPerThread; i++) {
                            String ns = "mix-" + (threadId % namespaces);
                            String key = "k" + (i % 32);
                            switch (i % 4) {
                                case 0 -> engine.put(ns, key, "v" + i, 0);
                                case 1 -> engine.get(ns, key);
                                case 2 -> engine.delete(ns, key);
                                default -> engine.stats(ns);
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }));
            }
            log.info("[输入] {} 线程跨 {} 空间混合 put/get/delete/stats 各 {} 次",
                    threads, namespaces, opsPerThread);
            start.countDown();
            for (Future<?> f : futures) {
                f.get(60, TimeUnit.SECONDS);
            }
            pool.shutdown();

            // 最终状态自洽：连续两次统计一致，且各空间之和等于全局
            int sum = 0;
            for (int ns = 0; ns < namespaces; ns++) {
                EngineStats s1 = engine.stats("mix-" + ns);
                EngineStats s2 = engine.stats("mix-" + ns);
                log.info("[判定] 空间 mix-{}: size={}（两次统计一致={}）", ns, s1.size(),
                        s1.size() == s2.size());
                assertThat(s1.size()).isEqualTo(s2.size());
                sum += s1.size();
            }
            assertThat(engine.size()).isEqualTo(sum);
        }
    }
}
