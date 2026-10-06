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
 * 跨空间并发测试：多线程在不同空间同时读写，不丢更新、计数不漂移、无死锁；
 * 各空间容量边界在并发下仍然精确。
 */
class NamespaceConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(NamespaceConcurrencyTest.class);

    @TempDir
    Path dir;

    @Test
    void concurrentCrossSpaceWritesExactCounts() throws Exception {
        String[] spaces = {"order", "payment", "stock", "default"};
        int threads = 8, keysPerThread = 30;
        int total = threads * keysPerThread;
        Path wal = dir.resolve("ns-conc1.wal");
        try (CacheEngine e = new CacheEngine(1000, wal, Clock.systemUTC())) {
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int id = t;
                futures.add(pool.submit(() -> {
                    await(start);
                    for (int i = 0; i < keysPerThread; i++) {
                        // 每个线程往所有空间写同名键：同名键在不同空间必须各自独立成立
                        for (String ns : spaces) {
                            assertThat(e.put(ns, "k-" + id + "-" + i, ns + "-v-" + id + "-" + i, 0))
                                    .isEqualTo(CacheStatus.OK);
                        }
                    }
                }));
            }
            start.countDown();
            for (Future<?> f : futures) {
                f.get(60, TimeUnit.SECONDS);
            }
            pool.shutdown();

            for (String ns : spaces) {
                EngineStats s = e.stats(ns);
                log.info("[判定] 空间 {} 并发写入后 size={}（期望 {}，不得漂移）", ns, s.size(), total);
                assertThat(s.size()).isEqualTo(total);
            }
            // 抽查同名键在各空间的值确实各归各
            assertThat(e.get("order", "k-3-7").value()).isEqualTo("order-v-3-7");
            assertThat(e.get("payment", "k-3-7").value()).isEqualTo("payment-v-3-7");
        }

        // 重启后跨空间并发写入的数据完整恢复
        try (CacheEngine reopened = new CacheEngine(1000, wal, Clock.systemUTC())) {
            int mismatches = 0;
            for (String ns : spaces) {
                for (int t = 0; t < threads; t++) {
                    for (int i = 0; i < keysPerThread; i++) {
                        var r = reopened.get(ns, "k-" + t + "-" + i);
                        if (!r.isOk() || !r.value().equals(ns + "-v-" + t + "-" + i)) {
                            mismatches++;
                        }
                    }
                }
                log.info("[恢复结果] 重启后空间 {} size={}（期望 {}）", ns, reopened.size(ns), total);
                assertThat(reopened.size(ns)).isEqualTo(total);
            }
            assertThat(mismatches).isZero();
        }
    }

    @Test
    void concurrentCapacityBoundaryExactPerSpace() throws Exception {
        int tinyCap = 20, threads = 10, attemptsPerThread = 10;
        int attempts = threads * attemptsPerThread;
        try (CacheEngine e = new CacheEngine(1000, java.util.Map.of("tiny", tinyCap),
                dir.resolve("ns-conc2.wal"), Clock.systemUTC())) {
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger tinyOk = new AtomicInteger();
            AtomicInteger tinyFull = new AtomicInteger();
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int id = t;
                futures.add(pool.submit(() -> {
                    await(start);
                    for (int i = 0; i < attemptsPerThread; i++) {
                        // 同一批线程同时挤压小空间与默认空间
                        CacheStatus s = e.put("tiny", "c-" + id + "-" + i, "v", 0);
                        if (s == CacheStatus.OK) {
                            tinyOk.incrementAndGet();
                        } else if (s == CacheStatus.CAPACITY_FULL) {
                            tinyFull.incrementAndGet();
                        }
                        assertThat(e.put("roomy-" + id + "-" + i, "v", 0)).isEqualTo(CacheStatus.OK);
                    }
                }));
            }
            start.countDown();
            for (Future<?> f : futures) {
                f.get(60, TimeUnit.SECONDS);
            }
            pool.shutdown();

            EngineStats tiny = e.stats("tiny");
            EngineStats dflt = e.stats();
            log.info("[判定] tiny 容量={} 并发写 {} 个键: OK={} FULL={} size={}; 默认空间 size={}（不受挤压）",
                    tinyCap, attempts, tinyOk.get(), tinyFull.get(), tiny.size(), dflt.size());
            assertThat(tinyOk.get() + tinyFull.get()).isEqualTo(attempts);
            assertThat(tinyOk.get()).isEqualTo(tinyCap);            // 恰好放满
            assertThat(tinyFull.get()).isEqualTo(attempts - tinyCap);
            assertThat(tiny.size()).isEqualTo(tinyCap);
            assertThat(tiny.evictions()).isEqualTo(attempts - tinyCap);
            assertThat(dflt.size()).isEqualTo(attempts);            // 默认空间全数写入
        }
    }

    @Test
    void mixedCrossSpaceOpsNoDeadlockConsistentState() throws Exception {
        MutableClock clock = MutableClock.startingAtEpoch();
        String[] spaces = {"s1", "s2", "s3"};
        try (CacheEngine e = new CacheEngine(64, dir.resolve("ns-conc3.wal"), clock)) {
            int threads = 9; // 3 的倍数，均匀落到 3 个空间
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger ops = new AtomicInteger();
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int id = t;
                String ns = spaces[id % spaces.length];
                futures.add(pool.submit(() -> {
                    await(start);
                    for (int i = 0; i < 300; i++) {
                        String key = "m-" + (id * 7 + i) % 40;
                        switch (i % 6) {
                            case 0 -> e.put(ns, key, "v" + i, 2000);
                            case 1 -> e.get(ns, key);
                            case 2 -> e.delete(ns, key);
                            case 3 -> e.purgeExpired(ns);
                            case 4 -> e.stats(ns);
                            default -> e.putBatch(ns, java.util.Map.of(key, "b" + i), 1000);
                        }
                        ops.incrementAndGet();
                        if (i % 50 == 0) {
                            clock.advanceMillis(500);
                        }
                    }
                }));
            }
            start.countDown();
            pool.shutdown();
            boolean terminated = pool.awaitTermination(60, TimeUnit.SECONDS);
            log.info("[判定] 跨空间混合并发 {} 次操作后线程池正常终止={}（无死锁）", ops.get(), terminated);
            assertThat(terminated).isTrue();

            // 最终状态一致性：各空间 size 连续统计稳定且与 stats 自洽
            for (String ns : spaces) {
                int size1 = e.size(ns);
                int size2 = e.size(ns);
                EngineStats s = e.stats(ns);
                log.info("[恢复结果] 空间 {} 最终 size={} (二次确认={}), hits={} misses={} expirations={} evictions={}",
                        ns, size1, size2, s.hits(), s.misses(), s.expirations(), s.evictions());
                assertThat(size1).isEqualTo(size2);
                assertThat(s.size()).isEqualTo(size1);
                assertThat(size1).isLessThanOrEqualTo(64);
            }
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
