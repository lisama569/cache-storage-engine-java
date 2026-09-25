package com.github.lisama569.cachestorageenginejava.tests;

import com.github.lisama569.cachestorageenginejava.core.CacheEngine;
import com.github.lisama569.cachestorageenginejava.core.CacheResult;
import com.github.lisama569.cachestorageenginejava.core.CacheStatus;
import com.github.lisama569.cachestorageenginejava.core.EngineStats;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 并发测试：同一键的更新/过期/删除互斥且可线性化；
 * 无丢失更新、无计数漂移、无死锁。
 */
class ConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyTest.class);

    @TempDir
    Path dir;

    @Test
    void concurrentPutsOnSameKeyNoLostUpdate() throws Exception {
        int threads = 4, writesPerThread = 100;
        try (CacheEngine engine = new CacheEngine(16, dir.resolve("same.wal"), Clock.systemUTC())) {
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int id = t;
                futures.add(pool.submit(() -> {
                    await(start);
                    for (int i = 0; i < writesPerThread; i++) {
                        CacheStatus s = engine.put("hot", "t" + id + "-" + i, 0);
                        assertThat(s).isEqualTo(CacheStatus.OK);
                    }
                }));
            }
            start.countDown();
            for (Future<?> f : futures) {
                f.get(60, TimeUnit.SECONDS);
            }
            pool.shutdown();

            CacheResult<String> r = engine.get("hot");
            log.info("[判定] {} 线程 x {} 次写同一键后: status={}, value={}（必须等于某次写入，不得丢失/损坏）",
                    threads, writesPerThread, r.status(), r.value());
            assertThat(r.status()).isEqualTo(CacheStatus.OK);
            assertThat(r.value()).matches("t\\d+-\\d+");
            assertThat(engine.size()).isEqualTo(1);
        }
    }

    @Test
    void concurrentDistinctKeysExactCountAndRestartConsistent() throws Exception {
        int threads = 8, keysPerThread = 25;
        int total = threads * keysPerThread;
        Path wal = dir.resolve("distinct.wal");
        try (CacheEngine engine = new CacheEngine(1000, wal, Clock.systemUTC())) {
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int id = t;
                futures.add(pool.submit(() -> {
                    await(start);
                    for (int i = 0; i < keysPerThread; i++) {
                        assertThat(engine.put("k-" + id + "-" + i, "v-" + id + "-" + i, 0))
                                .isEqualTo(CacheStatus.OK);
                    }
                }));
            }
            start.countDown();
            for (Future<?> f : futures) {
                f.get(60, TimeUnit.SECONDS);
            }
            pool.shutdown();

            EngineStats s = engine.stats();
            log.info("[判定] 并发写 {} 个不同键后 size={}（不得有计数漂移）", total, s.size());
            assertThat(s.size()).isEqualTo(total);
        }

        // 重启后所有并发写入必须完整恢复
        try (CacheEngine reopened = new CacheEngine(1000, wal, Clock.systemUTC())) {
            int missing = 0;
            for (int t = 0; t < threads; t++) {
                for (int i = 0; i < keysPerThread; i++) {
                    CacheResult<String> r = reopened.get("k-" + t + "-" + i);
                    if (!r.isOk() || !r.value().equals("v-" + t + "-" + i)) {
                        missing++;
                    }
                }
            }
            log.info("[恢复结果] 重启后校验 {} 个键，缺失/不一致 {} 个", total, missing);
            assertThat(missing).isZero();
            assertThat(reopened.size()).isEqualTo(total);
        }
    }

    @Test
    void concurrentReadWriteSameKeyIsLinearizable() throws Exception {
        int writes = 300;
        try (CacheEngine engine = new CacheEngine(16, dir.resolve("lin.wal"), Clock.systemUTC())) {
            engine.put("seq", "0", 0);
            ConcurrentLinkedQueue<String> anomalies = new ConcurrentLinkedQueue<>();
            CountDownLatch start = new CountDownLatch(1);

            ExecutorService pool = Executors.newFixedThreadPool(3);
            Future<?> writer = pool.submit(() -> {
                await(start);
                for (int i = 1; i <= writes; i++) {
                    engine.put("seq", String.valueOf(i), 0);
                }
            });
            List<Future<?>> readers = new ArrayList<>();
            for (int r = 0; r < 2; r++) {
                readers.add(pool.submit(() -> {
                    await(start);
                    long lastSeen = -1;
                    for (int i = 0; i < 2000; i++) {
                        CacheResult<String> res = engine.get("seq");
                        if (res.isOk()) {
                            long v = Long.parseLong(res.value());
                            if (v < lastSeen) {
                                anomalies.add("读到回退: " + lastSeen + " -> " + v);
                            }
                            lastSeen = Math.max(lastSeen, v);
                        }
                    }
                }));
            }
            start.countDown();
            writer.get(60, TimeUnit.SECONDS);
            for (Future<?> f : readers) {
                f.get(60, TimeUnit.SECONDS);
            }
            pool.shutdown();

            log.info("[判定] 单写者递增减读并发，观测到回退异常 {} 次（线性化要求为 0）", anomalies.size());
            assertThat(anomalies).isEmpty();
            assertThat(engine.get("seq").value()).isEqualTo(String.valueOf(writes));
        }
    }

    @Test
    void concurrentCapacityBoundaryIsExact() throws Exception {
        int capacity = 50, threads = 10, attemptsPerThread = 20;
        int attempts = threads * attemptsPerThread;
        try (CacheEngine engine = new CacheEngine(capacity, dir.resolve("cap.wal"), Clock.systemUTC())) {
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger ok = new AtomicInteger();
            AtomicInteger full = new AtomicInteger();
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int id = t;
                futures.add(pool.submit(() -> {
                    await(start);
                    for (int i = 0; i < attemptsPerThread; i++) {
                        CacheStatus s = engine.put("c-" + id + "-" + i, "v", 0);
                        if (s == CacheStatus.OK) {
                            ok.incrementAndGet();
                        } else if (s == CacheStatus.CAPACITY_FULL) {
                            full.incrementAndGet();
                        }
                    }
                }));
            }
            start.countDown();
            for (Future<?> f : futures) {
                f.get(60, TimeUnit.SECONDS);
            }
            pool.shutdown();

            EngineStats s = engine.stats();
            log.info("[判定] 容量={} 并发写 {} 个不同键: OK={} CAPACITY_FULL={} 最终size={}",
                    capacity, attempts, ok.get(), full.get(), s.size());
            assertThat(ok.get() + full.get()).isEqualTo(attempts);
            assertThat(ok.get()).isEqualTo(capacity);          // 恰好放满，不多不少
            assertThat(full.get()).isEqualTo(attempts - capacity);
            assertThat(s.size()).isEqualTo(capacity);
            assertThat(s.evictions()).isEqualTo(attempts - capacity);
        }
    }

    @Test
    void mixedOpsNoDeadlockAndConsistentFinalState() throws Exception {
        MutableClock clock = MutableClock.startingAtEpoch();
        try (CacheEngine engine = new CacheEngine(64, dir.resolve("mix.wal"), clock)) {
            int threads = 8;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger ops = new AtomicInteger();
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int id = t;
                futures.add(pool.submit(() -> {
                    await(start);
                    for (int i = 0; i < 300; i++) {
                        String key = "m-" + (id * 7 + i) % 40; // 键空间重叠，制造竞争
                        switch (i % 5) {
                            case 0 -> engine.put(key, "v" + i, 2000);
                            case 1 -> engine.get(key);
                            case 2 -> engine.delete(key);
                            case 3 -> engine.purgeExpired();
                            default -> engine.stats();
                        }
                        ops.incrementAndGet();
                        if (i % 50 == 0) {
                            clock.advanceMillis(500); // 推进时间制造过期
                        }
                    }
                }));
            }
            start.countDown();
            pool.shutdown();
            boolean terminated = pool.awaitTermination(60, TimeUnit.SECONDS);
            log.info("[判定] 混合并发 {} 次操作后线程池正常终止={}（无死锁）", ops.get(), terminated);
            assertThat(terminated).isTrue();

            // 最终状态一致性：size 连续两次统计一致，且 stats 内部自洽
            int size1 = engine.size();
            int size2 = engine.size();
            EngineStats s = engine.stats();
            log.info("[恢复结果] 最终 size={} (二次确认={}), hits={} misses={} expirations={}",
                    size1, size2, s.hits(), s.misses(), s.expirations());
            assertThat(size1).isEqualTo(size2);
            assertThat(s.size()).isEqualTo(size1);
            assertThat(size1).isLessThanOrEqualTo(64);
        }
    }

    @Test
    void concurrentDeleteAndPutSameKey() throws Exception {
        try (CacheEngine engine = new CacheEngine(16, dir.resolve("pd.wal"), Clock.systemUTC())) {
            int rounds = 200;
            ExecutorService pool = Executors.newFixedThreadPool(2);
            CountDownLatch start = new CountDownLatch(1);
            Set<CacheStatus> putStatuses = new HashSet<>();
            Set<CacheStatus> delStatuses = new HashSet<>();
            Future<?> putter = pool.submit(() -> {
                await(start);
                for (int i = 0; i < rounds; i++) {
                    synchronized (putStatuses) {
                        putStatuses.add(engine.put("contended", "v" + i, 0));
                    }
                }
            });
            Future<?> deleter = pool.submit(() -> {
                await(start);
                for (int i = 0; i < rounds; i++) {
                    synchronized (delStatuses) {
                        delStatuses.add(engine.delete("contended"));
                    }
                }
            });
            start.countDown();
            putter.get(60, TimeUnit.SECONDS);
            deleter.get(60, TimeUnit.SECONDS);
            pool.shutdown();

            log.info("[判定] 并发 put/delete 同一键: put状态集合={} delete状态集合={}",
                    putStatuses, delStatuses);
            // put 只允许 OK；delete 只允许 OK 或 NOT_FOUND（不得出现 EXPIRED/CAPACITY_FULL 等异常态）
            assertThat(putStatuses).containsExactly(CacheStatus.OK);
            assertThat(delStatuses).isSubsetOf(CacheStatus.OK, CacheStatus.NOT_FOUND);
            // 最终状态与最后一次操作一致：要么存在且可读，要么不存在
            CacheResult<String> r = engine.get("contended");
            assertThat(r.status()).isIn(CacheStatus.OK, CacheStatus.NOT_FOUND);
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
