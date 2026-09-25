package com.github.lisama569.cachestorageenginejava.tests;

import com.github.lisama569.cachestorageenginejava.core.CacheEngine;
import com.github.lisama569.cachestorageenginejava.core.CacheStatus;
import com.github.lisama569.cachestorageenginejava.core.EngineStats;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 容量与淘汰测试：容量不足时拒绝写入（CAPACITY_FULL），
 * 绝不静默丢弃未过期数据；过期条目优先被回收腾位。
 */
class CapacityEvictionTest {

    private static final Logger log = LoggerFactory.getLogger(CapacityEvictionTest.class);

    @TempDir
    Path dir;

    private CacheEngine newEngine(int capacity, MutableClock clock, String name) {
        return new CacheEngine(capacity, dir.resolve(name), clock);
    }

    @Test
    void capacityFullRejectsWithoutDroppingLiveData() {
        MutableClock clock = MutableClock.startingAtEpoch();
        try (CacheEngine engine = newEngine(2, clock, "a.wal")) {
            engine.put("a", "1", 0);
            engine.put("b", "2", 0);

            CacheStatus rejected = engine.put("c", "3", 0);
            log.info("[输入] 容量=2 已存 a,b; put(c) -> {}（判定：无过期可回收，必须拒绝而非淘汰存活数据）", rejected);
            assertThat(rejected).isEqualTo(CacheStatus.CAPACITY_FULL);

            assertThat(engine.get("a").value()).isEqualTo("1");
            assertThat(engine.get("b").value()).isEqualTo("2");
            assertThat(engine.get("c").status()).isEqualTo(CacheStatus.NOT_FOUND);
            assertThat(engine.size()).isEqualTo(2);

            EngineStats stats = engine.stats();
            log.info("[判定] size={}/{} evictions(拒绝次数)={}", stats.size(), stats.capacity(), stats.evictions());
            assertThat(stats.evictions()).isEqualTo(1);
        }
    }

    @Test
    void overwriteExistingKeySucceedsAtFullCapacity() {
        MutableClock clock = MutableClock.startingAtEpoch();
        try (CacheEngine engine = newEngine(2, clock, "b.wal")) {
            engine.put("a", "1", 0);
            engine.put("b", "2", 0);

            CacheStatus overwrite = engine.put("a", "1-new", 0);
            log.info("[判定] 容量已满时覆盖已有键 a -> {}（覆盖不占新槽位，应成功）", overwrite);
            assertThat(overwrite).isEqualTo(CacheStatus.OK);
            assertThat(engine.get("a").value()).isEqualTo("1-new");
            assertThat(engine.size()).isEqualTo(2);
        }
    }

    @Test
    void expiredEntriesAreReclaimedToMakeRoom() {
        MutableClock clock = MutableClock.startingAtEpoch();
        try (CacheEngine engine = newEngine(2, clock, "c.wal")) {
            engine.put("a", "1", 100);
            engine.put("b", "2", 0);
            clock.advanceMillis(200); // a 过期

            CacheStatus put = engine.put("c", "3", 0);
            log.info("[判定] a 已过期, put(c) -> {}（回收过期槽位后应成功）", put);
            assertThat(put).isEqualTo(CacheStatus.OK);
            assertThat(engine.get("a").status()).isEqualTo(CacheStatus.NOT_FOUND);
            assertThat(engine.get("b").value()).isEqualTo("2");
            assertThat(engine.get("c").value()).isEqualTo("3");
        }
    }

    @Test
    void batchPutIsAtomicWhenCapacityInsufficient() {
        MutableClock clock = MutableClock.startingAtEpoch();
        try (CacheEngine engine = newEngine(3, clock, "d.wal")) {
            engine.put("x", "0", 0);

            Map<String, String> batch = new LinkedHashMap<>();
            batch.put("n1", "1");
            batch.put("n2", "2");
            batch.put("n3", "3");
            CacheStatus status = engine.putBatch(batch, 0);
            log.info("[输入] 容量=3 已存 x; 批量写 3 个新键 -> {}（需 3 个新槽位仅剩 2 个，应整体拒绝）", status);
            assertThat(status).isEqualTo(CacheStatus.CAPACITY_FULL);

            // 原子性：任何一条都不应生效，已有数据不变
            assertThat(engine.get("n1").status()).isEqualTo(CacheStatus.NOT_FOUND);
            assertThat(engine.get("n2").status()).isEqualTo(CacheStatus.NOT_FOUND);
            assertThat(engine.get("n3").status()).isEqualTo(CacheStatus.NOT_FOUND);
            assertThat(engine.get("x").value()).isEqualTo("0");
            assertThat(engine.size()).isEqualTo(1);
            log.info("[判定] 批量拒绝后 size={}，已有键 x 未受影响", engine.size());
        }
    }

    @Test
    void batchPutSucceedsWhenFitting() {
        MutableClock clock = MutableClock.startingAtEpoch();
        try (CacheEngine engine = newEngine(3, clock, "e.wal")) {
            Map<String, String> batch = new LinkedHashMap<>();
            batch.put("a", "1");
            batch.put("b", "2");
            batch.put("c", "3");
            CacheStatus status = engine.putBatch(batch, 0);
            log.info("[判定] 容量=3 批量写 3 键 -> {}", status);
            assertThat(status).isEqualTo(CacheStatus.OK);
            assertThat(engine.size()).isEqualTo(3);
            assertThat(engine.getBatch(batch.keySet()).values())
                    .containsExactlyInAnyOrderEntriesOf(batch);
        }
    }

    @Test
    void batchPutCountsOnlyNewSlots() {
        MutableClock clock = MutableClock.startingAtEpoch();
        try (CacheEngine engine = newEngine(2, clock, "f.wal")) {
            engine.put("a", "1", 0);

            Map<String, String> batch = new LinkedHashMap<>();
            batch.put("a", "1-new"); // 覆盖，不占新槽位
            batch.put("b", "2");     // 新增，占 1 个槽位
            CacheStatus status = engine.putBatch(batch, 0);
            log.info("[判定] 容量=2 已存 a; 批量{a覆盖,b新增} -> {}（仅需 1 个新槽位，应成功）", status);
            assertThat(status).isEqualTo(CacheStatus.OK);
            assertThat(engine.get("a").value()).isEqualTo("1-new");
            assertThat(engine.get("b").value()).isEqualTo("2");
        }
    }

    @Test
    void statsRemainConsistent() {
        MutableClock clock = MutableClock.startingAtEpoch();
        try (CacheEngine engine = newEngine(2, clock, "g.wal")) {
            engine.put("a", "1", 0);
            engine.put("b", "2", 100);
            engine.get("a");          // hit
            engine.get("missing");    // miss
            clock.advanceMillis(200);
            engine.get("b");          // miss + 惰性过期回收
            engine.put("c", "3", 0);  // 占用 a 之外空出的槽位
            engine.put("d", "4", 0);  // 容量满 -> 拒绝

            EngineStats s = engine.stats();
            log.info("[判定] hits={} misses={} expirations={} evictions(拒绝)={} size={}",
                    s.hits(), s.misses(), s.expirations(), s.evictions(), s.size());
            assertThat(s.hits()).isEqualTo(1);
            assertThat(s.misses()).isEqualTo(2);
            assertThat(s.expirations()).isEqualTo(1);
            assertThat(s.evictions()).isEqualTo(1);
            assertThat(s.size()).isEqualTo(2);
        }
    }
}
