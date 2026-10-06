package com.github.lisama569.cachestorageenginejava.tests;

import com.github.lisama569.cachestorageenginejava.core.CacheEngine;
import com.github.lisama569.cachestorageenginejava.core.CacheStatus;
import com.github.lisama569.cachestorageenginejava.core.EngineStats;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 空间容量边界测试：每个空间独立容量、互不挤占；写满只影响本空间；
 * 拒绝原因可区分；不静默丢弃未过期数据；容量 0 空间的极端行为确定可预期。
 */
class NamespaceCapacityTest {

    private static final Logger log = LoggerFactory.getLogger(NamespaceCapacityTest.class);

    @TempDir
    Path dir;

    @Test
    void fullSpaceOnlyAffectsItself() {
        try (CacheEngine e = new CacheEngine(4, Map.of("tiny", 2), dir.resolve("cap1.wal"),
                MutableClock.startingAtEpoch())) {
            assertThat(e.put("tiny", "a", "1", 0)).isEqualTo(CacheStatus.OK);
            assertThat(e.put("tiny", "b", "2", 0)).isEqualTo(CacheStatus.OK);

            // tiny 写满：第三个键被拒绝，原因与 NOT_FOUND/EXPIRED 可区分
            CacheStatus rejected = e.put("tiny", "c", "3", 0);
            log.info("[输入] tiny 空间容量=2 已存 a,b; put(tiny.c) -> {}（判定：只拒绝本空间新写入）", rejected);
            assertThat(rejected).isEqualTo(CacheStatus.CAPACITY_FULL);

            // 其它空间照常可用，互不挤占
            assertThat(e.put("roomy", "c", "3", 0)).isEqualTo(CacheStatus.OK);
            assertThat(e.put("c", "3", 0)).isEqualTo(CacheStatus.OK); // 默认空间
            log.info("[判定] tiny 写满后: roomy.c={} default.c={}（其它空间照常写入）",
                    e.get("roomy", "c").status(), e.get("c").status());

            // tiny 内未过期数据未被静默丢弃
            assertThat(e.get("tiny", "a").value()).isEqualTo("1");
            assertThat(e.get("tiny", "b").value()).isEqualTo("2");
            assertThat(e.get("tiny", "c").status()).isEqualTo(CacheStatus.NOT_FOUND);
            assertThat(e.stats("tiny").evictions()).isEqualTo(1);
            assertThat(e.stats("roomy").evictions()).isZero();
        }
    }

    @Test
    void overwriteInFullSpaceStillWorks() {
        try (CacheEngine e = new CacheEngine(4, Map.of("tiny", 1), dir.resolve("cap2.wal"),
                MutableClock.startingAtEpoch())) {
            assertThat(e.put("tiny", "k", "v1", 0)).isEqualTo(CacheStatus.OK);
            // 容量满时覆盖已有键不占新槽位，仍可成功
            CacheStatus overwrite = e.put("tiny", "k", "v2", 0);
            log.info("[输入] tiny 容量=1 已存 k; 覆盖 put(tiny.k) -> {}（判定：覆盖不占新槽位）", overwrite);
            assertThat(overwrite).isEqualTo(CacheStatus.OK);
            assertThat(e.get("tiny", "k").value()).isEqualTo("v2");
            assertThat(e.stats("tiny").size()).isEqualTo(1);
        }
    }

    @Test
    void expiredEntriesFreeSlotsOnlyWithinOwnSpace() {
        MutableClock clock = MutableClock.startingAtEpoch();
        try (CacheEngine e = new CacheEngine(4, Map.of("tiny", 2), dir.resolve("cap3.wal"), clock)) {
            e.put("tiny", "x", "1", 100);   // 将过期
            e.put("tiny", "y", "2", 0);     // 永不过期
            clock.advanceMillis(200);       // tiny.x 过期

            // 容量判定前回收本空间过期条目 -> 新写入腾位成功
            CacheStatus afterPurge = e.put("tiny", "z", "3", 0);
            log.info("[输入] tiny 容量=2, x 已过期 y 存活; put(tiny.z) -> {}（判定：过期槽位在本空间内回收腾位）",
                    afterPurge);
            assertThat(afterPurge).isEqualTo(CacheStatus.OK);
            assertThat(e.get("tiny", "y").value()).isEqualTo("2"); // 未过期数据不动
            assertThat(e.stats("tiny").size()).isEqualTo(2);
        }
    }

    @Test
    void zeroCapacitySpaceRejectsDeterministically() {
        // 极端情形：空间总容量为 0，任何单条数据都放不进 -> 结果必须明确、可预期
        try (CacheEngine e = new CacheEngine(4, Map.of("sealed", 0), dir.resolve("cap4.wal"),
                MutableClock.startingAtEpoch())) {
            CacheStatus single = e.put("sealed", "k", "v", 0);
            CacheStatus batch = e.putBatch("sealed", Map.of("k2", "v2"), 0);
            CacheStatus read = e.get("sealed", "k").status();
            log.info("[输入] sealed 空间容量=0; [判定] 单写 -> {}, 批量写 -> {}, 读 -> {}（确定性拒绝，无歧义）",
                    single, batch, read);
            assertThat(single).isEqualTo(CacheStatus.CAPACITY_FULL);
            assertThat(batch).isEqualTo(CacheStatus.CAPACITY_FULL);
            assertThat(read).isEqualTo(CacheStatus.NOT_FOUND);

            EngineStats s = e.stats("sealed");
            assertThat(s.size()).isZero();
            assertThat(s.capacity()).isZero();
            assertThat(s.evictions()).isEqualTo(2); // 1 次单写 + 批量 1 条

            // 容量 0 不影响其它空间
            assertThat(e.put("open", "k", "v", 0)).isEqualTo(CacheStatus.OK);
        }
    }

    @Test
    void batchLargerThanSpaceCapacityRejectedAtomically() {
        try (CacheEngine e = new CacheEngine(4, Map.of("tiny", 2), dir.resolve("cap5.wal"),
                MutableClock.startingAtEpoch())) {
            e.put("tiny", "keep", "0", 0);
            // 批量 2 个新键 + 已有 1 条 = 3 > 容量 2 -> 整体拒绝
            CacheStatus rejected = e.putBatch("tiny", Map.of("n1", "1", "n2", "2"), 0);
            log.info("[输入] tiny 容量=2 已存 keep; 批量写 2 个新键 -> {}（判定：整体拒绝，一个都不生效）", rejected);
            assertThat(rejected).isEqualTo(CacheStatus.CAPACITY_FULL);
            assertThat(e.get("tiny", "n1").status()).isEqualTo(CacheStatus.NOT_FOUND);
            assertThat(e.get("tiny", "n2").status()).isEqualTo(CacheStatus.NOT_FOUND);
            assertThat(e.get("tiny", "keep").value()).isEqualTo("0");
            assertThat(e.stats("tiny").size()).isEqualTo(1);
        }
    }

    @Test
    void negativeCapacityRejectedAndReconfigurationWorks() {
        try (CacheEngine e = new CacheEngine(4, dir.resolve("cap6.wal"), MutableClock.startingAtEpoch())) {
            try {
                e.registerNamespace("bad", -1);
                throw new AssertionError("负数容量应被拒绝");
            } catch (IllegalArgumentException ex) {
                log.info("[判定] 注册负数容量被参数校验拒绝: {}", ex.getMessage());
            }
            // 运行期调整空间容量：只约束后续新写入，已有数据不动
            e.put("adj", "k1", "1", 0);
            e.put("adj", "k2", "2", 0);
            e.registerNamespace("adj", 2);
            CacheStatus full = e.put("adj", "k3", "3", 0);
            log.info("[输入] adj 空间先写 2 条再调容量为 2; put(adj.k3) -> {}（判定：新容量即刻生效，已有数据保留）",
                    full);
            assertThat(full).isEqualTo(CacheStatus.CAPACITY_FULL);
            assertThat(e.get("adj", "k1").value()).isEqualTo("1");
            assertThat(e.get("adj", "k2").value()).isEqualTo("2");
        }
    }
}
