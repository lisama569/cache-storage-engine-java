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
 * 空间容量边界测试：每空间独立上限、互不挤占；写满只影响本空间新写入；
 * 不静默丢弃未过期数据；极端情况（批量超过总容量、容量为 0）结果确定。
 */
class NamespaceCapacityTest {

    private static final Logger log = LoggerFactory.getLogger(NamespaceCapacityTest.class);

    @TempDir
    Path dir;

    @Test
    void fullNamespaceDoesNotAffectOthers() {
        MutableClock clock = MutableClock.startingAtEpoch();
        try (CacheEngine e = new CacheEngine(2, dir.resolve("cap1.wal"), clock)) {
            e.put("nsA", "a1", "1", 0);
            e.put("nsA", "a2", "2", 0);

            CacheStatus rejected = e.put("nsA", "a3", "3", 0);
            CacheStatus otherNs = e.put("nsB", "b1", "1", 0);
            log.info("[判定] nsA 容量=2 写满后 put(nsA,a3)->{}（应 CAPACITY_FULL）; "
                    + "put(nsB,b1)->{}（其它空间应照常 OK）", rejected, otherNs);
            assertThat(rejected).isEqualTo(CacheStatus.CAPACITY_FULL);
            assertThat(otherNs).isEqualTo(CacheStatus.OK);

            // 失败原因与键不存在/已过期可区分
            assertThat(e.get("nsA", "a3").status()).isEqualTo(CacheStatus.NOT_FOUND);
            // 未过期数据没有被悄悄丢掉
            assertThat(e.get("nsA", "a1").value()).isEqualTo("1");
            assertThat(e.get("nsA", "a2").value()).isEqualTo("2");
            assertThat(e.size("nsA")).isEqualTo(2);
            assertThat(e.size("nsB")).isEqualTo(1);
        }
    }

    @Test
    void perNamespaceCapacityOverrides() {
        MutableClock clock = MutableClock.startingAtEpoch();
        Map<String, Integer> overrides = Map.of("tiny", 1, "zero", 0);
        try (CacheEngine e = new CacheEngine(4, overrides, dir.resolve("cap2.wal"), clock)) {
            log.info("[输入] 默认容量=4, 覆盖 tiny=1, zero=0");
            assertThat(e.capacityOf("tiny")).isEqualTo(1);
            assertThat(e.capacityOf("zero")).isZero();
            assertThat(e.capacityOf("others")).isEqualTo(4);

            // tiny 空间只能放 1 条
            assertThat(e.put("tiny", "t1", "1", 0)).isEqualTo(CacheStatus.OK);
            CacheStatus tinyFull = e.put("tiny", "t2", "2", 0);
            log.info("[判定] tiny(容量1) 第二条写入 -> {}", tinyFull);
            assertThat(tinyFull).isEqualTo(CacheStatus.CAPACITY_FULL);

            // 容量 0 的空间：任何新键写入都被确定性拒绝，且原因可与 NOT_FOUND 区分
            CacheStatus zeroPut = e.put("zero", "z1", "1", 0);
            log.info("[判定] zero(容量0) 写入 -> {}（极端情况：单条数据即超空间总容量，确定性拒绝）", zeroPut);
            assertThat(zeroPut).isEqualTo(CacheStatus.CAPACITY_FULL);
            assertThat(e.get("zero", "z1").status()).isEqualTo(CacheStatus.NOT_FOUND);

            // 默认空间不受影响
            assertThat(e.put("others", "o1", "1", 0)).isEqualTo(CacheStatus.OK);
        }
    }

    @Test
    void batchLargerThanNamespaceCapacityIsRejectedAtomically() {
        MutableClock clock = MutableClock.startingAtEpoch();
        try (CacheEngine e = new CacheEngine(2, dir.resolve("cap3.wal"), clock)) {
            e.put("nsA", "keep", "v", 0);

            Map<String, String> batch = new LinkedHashMap<>();
            batch.put("n1", "1");
            batch.put("n2", "2");
            batch.put("n3", "3"); // 已有 1 条 + 3 个新键 > 容量 2
            CacheStatus status = e.putBatch("nsA", batch, 0);
            log.info("[判定] 批量 3 个新键写入容量剩 1 的 nsA -> {}（应整体拒绝）", status);
            assertThat(status).isEqualTo(CacheStatus.CAPACITY_FULL);

            // 整体拒绝：已有数据不变、批量一条都没写入
            assertThat(e.get("nsA", "keep").value()).isEqualTo("v");
            assertThat(e.get("nsA", "n1").status()).isEqualTo(CacheStatus.NOT_FOUND);
            assertThat(e.get("nsA", "n2").status()).isEqualTo(CacheStatus.NOT_FOUND);
            assertThat(e.get("nsA", "n3").status()).isEqualTo(CacheStatus.NOT_FOUND);
            assertThat(e.size("nsA")).isEqualTo(1);

            // 同样的批量放到空的空间则整体成功 —— 拒绝只由所属空间容量决定
            CacheStatus ok = e.putBatch("nsB", Map.of("n1", "1", "n2", "2"), 0);
            log.info("[判定] 同样批量写入空的 nsB(容量2) -> {}", ok);
            assertThat(ok).isEqualTo(CacheStatus.OK);
            assertThat(e.size("nsB")).isEqualTo(2);
        }
    }

    @Test
    void expiredEntriesAreReclaimedWithinTheirOwnNamespace() {
        MutableClock clock = MutableClock.startingAtEpoch();
        try (CacheEngine e = new CacheEngine(2, dir.resolve("cap4.wal"), clock)) {
            e.put("nsA", "temp", "t", 100); // 会过期
            e.put("nsA", "live", "v", 0);
            clock.advanceMillis(200); // temp 过期

            CacheStatus put = e.put("nsA", "new", "n", 0);
            log.info("[判定] nsA 内 temp 已过期, put(nsA,new) -> {}（回收本空间过期槽位后应成功）", put);
            assertThat(put).isEqualTo(CacheStatus.OK);
            assertThat(e.get("nsA", "temp").status()).isEqualTo(CacheStatus.NOT_FOUND);
            assertThat(e.size("nsA")).isEqualTo(2);

            // 覆盖已有键不占新槽位，写满时也能成功
            assertThat(e.put("nsA", "live", "v2", 0)).isEqualTo(CacheStatus.OK);
            assertThat(e.get("nsA", "live").value()).isEqualTo("v2");

            EngineStats sa = e.stats("nsA");
            log.info("[判定] nsA 统计: size={}/{} expirations={} evictions={}",
                    sa.size(), sa.capacity(), sa.expirations(), sa.evictions());
            assertThat(sa.expirations()).isEqualTo(1);
            assertThat(sa.evictions()).isZero();
        }
    }

    @Test
    void capacityFullIsDistinctFromNotFoundAndExpired() {
        MutableClock clock = MutableClock.startingAtEpoch();
        try (CacheEngine e = new CacheEngine(1, dir.resolve("cap5.wal"), clock)) {
            e.put("nsA", "only", "1", 0);
            CacheStatus full = e.put("nsA", "extra", "2", 0);
            CacheStatus missing = e.delete("nsA", "ghost");
            log.info("[判定] 写满拒绝={}（CAPACITY_FULL）; 删不存在的键={}（NOT_FOUND）—— 原因可区分",
                    full, missing);
            assertThat(full).isEqualTo(CacheStatus.CAPACITY_FULL);
            assertThat(missing).isEqualTo(CacheStatus.NOT_FOUND);
        }
    }
}
