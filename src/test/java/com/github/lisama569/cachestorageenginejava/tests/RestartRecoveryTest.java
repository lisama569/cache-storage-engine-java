package com.github.lisama569.cachestorageenginejava.tests;

import com.github.lisama569.cachestorageenginejava.core.CacheEngine;
import com.github.lisama569.cachestorageenginejava.core.CacheStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 异常重启与恢复测试：WAL 尾部不完整/损坏数据被识别并忽略，
 * 不导致启动失败、不读出脏数据；有效前缀完整恢复。
 */
class RestartRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(RestartRecoveryTest.class);

    /** v2 帧固定区长度：type(1)+nsLen(4)+keyLen(4)+valueLen(4)+expireAt(8)+crc(4)。 */
    private static final int FRAME_FIXED = 25;
    private static final int HEADER = 4;
    /** 默认空间名 "default" 的 UTF-8 字节长度。 */
    private static final int NS_LEN = 7;

    @TempDir
    Path dir;

    @Test
    void restartRestoresLiveEntries() {
        MutableClock clock = MutableClock.startingAtEpoch();
        Path wal = dir.resolve("r1.wal");
        try (CacheEngine e = new CacheEngine(16, wal, clock)) {
            e.put("persist", "p1", 0);
            e.put("short", "s1", 100);
            e.putBatch(java.util.Map.of("b1", "1", "b2", "2"), 0);
        }
        try (CacheEngine reopened = new CacheEngine(16, wal, clock)) {
            log.info("[恢复结果] persist={} short={} b1={} b2={}",
                    reopened.get("persist").status(), reopened.get("short").status(),
                    reopened.get("b1").status(), reopened.get("b2").status());
            assertThat(reopened.get("persist").value()).isEqualTo("p1");
            assertThat(reopened.get("short").value()).isEqualTo("s1"); // 时钟未推进，仍存活
            assertThat(reopened.get("b1").value()).isEqualTo("1");
            assertThat(reopened.get("b2").value()).isEqualTo("2");
            assertThat(reopened.size()).isEqualTo(4);
        }
    }

    @Test
    void deletePersistsAcrossRestart() {
        Path wal = dir.resolve("r2.wal");
        try (CacheEngine e = new CacheEngine(16, wal, Clock.systemUTC())) {
            e.put("a", "1", 0);
            e.put("b", "2", 0);
            assertThat(e.delete("a")).isEqualTo(CacheStatus.OK);
        }
        try (CacheEngine reopened = new CacheEngine(16, wal, Clock.systemUTC())) {
            log.info("[恢复结果] 删除后重启: a={} b={}",
                    reopened.get("a").status(), reopened.get("b").status());
            assertThat(reopened.get("a").status()).isEqualTo(CacheStatus.NOT_FOUND);
            assertThat(reopened.get("b").value()).isEqualTo("2");
        }
    }

    @Test
    void truncatedTailFrameIsIgnoredAndFileReusable() throws IOException {
        Path wal = dir.resolve("r3.wal");
        try (CacheEngine e = new CacheEngine(16, wal, Clock.systemUTC())) {
            e.put("k1", "v1", 0);
            e.put("k2", "v2", 0);
        }
        long sizeBefore = Files.size(wal);
        // 模拟崩溃时的撕裂写：尾部追加半个帧的垃圾字节
        try (RandomAccessFile raf = new RandomAccessFile(wal.toFile(), "rw")) {
            raf.seek(sizeBefore);
            raf.write(new byte[]{1, 0, 0, 0, 2, 0, 0}); // 不完整的帧头
        }
        long sizeCorrupt = Files.size(wal);
        log.info("[输入] WAL 正常大小={}B, 追加 {}B 不完整尾部模拟崩溃撕裂写",
                sizeBefore, sizeCorrupt - sizeBefore);

        try (CacheEngine reopened = new CacheEngine(16, wal, Clock.systemUTC())) {
            log.info("[恢复结果] k1={} k2={} size={}",
                    reopened.get("k1").status(), reopened.get("k2").status(), reopened.size());
            assertThat(reopened.get("k1").value()).isEqualTo("v1");
            assertThat(reopened.get("k2").value()).isEqualTo("v2");
            assertThat(reopened.size()).isEqualTo(2);
            // 损坏尾部被截断，文件可继续正常写入
            assertThat(reopened.put("k3", "v3", 0)).isEqualTo(CacheStatus.OK);
        }
        // 再次重启：截断后写入的 k3 也应存在
        try (CacheEngine third = new CacheEngine(16, wal, Clock.systemUTC())) {
            assertThat(third.get("k3").value()).isEqualTo("v3");
            assertThat(third.size()).isEqualTo(3);
        }
    }

    @Test
    void corruptedFrameStopsReplayAtThatPoint() throws IOException {
        Path wal = dir.resolve("r4.wal");
        try (CacheEngine e = new CacheEngine(16, wal, Clock.systemUTC())) {
            e.put("k1", "v1", 0); // 帧1: offset 4
            e.put("k2", "v2", 0); // 帧2: offset 4+25=29
            e.put("k3", "v3", 0); // 帧3
        }
        // 帧2 起始偏移：HEADER + (FRAME_FIXED + nsLen + keyLen + valueLen)
        int frame1Len = FRAME_FIXED + NS_LEN + 2 + 2;
        int frame2Offset = HEADER + frame1Len;
        // 翻转帧2 载荷中的一个字节（key 首字节），造成 CRC 不匹配
        try (RandomAccessFile raf = new RandomAccessFile(wal.toFile(), "rw")) {
            int payloadOffset = frame2Offset + FRAME_FIXED;
            raf.seek(payloadOffset);
            int b = raf.read();
            raf.seek(payloadOffset);
            raf.write(b ^ 0xFF);
        }
        log.info("[输入] 翻转帧2 载荷首字节（offset={}），模拟落盘数据损坏", frame2Offset + FRAME_FIXED);

        try (CacheEngine reopened = new CacheEngine(16, wal, Clock.systemUTC())) {
            CacheStatus k1 = reopened.get("k1").status();
            CacheStatus k2 = reopened.get("k2").status();
            CacheStatus k3 = reopened.get("k3").status();
            log.info("[恢复结果] 损坏点之前 k1={}（应保留）; 损坏帧 k2={} 及其后 k3={}（应被忽略）",
                    k1, k2, k3);
            assertThat(k1).isEqualTo(CacheStatus.OK);
            assertThat(k2).isEqualTo(CacheStatus.NOT_FOUND);
            assertThat(k3).isEqualTo(CacheStatus.NOT_FOUND);
            assertThat(reopened.size()).isEqualTo(1);
        }
    }

    @Test
    void freshStartOnMissingFile() {
        Path wal = dir.resolve("nonexistent.wal");
        try (CacheEngine e = new CacheEngine(16, wal, Clock.systemUTC())) {
            log.info("[恢复结果] 无 WAL 文件冷启动 size={}", e.size());
            assertThat(e.size()).isZero();
            assertThat(e.put("x", "1", 0)).isEqualTo(CacheStatus.OK);
        }
        try (CacheEngine reopened = new CacheEngine(16, wal, Clock.systemUTC())) {
            assertThat(reopened.get("x").value()).isEqualTo("1");
        }
    }

    @Test
    void compactPreservesStateAndShrinksFile() throws IOException {
        Path wal = dir.resolve("r5.wal");
        int rewritten;
        try (CacheEngine e = new CacheEngine(16, wal, Clock.systemUTC())) {
            e.put("keep", "v1", 0);
            e.put("drop", "v2", 0);
            e.put("keep", "v1-new", 0); // 覆盖产生历史垃圾
            e.delete("drop");           // 删除产生历史垃圾
            long sizeBeforeCompact = Files.size(wal);
            rewritten = e.compact();
            long sizeAfterCompact = Files.size(wal);
            log.info("[判定] 压缩前 WAL={}B, 压缩后={}B, 重写有效记录 {} 条",
                    sizeBeforeCompact, sizeAfterCompact, rewritten);
            assertThat(rewritten).isEqualTo(1); // 仅 keep 存活
            assertThat(sizeAfterCompact).isLessThan(sizeBeforeCompact);
        }
        try (CacheEngine reopened = new CacheEngine(16, wal, Clock.systemUTC())) {
            log.info("[恢复结果] 压缩后重启: keep={} drop={}",
                    reopened.get("keep").status(), reopened.get("drop").status());
            assertThat(reopened.get("keep").value()).isEqualTo("v1-new");
            assertThat(reopened.get("drop").status()).isEqualTo(CacheStatus.NOT_FOUND);
            assertThat(reopened.size()).isEqualTo(1);
        }
    }

    @Test
    void repeatedRestartCyclesAreStable() {
        Path wal = dir.resolve("r6.wal");
        for (int cycle = 0; cycle < 3; cycle++) {
            try (CacheEngine e = new CacheEngine(16, wal, Clock.systemUTC())) {
                e.put("cycle-" + cycle, "v" + cycle, 0);
                log.info("[恢复结果] 第 {} 轮重启后 size={}", cycle, e.size());
                assertThat(e.size()).isEqualTo(cycle + 1);
            }
        }
        try (CacheEngine e = new CacheEngine(16, wal, Clock.systemUTC())) {
            for (int cycle = 0; cycle < 3; cycle++) {
                assertThat(e.get("cycle-" + cycle).value()).isEqualTo("v" + cycle);
            }
        }
    }
}
