package com.github.lisama569.cachestorageenginejava.tests;

import com.github.lisama569.cachestorageenginejava.core.CacheEngine;
import com.github.lisama569.cachestorageenginejava.core.CacheStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.util.Map;
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 空间模型下的重启恢复测试：数据、空间归属与各空间占用恢复到与重启前一致；
 * 损坏尾部被识别并忽略；旧版 v1（无空间字段）WAL 自动迁移且数据归入默认空间。
 */
class NamespaceRestartRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(NamespaceRestartRecoveryTest.class);

    /** v2 帧固定区：type(1)+nsLen(4)+keyLen(4)+valueLen(4)+expireAt(8)+crc(4)。 */
    private static final int FRAME_FIXED_V2 = 25;
    private static final int HEADER = 4;
    private static final byte[] MAGIC = {'C', 'L', 'W'};

    @TempDir
    Path dir;

    @Test
    void restartRestoresNamespacesAndOccupancy() {
        MutableClock clock = MutableClock.startingAtEpoch();
        Path wal = dir.resolve("nr1.wal");
        Map<String, Integer> overrides = Map.of("tiny", 2);
        try (CacheEngine e = new CacheEngine(4, overrides, wal, clock)) {
            e.put("nsA", "a1", "1", 0);
            e.put("nsA", "a2", "2", 50);   // 重启前会过期
            e.put("nsB", "b1", "9", 0);
            e.put("tiny", "t1", "x", 0);
            e.delete("nsB", "b1");          // 删除也应跨重启生效
            e.put("nsB", "b2", "8", 0);
            clock.advanceMillis(100);       // nsA.a2 过期
            log.info("[输入] 重启前: nsA={{a1, a2(已过期)}}, nsB={{b2}}(b1已删), tiny={{t1}}; "
                    + "容量覆盖 tiny=2");
        }

        try (CacheEngine reopened = new CacheEngine(4, overrides, wal, clock)) {
            log.info("[恢复结果] nsA.a1={} nsA.a2={} nsB.b1={} nsB.b2={} tiny.t1={}",
                    reopened.get("nsA", "a1").status(), reopened.get("nsA", "a2").status(),
                    reopened.get("nsB", "b1").status(), reopened.get("nsB", "b2").status(),
                    reopened.get("tiny", "t1").status());
            assertThat(reopened.get("nsA", "a1").value()).isEqualTo("1");
            assertThat(reopened.get("nsA", "a2").status()).isEqualTo(CacheStatus.NOT_FOUND); // 过期不恢复
            assertThat(reopened.get("nsB", "b1").status()).isEqualTo(CacheStatus.NOT_FOUND); // 删除生效
            assertThat(reopened.get("nsB", "b2").value()).isEqualTo("8");
            assertThat(reopened.get("tiny", "t1").value()).isEqualTo("x");

            // 各空间占用与重启前一致
            log.info("[恢复结果] 各空间占用: nsA={} nsB={} tiny={}（容量覆盖仍生效 tiny 容量={}）",
                    reopened.size("nsA"), reopened.size("nsB"), reopened.size("tiny"),
                    reopened.capacityOf("tiny"));
            assertThat(reopened.size("nsA")).isEqualTo(1);
            assertThat(reopened.size("nsB")).isEqualTo(1);
            assertThat(reopened.size("tiny")).isEqualTo(1);
            assertThat(reopened.capacityOf("tiny")).isEqualTo(2);

            // 空间归属不串：nsA 的 a1 不会出现在 nsB
            assertThat(reopened.get("nsB", "a1").status()).isEqualTo(CacheStatus.NOT_FOUND);
            // 恢复后容量约束照常：tiny 只剩 1 个槽位
            assertThat(reopened.put("tiny", "t2", "y", 0)).isEqualTo(CacheStatus.OK);
            assertThat(reopened.put("tiny", "t3", "z", 0)).isEqualTo(CacheStatus.CAPACITY_FULL);
        }
    }

    @Test
    void corruptedTailIsIgnoredAcrossNamespaces() throws IOException {
        Path wal = dir.resolve("nr2.wal");
        try (CacheEngine e = new CacheEngine(16, wal, Clock.systemUTC())) {
            e.put("nsA", "k1", "v1", 0); // 帧1
            e.put("nsB", "k2", "v2", 0); // 帧2（将被损坏）
            e.put("nsA", "k3", "v3", 0); // 帧3（损坏点之后，应被忽略）
        }
        // 帧1 长度 = 固定区 + nsLen("nsA"=3) + key(2) + value(2)
        int frame1Len = FRAME_FIXED_V2 + 3 + 2 + 2;
        int frame2Offset = HEADER + frame1Len;
        try (RandomAccessFile raf = new RandomAccessFile(wal.toFile(), "rw")) {
            int payloadOffset = frame2Offset + FRAME_FIXED_V2; // 帧2 载荷首字节
            raf.seek(payloadOffset);
            int b = raf.read();
            raf.seek(payloadOffset);
            raf.write(b ^ 0xFF); // 翻转 -> CRC 不匹配
        }
        log.info("[输入] 翻转帧2(nsB.k2) 载荷首字节（offset={}），模拟落盘数据损坏",
                frame2Offset + FRAME_FIXED_V2);

        try (CacheEngine reopened = new CacheEngine(16, wal, Clock.systemUTC())) {
            CacheStatus k1 = reopened.get("nsA", "k1").status();
            CacheStatus k2 = reopened.get("nsB", "k2").status();
            CacheStatus k3 = reopened.get("nsA", "k3").status();
            log.info("[恢复结果] 损坏点之前 nsA.k1={}（应保留）; 损坏帧 nsB.k2={} 及其后 nsA.k3={}（应被忽略）",
                    k1, k2, k3);
            assertThat(k1).isEqualTo(CacheStatus.OK);
            assertThat(k2).isEqualTo(CacheStatus.NOT_FOUND);
            assertThat(k3).isEqualTo(CacheStatus.NOT_FOUND);
            assertThat(reopened.size()).isEqualTo(1);
            // 服务能继续正常读写（损坏尾部已被截断）
            assertThat(reopened.put("nsB", "after", "ok", 0)).isEqualTo(CacheStatus.OK);
        }
    }

    @Test
    void legacyV1WalMigratesIntoDefaultNamespace() throws IOException {
        Path wal = dir.resolve("nr3.wal");
        writeV1Wal(wal); // 手工构造 v1 日志：PUT legacy=old, PUT gone=x, DELETE gone
        log.info("[输入] 手工构造 v1 格式 WAL（无空间字段）: PUT legacy=old, PUT gone=x, DELETE gone");

        try (CacheEngine e = new CacheEngine(8, wal, Clock.systemUTC())) {
            String legacy = e.get("legacy").value(); // 旧 API = 默认空间
            log.info("[恢复结果] v1 数据落入默认空间: legacy={} gone={}",
                    legacy, e.get("gone").status());
            assertThat(legacy).isEqualTo("old");
            assertThat(e.get("gone").status()).isEqualTo(CacheStatus.NOT_FOUND);
            assertThat(e.get(CacheEngine.DEFAULT_NAMESPACE, "legacy").value()).isEqualTo("old");
            // 迁移后写入走 v2，互不干扰
            assertThat(e.put("nsNew", "k", "v", 0)).isEqualTo(CacheStatus.OK);
        }

        // 迁移后的文件再次重启（此时已是 v2），数据与空间归属保持一致
        try (CacheEngine reopened = new CacheEngine(8, wal, Clock.systemUTC())) {
            log.info("[恢复结果] 迁移后二次重启: default.legacy={} nsNew.k={}",
                    reopened.get("legacy").status(), reopened.get("nsNew", "k").status());
            assertThat(reopened.get("legacy").value()).isEqualTo("old");
            assertThat(reopened.get("nsNew", "k").value()).isEqualTo("v");
            assertThat(reopened.get("nsNew", "legacy").status()).isEqualTo(CacheStatus.NOT_FOUND);
        }
    }

    /** 按 v1 布局写一份 WAL：header(v1) + PUT legacy=old + PUT gone=x + DELETE gone。 */
    private static void writeV1Wal(Path file) throws IOException {
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            ch.write(ByteBuffer.wrap(new byte[]{MAGIC[0], MAGIC[1], MAGIC[2], 1}));
            ch.write(v1Frame((byte) 1, "legacy", "old", 0L));
            ch.write(v1Frame((byte) 1, "gone", "x", 0L));
            ch.write(v1Frame((byte) 2, "gone", "", 0L));
            ch.force(true);
        }
        assertThat(Files.size(file)).isGreaterThan(HEADER);
    }

    private static ByteBuffer v1Frame(byte type, String key, String value, long expireAt) {
        byte[] keyBytes = key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] valueBytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        CRC32 crc = new CRC32();
        crc.update(type);
        crc.update(ByteBuffer.allocate(4).putInt(keyBytes.length).array());
        crc.update(ByteBuffer.allocate(4).putInt(valueBytes.length).array());
        crc.update(ByteBuffer.allocate(8).putLong(expireAt).array());
        crc.update(keyBytes);
        crc.update(valueBytes);
        ByteBuffer buf = ByteBuffer.allocate(21 + keyBytes.length + valueBytes.length);
        buf.put(type);
        buf.putInt(keyBytes.length);
        buf.putInt(valueBytes.length);
        buf.putLong(expireAt);
        buf.putInt((int) crc.getValue());
        buf.put(keyBytes);
        buf.put(valueBytes);
        buf.flip();
        return buf;
    }
}
