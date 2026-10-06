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
 * 空间模型的重启恢复测试：各空间数据、空间归属与占用情况恢复到与重启前一致；
 * 损坏尾部被识别并忽略；旧版 v1 WAL（无空间字段）兼容恢复并自动升级为 v2。
 */
class NamespaceRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(NamespaceRecoveryTest.class);

    @TempDir
    Path dir;

    @Test
    void restartRestoresNamespaceOwnershipAndOccupancy() {
        Path wal = dir.resolve("ns-r1.wal");
        Map<String, Integer> caps = Map.of("tiny", 2, "roomy", 10);
        try (CacheEngine e = new CacheEngine(4, caps, wal, MutableClock.startingAtEpoch())) {
            e.put("tiny", "a", "1", 0);
            e.put("tiny", "b", "2", 0);
            e.put("roomy", "a", "100", 0);   // 同名键在另一空间
            e.put("plain", "p", 0);          // 默认空间
            e.delete("roomy", "a");
            e.put("roomy", "a", "101", 0);
            log.info("[输入] 重启前: tiny 占用 {}/{}, roomy 占用 {}/{}, default 占用 {}",
                    e.size("tiny"), e.capacity("tiny"), e.size("roomy"), e.capacity("roomy"), e.size());
        }
        try (CacheEngine reopened = new CacheEngine(4, caps, wal, MutableClock.startingAtEpoch())) {
            log.info("[恢复结果] tiny.a={} tiny.b={} roomy.a={} default.plain={} "
                            + "占用: tiny={}/{} roomy={}/{} default={}, 空间列表={}",
                    reopened.get("tiny", "a").status(), reopened.get("tiny", "b").status(),
                    reopened.get("roomy", "a").status(), reopened.get("plain").status(),
                    reopened.size("tiny"), reopened.capacity("tiny"),
                    reopened.size("roomy"), reopened.capacity("roomy"),
                    reopened.size(), reopened.namespaces());
            // 数据与空间归属一致
            assertThat(reopened.get("tiny", "a").value()).isEqualTo("1");
            assertThat(reopened.get("tiny", "b").value()).isEqualTo("2");
            assertThat(reopened.get("roomy", "a").value()).isEqualTo("101");
            assertThat(reopened.get("plain").value()).isEqualTo("p");
            // 各空间占用与重启前一致
            assertThat(reopened.size("tiny")).isEqualTo(2);
            assertThat(reopened.size("roomy")).isEqualTo(1);
            assertThat(reopened.size()).isEqualTo(1);
            // 容量配置恢复后仍生效：tiny 已满，新写入被拒；roomy 仍可写
            assertThat(reopened.put("tiny", "c", "3", 0)).isEqualTo(CacheStatus.CAPACITY_FULL);
            assertThat(reopened.put("roomy", "c", "3", 0)).isEqualTo(CacheStatus.OK);
        }
    }

    @Test
    void corruptedTailWithNamespacesIgnoredAndServiceStarts() throws IOException {
        Path wal = dir.resolve("ns-r2.wal");
        try (CacheEngine e = new CacheEngine(8, wal, Clock.systemUTC())) {
            e.put("order", "k1", "v1", 0);
            e.put("payment", "k1", "v2", 0);
        }
        long sizeBefore = Files.size(wal);
        // 模拟崩溃撕裂写：尾部追加半个 v2 帧的垃圾字节
        try (RandomAccessFile raf = new RandomAccessFile(wal.toFile(), "rw")) {
            raf.seek(sizeBefore);
            raf.write(new byte[]{1, 0, 0, 0, 5, 0, 0, 0, 2, 0});
        }
        log.info("[输入] WAL 正常大小={}B, 追加 10B 不完整尾部模拟崩溃撕裂写", sizeBefore);

        try (CacheEngine reopened = new CacheEngine(8, wal, Clock.systemUTC())) {
            log.info("[恢复结果] order.k1={} payment.k1={}（损坏尾部被忽略，服务正常启动）",
                    reopened.get("order", "k1").status(), reopened.get("payment", "k1").status());
            assertThat(reopened.get("order", "k1").value()).isEqualTo("v1");
            assertThat(reopened.get("payment", "k1").value()).isEqualTo("v2");
            // 截断后文件可继续写入且空间语义正常
            assertThat(reopened.put("stock", "k9", "v9", 0)).isEqualTo(CacheStatus.OK);
        }
        try (CacheEngine third = new CacheEngine(8, wal, Clock.systemUTC())) {
            assertThat(third.get("stock", "k9").value()).isEqualTo("v9");
            assertThat(third.get("order", "k1").value()).isEqualTo("v1");
        }
    }

    @Test
    void compactPreservesNamespaceOwnership() throws IOException {
        Path wal = dir.resolve("ns-r3.wal");
        int rewritten;
        try (CacheEngine e = new CacheEngine(8, wal, Clock.systemUTC())) {
            e.put("order", "keep", "o1", 0);
            e.put("order", "drop", "o2", 0);
            e.put("payment", "keep", "p1", 0); // 同名键，不同空间
            e.delete("order", "drop");
            long before = Files.size(wal);
            rewritten = e.compact();
            long after = Files.size(wal);
            log.info("[判定] 压缩前 WAL={}B, 压缩后={}B, 重写有效记录 {} 条（order.keep + payment.keep）",
                    before, after, rewritten);
            assertThat(rewritten).isEqualTo(2);
            assertThat(after).isLessThan(before);
        }
        try (CacheEngine reopened = new CacheEngine(8, wal, Clock.systemUTC())) {
            log.info("[恢复结果] 压缩后重启: order.keep={} order.drop={} payment.keep={}",
                    reopened.get("order", "keep").status(), reopened.get("order", "drop").status(),
                    reopened.get("payment", "keep").status());
            assertThat(reopened.get("order", "keep").value()).isEqualTo("o1");
            assertThat(reopened.get("order", "drop").status()).isEqualTo(CacheStatus.NOT_FOUND);
            assertThat(reopened.get("payment", "keep").value()).isEqualTo("p1");
        }
    }

    @Test
    void legacyV1WalRecoveredIntoDefaultSpaceAndUpgraded() throws IOException {
        Path wal = dir.resolve("ns-r4.wal");
        // 手工构造旧版 v1 WAL：header(VERSION=1) + 旧格式帧（无空间字段）
        writeLegacyV1Wal(wal);
        long v1Size = Files.size(wal);
        log.info("[输入] 手工构造 v1 旧格式 WAL（{}B, 2 条 PUT: legacy1, legacy2）", v1Size);

        try (CacheEngine e = new CacheEngine(8, wal, Clock.systemUTC())) {
            // 旧数据归属默认空间，旧式调用直接可见
            log.info("[恢复结果] v1 恢复: legacy1={} legacy2={}（归属默认空间）",
                    e.get("legacy1").status(), e.get("legacy2").status());
            assertThat(e.get("legacy1").value()).isEqualTo("old-1");
            assertThat(e.get("legacy2").value()).isEqualTo("old-2");
            assertThat(e.get(CacheEngine.DEFAULT_NAMESPACE, "legacy1").value()).isEqualTo("old-1");
            // 升级后新写入（含空间）正常工作
            assertThat(e.put("order", "new1", "n1", 0)).isEqualTo(CacheStatus.OK);
        }
        // 文件已被升级为 v2：版本号应为 2
        try (FileChannel ch = FileChannel.open(wal, StandardOpenOption.READ)) {
            ByteBuffer head = ByteBuffer.allocate(4);
            ch.read(head);
            head.flip();
            byte version = head.get(3);
            log.info("[判定] 升级后 WAL 版本号={}（期望 2），文件 {}B", version, Files.size(wal));
            assertThat(version).isEqualTo((byte) 2);
        }
        // 再次重启：旧数据 + 新数据都完整
        try (CacheEngine e = new CacheEngine(8, wal, Clock.systemUTC())) {
            assertThat(e.get("legacy1").value()).isEqualTo("old-1");
            assertThat(e.get("legacy2").value()).isEqualTo("old-2");
            assertThat(e.get("order", "new1").value()).isEqualTo("n1");
            assertThat(e.size()).isEqualTo(2);
            assertThat(e.size("order")).isEqualTo(1);
        }
    }

    /** 按 v1 旧格式写 WAL：header + PUT 帧（type|keyLen|valueLen|expireAt|crc|key|value）。 */
    private static void writeLegacyV1Wal(Path file) throws IOException {
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            ch.write(ByteBuffer.wrap(new byte[]{'C', 'L', 'W', 1}));
            writeV1PutFrame(ch, "legacy1", "old-1");
            writeV1PutFrame(ch, "legacy2", "old-2");
            ch.force(true);
        }
    }

    private static void writeV1PutFrame(FileChannel ch, String key, String value) throws IOException {
        byte[] kb = key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] vb = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        CRC32 crc = new CRC32();
        crc.update(1); // PUT
        crc.update(ByteBuffer.allocate(4).putInt(kb.length).array());
        crc.update(ByteBuffer.allocate(4).putInt(vb.length).array());
        crc.update(ByteBuffer.allocate(8).putLong(0L).array());
        crc.update(kb);
        crc.update(vb);
        ByteBuffer frame = ByteBuffer.allocate(21 + kb.length + vb.length);
        frame.put((byte) 1);
        frame.putInt(kb.length);
        frame.putInt(vb.length);
        frame.putLong(0L);
        frame.putInt((int) crc.getValue());
        frame.put(kb);
        frame.put(vb);
        frame.flip();
        while (frame.hasRemaining()) {
            ch.write(frame);
        }
    }
}
