package com.github.lisama569.cachestorageenginejava.persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * 追加式预写日志（append-only WAL），支持按空间（namespace）隔离的记录。
 *
 * <p>磁盘布局（当前版本 v2）：
 * <pre>
 * 文件头: MAGIC(3B 'C','L','W') + VERSION(1B = 2)
 * 帧:    type(1B) | nsLen(4B BE) | keyLen(4B BE) | valueLen(4B BE) | expireAtMillis(8B BE)
 *        | CRC32(4B BE，覆盖前述字段与 ns/key/value 载荷) | ns(UTF-8) | key(UTF-8) | value(UTF-8)
 * </pre>
 *
 * <p>版本兼容：v1 文件（无空间字段的旧格式）打开时按 v1 帧格式恢复，
 * 记录归属默认空间；恢复完成后立即把文件整体升级重写为 v2（逻辑记录不变），
 * 此后追加统一使用 v2 格式。已有数据不因升级丢失。
 *
 * <p>恢复语义：从文件头顺序校验每一帧；遇到头部损坏、尾部截断或 CRC 不匹配，
 * 立即停止并把文件截断到最后一条有效帧之后——损坏尾部被识别并忽略，不抛异常、
 * 不产生脏数据。每次追加都经过 fsync，崩溃窗口内未完成的帧自然落在被忽略尾部。
 */
public final class WriteAheadLog implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WriteAheadLog.class);

    static final byte[] MAGIC = {'C', 'L', 'W'};
    static final byte VERSION_1 = 1; // 旧格式：无空间字段
    static final byte VERSION_2 = 2; // 当前格式：含空间字段
    static final byte CURRENT_VERSION = VERSION_2;
    static final int HEADER_LEN = MAGIC.length + 1;
    /** v1 帧固定区：type(1)+keyLen(4)+valueLen(4)+expireAt(8)+crc(4)。 */
    static final int FRAME_FIXED_LEN_V1 = 1 + 4 + 4 + 8 + 4;
    /** v2 帧固定区：type(1)+nsLen(4)+keyLen(4)+valueLen(4)+expireAt(8)+crc(4)。 */
    static final int FRAME_FIXED_LEN_V2 = 1 + 4 + 4 + 4 + 8 + 4;

    private final Path file;
    private FileChannel channel;
    private final List<WalRecord> recovered = new ArrayList<>();
    private boolean recoveredDrained = false;

    public WriteAheadLog(Path file) {
        this.file = file;
        try {
            openAndRecover();
        } catch (IOException e) {
            throw new IllegalStateException("无法打开/恢复 WAL: " + file, e);
        }
    }

    private synchronized void openAndRecover() throws IOException {
        if (file.getParent() != null) {
            Files.createDirectories(file.getParent());
        }
        boolean fresh = !Files.exists(file) || Files.size(file) == 0;
        channel = FileChannel.open(file,
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);

        if (fresh) {
            channel.position(0);
            channel.write(ByteBuffer.wrap(buildHeader()));
            channel.force(true);
            return;
        }

        ByteBuffer head = ByteBuffer.allocate(HEADER_LEN);
        while (head.hasRemaining()) {
            int n = channel.read(head);
            if (n == -1) {
                break;
            }
        }
        head.flip();
        if (head.remaining() < HEADER_LEN) {
            log.warn("WAL 头部不完整（{}B），判定为空日志并重建: {}", head.remaining(), file);
            startFresh();
            return;
        }
        byte[] header = new byte[HEADER_LEN];
        head.get(header);
        for (int i = 0; i < MAGIC.length; i++) {
            if (header[i] != MAGIC[i]) {
                log.warn("WAL 头部魔数不匹配，判定为不可识别文件并重建: {}", file);
                startFresh();
                return;
            }
        }
        byte version = header[MAGIC.length];
        if (version != VERSION_1 && version != VERSION_2) {
            log.warn("WAL 版本号不可识别（{}），判定为不可识别文件并重建: {}", version, file);
            startFresh();
            return;
        }
        recoverFrames(version);
        if (version == VERSION_1) {
            upgradeV1ToV2();
        }
    }

    /** 顺序恢复帧；发现第一条无效帧即截断其后所有字节（含损坏帧）。 */
    private void recoverFrames(byte version) throws IOException {
        int fixedLen = version == VERSION_1 ? FRAME_FIXED_LEN_V1 : FRAME_FIXED_LEN_V2;
        long fileSize = channel.size();
        long position = HEADER_LEN;
        long validBoundary = HEADER_LEN;
        int frameIndex = 0;

        while (position < fileSize) {
            ByteBuffer fixed = ByteBuffer.allocate(fixedLen);
            readFully(channel, fixed, position);
            fixed.flip();
            if (fixed.remaining() < fixedLen) {
                // 固定区都没写完整：崩溃撕裂的帧头，整条尾部忽略
                log.warn("WAL 帧固定区不完整（offset={}, 期望 {}B 实际 {}B），从该位置起的尾部将被忽略并截断",
                        position, fixedLen, fixed.remaining());
                break;
            }
            byte type = fixed.get();
            int nsLen = version == VERSION_1 ? 0 : fixed.getInt();
            int keyLen = fixed.getInt();
            int valueLen = fixed.getInt();
            long expireAt = fixed.getLong();
            // 注意：固定区最后 4 字节是 CRC，需要连同载荷一起读取后再校验
            int crcStored = fixed.getInt();

            long frameStart = position;
            long totalLen = (long) fixedLen + nsLen + keyLen + valueLen;

            boolean valid = type == WalRecord.PUT || type == WalRecord.DELETE;
            valid &= nsLen >= 0 && keyLen >= 0 && valueLen >= 0;
            valid &= frameStart + totalLen <= fileSize; // 整帧必须完整落盘
            if (!valid) {
                log.warn("WAL 第 {} 帧无效/不完整（type={}, nsLen={}, keyLen={}, valueLen={}, offset={}），"
                        + "从该位置起的尾部数据将被忽略并截断", frameIndex, type, nsLen, keyLen, valueLen, frameStart);
                break;
            }

            ByteBuffer payload = ByteBuffer.allocate(nsLen + keyLen + valueLen);
            readFully(channel, payload, position + fixedLen);
            payload.flip();
            byte[] nsBytes = new byte[nsLen];
            byte[] keyBytes = new byte[keyLen];
            byte[] valueBytes = new byte[valueLen];
            payload.get(nsBytes);
            payload.get(keyBytes);
            if (valueLen > 0) {
                payload.get(valueBytes);
            }

            java.util.zip.CRC32 crc = new java.util.zip.CRC32();
            crc.update(type);
            if (version == VERSION_2) {
                crc.update(updateBuffer(nsLen));
            }
            crc.update(updateBuffer(keyLen));
            crc.update(updateBuffer(valueLen));
            crc.update(updateBufferLong(expireAt));
            crc.update(nsBytes);
            crc.update(keyBytes);
            crc.update(valueBytes);
            if ((int) crc.getValue() != crcStored) {
                log.warn("WAL 第 {} 帧 CRC 校验失败（offset={}），从该帧起的尾部数据将被忽略并截断",
                        frameIndex, frameStart);
                break;
            }

            String namespace = version == VERSION_1 ? WalRecord.DEFAULT_NAMESPACE
                    : new String(nsBytes, StandardCharsets.UTF_8);
            String key = new String(keyBytes, StandardCharsets.UTF_8);
            String value = valueLen == 0 ? null : new String(valueBytes, StandardCharsets.UTF_8);
            recovered.add(new WalRecord(type, namespace, key, value, expireAt));
            position += totalLen;
            validBoundary = position;
            frameIndex++;
        }

        if (validBoundary < fileSize) {
            try (FileChannel fc = FileChannel.open(file, StandardOpenOption.WRITE)) {
                fc.truncate(validBoundary);
                fc.force(true);
            }
            log.warn("WAL 尾部截断完成: size {} -> {}（忽略 {}B 不完整/损坏数据，恢复有效帧 {} 条）",
                    fileSize, validBoundary, fileSize - validBoundary, recovered.size());
        } else {
            log.info("WAL 恢复完成: {} 条有效记录，文件大小 {}B，无损坏尾部", recovered.size(), fileSize);
        }
        channel.position(validBoundary);
    }

    /**
     * v1 文件升级：把已恢复的逻辑记录按 v2 格式整体重写（记录内容不变，
     * 空间字段补默认空间），此后追加统一走 v2。重写失败则启动失败、文件保持原样。
     */
    private void upgradeV1ToV2() throws IOException {
        List<WalRecord> snapshot = List.copyOf(recovered);
        channel.truncate(0);
        channel.position(0);
        channel.write(ByteBuffer.wrap(buildHeader()));
        for (WalRecord r : snapshot) {
            ByteBuffer frame = buildFrame(r.type(), r.namespace(), r.key(),
                    r.value() == null ? "" : r.value(), r.expireAtMillis());
            while (frame.hasRemaining()) {
                channel.write(frame);
            }
        }
        channel.force(true);
        log.info("WAL 格式升级完成: v1 -> v2，{} 条记录已按新格式重写（归属默认空间），文件 {}",
                snapshot.size(), file);
    }

    private static void readFully(FileChannel ch, ByteBuffer dst, long position) throws IOException {
        long p = position;
        while (dst.hasRemaining()) {
            int n = ch.read(dst, p);
            if (n == -1) {
                return; // 留给上层按 remaining 判定不完整
            }
            p += n;
        }
    }

    private void startFresh() throws IOException {
        channel.truncate(0);
        channel.position(0);
        channel.write(ByteBuffer.wrap(buildHeader()));
        channel.force(true);
        recovered.clear();
    }

    private static byte[] buildHeader() {
        byte[] h = new byte[HEADER_LEN];
        System.arraycopy(MAGIC, 0, h, 0, MAGIC.length);
        h[MAGIC.length] = CURRENT_VERSION;
        return h;
    }

    /** 返回启动时恢复的有效记录；仅首次返回数据，之后返回空，防止重复重放。 */
    public synchronized List<WalRecord> recoveredRecords() {
        if (recoveredDrained) {
            return List.of();
        }
        recoveredDrained = true;
        List<WalRecord> copy = List.copyOf(recovered);
        recovered.clear();
        return copy;
    }

    /** 兼容旧版签名：写入默认空间。 */
    public void appendPut(String key, String value, long expireAtMillis) {
        appendPut(WalRecord.DEFAULT_NAMESPACE, key, value, expireAtMillis);
    }

    public synchronized void appendPut(String namespace, String key, String value, long expireAtMillis) {
        if (namespace == null || key == null || value == null) {
            throw new IllegalArgumentException("put 记录的 namespace/key/value 不能为空");
        }
        append(buildFrame(WalRecord.PUT, namespace, key, value, expireAtMillis));
    }

    /** 兼容旧版签名：写入默认空间。 */
    public void appendDelete(String key) {
        appendDelete(WalRecord.DEFAULT_NAMESPACE, key);
    }

    public synchronized void appendDelete(String namespace, String key) {
        if (namespace == null || key == null) {
            throw new IllegalArgumentException("delete 记录的 namespace/key 不能为空");
        }
        append(buildFrame(WalRecord.DELETE, namespace, key, "", 0L));
    }

    private void append(ByteBuffer frame) {
        try {
            while (frame.hasRemaining()) {
                channel.write(frame);
            }
            channel.force(true);
        } catch (IOException e) {
            throw new IllegalStateException("WAL 追加失败: " + file, e);
        }
    }

    static ByteBuffer buildFrame(byte type, String namespace, String key, String value,
                                 long expireAtMillis) {
        byte[] nsBytes = namespace.getBytes(StandardCharsets.UTF_8);
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(type);
        crc.update(updateBuffer(nsBytes.length));
        crc.update(updateBuffer(keyBytes.length));
        crc.update(updateBuffer(valueBytes.length));
        crc.update(updateBufferLong(expireAtMillis));
        crc.update(nsBytes);
        crc.update(keyBytes);
        crc.update(valueBytes);

        ByteBuffer buf = ByteBuffer.allocate(
                FRAME_FIXED_LEN_V2 + nsBytes.length + keyBytes.length + valueBytes.length);
        buf.put(type);
        buf.putInt(nsBytes.length);
        buf.putInt(keyBytes.length);
        buf.putInt(valueBytes.length);
        buf.putLong(expireAtMillis);
        buf.putInt((int) crc.getValue());
        buf.put(nsBytes);
        buf.put(keyBytes);
        buf.put(valueBytes);
        buf.flip();
        return buf;
    }

    private static byte[] updateBuffer(int v) {
        return ByteBuffer.allocate(4).putInt(v).array();
    }

    private static byte[] updateBufferLong(long v) {
        return ByteBuffer.allocate(8).putLong(v).array();
    }

    /**
     * 用当前有效快照原子重写日志（压缩）：写临时文件 + fsync + 原子替换。
     *
     * @return 新日志写入的记录数
     */
    public synchronized int rewrite(Collection<WalRecord> snapshot) {
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp." + System.nanoTime());
        try (FileChannel out = FileChannel.open(tmp,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            out.write(ByteBuffer.wrap(buildHeader()));
            for (WalRecord r : snapshot) {
                ByteBuffer f = r.isDelete()
                        ? buildFrame(WalRecord.DELETE, r.namespace(), r.key(), "", 0L)
                        : buildFrame(WalRecord.PUT, r.namespace(), r.key(),
                                r.value() == null ? "" : r.value(), r.expireAtMillis());
                while (f.hasRemaining()) {
                    out.write(f);
                }
            }
            out.force(true);
        } catch (IOException e) {
            throw new IllegalStateException("WAL 压缩写临时文件失败: " + tmp, e);
        }

        try {
            channel.close();
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
            channel = FileChannel.open(file,
                    StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            channel.position(channel.size());
            channel.force(true);
        } catch (IOException e) {
            try {
                channel = FileChannel.open(file,
                        StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
                channel.position(channel.size());
            } catch (IOException ignored) {
                // 替换后重开失败不影响数据文件本身
            }
            throw new IllegalStateException("WAL 压缩替换失败: " + file, e);
        }
        int count = snapshot.size();
        log.info("WAL 压缩完成: 重写 {} 条有效记录 -> {}", count, file);
        return count;
    }

    public synchronized Path getFile() {
        return file;
    }

    @Override
    public synchronized void close() {
        try {
            if (channel != null && channel.isOpen()) {
                channel.force(true);
                channel.close();
            }
        } catch (IOException e) {
            throw new IllegalStateException("WAL 关闭失败: " + file, e);
        }
    }
}
