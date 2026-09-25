package com.github.lisama569.cachestorageenginejava.persistence;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.BiConsumer;
import java.util.zip.CRC32;

/**
 * 追加式写前日志。
 *
 * <p>记录格式：
 * <pre>
 *   [type:1][keyLen:4][key bytes][payloadLen:4][payload bytes][crc32:8]
 * </pre>
 * type: 1=put, 2=remove；remove 记录 payloadLen 为 -1。
 * crc32 覆盖本条记录除 crc 字段外的全部字节。
 *
 * <p>重启回放时顺序读取，遇到不完整或 CRC 不匹配的记录（异常终止留下的尾部）
 * 即停止，并将文件截断到最后一条完好记录的末尾，保证后续追加从一致位置继续。
 */
public class WriteAheadLog implements Closeable {

    private static final byte TYPE_PUT = 1;
    private static final byte TYPE_REMOVE = 2;
    private static final int HEADER_LEN = 1 + 4; // type + keyLen
    private static final int CRC_LEN = 8;

    private final RandomAccessFile raf;
    private final FileChannel channel;

    public WriteAheadLog(Path file) throws IOException {
        Files.createDirectories(file.toAbsolutePath().getParent());
        this.raf = new RandomAccessFile(file.toFile(), "rw");
        this.channel = raf.getChannel();
        // 追加位置在 replay() 截断后确定；未回放前追加到文件末尾。
        this.raf.seek(this.raf.length());
    }

    /**
     * 回放日志：对每条完好记录回调 (key, payload)，payload 为 null 表示删除。
     * 遇到损坏/不完整尾部时截断文件并返回截断位置。
     *
     * @return 被截断丢弃的字节数（0 表示日志完好）
     */
    public synchronized long replay(BiConsumer<String, byte[]> consumer) throws IOException {
        long pos = 0;
        long lastGood = 0;
        long length = raf.length();
        while (pos < length) {
            raf.seek(pos);
            Long recordLen = tryReadRecord(consumer, length - pos);
            if (recordLen == null) {
                break; // 损坏或不完整：停止回放
            }
            pos += recordLen;
            lastGood = pos;
        }
        long discarded = length - lastGood;
        if (discarded > 0) {
            channel.truncate(lastGood);
        }
        raf.seek(lastGood);
        return discarded;
    }

    /** 读取一条记录；成功返回记录总长度，失败（不完整/校验错）返回 null。 */
    private Long tryReadRecord(BiConsumer<String, byte[]> consumer, long remaining) throws IOException {
        if (remaining < HEADER_LEN + 4 + CRC_LEN) {
            return null;
        }
        byte type = raf.readByte();
        int keyLen = raf.readInt();
        if (keyLen < 0 || keyLen > remaining) {
            return null;
        }
        byte[] keyBytes = new byte[keyLen];
        raf.readFully(keyBytes);
        int payloadLen = raf.readInt();
        if (payloadLen < -1 || payloadLen > remaining) {
            return null;
        }
        byte[] payload = payloadLen >= 0 ? new byte[payloadLen] : null;
        if (payload != null) {
            raf.readFully(payload);
        }
        long recordLen = (long) HEADER_LEN + keyLen + 4 + (payloadLen >= 0 ? payloadLen : 0) + CRC_LEN;
        if (recordLen > remaining) {
            return null;
        }
        long storedCrc = raf.readLong();

        CRC32 crc = new CRC32();
        ByteBuffer head = ByteBuffer.allocate(HEADER_LEN);
        head.put(type).putInt(keyLen);
        crc.update(head.array());
        crc.update(keyBytes);
        ByteBuffer lenBuf = ByteBuffer.allocate(4);
        lenBuf.putInt(payloadLen);
        crc.update(lenBuf.array());
        if (payload != null) {
            crc.update(payload);
        }
        if (crc.getValue() != storedCrc) {
            return null;
        }
        if (type == TYPE_PUT && payload == null) {
            return null; // 类型与内容不一致
        }
        if (type == TYPE_REMOVE && payload != null) {
            return null;
        }
        consumer.accept(new String(keyBytes, StandardCharsets.UTF_8), payload);
        return recordLen;
    }

    /** 追加一条 put 记录并落盘。 */
    public synchronized void appendPut(String key, byte[] payload) throws IOException {
        append(TYPE_PUT, key, payload);
    }

    /** 追加一条 remove 记录并落盘。 */
    public synchronized void appendRemove(String key) throws IOException {
        append(TYPE_REMOVE, key, null);
    }

    private void append(byte type, String key, byte[] payload) throws IOException {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        int payloadLen = payload == null ? -1 : payload.length;
        ByteBuffer buf = ByteBuffer.allocate(HEADER_LEN + keyBytes.length + 4
                + (payload == null ? 0 : payload.length) + CRC_LEN);
        buf.put(type).putInt(keyBytes.length).put(keyBytes).putInt(payloadLen);
        if (payload != null) {
            buf.put(payload);
        }
        CRC32 crc = new CRC32();
        crc.update(buf.array(), 0, buf.position());
        buf.putLong(crc.getValue());
        buf.flip();
        while (buf.hasRemaining()) {
            channel.write(buf);
        }
        channel.force(false); // 落盘，保证崩溃后可恢复
    }

    @Override
    public synchronized void close() throws IOException {
        channel.force(false);
        raf.close();
    }
}
