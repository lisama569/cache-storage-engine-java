package com.github.lisama569.cachestorageenginejava.cache;

import com.github.lisama569.cachestorageenginejava.config.CacheProperties;
import com.github.lisama569.cachestorageenginejava.persistence.WriteAheadLog;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 本地缓存存储引擎：TTL + LRU 淘汰 + 容量上限 + WAL 持久化。
 *
 * <p>一致性模型：所有公开方法均在同一监视锁（synchronized）下执行，
 * 同一键上的读、写、过期清理与淘汰构成全序，操作可线性化；
 * 每条变更先写 WAL 再改内存，崩溃后从 WAL 恢复到一致状态。
 *
 * <p>淘汰策略（确定性）：容量超限时，先淘汰已过期条目，再按 LRU 顺序淘汰
 * 最久未使用条目；若腾出的空间仍不足（如单条数据超过 maxBytes），
 * 返回 CAPACITY_EXCEEDED，本次写入整体不生效，不丢弃任何未过期数据。
 */
public class CacheEngine implements Closeable {

    private final CacheProperties properties;
    private final WriteAheadLog wal;
    private final Clock clock;
    /** accessOrder=true，迭代顺序即 LRU->MRU。 */
    private final LinkedHashMap<String, CacheEntry> store = new LinkedHashMap<>(16, 0.75f, true);
    private long currentBytes;

    public CacheEngine(CacheProperties properties, WriteAheadLog wal, Clock clock) {
        this.properties = properties;
        this.wal = wal;
        this.clock = clock;
    }

    /** 启动时从 WAL 恢复；损坏尾部由 WAL 层识别并截断。 */
    public synchronized void recover() throws IOException {
        wal.replay((key, payload) -> {
            if (payload == null) {
                applyRemove(key);
            } else {
                ByteBuffer buf = ByteBuffer.wrap(payload);
                long expireAt = buf.getLong();
                byte[] value = new byte[buf.remaining()];
                buf.get(value);
                applyPut(key, new CacheEntry(value, expireAt));
            }
        });
    }

    public synchronized CacheResult<byte[]> get(String key) {
        CacheEntry entry = store.get(key); // 访问即更新 LRU 顺序
        if (entry == null) {
            return CacheResult.of(CacheStatus.NOT_FOUND);
        }
        if (entry.isExpired(now())) {
            removeInternal(key);
            return CacheResult.of(CacheStatus.EXPIRED);
        }
        return CacheResult.ok(entry.value());
    }

    /**
     * 写入键值；ttlMillis <= 0 表示永不过期。
     * 容量不足且无法通过淘汰腾出空间时返回 CAPACITY_EXCEEDED，不改动任何数据。
     */
    public synchronized CacheResult<Void> put(String key, byte[] value, long ttlMillis) {
        if (exceedsHardLimit(value.length)) {
            return CacheResult.of(CacheStatus.CAPACITY_EXCEEDED);
        }
        CacheEntry entry = new CacheEntry(value, expireAt(ttlMillis));
        List<String> evicted = planEviction(Map.of(key, entry));
        if (evicted == null) {
            return CacheResult.of(CacheStatus.CAPACITY_EXCEEDED);
        }
        try {
            for (String victim : evicted) {
                wal.appendRemove(victim);
            }
            wal.appendPut(key, encode(entry));
        } catch (IOException e) {
            throw new UncheckedIOException("WAL 写入失败", e);
        }
        for (String victim : evicted) {
            applyRemove(victim);
        }
        applyPut(key, entry);
        return CacheResult.ok(null);
    }

    public synchronized CacheResult<Void> remove(String key) {
        CacheEntry entry = store.get(key);
        if (entry == null) {
            return CacheResult.of(CacheStatus.NOT_FOUND);
        }
        if (entry.isExpired(now())) {
            removeInternal(key);
            return CacheResult.of(CacheStatus.EXPIRED);
        }
        removeInternal(key);
        return CacheResult.ok(null);
    }

    /** 批量写入：先整体校验容量，任一不满足则全部不生效，返回失败原因。 */
    public synchronized CacheResult<Void> putAll(Map<String, byte[]> entries, long ttlMillis) {
        Map<String, CacheEntry> pending = new LinkedHashMap<>();
        for (Map.Entry<String, byte[]> e : entries.entrySet()) {
            if (exceedsHardLimit(e.getValue().length)) {
                return CacheResult.of(CacheStatus.CAPACITY_EXCEEDED);
            }
            pending.put(e.getKey(), new CacheEntry(e.getValue(), expireAt(ttlMillis)));
        }
        List<String> evicted = planEviction(pending);
        if (evicted == null) {
            return CacheResult.of(CacheStatus.CAPACITY_EXCEEDED);
        }
        try {
            for (String victim : evicted) {
                wal.appendRemove(victim);
            }
            for (Map.Entry<String, CacheEntry> e : pending.entrySet()) {
                wal.appendPut(e.getKey(), encode(e.getValue()));
            }
        } catch (IOException ex) {
            throw new UncheckedIOException("WAL 写入失败", ex);
        }
        for (String victim : evicted) {
            applyRemove(victim);
        }
        pending.forEach(this::applyPut);
        return CacheResult.ok(null);
    }

    public synchronized Map<String, CacheResult<byte[]>> getAll(Iterable<String> keys) {
        Map<String, CacheResult<byte[]>> results = new LinkedHashMap<>();
        for (String key : keys) {
            results.put(key, get(key));
        }
        return results;
    }

    /** 当前未过期条目数（会先清理已过期条目，保证计数不漂移）。 */
    public synchronized int size() {
        purgeExpired();
        return store.size();
    }

    /** 当前未过期数据占用字节数。 */
    public synchronized long currentBytes() {
        purgeExpired();
        return currentBytes;
    }

    @Override
    public synchronized void close() throws IOException {
        wal.close();
    }

    // ---------- 内部方法（均在监视锁下调用） ----------

    private long now() {
        return clock.millis();
    }

    private long expireAt(long ttlMillis) {
        return ttlMillis <= 0 ? -1 : now() + ttlMillis;
    }

    /** 单条数据超过硬上限（maxBytes），无论如何淘汰都放不下。 */
    private boolean exceedsHardLimit(int valueLength) {
        return properties.getMaxBytes() > 0 && valueLength > properties.getMaxBytes();
    }

    /**
     * 模拟淘汰：返回为容纳 pending 需要淘汰的键列表（先过期后 LRU）。
     * 若淘汰全部现有条目仍放不下，返回 null（容量不足）。
     */
    private List<String> planEviction(Map<String, CacheEntry> pending) {
        long bytes = currentBytes;
        int count = store.size();
        for (Map.Entry<String, CacheEntry> e : pending.entrySet()) {
            CacheEntry old = store.get(e.getKey());
            if (old != null) {
                bytes -= old.size();
            } else {
                count++;
            }
            bytes += e.getValue().size();
        }
        List<String> victims = new ArrayList<>();
        if (fits(count, bytes)) {
            return victims;
        }
        long now = now();
        // 第一遍：淘汰已过期条目
        for (Iterator<Map.Entry<String, CacheEntry>> it = store.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, CacheEntry> e = it.next();
            if (pending.containsKey(e.getKey())) {
                continue;
            }
            if (e.getValue().isExpired(now)) {
                victims.add(e.getKey());
                bytes -= e.getValue().size();
                count--;
            }
        }
        if (fits(count, bytes)) {
            return victims;
        }
        // 第二遍：按 LRU 顺序淘汰
        for (Map.Entry<String, CacheEntry> e : store.entrySet()) {
            if (pending.containsKey(e.getKey()) || victims.contains(e.getKey())) {
                continue;
            }
            victims.add(e.getKey());
            bytes -= e.getValue().size();
            count--;
            if (fits(count, bytes)) {
                return victims;
            }
        }
        return fits(count, bytes) ? victims : null;
    }

    private boolean fits(int count, long bytes) {
        boolean countOk = properties.getMaxEntries() <= 0 || count <= properties.getMaxEntries();
        boolean bytesOk = properties.getMaxBytes() <= 0 || bytes <= properties.getMaxBytes();
        return countOk && bytesOk;
    }

    private void applyPut(String key, CacheEntry entry) {
        CacheEntry old = store.put(key, entry);
        if (old != null) {
            currentBytes -= old.size();
        }
        currentBytes += entry.size();
    }

    private void applyRemove(String key) {
        CacheEntry old = store.remove(key);
        if (old != null) {
            currentBytes -= old.size();
        }
    }

    /** 过期清理：写 WAL 后从内存移除。 */
    private void removeInternal(String key) {
        try {
            wal.appendRemove(key);
        } catch (IOException e) {
            throw new UncheckedIOException("WAL 写入失败", e);
        }
        applyRemove(key);
    }

    private void purgeExpired() {
        long now = now();
        List<String> expired = new ArrayList<>();
        for (Map.Entry<String, CacheEntry> e : store.entrySet()) {
            if (e.getValue().isExpired(now)) {
                expired.add(e.getKey());
            }
        }
        for (String key : expired) {
            removeInternal(key);
        }
    }

    private static byte[] encode(CacheEntry entry) {
        ByteBuffer buf = ByteBuffer.allocate(8 + entry.value().length);
        buf.putLong(entry.expireAtMillis());
        buf.put(entry.value());
        return buf.array();
    }
}
