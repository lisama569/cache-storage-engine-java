package com.github.lisama569.cachestorageenginejava.core;

import com.github.lisama569.cachestorageenginejava.persistence.WalRecord;
import com.github.lisama569.cachestorageenginejava.persistence.WriteAheadLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 本地缓存存储引擎。
 *
 * <p>淘汰与过期策略（确定性）：
 * <ol>
 *   <li>容量判定前先回收已过期条目（惰性：读命中过期键；主动：每次写前与 {@link #purgeExpired()}）；</li>
 *   <li>本引擎不静默丢弃未过期数据：回收过期条目后仍放不下时，写入被拒绝并返回
 *       {@link CacheStatus#CAPACITY_FULL}，已有数据与计数不变；</li>
 *   <li>过期判定：{@code now >= expireAtMillis}，expireAtMillis == 0 表示永不过期。</li>
 * </ol>
 *
 * <p>并发模型：
 * <ul>
 *   <li>所有结构性变更（put/delete/批量写/压缩/主动清理）在单把可重入写锁下执行，
 *       同一键的更新、过期清理、删除互斥，天然串行，无锁顺序环；</li>
 *   <li>读路径无锁：{@link ConcurrentHashMap} + 过期判定 + CAS 删除；
 *       写路径整体替换条目对象，读者只能看到更新前或更新后的完整对象；</li>
 *   <li>每个对外方法构成一个线性化点；容量与所有计数只在写锁内修改，杜绝计数漂移。</li>
 * </ul>
 *
 * <p>持久化：先写 WAL（fsync）后改内存；过期条目的回收不需要写 DELETE
 * （重放时过期 PUT 会被跳过）；只有对存活条目的显式删除才写 DELETE。
 */
public final class CacheEngine implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CacheEngine.class);

    private final int capacity;
    private final WriteAheadLog wal;
    private final Clock clock;

    private final ConcurrentHashMap<String, CacheEntry> map = new ConcurrentHashMap<>();
    private final ReentrantLock writeLock = new ReentrantLock();
    private final AtomicLong accessSeq = new AtomicLong(0);

    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final AtomicLong expirations = new AtomicLong();
    /** 因容量不足被拒绝（未生效）的写入次数。 */
    private final AtomicLong evictions = new AtomicLong();

    public CacheEngine(int capacity, Path dataFile, Clock clock) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity 必须为正数: " + capacity);
        }
        this.capacity = capacity;
        this.clock = Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(dataFile, "dataFile");
        this.wal = new WriteAheadLog(dataFile);
        replay();
    }

    /** 按 WAL 顺序重放：PUT 覆盖旧值、DELETE 删除、已过期 PUT 跳过。 */
    private void replay() {
        List<WalRecord> records = wal.recoveredRecords();
        long now = clock.millis();
        int restored = 0;
        int skippedExpired = 0;
        int deletes = 0;
        for (WalRecord r : records) {
            if (r.isDelete()) {
                if (map.remove(r.key()) != null) {
                    deletes++;
                }
                continue;
            }
            if (r.expireAtMillis() > 0 && now >= r.expireAtMillis()) {
                skippedExpired++;
                continue;
            }
            map.put(r.key(),
                    new CacheEntry(r.key(), r.value(), r.expireAtMillis(), accessSeq.incrementAndGet()));
            restored++;
        }
        // 恢复时不因容量上限丢弃数据（上限只约束新写入），仅记录提示。
        if (map.size() > capacity) {
            log.warn("恢复后存活条目 {} 超过容量上限 {}，保留全部已恢复数据；后续写入将在回收过期后按容量判定",
                    map.size(), capacity);
        }
        log.info("缓存恢复结果: WAL 记录 {} 条 -> 存活恢复 {} 条，过期忽略 {} 条，删除生效 {} 条，当前存活 {}",
                records.size(), restored, skippedExpired, deletes, map.size());
    }

    /** 无锁读；命中过期键时惰性 CAS 回收（过期回收不写 WAL）。 */
    public CacheResult<String> get(String key) {
        Objects.requireNonNull(key, "key");
        CacheEntry entry = map.get(key);
        if (entry == null) {
            misses.incrementAndGet();
            return CacheResult.of(CacheStatus.NOT_FOUND);
        }
        long now = clock.millis();
        if (entry.isExpired(now)) {
            // CAS 删除：只有条目仍是同一个过期对象时才回收，绝不误删并发写入的新对象。
            if (map.remove(key, entry)) {
                expirations.incrementAndGet();
                log.debug("惰性过期回收: key={}, expireAt={}, now={}", key, entry.getExpireAtMillis(), now);
            }
            misses.incrementAndGet();
            return CacheResult.of(CacheStatus.EXPIRED);
        }
        entry.setLastAccessSeq(accessSeq.incrementAndGet());
        hits.incrementAndGet();
        return CacheResult.ok(entry.getValue());
    }

    /** ttlMillis &lt;= 0 表示永不过期。 */
    public CacheStatus put(String key, String value, long ttlMillis) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        writeLock.lock();
        try {
            long now = clock.millis();
            long expireAt = ttlMillis > 0 ? now + ttlMillis : 0L;
            CacheEntry existing = map.get(key);
            boolean replacing = existing != null && !existing.isExpired(now);

            if (!replacing) {
                if (existing != null) {
                    // 覆盖一个已过期槽位，先统一走过期回收逻辑。
                    purgeExpiredLocked(now);
                } else {
                    purgeExpiredLocked(now);
                }
                if (map.size() >= capacity) {
                    evictions.incrementAndGet();
                    log.info("写入被拒绝(CAPACITY_FULL): key={}, 当前存活={}, 容量={}", key, map.size(), capacity);
                    return CacheStatus.CAPACITY_FULL;
                }
            }
            // 先持久化后改内存；WAL 失败则内存不变（WAL 内部抛异常，调用方可见）。
            wal.appendPut(key, value, expireAt);
            map.put(key, new CacheEntry(key, value, expireAt, accessSeq.incrementAndGet()));
            return CacheStatus.OK;
        } finally {
            writeLock.unlock();
        }
    }

    public CacheStatus delete(String key) {
        Objects.requireNonNull(key, "key");
        writeLock.lock();
        try {
            long now = clock.millis();
            CacheEntry entry = map.get(key);
            if (entry == null) {
                return CacheStatus.NOT_FOUND;
            }
            if (entry.isExpired(now)) {
                if (map.remove(key, entry)) {
                    expirations.incrementAndGet();
                }
                return CacheStatus.EXPIRED;
            }
            wal.appendDelete(key);
            map.remove(key);
            return CacheStatus.OK;
        } finally {
            writeLock.unlock();
        }
    }

    public BatchOutcome getBatch(Collection<String> keys) {
        Objects.requireNonNull(keys, "keys");
        Map<String, String> values = new LinkedHashMap<>();
        Map<String, CacheStatus> failures = new LinkedHashMap<>();
        for (String key : keys) {
            CacheResult<String> r = get(key);
            if (r.isOk()) {
                values.put(key, r.value());
            } else {
                failures.put(key, r.status());
            }
        }
        return new BatchOutcome(values, failures);
    }

    /**
     * 原子批量写入：先校验全部键值非空；回收过期后若放不下则整体拒绝
     * （已有数据完全不变，不写任何 WAL）；否则全部生效。
     */
    public CacheStatus putBatch(Map<String, String> entries, long ttlMillis) {
        Objects.requireNonNull(entries, "entries");
        for (Map.Entry<String, String> e : entries.entrySet()) {
            Objects.requireNonNull(e.getKey(), "entry key");
            Objects.requireNonNull(e.getValue(), "entry value");
        }
        writeLock.lock();
        try {
            long now = clock.millis();
            long expireAt = ttlMillis > 0 ? now + ttlMillis : 0L;

            purgeExpiredLocked(now);

            long newSlots = 0;
            for (String key : entries.keySet()) {
                if (!map.containsKey(key)) {
                    newSlots++;
                }
            }
            if ((long) map.size() + newSlots > capacity) {
                evictions.addAndGet(entries.size());
                log.info("批量写入被拒绝(CAPACITY_FULL): 请求 {} 条, 新增槽位 {} 个, 当前存活={}, 容量={}",
                        entries.size(), newSlots, map.size(), capacity);
                return CacheStatus.CAPACITY_FULL;
            }
            for (Map.Entry<String, String> e : entries.entrySet()) {
                wal.appendPut(e.getKey(), e.getValue(), expireAt);
                map.put(e.getKey(),
                        new CacheEntry(e.getKey(), e.getValue(), expireAt, accessSeq.incrementAndGet()));
            }
            return CacheStatus.OK;
        } finally {
            writeLock.unlock();
        }
    }

    /** 显式清理所有已过期键，返回被回收的数量（过期回收不写 WAL）。 */
    public int purgeExpired() {
        writeLock.lock();
        try {
            return purgeExpiredLocked(clock.millis());
        } finally {
            writeLock.unlock();
        }
    }

    private int purgeExpiredLocked(long now) {
        int removed = 0;
        for (Map.Entry<String, CacheEntry> e : map.entrySet()) {
            if (e.getValue().isExpired(now) && map.remove(e.getKey(), e.getValue())) {
                removed++;
            }
        }
        if (removed > 0) {
            expirations.addAndGet(removed);
            log.debug("主动过期回收 {} 条, 当前存活={}", removed, map.size());
        }
        return removed;
    }

    /** 当前存活条目数（不包含已过期但尚未回收的条目）。 */
    public int size() {
        writeLock.lock();
        try {
            purgeExpiredLocked(clock.millis());
            return map.size();
        } finally {
            writeLock.unlock();
        }
    }

    public int capacity() {
        return capacity;
    }

    public EngineStats stats() {
        writeLock.lock();
        try {
            purgeExpiredLocked(clock.millis());
            return new EngineStats(map.size(), capacity,
                    hits.get(), misses.get(), expirations.get(), evictions.get());
        } finally {
            writeLock.unlock();
        }
    }

    /** 用当前存活快照重写压缩 WAL，返回重写记录数。 */
    public int compact() {
        writeLock.lock();
        try {
            long now = clock.millis();
            purgeExpiredLocked(now);
            List<WalRecord> snapshot = new ArrayList<>(map.size());
            for (CacheEntry e : map.values()) {
                snapshot.add(new WalRecord(WalRecord.PUT, e.getKey(), e.getValue(), e.getExpireAtMillis()));
            }
            return wal.rewrite(snapshot);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void close() {
        writeLock.lock();
        try {
            wal.close();
        } finally {
            writeLock.unlock();
        }
    }
}
