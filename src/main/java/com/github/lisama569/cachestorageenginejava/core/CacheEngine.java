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
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 本地缓存存储引擎，支持按空间（namespace）隔离。
 *
 * <p>空间模型：
 * <ul>
 *   <li>每条数据归属且仅归属一个空间；不同空间中同名的键是互不影响的两份数据；</li>
 *   <li>不指定空间的调用一律落入 {@link #DEFAULT_NAMESPACE}，行为与引入空间前完全一致；</li>
 *   <li>每个空间有各自独立的容量上限与统计计数，空间之间互不挤占：
 *       某空间写满只拒绝该空间的新写入，其它空间照常可用；</li>
 *   <li>未通过 {@link #registerNamespace} 显式注册的空间，在首次访问时以默认容量懒创建。</li>
 * </ul>
 *
 * <p>淘汰与过期策略（确定性，按空间独立判定）：
 * <ol>
 *   <li>容量判定前先回收该空间内已过期条目（惰性：读命中过期键；主动：每次写前与
 *       {@link #purgeExpired()}）；</li>
 *   <li>本引擎不静默丢弃未过期数据：回收过期条目后仍放不下时，写入被拒绝并返回
 *       {@link CacheStatus#CAPACITY_FULL}，已有数据与计数不变；</li>
 *   <li>空间容量为 0 时，该空间的一切新写入都确定性地返回
 *       {@link CacheStatus#CAPACITY_FULL}（覆盖"单条数据即超过空间总容量"的极端情形）；</li>
 *   <li>过期判定：{@code now >= expireAtMillis}，expireAtMillis == 0 表示永不过期。</li>
 * </ol>
 *
 * <p>并发模型：
 * <ul>
 *   <li>所有结构性变更（put/delete/批量写/压缩/主动清理/注册空间）在单把可重入写锁下执行，
 *       跨空间同样互斥，不存在多锁顺序环，无死锁；</li>
 *   <li>读路径无锁：{@link ConcurrentHashMap} + 过期判定 + CAS 删除；
 *       写路径整体替换条目对象，读者只能看到更新前或更新后的完整对象；</li>
 *   <li>每个对外方法构成一个线性化点；各空间容量与计数只在写锁内修改，杜绝计数漂移。</li>
 * </ul>
 *
 * <p>持久化：先写 WAL（fsync）后改内存；WAL 记录携带空间名，重启后每条数据恢复到
 * 原属空间，各空间占用情况与重启前一致；过期条目的回收不写 DELETE
 * （重放时过期 PUT 会被跳过）；只有对存活条目的显式删除才写 DELETE。
 */
public final class CacheEngine implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CacheEngine.class);

    /** 未显式指定空间时使用的默认空间。 */
    public static final String DEFAULT_NAMESPACE = WalRecord.DEFAULT_NAMESPACE;

    private final int defaultCapacity;
    private final WriteAheadLog wal;
    private final Clock clock;

    /** 空间名 -> 空间状态。 */
    private final ConcurrentHashMap<String, NamespaceStore> spaces = new ConcurrentHashMap<>();
    private final ReentrantLock writeLock = new ReentrantLock();
    private final AtomicLong accessSeq = new AtomicLong(0);

    /** 单个空间的独立状态：条目表、容量上限、统计计数。 */
    private static final class NamespaceStore {
        final String name;
        final ConcurrentHashMap<String, CacheEntry> map = new ConcurrentHashMap<>();
        /** 容量上限；只在写锁内修改，volatile 保证读侧可见。0 表示拒绝一切新写入。 */
        volatile int capacity;
        final AtomicLong hits = new AtomicLong();
        final AtomicLong misses = new AtomicLong();
        final AtomicLong expirations = new AtomicLong();
        /** 因容量不足被拒绝（未生效）的写入次数。 */
        final AtomicLong evictions = new AtomicLong();

        NamespaceStore(String name, int capacity) {
            this.name = name;
            this.capacity = capacity;
        }
    }

    public CacheEngine(int capacity, Path dataFile, Clock clock) {
        this(capacity, Map.of(), dataFile, clock);
    }

    /**
     * @param defaultCapacity      默认空间及未显式注册空间的容量上限（必须为正数）
     * @param namespaceCapacities  预注册空间及其独立容量（容量须 &ge; 0；0 表示该空间拒绝一切新写入）
     */
    public CacheEngine(int defaultCapacity, Map<String, Integer> namespaceCapacities,
                       Path dataFile, Clock clock) {
        if (defaultCapacity <= 0) {
            throw new IllegalArgumentException("默认容量必须为正数: " + defaultCapacity);
        }
        this.defaultCapacity = defaultCapacity;
        this.clock = Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(dataFile, "dataFile");
        Objects.requireNonNull(namespaceCapacities, "namespaceCapacities");
        for (Map.Entry<String, Integer> e : namespaceCapacities.entrySet()) {
            spaces.put(validateNamespace(e.getKey()),
                    new NamespaceStore(e.getKey(), validateCapacity(e.getKey(), e.getValue())));
        }
        this.wal = new WriteAheadLog(dataFile);
        replay();
    }

    private static String validateNamespace(String namespace) {
        Objects.requireNonNull(namespace, "namespace");
        if (namespace.isEmpty()) {
            throw new IllegalArgumentException("空间名不能为空");
        }
        return namespace;
    }

    private static int validateCapacity(String namespace, int capacity) {
        if (capacity < 0) {
            throw new IllegalArgumentException("空间 " + namespace + " 的容量不能为负数: " + capacity);
        }
        return capacity;
    }

    /** 取空间状态，不存在则以默认容量懒创建（读路径亦可用，computeIfAbsent 原子）。 */
    private NamespaceStore storeOf(String namespace) {
        validateNamespace(namespace);
        return spaces.computeIfAbsent(namespace, n -> {
            log.info("空间首次使用，按默认容量 {} 懒创建: {}", defaultCapacity, n);
            return new NamespaceStore(n, defaultCapacity);
        });
    }

    /**
     * 显式注册空间并设定独立容量；空间已存在时仅更新其容量上限
     * （容量只约束新写入，已有数据不受影响）。容量 0 表示拒绝该空间一切新写入。
     */
    public void registerNamespace(String namespace, int capacity) {
        validateNamespace(namespace);
        validateCapacity(namespace, capacity);
        writeLock.lock();
        try {
            NamespaceStore store = spaces.get(namespace);
            if (store == null) {
                spaces.put(namespace, new NamespaceStore(namespace, capacity));
                log.info("注册空间: {}, 容量={}", namespace, capacity);
            } else {
                store.capacity = capacity;
                log.info("更新空间容量: {}, 容量 {} -> {}（已有 {} 条数据不受影响）",
                        namespace, store.capacity, capacity, store.map.size());
            }
        } finally {
            writeLock.unlock();
        }
    }

    /** 当前已知空间名快照（含默认空间，若已被使用）。 */
    public Set<String> namespaces() {
        return new TreeSet<>(spaces.keySet());
    }

    /** 按 WAL 顺序重放：PUT 覆盖旧值、DELETE 删除、已过期 PUT 跳过；记录按空间名归位。 */
    private void replay() {
        List<WalRecord> records = wal.recoveredRecords();
        long now = clock.millis();
        int restored = 0;
        int skippedExpired = 0;
        int deletes = 0;
        for (WalRecord r : records) {
            NamespaceStore store = storeOf(r.namespace());
            if (r.isDelete()) {
                if (store.map.remove(r.key()) != null) {
                    deletes++;
                }
                continue;
            }
            if (r.expireAtMillis() > 0 && now >= r.expireAtMillis()) {
                skippedExpired++;
                continue;
            }
            store.map.put(r.key(),
                    new CacheEntry(r.key(), r.value(), r.expireAtMillis(), accessSeq.incrementAndGet()));
            restored++;
        }
        // 恢复时不因容量上限丢弃数据（上限只约束新写入），仅记录提示。
        for (NamespaceStore store : spaces.values()) {
            if (store.map.size() > store.capacity) {
                log.warn("空间 {} 恢复后存活条目 {} 超过容量上限 {}，保留全部已恢复数据；后续写入将在回收过期后按容量判定",
                        store.name, store.map.size(), store.capacity);
            }
        }
        log.info("缓存恢复结果: WAL 记录 {} 条 -> 存活恢复 {} 条，过期忽略 {} 条，删除生效 {} 条，涉及空间 {}",
                records.size(), restored, skippedExpired, deletes, namespaces());
    }

    public CacheResult<String> get(String key) {
        return get(DEFAULT_NAMESPACE, key);
    }

    /** 无锁读；命中过期键时惰性 CAS 回收（过期回收不写 WAL）。 */
    public CacheResult<String> get(String namespace, String key) {
        Objects.requireNonNull(key, "key");
        NamespaceStore store = storeOf(namespace);
        CacheEntry entry = store.map.get(key);
        if (entry == null) {
            store.misses.incrementAndGet();
            return CacheResult.of(CacheStatus.NOT_FOUND);
        }
        long now = clock.millis();
        if (entry.isExpired(now)) {
            // CAS 删除：只有条目仍是同一个过期对象时才回收，绝不误删并发写入的新对象。
            if (store.map.remove(key, entry)) {
                store.expirations.incrementAndGet();
                log.debug("惰性过期回收: ns={}, key={}, expireAt={}, now={}",
                        namespace, key, entry.getExpireAtMillis(), now);
            }
            store.misses.incrementAndGet();
            return CacheResult.of(CacheStatus.EXPIRED);
        }
        entry.setLastAccessSeq(accessSeq.incrementAndGet());
        store.hits.incrementAndGet();
        return CacheResult.ok(entry.getValue());
    }

    public CacheStatus put(String key, String value, long ttlMillis) {
        return put(DEFAULT_NAMESPACE, key, value, ttlMillis);
    }

    /** ttlMillis &lt;= 0 表示永不过期。容量判定只针对目标空间，不影响其它空间。 */
    public CacheStatus put(String namespace, String key, String value, long ttlMillis) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        NamespaceStore store = storeOf(namespace);
        writeLock.lock();
        try {
            long now = clock.millis();
            long expireAt = ttlMillis > 0 ? now + ttlMillis : 0L;
            CacheEntry existing = store.map.get(key);
            boolean replacing = existing != null && !existing.isExpired(now);

            if (!replacing) {
                purgeExpiredLocked(store, now);
                if (store.map.size() >= store.capacity) {
                    store.evictions.incrementAndGet();
                    log.info("写入被拒绝(CAPACITY_FULL): ns={}, key={}, 当前存活={}, 容量={}",
                            namespace, key, store.map.size(), store.capacity);
                    return CacheStatus.CAPACITY_FULL;
                }
            }
            // 先持久化后改内存；WAL 失败则内存不变（WAL 内部抛异常，调用方可见）。
            wal.appendPut(namespace, key, value, expireAt);
            store.map.put(key, new CacheEntry(key, value, expireAt, accessSeq.incrementAndGet()));
            return CacheStatus.OK;
        } finally {
            writeLock.unlock();
        }
    }

    public CacheStatus delete(String key) {
        return delete(DEFAULT_NAMESPACE, key);
    }

    public CacheStatus delete(String namespace, String key) {
        Objects.requireNonNull(key, "key");
        NamespaceStore store = storeOf(namespace);
        writeLock.lock();
        try {
            long now = clock.millis();
            CacheEntry entry = store.map.get(key);
            if (entry == null) {
                return CacheStatus.NOT_FOUND;
            }
            if (entry.isExpired(now)) {
                if (store.map.remove(key, entry)) {
                    store.expirations.incrementAndGet();
                }
                return CacheStatus.EXPIRED;
            }
            wal.appendDelete(namespace, key);
            store.map.remove(key);
            return CacheStatus.OK;
        } finally {
            writeLock.unlock();
        }
    }

    public BatchOutcome getBatch(Collection<String> keys) {
        return getBatch(DEFAULT_NAMESPACE, keys);
    }

    public BatchOutcome getBatch(String namespace, Collection<String> keys) {
        Objects.requireNonNull(keys, "keys");
        Map<String, String> values = new LinkedHashMap<>();
        Map<String, CacheStatus> failures = new LinkedHashMap<>();
        for (String key : keys) {
            CacheResult<String> r = get(namespace, key);
            if (r.isOk()) {
                values.put(key, r.value());
            } else {
                failures.put(key, r.status());
            }
        }
        return new BatchOutcome(values, failures);
    }

    public CacheStatus putBatch(Map<String, String> entries, long ttlMillis) {
        return putBatch(DEFAULT_NAMESPACE, entries, ttlMillis);
    }

    /**
     * 原子批量写入（单个空间内）：先校验全部键值非空；回收过期后若放不下则整体拒绝
     * （已有数据完全不变，不写任何 WAL）；否则全部生效。
     */
    public CacheStatus putBatch(String namespace, Map<String, String> entries, long ttlMillis) {
        Objects.requireNonNull(entries, "entries");
        for (Map.Entry<String, String> e : entries.entrySet()) {
            Objects.requireNonNull(e.getKey(), "entry key");
            Objects.requireNonNull(e.getValue(), "entry value");
        }
        NamespaceStore store = storeOf(namespace);
        writeLock.lock();
        try {
            long now = clock.millis();
            long expireAt = ttlMillis > 0 ? now + ttlMillis : 0L;

            purgeExpiredLocked(store, now);

            long newSlots = 0;
            for (String key : entries.keySet()) {
                if (!store.map.containsKey(key)) {
                    newSlots++;
                }
            }
            if ((long) store.map.size() + newSlots > store.capacity) {
                store.evictions.addAndGet(entries.size());
                log.info("批量写入被拒绝(CAPACITY_FULL): ns={}, 请求 {} 条, 新增槽位 {} 个, 当前存活={}, 容量={}",
                        namespace, entries.size(), newSlots, store.map.size(), store.capacity);
                return CacheStatus.CAPACITY_FULL;
            }
            for (Map.Entry<String, String> e : entries.entrySet()) {
                wal.appendPut(namespace, e.getKey(), e.getValue(), expireAt);
                store.map.put(e.getKey(),
                        new CacheEntry(e.getKey(), e.getValue(), expireAt, accessSeq.incrementAndGet()));
            }
            return CacheStatus.OK;
        } finally {
            writeLock.unlock();
        }
    }

    /** 显式清理所有空间的已过期键，返回被回收的总数量（过期回收不写 WAL）。 */
    public int purgeExpired() {
        writeLock.lock();
        try {
            long now = clock.millis();
            int total = 0;
            for (NamespaceStore store : spaces.values()) {
                total += purgeExpiredLocked(store, now);
            }
            return total;
        } finally {
            writeLock.unlock();
        }
    }

    /** 显式清理指定空间的已过期键，返回被回收的数量。 */
    public int purgeExpired(String namespace) {
        NamespaceStore store = storeOf(namespace);
        writeLock.lock();
        try {
            return purgeExpiredLocked(store, clock.millis());
        } finally {
            writeLock.unlock();
        }
    }

    private int purgeExpiredLocked(NamespaceStore store, long now) {
        int removed = 0;
        for (Map.Entry<String, CacheEntry> e : store.map.entrySet()) {
            if (e.getValue().isExpired(now) && store.map.remove(e.getKey(), e.getValue())) {
                removed++;
            }
        }
        if (removed > 0) {
            store.expirations.addAndGet(removed);
            log.debug("主动过期回收: ns={}, 回收 {} 条, 当前存活={}", store.name, removed, store.map.size());
        }
        return removed;
    }

    /** 默认空间当前存活条目数（不包含已过期但尚未回收的条目）。 */
    public int size() {
        return size(DEFAULT_NAMESPACE);
    }

    /** 指定空间当前存活条目数（不包含已过期但尚未回收的条目）。 */
    public int size(String namespace) {
        NamespaceStore store = storeOf(namespace);
        writeLock.lock();
        try {
            purgeExpiredLocked(store, clock.millis());
            return store.map.size();
        } finally {
            writeLock.unlock();
        }
    }

    /** 默认空间的容量上限。 */
    public int capacity() {
        return capacity(DEFAULT_NAMESPACE);
    }

    /** 指定空间的容量上限。 */
    public int capacity(String namespace) {
        return storeOf(namespace).capacity;
    }

    /** 默认空间统计。 */
    public EngineStats stats() {
        return stats(DEFAULT_NAMESPACE);
    }

    /** 指定空间统计。 */
    public EngineStats stats(String namespace) {
        NamespaceStore store = storeOf(namespace);
        writeLock.lock();
        try {
            purgeExpiredLocked(store, clock.millis());
            return new EngineStats(store.map.size(), store.capacity,
                    store.hits.get(), store.misses.get(), store.expirations.get(),
                    store.evictions.get());
        } finally {
            writeLock.unlock();
        }
    }

    /** 用全部空间的当前存活快照重写压缩 WAL，返回重写记录数。 */
    public int compact() {
        writeLock.lock();
        try {
            long now = clock.millis();
            List<WalRecord> snapshot = new ArrayList<>();
            for (NamespaceStore store : spaces.values()) {
                purgeExpiredLocked(store, now);
                for (CacheEntry e : store.map.values()) {
                    snapshot.add(new WalRecord(WalRecord.PUT, store.name, e.getKey(), e.getValue(),
                            e.getExpireAtMillis()));
                }
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
