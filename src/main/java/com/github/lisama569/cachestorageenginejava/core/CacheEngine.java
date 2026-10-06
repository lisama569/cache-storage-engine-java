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
import java.util.concurrent.locks.ReentrantLock;

/**
 * 本地缓存存储引擎（按命名空间隔离）。
 *
 * <p>空间模型：
 * <ul>
 *   <li>每条数据归属于一个命名空间；不同空间下同名键是两份互不影响的数据；</li>
 *   <li>不指定空间的旧 API 一律落入 {@link #DEFAULT_NAMESPACE}，行为与旧版一致，
 *       旧版 v1 WAL 恢复时也全部归入默认空间；</li>
 *   <li>空间无需预创建，首次写入自动存在；空间名为非空白字符串。</li>
 * </ul>
 *
 * <p>容量规则（按空间独立，互不挤占）：
 * <ol>
 *   <li>每个空间有独立的存活条目上限：默认 {@code defaultNamespaceCapacity}，
 *       可被构造器 {@code namespaceCapacities} 按空间名覆盖（允许覆盖为 0，
 *       表示该空间拒绝一切新键写入）；</li>
 *   <li>容量判定前先回收本空间已过期条目（惰性：读命中过期键；主动：每次写前与
 *       {@link #purgeExpired()}）；回收后仍放不下时写入被拒绝并返回
 *       {@link CacheStatus#CAPACITY_FULL}——只影响该空间的新写入，其它空间照常；</li>
 *   <li>本引擎不静默丢弃未过期数据：任何情况下都不会为了腾位淘汰存活条目；</li>
 *   <li>极端情况：批量写入的不同新键数量超过空间总容量（或空间容量为 0 时写入新键），
 *       确定性地整体返回 {@link CacheStatus#CAPACITY_FULL}，已有数据完全不变；</li>
 *   <li>容量上限只约束新写入：恢复/重放出的数据即使超过上限也完整保留。</li>
 * </ol>
 *
 * <p>过期判定：{@code now >= expireAtMillis}，expireAtMillis == 0 表示永不过期。
 *
 * <p>并发模型：
 * <ul>
 *   <li>所有结构性变更（put/delete/批量写/压缩/主动清理）在单把可重入写锁下执行，
 *       跨空间操作也互斥，无锁顺序环、不会死锁；</li>
 *   <li>读路径无锁：{@link ConcurrentHashMap} + 过期判定 + CAS 删除；
 *       写路径整体替换条目对象，读者只能看到更新前或更新后的完整对象；</li>
 *   <li>每个空间的存活计数只在写锁内递增；递减可能发生在无锁读路径的惰性过期
 *       CAS 回收（CAS 保证同一条目只递减一次），计数不会漂移；</li>
 *   <li>每个对外方法构成一个线性化点。</li>
 * </ul>
 *
 * <p>持久化：先写 WAL（fsync，帧内携带空间名）后改内存；过期条目的回收不需要写
 * DELETE（重放时过期 PUT 会被跳过）；只有对存活条目的显式删除才写 DELETE。
 */
public final class CacheEngine implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CacheEngine.class);

    /** 未显式指定空间时使用的默认空间名（与 WAL 默认空间一致）。 */
    public static final String DEFAULT_NAMESPACE = WalRecord.DEFAULT_NAMESPACE;

    private final int defaultNamespaceCapacity;
    private final Map<String, Integer> namespaceCapacities;
    private final WriteAheadLog wal;
    private final Clock clock;

    private final ConcurrentHashMap<NamespacedKey, CacheEntry> map = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, NamespaceState> states = new ConcurrentHashMap<>();
    private final ReentrantLock writeLock = new ReentrantLock();
    private final java.util.concurrent.atomic.AtomicLong accessSeq =
            new java.util.concurrent.atomic.AtomicLong(0);

    public CacheEngine(int capacity, Path dataFile, Clock clock) {
        this(capacity, Map.of(), dataFile, clock);
    }

    /**
     * @param defaultNamespaceCapacity 未单独配置的空间使用的默认容量上限（必须为正）
     * @param namespaceCapacities      空间名 -> 独立容量上限（覆盖默认值；允许为 0，
     *                                 表示该空间拒绝一切新键写入）
     */
    public CacheEngine(int defaultNamespaceCapacity, Map<String, Integer> namespaceCapacities,
                       Path dataFile, Clock clock) {
        if (defaultNamespaceCapacity <= 0) {
            throw new IllegalArgumentException("默认空间容量必须为正数: " + defaultNamespaceCapacity);
        }
        Objects.requireNonNull(namespaceCapacities, "namespaceCapacities");
        for (Map.Entry<String, Integer> e : namespaceCapacities.entrySet()) {
            validateNamespace(e.getKey());
            if (e.getValue() == null || e.getValue() < 0) {
                throw new IllegalArgumentException("空间容量不能为负: " + e.getKey() + "=" + e.getValue());
            }
        }
        this.defaultNamespaceCapacity = defaultNamespaceCapacity;
        this.namespaceCapacities = Map.copyOf(namespaceCapacities);
        this.clock = Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(dataFile, "dataFile");
        this.wal = new WriteAheadLog(dataFile);
        replay();
    }

    private static String validateNamespace(String namespace) {
        Objects.requireNonNull(namespace, "namespace");
        if (namespace.isBlank()) {
            throw new IllegalArgumentException("空间名不能为空白");
        }
        return namespace;
    }

    private NamespaceState state(String namespace) {
        return states.computeIfAbsent(namespace, ns -> new NamespaceState());
    }

    /** 按 WAL 顺序重放：PUT 覆盖旧值、DELETE 删除、已过期 PUT 跳过；按记录自带空间归位。 */
    private void replay() {
        List<WalRecord> records = wal.recoveredRecords();
        long now = clock.millis();
        int restored = 0;
        int skippedExpired = 0;
        int deletes = 0;
        for (WalRecord r : records) {
            String ns = r.namespace();
            NamespacedKey nk = new NamespacedKey(ns, r.key());
            NamespaceState st = state(ns);
            if (r.isDelete()) {
                if (map.remove(nk) != null) {
                    st.liveCount.decrementAndGet();
                    deletes++;
                }
                continue;
            }
            if (r.expireAtMillis() > 0 && now >= r.expireAtMillis()) {
                skippedExpired++;
                continue;
            }
            CacheEntry prev = map.put(nk,
                    new CacheEntry(r.key(), r.value(), r.expireAtMillis(), accessSeq.incrementAndGet()));
            if (prev == null) {
                st.liveCount.incrementAndGet();
            }
            restored++;
        }
        // 恢复时不因容量上限丢弃数据（上限只约束新写入），按空间分别记录提示。
        for (Map.Entry<String, NamespaceState> e : states.entrySet()) {
            long live = e.getValue().liveCount.get();
            int cap = capacityOf(e.getKey());
            if (live > cap) {
                log.warn("空间 '{}' 恢复后存活条目 {} 超过容量上限 {}，保留全部已恢复数据；"
                        + "后续写入将在回收过期后按容量判定", e.getKey(), live, cap);
            }
        }
        log.info("缓存恢复结果: WAL 记录 {} 条 -> 存活恢复 {} 条，过期忽略 {} 条，删除生效 {} 条，"
                + "涉及空间 {} 个，当前总存活 {}", records.size(), restored, skippedExpired, deletes,
                states.size(), totalLive());
    }

    private long totalLive() {
        long sum = 0;
        for (NamespaceState st : states.values()) {
            sum += st.liveCount.get();
        }
        return sum;
    }

    // ---------- 读 ----------

    /** 无锁读；命中过期键时惰性 CAS 回收（过期回收不写 WAL）。 */
    public CacheResult<String> get(String namespace, String key) {
        validateNamespace(namespace);
        Objects.requireNonNull(key, "key");
        NamespaceState st = state(namespace);
        CacheEntry entry = map.get(new NamespacedKey(namespace, key));
        if (entry == null) {
            st.misses.incrementAndGet();
            return CacheResult.of(CacheStatus.NOT_FOUND);
        }
        long now = clock.millis();
        if (entry.isExpired(now)) {
            // CAS 删除：只有条目仍是同一个过期对象时才回收，绝不误删并发写入的新对象。
            if (map.remove(new NamespacedKey(namespace, key), entry)) {
                st.liveCount.decrementAndGet();
                st.expirations.incrementAndGet();
                log.debug("惰性过期回收: ns={}, key={}, expireAt={}, now={}",
                        namespace, key, entry.getExpireAtMillis(), now);
            }
            st.misses.incrementAndGet();
            return CacheResult.of(CacheStatus.EXPIRED);
        }
        entry.setLastAccessSeq(accessSeq.incrementAndGet());
        st.hits.incrementAndGet();
        return CacheResult.ok(entry.getValue());
    }

    public CacheResult<String> get(String key) {
        return get(DEFAULT_NAMESPACE, key);
    }

    // ---------- 写 ----------

    /** ttlMillis &lt;= 0 表示永不过期。 */
    public CacheStatus put(String namespace, String key, String value, long ttlMillis) {
        validateNamespace(namespace);
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        writeLock.lock();
        try {
            long now = clock.millis();
            long expireAt = ttlMillis > 0 ? now + ttlMillis : 0L;
            NamespacedKey nk = new NamespacedKey(namespace, key);
            NamespaceState st = state(namespace);
            CacheEntry existing = map.get(nk);
            boolean replacing = existing != null && !existing.isExpired(now);

            if (!replacing) {
                purgeExpiredLocked(now);
                int cap = capacityOf(namespace);
                if (st.liveCount.get() >= cap) {
                    st.evictions.incrementAndGet();
                    log.info("写入被拒绝(CAPACITY_FULL): ns={}, key={}, 本空间存活={}, 本空间容量={}",
                            namespace, key, st.liveCount.get(), cap);
                    return CacheStatus.CAPACITY_FULL;
                }
            }
            // 先持久化后改内存；WAL 失败则内存不变（WAL 内部抛异常，调用方可见）。
            wal.appendPut(namespace, key, value, expireAt);
            map.put(nk, new CacheEntry(key, value, expireAt, accessSeq.incrementAndGet()));
            if (!replacing) {
                st.liveCount.incrementAndGet();
            }
            return CacheStatus.OK;
        } finally {
            writeLock.unlock();
        }
    }

    public CacheStatus put(String key, String value, long ttlMillis) {
        return put(DEFAULT_NAMESPACE, key, value, ttlMillis);
    }

    public CacheStatus delete(String namespace, String key) {
        validateNamespace(namespace);
        Objects.requireNonNull(key, "key");
        writeLock.lock();
        try {
            long now = clock.millis();
            NamespacedKey nk = new NamespacedKey(namespace, key);
            NamespaceState st = state(namespace);
            CacheEntry entry = map.get(nk);
            if (entry == null) {
                return CacheStatus.NOT_FOUND;
            }
            if (entry.isExpired(now)) {
                if (map.remove(nk, entry)) {
                    st.liveCount.decrementAndGet();
                    st.expirations.incrementAndGet();
                }
                return CacheStatus.EXPIRED;
            }
            wal.appendDelete(namespace, key);
            map.remove(nk);
            st.liveCount.decrementAndGet();
            return CacheStatus.OK;
        } finally {
            writeLock.unlock();
        }
    }

    public CacheStatus delete(String key) {
        return delete(DEFAULT_NAMESPACE, key);
    }

    // ---------- 批量 ----------

    public BatchOutcome getBatch(String namespace, Collection<String> keys) {
        validateNamespace(namespace);
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

    public BatchOutcome getBatch(Collection<String> keys) {
        return getBatch(DEFAULT_NAMESPACE, keys);
    }

    /**
     * 原子批量写入（单个空间内）：先校验全部键值非空；回收过期后若本空间放不下则整体
     * 拒绝（已有数据完全不变，不写任何 WAL）；否则全部生效。批量中新键数量超过空间
     * 总容量时确定性地整体拒绝。
     */
    public CacheStatus putBatch(String namespace, Map<String, String> entries, long ttlMillis) {
        validateNamespace(namespace);
        Objects.requireNonNull(entries, "entries");
        for (Map.Entry<String, String> e : entries.entrySet()) {
            Objects.requireNonNull(e.getKey(), "entry key");
            Objects.requireNonNull(e.getValue(), "entry value");
        }
        writeLock.lock();
        try {
            long now = clock.millis();
            long expireAt = ttlMillis > 0 ? now + ttlMillis : 0L;
            NamespaceState st = state(namespace);

            purgeExpiredLocked(now);

            long newSlots = 0;
            for (String key : entries.keySet()) {
                if (!map.containsKey(new NamespacedKey(namespace, key))) {
                    newSlots++;
                }
            }
            int cap = capacityOf(namespace);
            if (st.liveCount.get() + newSlots > cap) {
                st.evictions.addAndGet(entries.size());
                log.info("批量写入被拒绝(CAPACITY_FULL): ns={}, 请求 {} 条, 新增槽位 {} 个, 本空间存活={}, 本空间容量={}",
                        namespace, entries.size(), newSlots, st.liveCount.get(), cap);
                return CacheStatus.CAPACITY_FULL;
            }
            for (Map.Entry<String, String> e : entries.entrySet()) {
                wal.appendPut(namespace, e.getKey(), e.getValue(), expireAt);
                NamespacedKey nk = new NamespacedKey(namespace, e.getKey());
                boolean isNew = !map.containsKey(nk);
                map.put(nk, new CacheEntry(e.getKey(), e.getValue(), expireAt,
                        accessSeq.incrementAndGet()));
                if (isNew) {
                    st.liveCount.incrementAndGet();
                }
            }
            return CacheStatus.OK;
        } finally {
            writeLock.unlock();
        }
    }

    public CacheStatus putBatch(Map<String, String> entries, long ttlMillis) {
        return putBatch(DEFAULT_NAMESPACE, entries, ttlMillis);
    }

    // ---------- 维护与统计 ----------

    /** 显式清理所有空间的已过期键，返回被回收的数量（过期回收不写 WAL）。 */
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
        for (Map.Entry<NamespacedKey, CacheEntry> e : map.entrySet()) {
            if (e.getValue().isExpired(now) && map.remove(e.getKey(), e.getValue())) {
                NamespaceState st = state(e.getKey().namespace());
                st.liveCount.decrementAndGet();
                st.expirations.incrementAndGet();
                removed++;
            }
        }
        if (removed > 0) {
            log.debug("主动过期回收 {} 条, 当前总存活={}", removed, totalLive());
        }
        return removed;
    }

    /** 指定空间当前存活条目数（不包含已过期但尚未回收的条目）。 */
    public int size(String namespace) {
        validateNamespace(namespace);
        writeLock.lock();
        try {
            purgeExpiredLocked(clock.millis());
            return (int) state(namespace).liveCount.get();
        } finally {
            writeLock.unlock();
        }
    }

    /** 全部空间的总存活条目数。 */
    public int size() {
        writeLock.lock();
        try {
            purgeExpiredLocked(clock.millis());
            return (int) totalLive();
        } finally {
            writeLock.unlock();
        }
    }

    /** 指定空间的容量上限（覆盖值或默认值）。 */
    public int capacityOf(String namespace) {
        validateNamespace(namespace);
        Integer override = namespaceCapacities.get(namespace);
        return override != null ? override : defaultNamespaceCapacity;
    }

    /** 默认空间容量上限（兼容旧语义：即构造参数 capacity）。 */
    public int capacity() {
        return defaultNamespaceCapacity;
    }

    /** 当前已知（有过读写或恢复数据）的空间名快照。 */
    public Set<String> namespaces() {
        return new TreeSet<>(states.keySet());
    }

    /** 指定空间的统计快照。 */
    public EngineStats stats(String namespace) {
        validateNamespace(namespace);
        writeLock.lock();
        try {
            purgeExpiredLocked(clock.millis());
            NamespaceState st = state(namespace);
            return new EngineStats((int) st.liveCount.get(), capacityOf(namespace),
                    st.hits.get(), st.misses.get(), st.expirations.get(), st.evictions.get());
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 全引擎聚合统计：size/各计数器为所有空间之和；capacity 字段保持旧语义
     * （默认空间容量上限，即构造参数 capacity）。
     */
    public EngineStats stats() {
        writeLock.lock();
        try {
            purgeExpiredLocked(clock.millis());
            long size = 0, hits = 0, misses = 0, expirations = 0, evictions = 0;
            for (NamespaceState st : states.values()) {
                size += st.liveCount.get();
                hits += st.hits.get();
                misses += st.misses.get();
                expirations += st.expirations.get();
                evictions += st.evictions.get();
            }
            return new EngineStats((int) size, defaultNamespaceCapacity,
                    hits, misses, expirations, evictions);
        } finally {
            writeLock.unlock();
        }
    }

    /** 用当前存活快照重写压缩 WAL（帧内保留空间归属），返回重写记录数。 */
    public int compact() {
        writeLock.lock();
        try {
            long now = clock.millis();
            purgeExpiredLocked(now);
            List<WalRecord> snapshot = new ArrayList<>(map.size());
            for (Map.Entry<NamespacedKey, CacheEntry> e : map.entrySet()) {
                CacheEntry v = e.getValue();
                snapshot.add(new WalRecord(WalRecord.PUT, e.getKey().namespace(),
                        v.getKey(), v.getValue(), v.getExpireAtMillis()));
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
