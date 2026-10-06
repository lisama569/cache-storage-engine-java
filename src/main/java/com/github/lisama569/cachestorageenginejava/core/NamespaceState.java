package com.github.lisama569.cachestorageenginejava.core;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 单个空间的运行态（包私有）。
 *
 * <p>并发约定与 {@link CacheEngine} 一致：
 * <ul>
 *   <li>{@code liveCount} 的递增只发生在写锁内（新键写入/恢复重放）；
 *       递减可能发生在写锁内（删除/主动清理）或无锁读路径（惰性过期 CAS 回收，
 *       CAS 保证同一条目只被递减一次），因此用 AtomicLong 保证跨路径一致；</li>
 *   <li>统计计数器全部为 AtomicLong，读路径可无锁更新。</li>
 * </ul>
 */
final class NamespaceState {

    /** 当前存活条目数（不含已过期未回收）。 */
    final AtomicLong liveCount = new AtomicLong();
    final AtomicLong hits = new AtomicLong();
    final AtomicLong misses = new AtomicLong();
    final AtomicLong expirations = new AtomicLong();
    /** 因本空间容量不足被拒绝（未生效）的写入次数。 */
    final AtomicLong evictions = new AtomicLong();
}
