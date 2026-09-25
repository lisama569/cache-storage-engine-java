package com.github.lisama569.cachestorageenginejava.core;

/**
 * 引擎统计快照。
 */
public record EngineStats(int size, int capacity, long hits, long misses, long expirations, long evictions) {
}
