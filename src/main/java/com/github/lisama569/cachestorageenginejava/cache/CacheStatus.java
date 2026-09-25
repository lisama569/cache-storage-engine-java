package com.github.lisama569.cachestorageenginejava.cache;

/**
 * 可区分的操作结果状态。
 */
public enum CacheStatus {
    OK,
    NOT_FOUND,
    EXPIRED,
    CAPACITY_EXCEEDED
}
