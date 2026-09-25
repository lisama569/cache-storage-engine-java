package com.github.lisama569.cachestorageenginejava.cache;

/**
 * 缓存操作结果，携带可区分的状态与可选的值。
 */
public record CacheResult<V>(CacheStatus status, V value) {

    public static <V> CacheResult<V> ok(V value) {
        return new CacheResult<>(CacheStatus.OK, value);
    }

    public static <V> CacheResult<V> of(CacheStatus status) {
        return new CacheResult<>(status, null);
    }

    public boolean isOk() {
        return status == CacheStatus.OK;
    }
}
