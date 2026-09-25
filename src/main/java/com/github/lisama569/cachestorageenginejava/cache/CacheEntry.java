package com.github.lisama569.cachestorageenginejava.cache;

/**
 * 缓存条目：值 + 过期时间戳（expireAtMillis <= 0 表示永不过期）。
 */
record CacheEntry(byte[] value, long expireAtMillis) {

    boolean isExpired(long nowMillis) {
        return expireAtMillis > 0 && nowMillis >= expireAtMillis;
    }

    int size() {
        return value.length;
    }
}
