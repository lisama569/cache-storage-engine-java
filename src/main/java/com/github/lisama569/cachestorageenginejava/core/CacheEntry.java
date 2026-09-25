package com.github.lisama569.cachestorageenginejava.core;

/**
 * 缓存条目。
 *
 * <p>不可变部分（key/value/expireAtMillis）在发布后不再修改——写路径通过
 * “整体替换条目对象”完成更新，保证无锁读路径的线性化；
 * 仅 {@code lastAccessSeq} 会被读路径单调推进（允许并发下的良性竞争）。
 */
public final class CacheEntry {

    private final String key;
    private final String value;
    private final long expireAtMillis;
    private volatile long lastAccessSeq;

    public CacheEntry(String key, String value, long expireAtMillis, long lastAccessSeq) {
        this.key = key;
        this.value = value;
        this.expireAtMillis = expireAtMillis;
        this.lastAccessSeq = lastAccessSeq;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public long getExpireAtMillis() {
        return expireAtMillis;
    }

    public long getLastAccessSeq() {
        return lastAccessSeq;
    }

    public void setLastAccessSeq(long lastAccessSeq) {
        this.lastAccessSeq = lastAccessSeq;
    }

    /** expireAtMillis == 0 表示永不过期；到期时刻本身即视为已过期（now >= expireAt）。 */
    public boolean isExpired(long nowMillis) {
        return expireAtMillis > 0 && nowMillis >= expireAtMillis;
    }
}
