package com.github.lisama569.cachestorageenginejava.persistence;

/**
 * WAL 记录：类型 + 所属空间 + 键 + 值 + 过期时间戳(0=永不过期)。
 */
public record WalRecord(byte type, String namespace, String key, String value, long expireAtMillis) {

    public static final byte PUT = 1;
    public static final byte DELETE = 2;

    /** 未显式指定空间时使用的默认空间名（v1 格式记录恢复后也归属该空间）。 */
    public static final String DEFAULT_NAMESPACE = "default";

    /** 兼容旧版 4 参数构造：归属默认空间。 */
    public WalRecord(byte type, String key, String value, long expireAtMillis) {
        this(type, DEFAULT_NAMESPACE, key, value, expireAtMillis);
    }

    public boolean isPut() {
        return type == PUT;
    }

    public boolean isDelete() {
        return type == DELETE;
    }
}
