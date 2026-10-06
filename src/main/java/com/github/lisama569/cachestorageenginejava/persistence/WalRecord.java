package com.github.lisama569.cachestorageenginejava.persistence;

/**
 * WAL 记录：类型 + 空间 + 键 + 值 + 过期时间戳(0=永不过期)。
 *
 * <p>兼容说明：v1 日志帧没有空间字段，恢复时统一归入
 * {@link #DEFAULT_NAMESPACE}；保留 4 参数构造器以兼容旧调用点。
 */
public record WalRecord(byte type, String namespace, String key, String value, long expireAtMillis) {

    public static final byte PUT = 1;
    public static final byte DELETE = 2;

    /** 未显式指定空间时使用的默认空间名。 */
    public static final String DEFAULT_NAMESPACE = "default";

    /** 兼容 v1 调用点：不指定空间即默认空间。 */
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
