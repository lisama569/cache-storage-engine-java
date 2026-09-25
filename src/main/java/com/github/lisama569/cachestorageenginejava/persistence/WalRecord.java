package com.github.lisama569.cachestorageenginejava.persistence;

/**
 * WAL 记录：类型 + 键 + 值 + 过期时间戳(0=永不过期)。
 */
public record WalRecord(byte type, String key, String value, long expireAtMillis) {

    public static final byte PUT = 1;
    public static final byte DELETE = 2;

    public boolean isPut() {
        return type == PUT;
    }

    public boolean isDelete() {
        return type == DELETE;
    }
}
