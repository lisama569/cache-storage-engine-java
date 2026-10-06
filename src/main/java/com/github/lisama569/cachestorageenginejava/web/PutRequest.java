package com.github.lisama569.cachestorageenginejava.web;

/**
 * 单键写入请求。namespace 为 null 时使用默认空间。
 */
public record PutRequest(String key, String value, Long ttlMillis, String namespace) {

    /** 兼容旧版 3 参数构造（默认空间）。 */
    public PutRequest(String key, String value, Long ttlMillis) {
        this(key, value, ttlMillis, null);
    }
}
