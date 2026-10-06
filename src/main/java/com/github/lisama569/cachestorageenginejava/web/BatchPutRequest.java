package com.github.lisama569.cachestorageenginejava.web;

import java.util.Map;

/**
 * 批量写入请求（单个空间内原子生效）。namespace 为 null 时使用默认空间。
 */
public record BatchPutRequest(Map<String, String> entries, Long ttlMillis, String namespace) {

    /** 兼容旧版 2 参数构造（默认空间）。 */
    public BatchPutRequest(Map<String, String> entries, Long ttlMillis) {
        this(entries, ttlMillis, null);
    }
}
