package com.github.lisama569.cachestorageenginejava.web;

import java.util.List;

/**
 * 批量读取/删除请求。namespace 为 null 时使用默认空间。
 */
public record KeysRequest(List<String> keys, String namespace) {

    /** 兼容旧版 1 参数构造（默认空间）。 */
    public KeysRequest(List<String> keys) {
        this(keys, null);
    }
}
