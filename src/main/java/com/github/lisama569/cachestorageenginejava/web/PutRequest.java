package com.github.lisama569.cachestorageenginejava.web;

/**
 * 单键写入请求。namespace 为空（缺省）时写入默认空间，与旧版行为一致。
 */
public record PutRequest(String key, String value, Long ttlMillis, String namespace) {
}
