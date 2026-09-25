package com.github.lisama569.cachestorageenginejava.web;

/**
 * 单键写入请求。
 */
public record PutRequest(String key, String value, Long ttlMillis) {
}
