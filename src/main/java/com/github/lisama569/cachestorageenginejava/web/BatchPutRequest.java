package com.github.lisama569.cachestorageenginejava.web;

import java.util.Map;

/**
 * 批量写入请求。
 */
public record BatchPutRequest(Map<String, String> entries, Long ttlMillis) {
}
