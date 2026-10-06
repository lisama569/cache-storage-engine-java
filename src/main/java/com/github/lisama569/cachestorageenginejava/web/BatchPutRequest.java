package com.github.lisama569.cachestorageenginejava.web;

import java.util.Map;

/**
 * 批量写入请求。namespace 为空（缺省）时写入默认空间，与旧版行为一致。
 */
public record BatchPutRequest(Map<String, String> entries, Long ttlMillis, String namespace) {
}
