package com.github.lisama569.cachestorageenginejava.web;

import java.util.List;

/**
 * 批量读取/删除请求。namespace 为空（缺省）时操作默认空间，与旧版行为一致。
 */
public record KeysRequest(List<String> keys, String namespace) {
}
