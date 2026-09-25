package com.github.lisama569.cachestorageenginejava.web;

import java.util.List;

/**
 * 批量读取/删除请求。
 */
public record KeysRequest(List<String> keys) {
}
