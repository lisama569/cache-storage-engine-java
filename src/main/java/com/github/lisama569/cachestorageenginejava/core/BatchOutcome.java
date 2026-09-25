package com.github.lisama569.cachestorageenginejava.core;

import java.util.Map;

/**
 * 批量读结果：命中值与失败原因（NOT_FOUND / EXPIRED）。
 */
public record BatchOutcome(Map<String, String> values, Map<String, CacheStatus> failures) {
}
