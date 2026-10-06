package com.github.lisama569.cachestorageenginejava.core;

import java.util.Objects;

/**
 * 空间内键：(namespace, key) 二元组，作为引擎内部映射的复合键。
 * 不同空间下同名 key 是两个不同的 NamespacedKey，互不影响。
 */
public record NamespacedKey(String namespace, String key) {

    public NamespacedKey {
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(key, "key");
    }
}
