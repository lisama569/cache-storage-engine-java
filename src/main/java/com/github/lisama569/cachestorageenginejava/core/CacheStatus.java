package com.github.lisama569.cachestorageenginejava.core;

/**
 * 可区分的操作结果状态。
 */
public enum CacheStatus {
    /** 操作成功。 */
    OK,
    /** 键不存在。 */
    NOT_FOUND,
    /** 键存在但已过期。 */
    EXPIRED,
    /** 容量不足：无法在不淘汰未过期数据的前提下写入。 */
    CAPACITY_FULL
}
