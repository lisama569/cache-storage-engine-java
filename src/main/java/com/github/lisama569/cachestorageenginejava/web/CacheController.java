package com.github.lisama569.cachestorageenginejava.web;

import com.github.lisama569.cachestorageenginejava.core.BatchOutcome;
import com.github.lisama569.cachestorageenginejava.core.CacheEngine;
import com.github.lisama569.cachestorageenginejava.core.CacheResult;
import com.github.lisama569.cachestorageenginejava.core.CacheStatus;
import com.github.lisama569.cachestorageenginejava.core.EngineStats;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * 缓存 REST 接口。
 *
 * <p>空间隔离：所有端点接受可选空间参数——GET/DELETE/stats 用查询参数 {@code ?ns=}，
 * 批量端点在请求体中带 {@code "namespace"} 字段；缺省时落入默认空间，
 * 与引入空间前的行为完全一致。
 *
 * <p>失败原因通过 HTTP 状态码 + 响应体 code 双重区分：
 * NOT_FOUND=404，EXPIRED=410，CAPACITY_FULL=507。
 */
@RestController
@RequestMapping("/api/cache")
public class CacheController {

    private final CacheEngine engine;

    public CacheController(CacheEngine engine) {
        this.engine = engine;
    }

    private static String ns(String namespace) {
        return namespace == null ? CacheEngine.DEFAULT_NAMESPACE : namespace;
    }

    @GetMapping("/{key}")
    public ResponseEntity<ApiResponse<String>> get(@PathVariable String key,
                                                   @RequestParam(required = false) String ns) {
        CacheResult<String> r = engine.get(ns(ns), key);
        return switch (r.status()) {
            case OK -> ResponseEntity.ok(ApiResponse.ok(r.value()));
            case NOT_FOUND -> ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(ApiResponse.fail("NOT_FOUND", "键不存在: " + key));
            case EXPIRED -> ResponseEntity.status(HttpStatus.GONE)
                    .body(ApiResponse.fail("EXPIRED", "键已过期: " + key));
            default -> ResponseEntity.internalServerError()
                    .body(ApiResponse.fail(r.status().name(), "未预期状态"));
        };
    }

    @PostMapping
    public ResponseEntity<ApiResponse<Void>> put(@RequestBody PutRequest request) {
        CacheStatus status = engine.put(ns(request.namespace()), request.key(), request.value(),
                request.ttlMillis() == null ? 0L : request.ttlMillis());
        return switch (status) {
            case OK -> ResponseEntity.ok(ApiResponse.ok(null));
            case CAPACITY_FULL -> ResponseEntity.status(HttpStatus.INSUFFICIENT_STORAGE)
                    .body(ApiResponse.fail("CAPACITY_FULL", "容量不足，写入被拒绝（未淘汰任何未过期数据）"));
            default -> ResponseEntity.internalServerError()
                    .body(ApiResponse.fail(status.name(), "未预期状态"));
        };
    }

    @DeleteMapping("/{key}")
    public ResponseEntity<ApiResponse<Void>> delete(@PathVariable String key,
                                                    @RequestParam(required = false) String ns) {
        CacheStatus status = engine.delete(ns(ns), key);
        return switch (status) {
            case OK -> ResponseEntity.ok(ApiResponse.ok(null));
            case NOT_FOUND -> ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(ApiResponse.fail("NOT_FOUND", "键不存在: " + key));
            case EXPIRED -> ResponseEntity.status(HttpStatus.GONE)
                    .body(ApiResponse.fail("EXPIRED", "键已过期: " + key));
            default -> ResponseEntity.internalServerError()
                    .body(ApiResponse.fail(status.name(), "未预期状态"));
        };
    }

    /** 批量读：HTTP 200，逐键给出命中值或失败原因。 */
    @PostMapping("/batch-get")
    public ApiResponse<BatchOutcome> batchGet(@RequestBody KeysRequest request) {
        return ApiResponse.ok(engine.getBatch(ns(request.namespace()), request.keys()));
    }

    /** 批量写：原子生效；容量不足整体拒绝（507），已有数据不变。 */
    @PostMapping("/batch-put")
    public ResponseEntity<ApiResponse<Void>> batchPut(@RequestBody BatchPutRequest request) {
        CacheStatus status = engine.putBatch(ns(request.namespace()), request.entries(),
                request.ttlMillis() == null ? 0L : request.ttlMillis());
        return switch (status) {
            case OK -> ResponseEntity.ok(ApiResponse.ok(null));
            case CAPACITY_FULL -> ResponseEntity.status(HttpStatus.INSUFFICIENT_STORAGE)
                    .body(ApiResponse.fail("CAPACITY_FULL", "容量不足，批量写入整体拒绝"));
            default -> ResponseEntity.internalServerError()
                    .body(ApiResponse.fail(status.name(), "未预期状态"));
        };
    }

    /** 批量删：HTTP 200，逐键给出结果状态。 */
    @PostMapping("/batch-delete")
    public ApiResponse<Map<String, CacheStatus>> batchDelete(@RequestBody KeysRequest request) {
        Map<String, CacheStatus> results = new LinkedHashMap<>();
        for (String key : request.keys()) {
            results.put(key, engine.delete(ns(request.namespace()), key));
        }
        return ApiResponse.ok(results);
    }

    /** 主动清理过期键（不指定空间时清理所有空间），返回回收数量。 */
    @PostMapping("/purge-expired")
    public ApiResponse<Integer> purgeExpired(@RequestParam(required = false) String ns) {
        return ApiResponse.ok(ns == null ? engine.purgeExpired() : engine.purgeExpired(ns));
    }

    /** 统计：不指定空间时返回默认空间统计。 */
    @GetMapping("/stats")
    public ApiResponse<EngineStats> stats(@RequestParam(required = false) String ns) {
        return ApiResponse.ok(ns == null ? engine.stats() : engine.stats(ns));
    }

    /** 当前已知空间列表。 */
    @GetMapping("/namespaces")
    public ApiResponse<Set<String>> namespaces() {
        return ApiResponse.ok(engine.namespaces());
    }
}
