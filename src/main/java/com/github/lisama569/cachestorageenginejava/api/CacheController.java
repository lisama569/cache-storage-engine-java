package com.github.lisama569.cachestorageenginejava.api;

import com.github.lisama569.cachestorageenginejava.cache.CacheEngine;
import com.github.lisama569.cachestorageenginejava.cache.CacheResult;
import com.github.lisama569.cachestorageenginejava.cache.CacheStatus;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 缓存引擎 REST 入口。失败原因通过 HTTP 状态码与响应体 status 字段区分：
 * NOT_FOUND -> 404, EXPIRED -> 410, CAPACITY_EXCEEDED -> 507。
 */
@RestController
@RequestMapping("/cache")
public class CacheController {

    private final CacheEngine engine;

    public CacheController(CacheEngine engine) {
        this.engine = engine;
    }

    @GetMapping("/{key}")
    public ResponseEntity<?> get(@PathVariable String key) {
        CacheResult<byte[]> result = engine.get(key);
        if (!result.isOk()) {
            return error(result.status());
        }
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .body(result.value());
    }

    @PutMapping("/{key}")
    public ResponseEntity<?> put(@PathVariable String key,
                                 @RequestParam(defaultValue = "0") long ttlMillis,
                                 @RequestBody byte[] value) {
        CacheResult<Void> result = engine.put(key, value, ttlMillis);
        return result.isOk() ? ResponseEntity.ok(Map.of("status", "OK")) : error(result.status());
    }

    @DeleteMapping("/{key}")
    public ResponseEntity<?> delete(@PathVariable String key) {
        CacheResult<Void> result = engine.remove(key);
        return result.isOk() ? ResponseEntity.ok(Map.of("status", "OK")) : error(result.status());
    }

    /** 批量写入：请求体为 {key: base64(value)}，整体成功或整体失败。 */
    @PostMapping("/batch")
    public ResponseEntity<?> putAll(@RequestParam(defaultValue = "0") long ttlMillis,
                                    @RequestBody Map<String, String> entries) {
        Map<String, byte[]> decoded = new LinkedHashMap<>();
        entries.forEach((k, v) -> decoded.put(k, Base64.getDecoder().decode(v)));
        CacheResult<Void> result = engine.putAll(decoded, ttlMillis);
        return result.isOk() ? ResponseEntity.ok(Map.of("status", "OK")) : error(result.status());
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        return Map.of("size", engine.size(), "bytes", engine.currentBytes());
    }

    private ResponseEntity<Map<String, String>> error(CacheStatus status) {
        HttpStatus http = switch (status) {
            case NOT_FOUND -> HttpStatus.NOT_FOUND;
            case EXPIRED -> HttpStatus.GONE;
            case CAPACITY_EXCEEDED -> HttpStatus.INSUFFICIENT_STORAGE;
            default -> HttpStatus.INTERNAL_SERVER_ERROR;
        };
        return ResponseEntity.status(http).body(Map.of("status", status.name()));
    }
}
