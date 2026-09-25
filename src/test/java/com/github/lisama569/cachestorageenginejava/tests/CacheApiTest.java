package com.github.lisama569.cachestorageenginejava.tests;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * REST 接口对外访问测试：验证 HTTP 状态码/响应 code 对失败原因可区分
 * （NOT_FOUND=404, EXPIRED=410, CAPACITY_FULL=507）与批量操作原子性。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CacheApiTest {

    private static final Logger log = LoggerFactory.getLogger(CacheApiTest.class);
    private static Path dataDir;

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @BeforeAll
    static void init() throws Exception {
        dataDir = Files.createTempDirectory("cache-api-test");
        log.info("[输入] 测试数据目录: {}", dataDir);
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) throws Exception {
        if (dataDir == null) {
            dataDir = Files.createTempDirectory("cache-api-test");
        }
        registry.add("cache.max-entries", () -> 3);
        registry.add("cache.data-file", () -> dataDir.resolve("api.wal").toString());
    }

    private String url(String path) {
        return "http://localhost:" + port + "/api/cache" + path;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> body(ResponseEntity<Map> resp) {
        return resp.getBody();
    }

    @Test
    @Order(1)
    void readMissingReturns404() {
        ResponseEntity<Map> resp = rest.getForEntity(url("/missing"), Map.class);
        log.info("[判定] GET 不存在键 -> HTTP {} body={}", resp.getStatusCode(), resp.getBody());
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(body(resp).get("code")).isEqualTo("NOT_FOUND");
    }

    @Test
    @Order(2)
    void putAndGetRoundTrip() {
        Map<String, Object> req = new LinkedHashMap<>();
        req.put("key", "k1");
        req.put("value", "v1");
        req.put("ttlMillis", null); // 永不过期
        ResponseEntity<Map> put = rest.postForEntity(url(""), req, Map.class);
        log.info("[判定] PUT k1 -> HTTP {}", put.getStatusCode());
        assertThat(put.getStatusCode()).isEqualTo(HttpStatus.OK);

        ResponseEntity<Map> got = rest.getForEntity(url("/k1"), Map.class);
        log.info("[判定] GET k1 -> HTTP {} body={}", got.getStatusCode(), got.getBody());
        assertThat(got.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(body(got).get("data")).isEqualTo("v1");
    }

    @Test
    @Order(3)
    void capacityFullReturns507AndKeepsLiveData() {
        for (String k : List.of("k2", "k3")) {
            Map<String, Object> req = new LinkedHashMap<>();
            req.put("key", k);
            req.put("value", k.toUpperCase());
            req.put("ttlMillis", 0);
            assertThat(rest.postForEntity(url(""), req, Map.class).getStatusCode())
                    .isEqualTo(HttpStatus.OK);
        }
        Map<String, Object> overflow = new LinkedHashMap<>();
        overflow.put("key", "k4");
        overflow.put("value", "V4");
        overflow.put("ttlMillis", 0);
        ResponseEntity<Map> rejected = rest.postForEntity(url(""), overflow, Map.class);
        log.info("[输入] 容量=3 已满, PUT k4 -> HTTP {} body={}",
                rejected.getStatusCode(), rejected.getBody());
        assertThat(rejected.getStatusCode()).isEqualTo(HttpStatus.INSUFFICIENT_STORAGE);
        assertThat(body(rejected).get("code")).isEqualTo("CAPACITY_FULL");

        // 存活数据未被静默丢弃
        for (String k : List.of("k1", "k2", "k3")) {
            assertThat(rest.getForEntity(url("/" + k), Map.class).getStatusCode())
                    .isEqualTo(HttpStatus.OK);
        }
        assertThat(rest.getForEntity(url("/k4"), Map.class).getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    @Order(4)
    void expiredKeyReturns410() throws Exception {
        // 容量已满，先删 k3 腾出一个槽位
        ResponseEntity<Map> del = rest.exchange(url("/k3"), org.springframework.http.HttpMethod.DELETE,
                null, Map.class);
        assertThat(del.getStatusCode()).isEqualTo(HttpStatus.OK);

        Map<String, Object> req = new LinkedHashMap<>();
        req.put("key", "ephemeral");
        req.put("value", "soon");
        req.put("ttlMillis", 120);
        ResponseEntity<Map> put = rest.postForEntity(url(""), req, Map.class);
        log.info("[输入] 删除 k3 腾位后 PUT ephemeral(ttl=120ms) -> HTTP {}", put.getStatusCode());
        assertThat(put.getStatusCode()).isEqualTo(HttpStatus.OK);
        Thread.sleep(250);
        ResponseEntity<Map> got = rest.getForEntity(url("/ephemeral"), Map.class);
        log.info("[判定] ttl=120ms 等待 250ms 后 GET -> HTTP {} body={}",
                got.getStatusCode(), got.getBody());
        assertThat(got.getStatusCode()).isEqualTo(HttpStatus.GONE);
        assertThat(body(got).get("code")).isEqualTo("EXPIRED");
    }

    @Test
    @Order(5)
    void batchOperationsReportPerKeyOutcomes() throws Exception {
        // 写入一个短 TTL 键并等待其过期（期间不触碰，保持“已过期未回收”状态）
        Map<String, Object> shortLived = new LinkedHashMap<>();
        shortLived.put("key", "ephemeral2");
        shortLived.put("value", "tmp");
        shortLived.put("ttlMillis", 100);
        assertThat(rest.postForEntity(url(""), shortLived, Map.class).getStatusCode())
                .isEqualTo(HttpStatus.OK);
        Thread.sleep(200);

        // 当前存活 k1,k2；ephemeral2 已过期未回收；批量读逐键给结果
        Map<String, Object> getReq = Map.of("keys", List.of("k1", "missing", "ephemeral2"));
        ResponseEntity<Map> got = rest.postForEntity(url("/batch-get"), getReq, Map.class);
        log.info("[判定] batch-get -> {}", got.getBody());
        Map<?, ?> data = (Map<?, ?>) body(got).get("data");
        assertThat(((Map<?, ?>) data.get("values")).get("k1")).isEqualTo("v1");
        assertThat(((Map<?, ?>) data.get("failures")).get("missing")).isEqualTo("NOT_FOUND");
        assertThat(((Map<?, ?>) data.get("failures")).get("ephemeral2")).isEqualTo("EXPIRED");

        // 批量写 2 个新键但仅剩 0 个空槽 -> 整体 507，且一个都不生效
        Map<String, Object> putReq = new LinkedHashMap<>();
        putReq.put("entries", Map.of("n1", "1", "n2", "2"));
        putReq.put("ttlMillis", 0);
        ResponseEntity<Map> rejected = rest.postForEntity(url("/batch-put"), putReq, Map.class);
        log.info("[判定] 容量满时 batch-put 2 新键 -> HTTP {}", rejected.getStatusCode());
        assertThat(rejected.getStatusCode()).isEqualTo(HttpStatus.INSUFFICIENT_STORAGE);
        assertThat(rest.getForEntity(url("/n1"), Map.class).getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(rest.getForEntity(url("/n2"), Map.class).getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);

        // 批量删：含存活、不存在键，逐键结果
        Map<String, Object> delReq = Map.of("keys", List.of("k1", "ghost"));
        ResponseEntity<Map> del = rest.postForEntity(url("/batch-delete"), delReq, Map.class);
        Map<?, ?> delData = (Map<?, ?>) body(del).get("data");
        log.info("[判定] batch-delete -> {}", delData);
        assertThat(delData.get("k1")).isEqualTo("OK");
        assertThat(delData.get("ghost")).isEqualTo("NOT_FOUND");
    }

    @Test
    @Order(6)
    void statsEndpointExposesState() {
        ResponseEntity<Map> resp = rest.getForEntity(url("/stats"), Map.class);
        log.info("[判定] GET stats -> {}", resp.getBody());
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        Map<?, ?> data = (Map<?, ?>) body(resp).get("data");
        assertThat(data.get("capacity")).isEqualTo(3);
        assertThat((Integer) data.get("size")).isLessThanOrEqualTo(3);
    }
}
