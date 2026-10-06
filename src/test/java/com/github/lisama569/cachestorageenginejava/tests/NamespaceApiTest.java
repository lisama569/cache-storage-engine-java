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
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * REST 层空间隔离测试：请求体/查询参数可指定空间；缺省空间与旧行为一致；
 * 空间容量覆盖经配置生效，写满返回 507 且不影响其它空间。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NamespaceApiTest {

    private static final Logger log = LoggerFactory.getLogger(NamespaceApiTest.class);
    private static Path dataDir;

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @BeforeAll
    static void init() throws Exception {
        dataDir = Files.createTempDirectory("cache-ns-api-test");
        log.info("[输入] 测试数据目录: {}", dataDir);
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) throws Exception {
        if (dataDir == null) {
            dataDir = Files.createTempDirectory("cache-ns-api-test");
        }
        registry.add("cache.max-entries", () -> 8);
        registry.add("cache.data-file", () -> dataDir.resolve("ns-api.wal").toString());
        registry.add("cache.namespace-capacities.tiny", () -> 1);
    }

    private String url(String path) {
        return "http://localhost:" + port + "/api/cache" + path;
    }

    @Test
    @Order(1)
    void putAndGetWithExplicitNamespace() {
        Map<String, Object> body = Map.of("key", "k", "value", "in-a", "namespace", "nsA");
        ResponseEntity<Map> put = rest.postForEntity(url(""), body, Map.class);
        assertThat(put.getStatusCode()).isEqualTo(HttpStatus.OK);

        ResponseEntity<Map> inA = rest.getForEntity(url("/k?namespace=nsA"), Map.class);
        ResponseEntity<Map> inB = rest.getForEntity(url("/k?namespace=nsB"), Map.class);
        ResponseEntity<Map> inDefault = rest.getForEntity(url("/k"), Map.class);
        log.info("[判定] 写入 nsA 后: nsA 读={} nsB 读={} 默认空间读={}",
                inA.getStatusCode(), inB.getStatusCode(), inDefault.getStatusCode());
        assertThat(inA.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(inA.getBody().get("data")).isEqualTo("in-a");
        assertThat(inB.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(inDefault.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND); // 默认空间互不可见
    }

    @Test
    @Order(2)
    void omittedNamespaceKeepsLegacyBehavior() {
        // 旧式请求体（无 namespace 字段）落入默认空间
        Map<String, Object> body = Map.of("key", "legacy", "value", "v0");
        ResponseEntity<Map> put = rest.postForEntity(url(""), body, Map.class);
        assertThat(put.getStatusCode()).isEqualTo(HttpStatus.OK);

        ResponseEntity<Map> got = rest.getForEntity(url("/legacy"), Map.class);
        ResponseEntity<Map> gotExplicit = rest.getForEntity(url("/legacy?namespace=default"), Map.class);
        log.info("[判定] 旧式写入后: 默认读={} 显式 default 读={}（应都命中同一空间）",
                got.getStatusCode(), gotExplicit.getStatusCode());
        assertThat(got.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(gotExplicit.getBody().get("data")).isEqualTo("v0");
    }

    @Test
    @Order(3)
    void configuredNamespaceCapacityIsEnforcedAndIsolated() {
        // tiny 空间配置容量=1：第二条写入 507，其它空间不受影响
        rest.postForEntity(url(""), Map.of("key", "t1", "value", "1", "namespace", "tiny"), Map.class);
        ResponseEntity<Map> full = rest.postForEntity(url(""),
                Map.of("key", "t2", "value", "2", "namespace", "tiny"), Map.class);
        ResponseEntity<Map> other = rest.postForEntity(url(""),
                Map.of("key", "t2", "value", "2", "namespace", "roomy"), Map.class);
        log.info("[判定] tiny(容量1) 第二条写入 -> {} {}; roomy 空间写入 -> {}",
                full.getStatusCode(), full.getBody() != null ? full.getBody().get("code") : null,
                other.getStatusCode());
        assertThat(full.getStatusCode()).isEqualTo(HttpStatus.INSUFFICIENT_STORAGE);
        assertThat(full.getBody().get("code")).isEqualTo("CAPACITY_FULL");
        assertThat(other.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    @Order(4)
    void batchAndStatsAreNamespaceScoped() {
        Map<String, Object> batch = Map.of(
                "entries", Map.of("b1", "1", "b2", "2"),
                "namespace", "batchNs");
        ResponseEntity<Map> put = rest.postForEntity(url("/batch-put"), batch, Map.class);
        assertThat(put.getStatusCode()).isEqualTo(HttpStatus.OK);

        Map<String, Object> keys = Map.of("keys", java.util.List.of("b1", "b2", "b3"),
                "namespace", "batchNs");
        ResponseEntity<Map> got = rest.postForEntity(url("/batch-get"), keys, Map.class);
        log.info("[判定] batchNs 批量读 -> {}", got.getBody());
        Map<String, Object> data = (Map<String, Object>) got.getBody().get("data");
        assertThat((Map<String, Object>) data.get("values"))
                .containsEntry("b1", "1").containsEntry("b2", "2");
        assertThat((Map<String, Object>) data.get("failures")).containsKey("b3");

        ResponseEntity<Map> stats = rest.getForEntity(url("/stats?namespace=batchNs"), Map.class);
        Map<String, Object> sdata = (Map<String, Object>) stats.getBody().get("data");
        log.info("[判定] batchNs 统计 -> {}", sdata);
        assertThat(sdata.get("size")).isEqualTo(2);
        assertThat(sdata.get("capacity")).isEqualTo(8);

        ResponseEntity<Map> list = rest.getForEntity(url("/namespaces"), Map.class);
        log.info("[判定] 空间列表 -> {}", list.getBody().get("data"));
        assertThat((java.util.Collection<String>) list.getBody().get("data"))
                .contains("nsA", "tiny", "batchNs", "default");
    }

    @Test
    @Order(5)
    void deleteWithNamespace() {
        ResponseEntity<Map> del = rest.exchange(url("/k?namespace=nsA"), HttpMethod.DELETE,
                HttpEntity.EMPTY, Map.class);
        ResponseEntity<Map> after = rest.getForEntity(url("/k?namespace=nsA"), Map.class);
        log.info("[判定] 删除 nsA.k -> {}; 删后读 -> {}", del.getStatusCode(), after.getStatusCode());
        assertThat(del.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(after.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }
}
