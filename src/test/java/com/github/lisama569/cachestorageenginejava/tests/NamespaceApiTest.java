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
import org.springframework.http.HttpMethod;
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
 * REST 层空间隔离端到端测试：查询参数/请求体指定空间；
 * 空间容量独立（507 只出现在写满的空间）；缺省空间行为不变。
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
        dataDir = Files.createTempDirectory("ns-api-test");
        log.info("[输入] 测试数据目录: {}", dataDir);
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) throws Exception {
        if (dataDir == null) {
            dataDir = Files.createTempDirectory("ns-api-test");
        }
        registry.add("cache.max-entries", () -> 10);
        registry.add("cache.data-file", () -> dataDir.resolve("ns-api.wal").toString());
        registry.add("cache.namespace-capacities.tiny", () -> 1);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object o) {
        return (Map<String, Object>) o;
    }

    @SuppressWarnings("unchecked")
    private static List<String> asList(Object o) {
        return (List<String>) o;
    }

    private String url(String path) {
        return "http://localhost:" + port + "/api/cache" + path;
    }

    @Test
    @Order(1)
    void sameKeyIsolatedAcrossSpacesOverHttp() {
        Map<String, Object> orderPut = new LinkedHashMap<>();
        orderPut.put("key", "sku");
        orderPut.put("value", "order-view");
        orderPut.put("namespace", "order");
        assertThat(rest.postForEntity(url(""), orderPut, Map.class).getStatusCode())
                .isEqualTo(HttpStatus.OK);

        Map<String, Object> paymentPut = new LinkedHashMap<>();
        paymentPut.put("key", "sku");
        paymentPut.put("value", "payment-view");
        paymentPut.put("namespace", "payment");
        assertThat(rest.postForEntity(url(""), paymentPut, Map.class).getStatusCode())
                .isEqualTo(HttpStatus.OK);

        ResponseEntity<Map> orderGet = rest.getForEntity(url("/sku?ns=order"), Map.class);
        ResponseEntity<Map> paymentGet = rest.getForEntity(url("/sku?ns=payment"), Map.class);
        ResponseEntity<Map> defaultGet = rest.getForEntity(url("/sku"), Map.class);
        log.info("[输入] order.sku=order-view, payment.sku=payment-view; "
                        + "[判定] GET ?ns=order -> {}, ?ns=payment -> {}, 缺省空间 -> HTTP {}",
                orderGet.getBody().get("data"), paymentGet.getBody().get("data"),
                defaultGet.getStatusCode());
        assertThat(orderGet.getBody().get("data")).isEqualTo("order-view");
        assertThat(paymentGet.getBody().get("data")).isEqualTo("payment-view");
        assertThat(defaultGet.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    @Order(2)
    void perSpaceCapacityFullReturns507OnlyInThatSpace() {
        // tiny 空间容量=1（经 cache.namespace-capacities.tiny 配置）
        Map<String, Object> first = new LinkedHashMap<>();
        first.put("key", "only");
        first.put("value", "1");
        first.put("namespace", "tiny");
        assertThat(rest.postForEntity(url(""), first, Map.class).getStatusCode())
                .isEqualTo(HttpStatus.OK);

        Map<String, Object> overflow = new LinkedHashMap<>();
        overflow.put("key", "extra");
        overflow.put("value", "2");
        overflow.put("namespace", "tiny");
        ResponseEntity<Map> rejected = rest.postForEntity(url(""), overflow, Map.class);
        log.info("[输入] tiny 空间容量=1 已存 only; 再写 extra -> HTTP {} body={}",
                rejected.getStatusCode(), rejected.getBody());
        assertThat(rejected.getStatusCode()).isEqualTo(HttpStatus.INSUFFICIENT_STORAGE);
        assertThat(rejected.getBody().get("code")).isEqualTo("CAPACITY_FULL");

        // 其它空间不受影响
        Map<String, Object> other = new LinkedHashMap<>();
        other.put("key", "extra");
        other.put("value", "2");
        other.put("namespace", "order");
        assertThat(rest.postForEntity(url(""), other, Map.class).getStatusCode())
                .isEqualTo(HttpStatus.OK);
    }

    @Test
    @Order(3)
    void batchAndStatsScopedByNamespace() {
        Map<String, Object> batchPut = new LinkedHashMap<>();
        batchPut.put("entries", Map.of("b1", "1", "b2", "2"));
        batchPut.put("namespace", "order");
        assertThat(rest.postForEntity(url("/batch-put"), batchPut, Map.class).getStatusCode())
                .isEqualTo(HttpStatus.OK);

        Map<String, Object> batchGet = Map.of("keys", List.of("b1", "b2"), "namespace", "order");
        ResponseEntity<Map> got = rest.postForEntity(url("/batch-get"), batchGet, Map.class);
        Map<String, Object> data = asMap(got.getBody().get("data"));
        log.info("[判定] order 空间批量读 -> {}", data);
        assertThat(asMap(data.get("values")))
                .containsExactlyInAnyOrderEntriesOf(Map.of("b1", "1", "b2", "2"));

        // 跨空间批量读：payment 里查不到 order 的键
        Map<String, Object> crossGet = Map.of("keys", List.of("b1"), "namespace", "payment");
        ResponseEntity<Map> cross = rest.postForEntity(url("/batch-get"), crossGet, Map.class);
        Map<String, Object> crossData = asMap(cross.getBody().get("data"));
        assertThat(asMap(crossData.get("failures"))).containsEntry("b1", "NOT_FOUND");

        ResponseEntity<Map> tinyStats = rest.getForEntity(url("/stats?ns=tiny"), Map.class);
        ResponseEntity<Map> orderStats = rest.getForEntity(url("/stats?ns=order"), Map.class);
        Map<String, Object> tinyData = asMap(tinyStats.getBody().get("data"));
        Map<String, Object> orderData = asMap(orderStats.getBody().get("data"));
        log.info("[判定] stats?ns=tiny -> {}, stats?ns=order -> {}", tinyData, orderData);
        assertThat(tinyData.get("capacity")).isEqualTo(1);
        assertThat(tinyData.get("size")).isEqualTo(1);
        assertThat(tinyData.get("evictions")).isEqualTo(1);
        assertThat((Integer) orderData.get("size")).isGreaterThanOrEqualTo(3);

        ResponseEntity<Map> nsList = rest.getForEntity(url("/namespaces"), Map.class);
        log.info("[判定] GET /namespaces -> {}", nsList.getBody());
        assertThat(asList(nsList.getBody().get("data")))
                .contains("order", "payment", "tiny");
    }

    @Test
    @Order(4)
    void deleteScopedByNamespace() {
        ResponseEntity<Map> del = rest.exchange(url("/sku?ns=order"), HttpMethod.DELETE,
                null, Map.class);
        assertThat(del.getStatusCode()).isEqualTo(HttpStatus.OK);
        ResponseEntity<Map> orderGet = rest.getForEntity(url("/sku?ns=order"), Map.class);
        ResponseEntity<Map> paymentGet = rest.getForEntity(url("/sku?ns=payment"), Map.class);
        log.info("[判定] 删除 order.sku 后: order -> HTTP {}, payment -> {}（删除不越界）",
                orderGet.getStatusCode(), paymentGet.getBody().get("data"));
        assertThat(orderGet.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(paymentGet.getBody().get("data")).isEqualTo("payment-view");
    }
}
