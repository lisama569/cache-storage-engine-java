package com.github.lisama569.cachestorageenginejava.tests;

import com.github.lisama569.cachestorageenginejava.core.BatchOutcome;
import com.github.lisama569.cachestorageenginejava.core.CacheEngine;
import com.github.lisama569.cachestorageenginejava.core.CacheStatus;
import com.github.lisama569.cachestorageenginejava.core.EngineStats;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 空间隔离语义测试：不同空间同名键互不影响；不指定空间时行为与旧版一致；
 * 读、写、删、批量读写、统计均可指定空间。
 */
class NamespaceIsolationTest {

    private static final Logger log = LoggerFactory.getLogger(NamespaceIsolationTest.class);

    @TempDir
    Path dir;

    private CacheEngine newEngine(String name) {
        return new CacheEngine(8, dir.resolve(name), MutableClock.startingAtEpoch());
    }

    @Test
    void sameKeyInDifferentSpacesIsIndependent() {
        try (CacheEngine e = newEngine("iso1.wal")) {
            assertThat(e.put("order", "k1", "from-order", 0)).isEqualTo(CacheStatus.OK);
            assertThat(e.put("payment", "k1", "from-payment", 0)).isEqualTo(CacheStatus.OK);

            String orderVal = e.get("order", "k1").value();
            String paymentVal = e.get("payment", "k1").value();
            log.info("[输入] order.k1=from-order, payment.k1=from-payment; "
                    + "[判定] 各自空间读回 order.k1={} payment.k1={}（同名键互不影响）", orderVal, paymentVal);
            assertThat(orderVal).isEqualTo("from-order");
            assertThat(paymentVal).isEqualTo("from-payment");

            // 覆盖一个空间不影响另一个
            assertThat(e.put("order", "k1", "order-v2", 0)).isEqualTo(CacheStatus.OK);
            assertThat(e.get("payment", "k1").value()).isEqualTo("from-payment");

            // 删除一个空间不影响另一个
            assertThat(e.delete("order", "k1")).isEqualTo(CacheStatus.OK);
            CacheStatus orderAfter = e.get("order", "k1").status();
            CacheStatus paymentAfter = e.get("payment", "k1").status();
            log.info("[判定] 删除 order.k1 后: order.k1={} payment.k1={}（删除不越界）", orderAfter, paymentAfter);
            assertThat(orderAfter).isEqualTo(CacheStatus.NOT_FOUND);
            assertThat(paymentAfter).isEqualTo(CacheStatus.OK);
        }
    }

    @Test
    void defaultSpaceMatchesLegacyBehavior() {
        try (CacheEngine e = newEngine("iso2.wal")) {
            // 旧式调用（不指定空间）与显式默认空间指向同一份数据
            assertThat(e.put("legacy", "v1", 0)).isEqualTo(CacheStatus.OK);
            assertThat(e.get(CacheEngine.DEFAULT_NAMESPACE, "legacy").value()).isEqualTo("v1");
            assertThat(e.put(CacheEngine.DEFAULT_NAMESPACE, "legacy2", "v2", 0)).isEqualTo(CacheStatus.OK);
            assertThat(e.get("legacy2").value()).isEqualTo("v2");

            // 默认空间的数据对其它空间不可见
            CacheStatus inOther = e.get("order", "legacy").status();
            log.info("[输入] 默认空间写入 legacy/legacy2; [判定] 在 order 空间读 legacy -> {}（空间互相不可见）", inOther);
            assertThat(inOther).isEqualTo(CacheStatus.NOT_FOUND);
            assertThat(e.size()).isEqualTo(2);
            assertThat(e.size("order")).isZero();
        }
    }

    @Test
    void batchOperationsAreScopedToSpace() {
        try (CacheEngine e = newEngine("iso3.wal")) {
            assertThat(e.putBatch("order", Map.of("a", "1", "b", "2"), 0)).isEqualTo(CacheStatus.OK);
            assertThat(e.putBatch("payment", Map.of("a", "100"), 0)).isEqualTo(CacheStatus.OK);

            BatchOutcome orderBatch = e.getBatch("order", List.of("a", "b", "c"));
            BatchOutcome paymentBatch = e.getBatch("payment", List.of("a", "b"));
            log.info("[输入] order 批量写 a=1,b=2; payment 批量写 a=100; "
                    + "[判定] order 批量读={} payment 批量读={}", orderBatch, paymentBatch);
            assertThat(orderBatch.values()).containsExactlyInAnyOrderEntriesOf(Map.of("a", "1", "b", "2"));
            assertThat(orderBatch.failures()).containsEntry("c", CacheStatus.NOT_FOUND);
            assertThat(paymentBatch.values()).containsExactly(Map.entry("a", "100"));
            assertThat(paymentBatch.failures()).containsEntry("b", CacheStatus.NOT_FOUND);

            // 批量写只占用目标空间
            assertThat(e.size("order")).isEqualTo(2);
            assertThat(e.size("payment")).isEqualTo(1);
        }
    }

    @Test
    void statsAreTrackedPerSpace() {
        try (CacheEngine e = newEngine("iso4.wal")) {
            e.put("order", "k", "v", 0);
            e.get("order", "k");          // order: hit
            e.get("order", "ghost");      // order: miss
            e.get("payment", "ghost");    // payment: miss

            EngineStats order = e.stats("order");
            EngineStats payment = e.stats("payment");
            EngineStats dflt = e.stats();
            log.info("[判定] order stats={} payment stats={} default stats={}（统计按空间独立计数）",
                    order, payment, dflt);
            assertThat(order.hits()).isEqualTo(1);
            assertThat(order.misses()).isEqualTo(1);
            assertThat(order.size()).isEqualTo(1);
            assertThat(payment.hits()).isZero();
            assertThat(payment.misses()).isEqualTo(1);
            assertThat(payment.size()).isZero();
            assertThat(dflt.hits()).isZero();
            assertThat(dflt.size()).isZero();
        }
    }

    @Test
    void namespacesListingAndLazyCreation() {
        try (CacheEngine e = new CacheEngine(4, Map.of("prealloc", 2), dir.resolve("iso5.wal"),
                MutableClock.startingAtEpoch())) {
            assertThat(e.capacity("prealloc")).isEqualTo(2);
            // 未注册空间首次使用时按默认容量懒创建
            assertThat(e.put("adhoc", "k", "v", 0)).isEqualTo(CacheStatus.OK);
            assertThat(e.capacity("adhoc")).isEqualTo(4);

            log.info("[判定] 预注册 prealloc(容量2) + 懒创建 adhoc(容量4): 空间列表={} 容量 prealloc={} adhoc={}",
                    e.namespaces(), e.capacity("prealloc"), e.capacity("adhoc"));
            assertThat(e.namespaces()).contains("prealloc", "adhoc");
        }
    }

    @Test
    void purgeExpiredScopedToSpace() {
        MutableClock clock = MutableClock.startingAtEpoch();
        try (CacheEngine e = new CacheEngine(8, dir.resolve("iso6.wal"), clock)) {
            e.put("order", "temp", "v", 100);
            e.put("payment", "temp", "v", 100);
            clock.advanceMillis(200); // 两个空间的 temp 都过期

            int purgedOrder = e.purgeExpired("order");
            // 注意：先取状态再打印——get 命中过期键会触发惰性回收，日志求值不能先于断言消费它
            CacheStatus paymentTemp = e.get("payment", "temp").status();
            log.info("[输入] 两空间各写一个 ttl=100ms 的 temp 并推进时钟 200ms; "
                    + "[判定] 只清理 order 空间: 回收 {} 条, payment.temp={}", purgedOrder, paymentTemp);
            assertThat(purgedOrder).isEqualTo(1);
            assertThat(e.size("order")).isZero();
            // payment 的过期条目未被顺手清理（物理回收按空间独立）
            assertThat(paymentTemp).isEqualTo(CacheStatus.EXPIRED);
        }
    }
}
