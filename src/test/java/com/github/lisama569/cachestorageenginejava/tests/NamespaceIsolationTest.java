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
 * 空间隔离测试：不同空间下同名键互不影响；删/改/统计都按空间隔离；
 * 不指定空间的旧 API 落入默认空间且行为不变。
 */
class NamespaceIsolationTest {

    private static final Logger log = LoggerFactory.getLogger(NamespaceIsolationTest.class);

    @TempDir
    Path dir;

    private CacheEngine newEngine(int capacity, String name) {
        return new CacheEngine(capacity, dir.resolve(name), MutableClock.startingAtEpoch());
    }

    @Test
    void sameKeyInDifferentNamespacesAreIndependent() {
        try (CacheEngine e = newEngine(8, "iso1.wal")) {
            log.info("[输入] nsA put(k, A), nsB put(k, B)");
            assertThat(e.put("nsA", "k", "A", 0)).isEqualTo(CacheStatus.OK);
            assertThat(e.put("nsB", "k", "B", 0)).isEqualTo(CacheStatus.OK);

            String a = e.get("nsA", "k").value();
            String b = e.get("nsB", "k").value();
            log.info("[判定] nsA.k={} nsB.k={}（同名键应各存各的）", a, b);
            assertThat(a).isEqualTo("A");
            assertThat(b).isEqualTo("B");

            // 覆盖 nsA 不影响 nsB
            assertThat(e.put("nsA", "k", "A2", 0)).isEqualTo(CacheStatus.OK);
            // 删除 nsB 不影响 nsA
            assertThat(e.delete("nsB", "k")).isEqualTo(CacheStatus.OK);

            log.info("[判定] 覆盖 nsA、删除 nsB 后: nsA.k={} nsB.k={}",
                    e.get("nsA", "k").value(), e.get("nsB", "k").status());
            assertThat(e.get("nsA", "k").value()).isEqualTo("A2");
            assertThat(e.get("nsB", "k").status()).isEqualTo(CacheStatus.NOT_FOUND);
            assertThat(e.size("nsA")).isEqualTo(1);
            assertThat(e.size("nsB")).isZero();
        }
    }

    @Test
    void legacyApiFallsIntoDefaultNamespace() {
        try (CacheEngine e = newEngine(8, "iso2.wal")) {
            e.put("k", "legacy", 0); // 旧 API：不指定空间
            log.info("[输入] 旧 API put(k, legacy); 显式默认空间与命名空间各读一次");
            assertThat(e.get(CacheEngine.DEFAULT_NAMESPACE, "k").value()).isEqualTo("legacy");
            assertThat(e.get("other", "k").status()).isEqualTo(CacheStatus.NOT_FOUND);

            // 显式写默认空间，旧 API 能读到 —— 两者是同一个空间
            assertThat(e.put(CacheEngine.DEFAULT_NAMESPACE, "k2", "v2", 0)).isEqualTo(CacheStatus.OK);
            assertThat(e.get("k2").value()).isEqualTo("v2");
            log.info("[判定] 旧 API 与显式默认空间互通；other 空间看不到默认空间的键");
        }
    }

    @Test
    void batchOperationsAreNamespaceScoped() {
        try (CacheEngine e = newEngine(16, "iso3.wal")) {
            assertThat(e.putBatch("nsA", Map.of("x", "1", "y", "2"), 0)).isEqualTo(CacheStatus.OK);
            assertThat(e.putBatch("nsB", Map.of("x", "9"), 0)).isEqualTo(CacheStatus.OK);
            log.info("[输入] nsA 批量写 x,y; nsB 批量写 x");

            BatchOutcome a = e.getBatch("nsA", List.of("x", "y", "z"));
            BatchOutcome b = e.getBatch("nsB", List.of("x", "y"));
            log.info("[判定] nsA 批量读 values={} failures={}; nsB 批量读 values={} failures={}",
                    a.values(), a.failures(), b.values(), b.failures());
            assertThat(a.values()).containsExactlyInAnyOrderEntriesOf(Map.of("x", "1", "y", "2"));
            assertThat(a.failures()).containsEntry("z", CacheStatus.NOT_FOUND);
            assertThat(b.values()).containsExactly(Map.entry("x", "9"));
            assertThat(b.failures()).containsEntry("y", CacheStatus.NOT_FOUND);
        }
    }

    @Test
    void statsAreNamespaceScoped() {
        try (CacheEngine e = newEngine(4, "iso4.wal")) {
            e.put("nsA", "a", "1", 0);
            e.put("nsA", "b", "2", 0);
            e.put("nsB", "c", "3", 0);
            e.get("nsA", "a");          // nsA 命中
            e.get("nsB", "missing");    // nsB 未命中

            EngineStats sa = e.stats("nsA");
            EngineStats sb = e.stats("nsB");
            EngineStats all = e.stats();
            log.info("[判定] nsA stats={}; nsB stats={}; 聚合 stats={}", sa, sb, all);
            assertThat(sa.size()).isEqualTo(2);
            assertThat(sa.hits()).isEqualTo(1);
            assertThat(sa.misses()).isZero();
            assertThat(sb.size()).isEqualTo(1);
            assertThat(sb.hits()).isZero();
            assertThat(sb.misses()).isEqualTo(1);
            // 聚合统计 = 各空间之和
            assertThat(all.size()).isEqualTo(3);
            assertThat(all.hits()).isEqualTo(1);
            assertThat(all.misses()).isEqualTo(1);
        }
    }

    @Test
    void namespaceNamesAreValidated() {
        try (CacheEngine e = newEngine(4, "iso5.wal")) {
            log.info("[输入] 空白空间名 put / null 空间名 get（应抛参数异常而非静默落库）");
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> e.put("  ", "k", "v", 0))
                    .isInstanceOf(IllegalArgumentException.class);
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> e.get(null, "k"))
                    .isInstanceOf(NullPointerException.class);
        }
    }
}
