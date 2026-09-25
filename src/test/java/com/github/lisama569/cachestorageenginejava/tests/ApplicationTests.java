package com.github.lisama569.cachestorageenginejava.tests;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.file.Path;

/**
 * Spring 上下文加载冒烟测试（引擎 Bean 完成 WAL 打开/恢复）。
 */
@SpringBootTest
public class ApplicationTests {

    @TempDir
    static Path tempDir;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("cache.data-file", () -> tempDir.resolve("ctx.wal").toString());
    }

    @Test
    public void contextLoads() {
    }
}
