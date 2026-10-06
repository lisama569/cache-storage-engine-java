package com.github.lisama569.cachestorageenginejava.config;

import com.github.lisama569.cachestorageenginejava.core.CacheEngine;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

/**
 * 引擎 Spring 装配：WAL 在构造期完成打开/恢复（含 v1->v2 迁移），Bean 销毁时 fsync 关闭。
 */
@Configuration
@EnableConfigurationProperties(CacheProperties.class)
public class CacheConfiguration {

    @Bean(destroyMethod = "close")
    public CacheEngine cacheEngine(CacheProperties properties, Clock clock) {
        CacheEngine engine = new CacheEngine(properties.getMaxEntries(),
                properties.getNamespaceCapacities(), properties.getDataFile(), clock);
        if (properties.isCompactOnStartup()) {
            engine.compact();
        }
        return engine;
    }

    @Bean
    public Clock cacheClock() {
        return Clock.systemUTC();
    }
}
