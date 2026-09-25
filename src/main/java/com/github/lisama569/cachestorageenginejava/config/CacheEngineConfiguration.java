package com.github.lisama569.cachestorageenginejava.config;

import com.github.lisama569.cachestorageenginejava.cache.CacheEngine;
import com.github.lisama569.cachestorageenginejava.persistence.WriteAheadLog;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;

@Configuration
@EnableConfigurationProperties(CacheProperties.class)
public class CacheEngineConfiguration {

    @Bean(destroyMethod = "close")
    public CacheEngine cacheEngine(CacheProperties properties) throws IOException {
        WriteAheadLog wal = new WriteAheadLog(Path.of(properties.getDataDir(), "cache.wal"));
        CacheEngine engine = new CacheEngine(properties, wal, Clock.systemUTC());
        engine.recover();
        return engine;
    }
}
