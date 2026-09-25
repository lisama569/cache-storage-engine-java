package com.github.lisama569.cachestorageenginejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.nio.file.Path;

/**
 * 缓存引擎配置项（前缀 cache.*）。
 */
@ConfigurationProperties(prefix = "cache")
public class CacheProperties {

    /** 最大存活条目数；达到上限且回收过期后仍放不下时，写入返回 CAPACITY_FULL。 */
    private int maxEntries = 1024;

    /** WAL 数据文件路径。 */
    private Path dataFile = Path.of("data", "cache.wal");

    /** 启动恢复完成后是否立即压缩 WAL（丢弃过期/被覆盖的历史记录）。 */
    private boolean compactOnStartup = false;

    public int getMaxEntries() {
        return maxEntries;
    }

    public void setMaxEntries(int maxEntries) {
        this.maxEntries = maxEntries;
    }

    public Path getDataFile() {
        return dataFile;
    }

    public void setDataFile(Path dataFile) {
        this.dataFile = dataFile;
    }

    public boolean isCompactOnStartup() {
        return compactOnStartup;
    }

    public void setCompactOnStartup(boolean compactOnStartup) {
        this.compactOnStartup = compactOnStartup;
    }
}
