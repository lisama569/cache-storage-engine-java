package com.github.lisama569.cachestorageenginejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 缓存引擎配置项。
 */
@ConfigurationProperties(prefix = "cache")
public class CacheProperties {

    /** 最大条目数，<=0 表示不限制。 */
    private int maxEntries = 10000;

    /** 最大占用字节数（按 value 长度计），<=0 表示不限制。 */
    private long maxBytes = 16L * 1024 * 1024;

    /** 持久化文件目录。 */
    private String dataDir = "data";

    public int getMaxEntries() {
        return maxEntries;
    }

    public void setMaxEntries(int maxEntries) {
        this.maxEntries = maxEntries;
    }

    public long getMaxBytes() {
        return maxBytes;
    }

    public void setMaxBytes(long maxBytes) {
        this.maxBytes = maxBytes;
    }

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }
}
