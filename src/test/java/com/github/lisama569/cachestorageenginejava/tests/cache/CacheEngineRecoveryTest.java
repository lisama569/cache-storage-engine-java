package com.github.lisama569.cachestorageenginejava.tests.cache;

import com.github.lisama569.cachestorageenginejava.cache.CacheEngine;
import com.github.lisama569.cachestorageenginejava.cache.CacheStatus;
import com.github.lisama569.cachestorageenginejava.config.CacheProperties;
import com.github.lisama569.cachestorageenginejava.persistence.WriteAheadLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.*;

class CacheEngineRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(CacheEngineRecoveryTest.class);

    @TempDir
    Path dir;

    private Path walFile() {
        return dir.resolve("cache.wal");
    }

    private CacheEngine openEngine(CacheProperties props, MutableClock clock) throws Exception {
        CacheEngine e = new CacheEngine(props, new WriteAheadLog(walFile()), clock);
        e.recover();
        return e;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void normalRestartRecoversAllData() throws Exception {
        CacheProperties props = new CacheProperties();
        MutableClock clock = new MutableClock(0);
        CacheEngine engine = openEngine(props, clock);
        log.info("输入: put a,b,c 后正常关闭, 重新打开引擎");
        engine.put("a", bytes("1"), 0);
        engine.put("b", bytes("2"), 0);
        engine.put("c", bytes("3"), 0);
        engine.remove("b");
        engine.close();

        CacheEngine recovered = openEngine(props, clock);
        log.info("恢复结果: a={}, b={}, c={}, size={}",
                recovered.get("a").status(), recovered.get("b").status(),
                recovered.get("c").status(), recovered.size());
        log.info("判定依据: a/c 应恢复为 OK, b 已删除应为 NOT_FOUND, size=2");
        assertEquals(CacheStatus.OK, recovered.get("a").status());
        assertEquals(CacheStatus.NOT_FOUND, recovered.get("b").status());
        assertEquals(CacheStatus.OK, recovered.get("c").status());
        assertEquals(2, recovered.size());
        recovered.close();
    }

    @Test
    void ttlSurvivesRestart() throws Exception {
        CacheProperties props = new CacheProperties();
        MutableClock clock = new MutableClock(0);
        CacheEngine engine = openEngine(props, clock);
        log.info("输入: put temp(ttl=1000), 重启后推进时钟 1000ms");
        engine.put("temp", bytes("x"), 1000);
        engine.close();

        CacheEngine recovered = openEngine(props, clock);
        CacheStatus beforeExpiry = recovered.get("temp").status();
        log.info("恢复结果(未过期): temp={}", beforeExpiry);
        assertEquals(CacheStatus.OK, beforeExpiry);
        clock.advance(1000);
        CacheStatus afterExpiry = recovered.get("temp").status();
        log.info("恢复结果(过期后): temp={}", afterExpiry);
        log.info("判定依据: TTL 元数据随 WAL 恢复, 过期后应为 EXPIRED");
        assertEquals(CacheStatus.EXPIRED, afterExpiry);
        recovered.close();
    }

    @Test
    void truncatedTailRecordIsIgnored() throws Exception {
        CacheProperties props = new CacheProperties();
        MutableClock clock = new MutableClock(0);
        CacheEngine engine = openEngine(props, clock);
        log.info("输入: put good1, good2 后模拟崩溃: 文件尾部被截断(不完整的第三条记录)");
        engine.put("good1", bytes("v1"), 0);
        engine.put("good2", bytes("v2"), 0);
        engine.close();

        long len = Files.size(walFile());
        // 模拟崩溃: 追加半条记录（写入过程中断电）
        Files.write(walFile(), new byte[]{1, 0, 0, 0, 5, 'p', 'a'}, StandardOpenOption.APPEND);
        log.info("判定依据: 尾部 {} 字节为不完整记录, 应被识别并忽略, 不影响启动", Files.size(walFile()) - len);

        CacheEngine recovered = openEngine(props, clock);
        log.info("恢复结果: good1={}, good2={}, size={}",
                recovered.get("good1").status(), recovered.get("good2").status(), recovered.size());
        assertEquals(CacheStatus.OK, recovered.get("good1").status());
        assertEquals(CacheStatus.OK, recovered.get("good2").status());
        assertEquals(2, recovered.size());
        // 截断后仍可继续写入并再次恢复
        recovered.put("after-crash", bytes("v3"), 0);
        recovered.close();
        CacheEngine again = openEngine(props, clock);
        log.info("恢复结果(二次重启): after-crash={}", again.get("after-crash").status());
        assertEquals(CacheStatus.OK, again.get("after-crash").status());
        again.close();
    }

    @Test
    void corruptedCrcRecordIsIgnored() throws Exception {
        CacheProperties props = new CacheProperties();
        MutableClock clock = new MutableClock(0);
        CacheEngine engine = openEngine(props, clock);
        log.info("输入: put k1, k2 后篡改最后一条记录的一个字节(模拟磁盘损坏)");
        engine.put("k1", bytes("v1"), 0);
        engine.put("k2", bytes("v2"), 0);
        engine.close();

        long len = Files.size(walFile());
        try (RandomAccessFile raf = new RandomAccessFile(walFile().toFile(), "rw")) {
            raf.seek(len - 3); // 破坏 CRC 区域
            raf.write(raf.read() ^ 0xFF);
        }
        CacheEngine recovered = openEngine(props, clock);
        log.info("恢复结果: k1={}, k2={}, size={}",
                recovered.get("k1").status(), recovered.get("k2").status(), recovered.size());
        log.info("判定依据: 最后一条记录 CRC 不匹配, 应被忽略(读出脏数据属于失败); k1 应保留");
        assertEquals(CacheStatus.OK, recovered.get("k1").status());
        assertEquals(CacheStatus.NOT_FOUND, recovered.get("k2").status());
        assertEquals(1, recovered.size());
        recovered.close();
    }

    @Test
    void emptyLogStartsClean() throws Exception {
        CacheProperties props = new CacheProperties();
        log.info("输入: 全新目录首次启动");
        CacheEngine engine = openEngine(props, new MutableClock(0));
        log.info("恢复结果: size={}", engine.size());
        assertEquals(0, engine.size());
        engine.close();
    }
}
