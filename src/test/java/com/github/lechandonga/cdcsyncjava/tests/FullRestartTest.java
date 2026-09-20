package com.github.lechandonga.cdcsyncjava.tests;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.lechandonga.cdcsyncjava.engine.BackpressurePolicy;
import com.github.lechandonga.cdcsyncjava.engine.SyncConfig;
import com.github.lechandonga.cdcsyncjava.engine.SyncEngine;
import com.github.lechandonga.cdcsyncjava.source.InMemorySourceDatabase;
import com.github.lechandonga.cdcsyncjava.support.TestFixtures;
import com.github.lechandonga.cdcsyncjava.target.FileTargetStore;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 完整进程重启：先正常停止（位点与数据一致），
 * 再以全新适配器实例从同一磁盘目录启动，验证无缝续传且无重复/丢失。
 */
class FullRestartTest {

    @TempDir
    Path tempDir;

    private SyncConfig config() {
        return SyncConfig.builder(tempDir.resolve("state"))
                .batchSize(3).parallelism(3).bufferCapacity(6)
                .backpressurePolicy(BackpressurePolicy.BLOCK).pollTimeoutMillis(2).build();
    }

    @Test
    void cleanRestartResumesFromCheckpointAndContinues() throws Exception {
        InMemorySourceDatabase source = TestFixtures.newSource();
        for (int i = 1; i <= 10; i++) {
            source.put("k" + i, TestFixtures.row("n" + i, i));
        }
        Path targetDir = tempDir.resolve("target");
        FileTargetStore target = new FileTargetStore(targetDir);
        SyncEngine engine = new SyncEngine(source, target, config());
        Thread r1 = new Thread(() -> engine.run(10));
        r1.start();
        engine.awaitReached(10, 5000);
        r1.join(5000);
        assertEquals(10, target.allLiveRows().size());

        // 模拟完整进程重启：源端、目标端、引擎全部新建，只有磁盘目录复用
        for (int i = 11; i <= 20; i++) {
            source.put("k" + i, TestFixtures.row("n" + i, i));
        }
        FileTargetStore reopenedTarget = new FileTargetStore(targetDir);
        SyncEngine restarted = new SyncEngine(source, reopenedTarget, config());
        Thread r2 = new Thread(() -> restarted.run(20));
        r2.start();
        restarted.awaitReached(20, 5000);
        r2.join(5000);

        assertEquals(20, reopenedTarget.allLiveRows().size());
        assertEquals(20, reopenedTarget.lastAppliedCommitSeq());
        // 重启前 10 行与重启后 10 行均在，且无重复应用
        for (int i = 1; i <= 20; i++) {
            assertEquals("n" + i, reopenedTarget.get("k" + i).values().get("name"));
        }
        assertEquals(10, restarted.stats().applied(), "仅新到达的 10 条计为应用");
        assertEquals(0, restarted.stats().rejected());
    }
}
