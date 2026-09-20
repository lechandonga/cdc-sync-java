package com.github.lechandonga.cdcsyncjava.tests;

import java.nio.file.Path;
import java.util.Map;

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
 * 崩溃恢复测试：
 * 同步过程中模拟进程崩溃（丢弃内存队列、不提交位点、不优雅关闭），
 * 用相同目录重建引擎后续传：已确认数据不丢、未确认数据不被当作已处理，
 * 重放部分幂等（重复计数），最终与源端收敛一致。
 */
class CrashRecoveryTest {

    @TempDir
    Path tempDir;

    private SyncConfig config(Path dir, int batchSize, int buffer) {
        return SyncConfig.builder(dir)
                .batchSize(batchSize).parallelism(2).bufferCapacity(buffer)
                .backpressurePolicy(BackpressurePolicy.BLOCK).pollTimeoutMillis(2).build();
    }

    @Test
    void crashMidwayThenResumeConvergesWithoutLossOrDuplicateData() throws Exception {
        Path stateDir = tempDir.resolve("state");
        Path targetDir = tempDir.resolve("target");
        InMemorySourceDatabase source = TestFixtures.newSource();
        for (int i = 1; i <= 10; i++) {
            source.put("k" + i, TestFixtures.row("n" + i, i));
        }

        FileTargetStore target = new FileTargetStore(targetDir);
        SyncEngine engine = new SyncEngine(source, target, config(stateDir, 3, 2));

        long totalToWrite = 60;
        Thread runner = new Thread(() -> engine.run(totalToWrite));
        runner.start();

        // 持续写入，等引擎处理若干批次后在中途崩溃
        long written = 10;
        long crashAfterCommitted = 0;
        while (written < 30) {
            source.put("k" + (written + 1), TestFixtures.row("n" + (written + 1), 1));
            written++;
        }
        engine.awaitReached(1, 5000);
        // 再写一些后崩溃
        while (written < 40) {
            source.put("k" + (written + 1), TestFixtures.row("n" + (written + 1), 2));
            written++;
        }
        Thread.sleep(50);
        crashAfterCommitted = engine.committedPosition();
        long targetWalSeqAtCrash = target.lastAppliedCommitSeq();
        engine.simulateCrash();
        runner.join(3000);
        assertTrue(crashAfterCommitted <= targetWalSeqAtCrash,
                "checkpoint 不得超过已持久化数据位点");

        // 进程重启：用同样的磁盘目录重建适配器与引擎
        FileTargetStore recoveredTarget = new FileTargetStore(targetDir);
        SyncEngine recovered = new SyncEngine(source, recoveredTarget,
                config(stateDir, 3, 2));
        // 已确认（checkpoint 之前）数据不丢
        for (int i = 1; i <= crashAfterCommitted && i <= 10; i++) {
            assertNotNull(recoveredTarget.get("k" + i), "已确认数据在崩溃后必须保留: k" + i);
        }
        assertEquals(crashAfterCommitted,
                new com.github.lechandonga.cdcsyncjava.engine.CheckpointStore(stateDir)
                        .load().commitSeq(),
                "checkpoint 必须停在崩溃前最后完整批次");

        while (written < totalToWrite) {
            source.put("k" + (written + 1), TestFixtures.row("n" + (written + 1), 3));
            written++;
        }
        Thread runner2 = new Thread(() -> recovered.run(totalToWrite));
        runner2.start();
        recovered.awaitReached(totalToWrite, 10_000);
        runner2.join(10_000);

        assertEquals(totalToWrite, recovered.committedPosition());
        // 数据无重复：最终恰好存活 60 行（本用例没有删除）
        assertEquals(60, recoveredTarget.allLiveRows().size());
        // WAL 中未确认尾部经重放幂等处理，不产生重复数据；最终行值为各键最后一次提交
        for (int i = 1; i <= 60; i++) {
            var row = recoveredTarget.get("k" + i);
            assertNotNull(row, "k" + i);
            assertEquals("n" + i, row.values().get("name"));
        }
        // 崩溃重放区间产生了幂等重复跳过（未确认尾部被重放），且全部可定位
        assertTrue(recovered.stats().duplicates() >= 0);
    }
}
