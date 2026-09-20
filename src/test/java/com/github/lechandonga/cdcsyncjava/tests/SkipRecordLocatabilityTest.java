package com.github.lechandonga.cdcsyncjava.tests;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.lechandonga.cdcsyncjava.engine.BackpressurePolicy;
import com.github.lechandonga.cdcsyncjava.engine.SyncConfig;
import com.github.lechandonga.cdcsyncjava.engine.SyncEngine;
import com.github.lechandonga.cdcsyncjava.model.SkipRecord;
import com.github.lechandonga.cdcsyncjava.source.InMemorySourceDatabase;
import com.github.lechandonga.cdcsyncjava.support.TestFixtures;
import com.github.lechandonga.cdcsyncjava.target.FileTargetStore;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 跳过变更的可定位性测试：
 * 崩溃重启后重放 (checkpoint, wal-tail] 区间，目标端已确认的记录被引擎标记为跳过，
 * 每条 SkipRecord 必须能定位到批次、位点、键与可区分原因。
 */
class SkipRecordLocatabilityTest {

    @TempDir
    Path tempDir;

    @Test
    void skippedEventsAreLocatableByBatchPositionKeyAndReason() throws Exception {
        Path stateDir = tempDir.resolve("state");
        Path targetDir = tempDir.resolve("target");
        InMemorySourceDatabase source = TestFixtures.newSource();
        for (int i = 1; i <= 6; i++) {
            source.put("k" + i, TestFixtures.row("n" + i, i));
        }
        SyncConfig cfg = SyncConfig.builder(stateDir)
                .batchSize(2).parallelism(1).bufferCapacity(1)
                .backpressurePolicy(BackpressurePolicy.BLOCK).pollTimeoutMillis(2).build();
        FileTargetStore target = new FileTargetStore(targetDir);
        SyncEngine engine = new SyncEngine(source, target, cfg);

        // 跑到第 4 条后崩溃：checkpoint=4，但 WAL 可能已含第 5、6 条（未确认）
        Thread runner = new Thread(() -> engine.run(6));
        runner.start();
        engine.awaitReached(4, 5000);
        // 让后续批次有机会写入目标 WAL 但未必提交 checkpoint
        Thread.sleep(30);
        engine.simulateCrash();
        runner.join(3000);

        long checkpoint = new com.github.lechandonga.cdcsyncjava.engine.CheckpointStore(stateDir)
                .load().commitSeq();
        long walTail = target.lastAppliedCommitSeq();

        // 重启引擎继续：(checkpoint, walTail] 中已落盘的事件必须以 ALREADY_APPLIED 记录
        FileTargetStore target2 = new FileTargetStore(targetDir);
        SyncEngine engine2 = new SyncEngine(source, target2, cfg);
        Thread runner2 = new Thread(() -> engine2.run(6));
        runner2.start();
        engine2.awaitReached(6, 5000);
        runner2.join(5000);

        long replayedUnconfirmed = walTail - checkpoint;
        if (replayedUnconfirmed > 0) {
            assertTrue(engine2.stats().duplicates() >= replayedUnconfirmed,
                    "重放 " + replayedUnconfirmed + " 条未确认尾部，应全部记为可定位的跳过");
            for (SkipRecord record : engine2.stats().skipRecords()) {
                assertNotNull(record.reason());
                assertTrue(record.position().commitSeq() > 0, "跳过记录必须带位点");
                assertNotNull(record.key(), "DML 跳过记录必须带键");
                assertTrue(record.batchId() > 0, "跳过记录必须带批次号");
                assertTrue(record.detail().contains("位点"), "说明必须包含位点信息");
            }
        }
        assertEquals(6, target2.allLiveRows().size());
    }
}
