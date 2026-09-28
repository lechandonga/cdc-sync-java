package com.github.lechandonga.cdcsyncjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.lechandonga.cdcsyncjava.engine.BackpressurePolicy;
import com.github.lechandonga.cdcsyncjava.engine.CheckpointStore;
import com.github.lechandonga.cdcsyncjava.engine.InitProgressStore;
import com.github.lechandonga.cdcsyncjava.engine.SyncConfig;
import com.github.lechandonga.cdcsyncjava.engine.SyncEngine;
import com.github.lechandonga.cdcsyncjava.engine.SyncException;
import com.github.lechandonga.cdcsyncjava.model.ColumnType;
import com.github.lechandonga.cdcsyncjava.model.RejectReason;
import com.github.lechandonga.cdcsyncjava.model.SchemaChange;
import com.github.lechandonga.cdcsyncjava.source.InMemorySourceDatabase;
import com.github.lechandonga.cdcsyncjava.support.TestFixtures;
import com.github.lechandonga.cdcsyncjava.target.FileTargetStore;

/**
 * 首次初始化（快照阶段）中断后的重启续跑：
 * <ul>
 *   <li>初始化期间源端发生结构变化，进程没跑完就终止，重启后从上次未完成处继续到完成；</li>
 *   <li>无论重启/重试多少次，都不会每次启动都失败，也不会把未完成初始化当成已完成跳过；</li>
 *   <li>现场状态确实对不上时（位点超前数据 / 目标存储损坏），拒绝启动并给出能区分的原因，
 *       且不污染已有数据。</li>
 * </ul>
 */
class InitializationRecoveryTest {

    @TempDir
    Path tempDir;

    private SyncConfig config(Path stateDir) {
        return SyncConfig.builder(stateDir)
                .batchSize(3).parallelism(2).bufferCapacity(4)
                .backpressurePolicy(BackpressurePolicy.BLOCK).pollTimeoutMillis(2).build();
    }

    /** 在快照应用到第 crashAfterRows 行之后确定性模拟进程崩溃。 */
    private SyncEngine crashDuringSnapshot(InMemorySourceDatabase source, Path stateDir,
                                          Path targetDir, long goal, int crashAfterRows)
            throws InterruptedException {
        FileTargetStore target = new FileTargetStore(targetDir);
        SyncEngine engine = new SyncEngine(source, target, config(stateDir));
        java.util.concurrent.atomic.AtomicInteger rows =
                new java.util.concurrent.atomic.AtomicInteger();
        engine.snapshotRowHook = () -> {
            if (rows.incrementAndGet() == crashAfterRows) {
                engine.simulateCrash();
            }
        };
        Thread runner = new Thread(() -> {
            try {
                engine.run(goal);
            } catch (SyncException expected) {
                assertEquals(RejectReason.INITIALIZATION_INCOMPLETE, expected.reason());
            }
        });
        runner.start();
        runner.join(10_000);
        return engine;
    }

    @Test
    void initializationInterruptedDuringSchemaEvolutionResumesAcrossRestarts() throws Exception {
        Path stateDir = tempDir.resolve("state");
        Path targetDir = tempDir.resolve("target");
        InMemorySourceDatabase source = TestFixtures.newSource();
        for (int i = 1; i <= 80; i++) {
            source.put("k" + i, TestFixtures.row("v1-" + i, i));
        }
        // 初始化期间发生结构变化：新增可空列，之后既有旧结构行也有新结构行
        source.evolve(SchemaChange.addColumn("email", ColumnType.STRING, true));
        source.put("k81", Map.of("name", "v2-81", "age", 81, "email", "81@x"));
        source.put("k1", Map.of("name", "v2-k1", "age", 101, "email", "1@x"));
        long goal = source.latestCommitSeq(); // 83

        // 第一次：快照阶段确定性地在中途崩溃（快照共 81 行，在第 20 行后杀）
        crashDuringSnapshot(source, stateDir, targetDir, goal, 20);
        var wal1 = targetDir.resolve("target-wal.log");
        FileTargetStore targetAfterCrash = new FileTargetStore(targetDir);
        long rowsAfterCrash = targetAfterCrash.allRows().size();
        assertTrue(rowsAfterCrash > 0 && rowsAfterCrash < 81,
                "崩溃时初始化应处于未完成状态，实际行数=" + rowsAfterCrash);
        assertEquals(0L, new CheckpointStore(stateDir).load().commitSeq(),
                "未完成初始化不得提交 checkpoint（不能被当成已完成跳过）");
        assertTrue(new InitProgressStore(stateDir).exists(),
                "未完成初始化标记必须保留，供重启续跑");

        // 第二次：重启续跑，用钩子在续跑的快照期间再杀一次（模拟反复中断）
        {
            FileTargetStore target = new FileTargetStore(targetDir);
            SyncEngine engine2 = new SyncEngine(source, target, config(stateDir));
            java.util.concurrent.atomic.AtomicInteger rows2 =
                    new java.util.concurrent.atomic.AtomicInteger();
            engine2.snapshotRowHook = () -> {
                if (rows2.incrementAndGet() == 40) {
                    engine2.simulateCrash();
                }
            };
            Thread t2 = new Thread(() -> {
                try {
                    engine2.run(goal);
                } catch (SyncException expected) {
                    assertEquals(RejectReason.INITIALIZATION_INCOMPLETE, expected.reason());
                }
            });
            t2.start();
            t2.join(10_000);
            assertTrue(new InitProgressStore(stateDir).exists(),
                    "第二次仍未完成时标记必须继续保留");
        }

        // 第三次：全新进程实例重启，必须续跑到完成，不再每次失败
        FileTargetStore finalTarget = new FileTargetStore(targetDir);
        SyncEngine engine3 = new SyncEngine(source, finalTarget, config(stateDir));
        Thread runner3 = new Thread(() -> engine3.run(goal));
        runner3.start();
        engine3.awaitReached(goal, 15_000);
        runner3.join(15_000);

        assertFalse(new InitProgressStore(stateDir).exists(),
                "初始化完成后标记必须删除");
        assertEquals(goal, new CheckpointStore(stateDir).load().commitSeq());
        assertEquals(goal, finalTarget.lastAppliedCommitSeq());
        assertEquals(2, finalTarget.currentSchemaVersion(), "结构变化必须已追平到 v2");

        // 引擎3 可能在 awaitReached 返回后仍在收尾；确认 run 已真正结束
        assertTrue(!runner3.isAlive(), "runner3 必须已结束");
        assertEquals(81, finalTarget.allLiveRows().size());
        for (int i = 2; i <= 80; i++) {
            var row = finalTarget.get("k" + i);
            assertNotNull(row, "k" + i);
            assertEquals("v1-" + i, row.values().get("name"));
            assertTrue(row.values().containsKey("email"), "旧结构行必须补齐新列: k" + i);
            assertNullValue(row.values().get("email"), "旧结构行新列为 null: k" + i);
        }
        assertEquals("v2-k1", finalTarget.get("k1").values().get("name"));
        assertEquals("1@x", finalTarget.get("k1").values().get("email"));
        assertEquals("81@x", finalTarget.get("k81").values().get("email"));
    }

    private static void assertNullValue(Object value, String message) {
        assertEquals(null, value, message);
    }

    @Test
    void checkpointAheadOfDataIsRejectedWithDistinctReasonAndDoesNotPollute() throws Exception {
        Path stateDir = tempDir.resolve("state-ahead");
        Path targetDir = tempDir.resolve("target-ahead");
        // 目标端空目录（无 WAL），却写入一个超前的 checkpoint：现场对不上
        Files.createDirectories(targetDir);
        SyncConfig cfg = config(stateDir);
        new CheckpointStore(stateDir).store(
                new com.github.lechandonga.cdcsyncjava.model.SourcePosition(42));
        FileTargetStore target = new FileTargetStore(targetDir);
        SyncEngine engine = new SyncEngine(TestFixtures.newSource(), target, cfg);

        SyncException e = assertThrows(SyncException.class, () -> engine.run(100));
        assertEquals(RejectReason.STATE_CHECKPOINT_AHEAD_OF_DATA, e.reason(),
                "位点超前数据必须给出可区分原因");
        // 拒绝启动不写任何状态：目标仍为空、checkpoint 原样保留（不污染已有数据）
        assertFalse(target.initialized());
        assertEquals(42L, new CheckpointStore(stateDir).load().commitSeq());
    }

    @Test
    void corruptTargetWalDuringIncompleteInitIsRejectedWithoutOverwriting() throws Exception {
        Path stateDir = tempDir.resolve("state-corrupt");
        Path targetDir = tempDir.resolve("target-corrupt");
        Files.createDirectories(targetDir);

        // 直接构造（生产 wiring 的打开方式）也必须拒绝
        new InitProgressStore(stateDir).begin(50);
        Path wal = targetDir.resolve("target-wal.log");
        // 列类型无法解析 -> 初始记录不可恢复地损坏
        Files.writeString(wal, "I|id|name:NOT_A_REAL_TYPE:true\n");
        SyncException e = assertThrows(SyncException.class,
                () -> new FileTargetStore(targetDir, true));
        assertEquals(RejectReason.TARGET_STATE_CORRUPT, e.reason(),
                "损坏现场必须给出可区分原因");

        // 经引擎启动路径：先在空目录打开存储并包代理，再放损坏 WAL 与未完成标记，
        // run() 在续跑前严格重开 -> 以同一原因拒绝，且不截断、不删标记
        Path stateDir2 = tempDir.resolve("state-corrupt2");
        Path targetDir2 = tempDir.resolve("target-corrupt2");
        Files.createDirectories(targetDir2);
        Path wal2 = targetDir2.resolve("target-wal.log");
        FileTargetStore empty = new FileTargetStore(targetDir2);
        var swappable =
                new com.github.lechandonga.cdcsyncjava.target.SwappableTargetStore(empty);
        Files.writeString(wal2, "I|id|name:NOT_A_REAL_TYPE:true\n");
        new InitProgressStore(stateDir2).begin(50);
        SyncEngine engine = new SyncEngine(TestFixtures.newSource(), swappable,
                config(stateDir2));
        SyncException viaRun = assertThrows(SyncException.class, () -> engine.run(100));
        assertEquals(RejectReason.TARGET_STATE_CORRUPT, viaRun.reason());

        // 损坏文件原样保留，未被截断/覆盖；未完成标记也不能被误删
        List<String> lines = Files.readAllLines(wal2);
        assertEquals(1, lines.size());
        assertTrue(lines.get(0).startsWith("I|id|name:NOT_A_REAL_TYPE"));
        assertTrue(new InitProgressStore(stateDir2).exists());
    }
}
