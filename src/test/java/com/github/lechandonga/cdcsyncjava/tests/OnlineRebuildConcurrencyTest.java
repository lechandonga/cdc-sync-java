package com.github.lechandonga.cdcsyncjava.tests;

import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.lechandonga.cdcsyncjava.engine.BackpressurePolicy;
import com.github.lechandonga.cdcsyncjava.engine.CheckpointStore;
import com.github.lechandonga.cdcsyncjava.engine.SyncConfig;
import com.github.lechandonga.cdcsyncjava.engine.SyncEngine;
import com.github.lechandonga.cdcsyncjava.model.ColumnType;
import com.github.lechandonga.cdcsyncjava.model.SchemaChange;
import com.github.lechandonga.cdcsyncjava.model.SourcePosition;
import com.github.lechandonga.cdcsyncjava.source.InMemorySourceDatabase;
import com.github.lechandonga.cdcsyncjava.support.TestFixtures;
import com.github.lechandonga.cdcsyncjava.target.FileTargetStore;
import com.github.lechandonga.cdcsyncjava.target.RebuildCoordinator;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 运行中全量重建（同步不停止、读取方不停机）：
 * <ul>
 *   <li>重建期间读取方始终读到完整可用的旧库，绝无半成品；</li>
 *   <li>重建开始后源端继续写入的增量（含 DDL 结构变化）被本次重建自动追平；</li>
 *   <li>整次重建只切换一次，切换后结果与“正常全量+增量”一致，无重复/丢失；</li>
 *   <li>重建中途进程终止（影子保留），重启识别现场接着续传，重复多次状态不乱。</li>
 * </ul>
 */
class OnlineRebuildConcurrencyTest {

    @TempDir
    Path tempDir;

    private SyncConfig config(Path state) {
        return SyncConfig.builder(state)
                .batchSize(2).parallelism(2).bufferCapacity(4)
                .backpressurePolicy(BackpressurePolicy.BLOCK).pollTimeoutMillis(2).build();
    }

    /**
     * 构造绑定重建协调器的引擎。引擎在线写入全部落到 coordinator.current()；
     * 构造参数里的 FileTargetStore 仅作占位，使用独立目录避免干扰 current/shadow 布局。
     */
    private SyncEngine engineWithCoordinator(InMemorySourceDatabase source, Path stateDir,
                                             RebuildCoordinator coordinator, String unused) {
        FileTargetStore placeholder = new FileTargetStore(tempDir.resolve(unused));
        return new SyncEngine(source, placeholder, config(stateDir))
                .withRebuildCoordinator(coordinator);
    }

    /** 用“建库快照 + 切换 + checkpoint”模拟一个已经在线服务的目标库。 */
    private void bootstrapOnlineStore(InMemorySourceDatabase source, Path stateDir,
                                      RebuildCoordinator coordinator, long seq) {
        coordinator.startOrResume(source.readSnapshot(0));
        coordinator.commitSwap();
        new CheckpointStore(stateDir).store(new SourcePosition(seq));
    }

    @Test
    void rebuildWhileStreamingKeepsReadableCurrentAndCatchesUpIncludingDdl() throws Exception {
        InMemorySourceDatabase source = TestFixtures.newSource();
        for (int i = 1; i <= 10; i++) {
            source.put("k" + i, TestFixtures.row("old-" + i, i));
        }
        Path stateDir = tempDir.resolve("state");
        Path rebuildDir = tempDir.resolve("rebuild");
        RebuildCoordinator coordinator = new RebuildCoordinator(rebuildDir);
        bootstrapOnlineStore(source, stateDir, coordinator, 10);

        SyncEngine engine = engineWithCoordinator(source, stateDir, coordinator, "unused1");
        Thread runner = new Thread(() -> engine.run(Long.MAX_VALUE));
        runner.start();
        // 引擎从 checkpoint=10 续传增量，先确认存活
        Thread.sleep(100);
        assertTrue(engine.isRunning());
        assertEquals(10, coordinator.current().allLiveRows().size());

        // 在线发起重建（同步不停止）；追赶上界取发起瞬间源端位点作为下界，
        // 实际切换发生在影子追平源端实时尾部时，期间持续写入都由本次重建追平
        long rebuildStart = source.latestCommitSeq();
        engine.requestOnlineRebuild(rebuildStart);
        assertTrue(coordinator.isRebuilding());

        // 重建期间读取方持续读取：始终是完整旧库（10 行），且源端持续写入（含结构变化）
        boolean sawRebuilding = false;
        for (int t = 0; t < 50 && coordinator.isRebuilding(); t++) {
            sawRebuilding = true;
            assertEquals(10, coordinator.current().allLiveRows().size(),
                    "重建进行中读取方必须一直看到完整旧库");
            assertNull(coordinator.current().get("k1").values().get("email"),
                    "切换前读取方不得看到新结构");
            // 重建开始后的增量：结构变化 + 更新 + 新增，commitSeq 全部大于快照 HW，由 fan-out 追平
            if (!source.currentSchema().columns().stream()
                    .anyMatch(c -> c.name().equals("email"))) {
                source.evolve(SchemaChange.addColumn("email", ColumnType.STRING, true));
            }
            source.put("k1", Map.of("name", "new-1", "age", 101, "email", "k1@x"));
            source.put("k11", Map.of("name", "new-11", "age", 111, "email", "k11@x"));
            Thread.sleep(5);
        }
        assertTrue(sawRebuilding, "用例必须确实观察到重建进行中的窗口");

        long finalSeq = source.latestCommitSeq();
        engine.awaitReached(finalSeq, 10_000);
        waitUntil(() -> !coordinator.isRebuilding(), 10_000);

        // 切换后继续写入，验证引擎没被重建打断，仍正常同步新 current
        source.put("k12", Map.of("name", "new-12", "age", 112, "email", "k12@x"));
        engine.awaitReached(source.latestCommitSeq(), 10_000);
        engine.stop();
        runner.join(5_000);

        // 与“正常全量+增量”一致：12 行存活、k1 最后值、新结构生效、无拒绝
        assertEquals(12, coordinator.current().allLiveRows().size());
        assertEquals("new-1", coordinator.current().get("k1").values().get("name"));
        assertEquals(101, coordinator.current().get("k1").values().get("age"));
        assertEquals("k1@x", coordinator.current().get("k1").values().get("email"));
        assertEquals("k12@x", coordinator.current().get("k12").values().get("email"));
        assertEquals(0, engine.stats().rejected(), "重建全程不应产生拒绝");
    }

    @Test
    void interruptedOnlineRebuildResumesAcrossRestartsAndConverges() throws Exception {
        InMemorySourceDatabase source = TestFixtures.newSource();
        for (int i = 1; i <= 8; i++) {
            source.put("k" + i, TestFixtures.row("old-" + i, i));
        }
        Path stateDir = tempDir.resolve("state");
        Path rebuildDir = tempDir.resolve("rebuild");
        RebuildCoordinator coordinator = new RebuildCoordinator(rebuildDir);
        bootstrapOnlineStore(source, stateDir, coordinator, 8);

        SyncEngine engine = engineWithCoordinator(source, stateDir, coordinator, "u1");
        Thread runner = new Thread(() -> engine.run(40));
        runner.start();
        Thread.sleep(100);

        // 发起重建，重建期间持续写入（含 DDL），影子追了一部分后模拟进程崩溃
        engine.requestOnlineRebuild(source.latestCommitSeq());
        source.evolve(SchemaChange.addColumn("email", ColumnType.STRING, true));
        // 等源端产生 HW 之后的增量并被 fan-out，再继续追加
        waitUntil(() -> !coordinator.isRebuilding()
                        || coordinator.shadowProgressSeq() > coordinator.snapshotHighWatermark(),
                3_000);
        assertTrue(coordinator.isRebuilding(), "仅 HW 之上的增量到达后才允许切换，此刻不应已切换");
        for (int i = 9; i <= 14; i++) {
            source.put("k" + i, Map.of("name", "n" + i, "age", i, "email", "k" + i + "@x"));
        }
        waitUntil(() -> coordinator.shadowProgressSeq() >= 14, 10_000);
        long shadowProgressAtCrash = coordinator.shadowProgressSeq();
        engine.simulateCrash();
        runner.join(5_000);
        assertTrue(coordinator.isRebuilding(), "崩溃后未完成影子必须保留");
        // 读取方在崩溃窗口只看到完整旧库
        assertEquals(8, coordinator.current().allLiveRows().size());

        // 第二次启动：仅重建协调器（自动识别未完成影子），不做任何手工修复
        RebuildCoordinator resumedCoordinator = new RebuildCoordinator(rebuildDir);
        assertTrue(resumedCoordinator.isRebuilding(), "重启必须识别可续传的影子库");
        assertEquals(shadowProgressAtCrash, resumedCoordinator.shadowProgressSeq());
        assertEquals(8, resumedCoordinator.current().allLiveRows().size());

        // 再制造一次中断：续传引擎跑起来、影子继续推进后再次崩溃（重复中断不能搞乱状态）
        for (int i = 15; i <= 16; i++) {
            source.put("k" + i, Map.of("name", "n" + i, "age", i, "email", "k" + i + "@x"));
        }
        SyncEngine resumed1 = engineWithCoordinator(source, stateDir, resumedCoordinator, "u2");
        Thread runner2 = new Thread(() -> resumed1.run(40));
        runner2.start();
        waitUntil(() -> resumedCoordinator.shadowProgressSeq() > shadowProgressAtCrash, 10_000);
        long shadowProgressAtCrash2 = resumedCoordinator.shadowProgressSeq();
        resumed1.simulateCrash();
        runner2.join(5_000);
        assertTrue(resumedCoordinator.isRebuilding());
        assertEquals(8, resumedCoordinator.current().allLiveRows().size());

        // 第三次启动：续传到底，最终切换，结果与源端一致
        RebuildCoordinator finalCoordinator = new RebuildCoordinator(rebuildDir);
        assertTrue(finalCoordinator.isRebuilding());
        assertEquals(shadowProgressAtCrash2, finalCoordinator.shadowProgressSeq());
        for (int i = 17; i <= 18; i++) {
            source.put("k" + i, Map.of("name", "n" + i, "age", i, "email", "k" + i + "@x"));
        }
        long finalSeq = source.latestCommitSeq();
        SyncEngine resumed2 = engineWithCoordinator(source, stateDir, finalCoordinator, "u3");
        Thread runner3 = new Thread(() -> resumed2.run(finalSeq));
        runner3.start();
        resumed2.awaitReached(finalSeq, 10_000);
        waitUntil(() -> !finalCoordinator.isRebuilding(), 10_000);
        runner3.join(5_000);

        // 旧键平移到新结构（email 补 null）、新键带 email，18 行无重复无丢失
        assertEquals(18, finalCoordinator.current().allLiveRows().size());
        assertNull(finalCoordinator.current().get("k1").values().get("email"));
        assertEquals("old-1", finalCoordinator.current().get("k1").values().get("name"));
        assertEquals("k18@x", finalCoordinator.current().get("k18").values().get("email"));
        assertEquals(0, resumed2.stats().rejected());
    }

    private static void waitUntil(BooleanCondition cond, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (cond.test()) {
                return;
            }
            Thread.sleep(5);
        }
        fail("条件等待超时");
    }

    private interface BooleanCondition {
        boolean test();
    }
}
