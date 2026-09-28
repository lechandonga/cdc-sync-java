package com.github.lechandonga.cdcsyncjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.lechandonga.cdcsyncjava.engine.BackpressurePolicy;
import com.github.lechandonga.cdcsyncjava.engine.OnlineRebuildService;
import com.github.lechandonga.cdcsyncjava.engine.SyncConfig;
import com.github.lechandonga.cdcsyncjava.engine.SyncEngine;
import com.github.lechandonga.cdcsyncjava.model.ColumnType;
import com.github.lechandonga.cdcsyncjava.model.SchemaChange;
import com.github.lechandonga.cdcsyncjava.source.InMemorySourceDatabase;
import com.github.lechandonga.cdcsyncjava.support.TestFixtures;
import com.github.lechandonga.cdcsyncjava.target.FileTargetStore;
import com.github.lechandonga.cdcsyncjava.target.LocalTargetStore;
import com.github.lechandonga.cdcsyncjava.target.RebuildCoordinator;
import com.github.lechandonga.cdcsyncjava.target.SwappableTargetStore;
import com.github.lechandonga.cdcsyncjava.target.TargetRow;

/**
 * 同步持续运行期间的在线全量重建：
 * <ul>
 *   <li>重建与常规增量同步同时进行，源端在重建开始后继续写入（含结构变化），重建自动追平；</li>
 *   <li>重建全程读取方始终读到完整可用数据，绝无半成品；最终只原子切换一次；</li>
 *   <li>重建结果与“正常全量 + 增量”独立流水线结果一致（不重、不丢、同键最终值正确）；</li>
 *   <li>重建中途被打断保留影子进度，重试接着来，重复几次状态不乱。</li>
 * </ul>
 */
class OnlineRebuildConcurrencyTest {

    @TempDir
    Path tempDir;

    private SyncConfig liveConfig(Path stateDir) {
        // 在线切换仅支持 BLOCK：等待式背压保证切换窗口不丢批次
        return SyncConfig.builder(stateDir)
                .batchSize(3).parallelism(3).bufferCapacity(4)
                .backpressurePolicy(BackpressurePolicy.BLOCK).pollTimeoutMillis(2).build();
    }

    private SyncConfig referenceConfig(Path stateDir) {
        return SyncConfig.builder(stateDir)
                .batchSize(5).parallelism(2).bufferCapacity(8)
                .backpressurePolicy(BackpressurePolicy.BLOCK).pollTimeoutMillis(2).build();
    }

    @Test
    void onlineRebuildWhileSyncKeepsRunningWithConcurrentDmlAndDdlConverges() throws Exception {
        // ---- 1) 先建出一个已在线服务的初始库（扮演线上 current） ----
        InMemorySourceDatabase source = TestFixtures.newSource();
        Path rbDir = tempDir.resolve("rb");
        RebuildCoordinator bootstrap = new RebuildCoordinator(rbDir);
        for (int i = 1; i <= 10; i++) {
            source.put("k" + i, TestFixtures.row("old-" + i, i));
        }
        bootstrap.startOrResume(source.readSnapshot(0), source);
        bootstrap.commitSwap();
        assertEquals(10, bootstrap.current().allLiveRows().size());

        // ---- 2) 常规增量同步持续运行（经可热切换存储访问 current） ----
        FileTargetStore liveTarget = new FileTargetStore(rbDir.resolve("current"));
        SwappableTargetStore swappable = new SwappableTargetStore(liveTarget);
        SyncEngine live = new SyncEngine(source, swappable, liveConfig(tempDir.resolve("live-state")));
        Thread liveRunner = new Thread(() -> live.run(Long.MAX_VALUE));
        liveRunner.start();
        live.awaitReached(10, 10_000);

        // ---- 3) 发起在线重建，同时源端持续写入（包含新增列 DDL） ----
        RebuildCoordinator coordinator = new RebuildCoordinator(rbDir);
        OnlineRebuildService rebuild = new OnlineRebuildService(
                source, coordinator, live, 3, 2);

        // 切换位点在重建线程起跑后、源端写入完成时确定（保证重建开始后仍有增量要追）
        final java.util.concurrent.atomic.AtomicLong swapAtRef =
                new java.util.concurrent.atomic.AtomicLong();
        Thread rebuildThread = new Thread(() -> {
            try {
                while (swapAtRef.get() == 0) {
                    Thread.sleep(1);
                }
                rebuild.rebuildAndSwitch(swapAtRef.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        rebuildThread.start();

        // 重建期间读取方持续采样：必须始终读到完整旧/新库，不出现半成品
        // 最终位点 = 初始10 + 重建期21 + 尾部20 + k1更新 + k40删除 = 53
        final long finalSeq = 53L;
        java.util.concurrent.atomic.AtomicBoolean sawCompleteView =
                new java.util.concurrent.atomic.AtomicBoolean(true);
        Thread readerSampler = new Thread(() -> {
            while (live.committedPosition() < finalSeq) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                LocalTargetStore view = swappable.currentDelegate();
                // current 始终完整：要么旧库（含 k1..k10），要么新库；绝不能读到空/半库
                if (view.initialized() && view.get("k1") == null) {
                    sawCompleteView.set(false);
                }
            }
        });
        readerSampler.setDaemon(true);
        readerSampler.start();

        // 重建开始后继续写入：更新旧键 + 插入新键 + 结构变化（新增可空列）
        for (int i = 1; i <= 5; i++) {
            source.put("k" + i, TestFixtures.row("mid-" + i, 50 + i));
        }
        source.evolve(SchemaChange.addColumn("email", ColumnType.STRING, true));
        for (int i = 6; i <= 20; i++) {
            source.put("k" + i, Map.of("name", "new-" + i, "age", 60 + i,
                    "email", i + "@x"));
        }
        long swapAt = source.latestCommitSeq(); // 31
        swapAtRef.set(swapAt);
        rebuildThread.join(15_000);
        assertFalse(rebuildThread.isAlive(), "重建应已完成单次切换");
        // 只切换一次：重建协调器已不再处于 rebuilding
        assertFalse(coordinator.isRebuilding());
        readerSampler.join(2_000);
        assertTrue(sawCompleteView.get(), "重建期间读取方绝不能读到空/半成品库");

        // ---- 4) 切换后常规同步继续在新库上运行，追平到 finalSeq 且结构为 v2 ----
        for (int i = 21; i <= 40; i++) {
            source.put("k" + i, Map.of("name", "tail-" + i, "age", 80 + i,
                    "email", i + "@x"));
        }
        source.put("k1", Map.of("name", "final-k1", "age", 999, "email", "final@x"));
        source.delete("k40");
        live.awaitReached(finalSeq, 15_000);
        live.stop();
        liveRunner.join(5_000);

        assertEquals(finalSeq, live.committedPosition());
        FileTargetStore finalView = new FileTargetStore(rbDir.resolve("current"));
        assertEquals(2, finalView.currentSchemaVersion());
        assertEquals(39, finalView.allLiveRows().size(), "40 键中 k40 删除 => 39 存活");

        // ---- 5) 独立的“正常全量+增量”流水线结果必须与在线重建结果逐键一致 ----
        FileTargetStore reference = new FileTargetStore(tempDir.resolve("reference"));
        SyncEngine refEngine = new SyncEngine(source, reference,
                referenceConfig(tempDir.resolve("ref-state")));
        Thread refRunner = new Thread(() -> refEngine.run(finalSeq));
        refRunner.start();
        refEngine.awaitReached(finalSeq, 15_000);
        refRunner.join(15_000);

        assertEquals(reference.allLiveRows().size(), finalView.allLiveRows().size());
        for (TargetRow refRow : reference.allLiveRows()) {
            TargetRow actual = finalView.get(refRow.key());
            assertNotNull(actual, "键 " + refRow.key() + " 在重建结果中缺失");
            assertEquals(refRow.values(), actual.values(),
                    "键 " + refRow.key() + " 与正常全量+增量结果不一致");
            assertEquals(refRow.commitSeq(), actual.commitSeq(),
                    "键 " + refRow.key() + " 最终生效位点不一致");
            assertEquals(2, actual.schemaVersion());
        }
        TargetRow k40 = finalView.get("k40");
        assertTrue(k40 == null || k40.deleted(), "k40 应为删除墓碑或不存在");
        assertEquals("final-k1", finalView.get("k1").values().get("name"));
        assertEquals("final@x", finalView.get("k1").values().get("email"));
    }

    @Test
    void interruptedOnlineRebuildResumesShadowAndCompletesAcrossRetries() throws Exception {
        // 初始在线库
        InMemorySourceDatabase source = TestFixtures.newSource();
        Path rbDir = tempDir.resolve("rb2");
        RebuildCoordinator bootstrap = new RebuildCoordinator(rbDir);
        for (int i = 1; i <= 8; i++) {
            source.put("k" + i, TestFixtures.row("old-" + i, i));
        }
        bootstrap.startOrResume(source.readSnapshot(0), source);
        bootstrap.commitSwap();

        SwappableTargetStore swappable = new SwappableTargetStore(
                new FileTargetStore(rbDir.resolve("current")));
        SyncEngine live = new SyncEngine(source, swappable,
                liveConfig(tempDir.resolve("live-state2")));
        Thread liveRunner = new Thread(() -> live.run(Long.MAX_VALUE));
        liveRunner.start();
        live.awaitReached(8, 10_000);

        // 重建期间持续写入（含 DDL），写入与重建追赶交织进行
        // 第一次重建：先起跑并等待目标位点；目标 = 初始8 + 17 DML + 1 DDL + 5 DML = 31
        RebuildCoordinator first = new RebuildCoordinator(rbDir);
        OnlineRebuildService rebuild1 = new OnlineRebuildService(source, first, live, 2, 2);
        final long goal = 31L;
        Thread t1 = new Thread(() -> {
            try {
                rebuild1.rebuildAndSwitch(goal);
            } catch (Exception ignored) {
                // 预期在追赶到 goal 之前被中断
            }
        });
        t1.start();

        // 边写边观测：一旦影子位点进入 (快照位点, 源端最新位点) 区间立即中断，
        // 该区间就是“重建正在追赶、尚未追平”的确定性窗口
        long shadowAtInterrupt = -1L;
        int phase = 0;
        outer:
        for (phase = 0; phase < 23; phase++) {
            if (phase < 17) {
                int i = 9 + phase;
                source.put("k" + i, TestFixtures.row("stream-" + i, i));
            } else if (phase == 17) {
                source.evolve(SchemaChange.addColumn("email", ColumnType.STRING, true));
            } else {
                int i = 26 + (phase - 18);
                source.put("k" + i, Map.of("name", "v2-" + i, "age", i, "email", i + "@x"));
            }
            Thread.sleep(1);
            long latest = source.latestCommitSeq();
            if (first.isRebuilding()) {
                long shadowSeq = first.shadowLastAppliedCommitSeq();
                if (shadowSeq > 8 && shadowSeq < latest) {
                    shadowAtInterrupt = shadowSeq;
                    break outer;
                }
            }
        }
        assertTrue(shadowAtInterrupt > 8 && shadowAtInterrupt < goal,
                "中断点必须落在重建追赶途中，影子位点=" + shadowAtInterrupt);
        // 补齐循环中因提前中断未写入的剩余提交，使源端达到 goal=31
        for (phase = phase + 1; phase < 23; phase++) {
            if (phase < 17) {
                int i = 9 + phase;
                source.put("k" + i, TestFixtures.row("stream-" + i, i));
            } else if (phase == 17) {
                source.evolve(SchemaChange.addColumn("email", ColumnType.STRING, true));
            } else {
                int i = 26 + (phase - 18);
                source.put("k" + i, Map.of("name", "v2-" + i, "age", i, "email", i + "@x"));
            }
        }
        assertEquals(goal, source.latestCommitSeq());
        // 用 coordinator.abort 模拟重建进程中断（影子目录与 manifest 保留），live 同步不受影响
        first.abort();
        t1.interrupt();
        t1.join(5_000);

        // 读取方仍然看到完整在线库（live 同步一直在推进，但绝不会看到影子半成品）
        FileTargetStore currentView = new FileTargetStore(rbDir.resolve("current"));
        assertTrue(currentView.initialized() && currentView.allLiveRows().size() >= 8,
                "中断重建期间读取方必须始终有完整库");
        assertNotNull(currentView.get("k1"));

        // 第二次重建：全新协调器实例识别未完成影子库，从 manifest 位点续传直到完成
        for (int i = 31; i <= 40; i++) {
            source.put("k" + i, Map.of("name", "late-" + i, "age", i, "email", i + "@x"));
        }
        long finalGoal = source.latestCommitSeq(); // 41
        RebuildCoordinator second = new RebuildCoordinator(rbDir);
        assertTrue(second.isRebuilding(), "中断的影子库必须被识别为可续传重建");
        // 追赶进度按整批边界持久化：续传位点 >= 观察到的影子尾位点（批内尾部随整批一起保留），
        // 但必须仍严格早于 goal（确实是“未追平”的中间状态）
        assertTrue(second.caughtUpTo() >= shadowAtInterrupt,
                "续传位点不得落后于已落盘影子位点: caughtUpTo=" + second.caughtUpTo()
                        + " shadowTail=" + shadowAtInterrupt);
        assertTrue(second.caughtUpTo() < goal,
                "中断时重建不得已追平 goal，caughtUpTo=" + second.caughtUpTo());
        OnlineRebuildService rebuild2 = new OnlineRebuildService(source, second, live, 3, 2);
        rebuild2.rebuildAndSwitch(finalGoal);
        assertFalse(second.isRebuilding());

        live.awaitReached(finalGoal, 10_000);
        FileTargetStore finalView = new FileTargetStore(rbDir.resolve("current"));
        // 不重不丢：1..40 共 40 键，结构 v2，新列齐全
        assertEquals(40, finalView.allLiveRows().size());
        assertEquals(2, finalView.currentSchemaVersion());
        for (int i = 26; i <= 40; i++) {
            assertEquals(i + "@x", finalView.get("k" + i).values().get("email"), "k" + i);
        }
        live.stop();
        liveRunner.join(5_000);
    }
}
