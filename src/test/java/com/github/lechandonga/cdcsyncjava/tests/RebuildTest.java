package com.github.lechandonga.cdcsyncjava.tests;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.lechandonga.cdcsyncjava.model.ChangeEvent;
import com.github.lechandonga.cdcsyncjava.model.OpType;
import com.github.lechandonga.cdcsyncjava.model.RowData;
import com.github.lechandonga.cdcsyncjava.model.SourcePosition;
import com.github.lechandonga.cdcsyncjava.source.InMemorySourceDatabase;
import com.github.lechandonga.cdcsyncjava.source.Snapshot;
import com.github.lechandonga.cdcsyncjava.support.TestFixtures;
import com.github.lechandonga.cdcsyncjava.target.RebuildCoordinator;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 全量重建测试：
 * 重建期间读取方继续看到完整旧库（绝无半成品）；重建可中断并从影子 manifest 安全续传；
 * 提交切换是原子的；切换后新库完整正确。
 */
class RebuildTest {

    @TempDir
    Path tempDir;

    @Test
    void rebuildIsInvisibleUntilAtomicSwap() {
        InMemorySourceDatabase source = TestFixtures.newSource();
        for (int i = 1; i <= 5; i++) {
            source.put("k" + i, TestFixtures.row("old-" + i, i));
        }
        RebuildCoordinator coordinator = new RebuildCoordinator(tempDir.resolve("rb"));
        // 初始库（扮演线上已有库）
        coordinator.startOrResume(source.readSnapshot(0));
        coordinator.commitSwap();
        assertEquals(5, coordinator.current().allLiveRows().size());

        // 源端数据发生漂移，发起重建
        for (int i = 1; i <= 5; i++) {
            source.put("k" + i, TestFixtures.row("new-" + i, 100 + i));
        }
        Snapshot snapshot = source.readSnapshot(0);
        coordinator.startOrResume(snapshot);
        // 重建中：读取方依然看到完整旧库，不接触影子半成品
        assertTrue(coordinator.isRebuilding());
        assertEquals(5, coordinator.current().allLiveRows().size());
        assertEquals("old-1", coordinator.current().get("k1").values().get("name"));
        assertEquals(5, coordinator.shadowLiveRows().size(), "影子库含重建快照的 5 行");

        coordinator.commitSwap();
        assertFalse(coordinator.isRebuilding());
        assertEquals("new-1", coordinator.current().get("k1").values().get("name"));
        assertEquals(101, coordinator.current().get("k1").values().get("age"));
        assertEquals(5, coordinator.current().allLiveRows().size());
    }

    @Test
    void interruptedRebuildResumesFromShadowAndCompletes() throws Exception {
        InMemorySourceDatabase source = TestFixtures.newSource();
        for (int i = 1; i <= 4; i++) {
            source.put("k" + i, TestFixtures.row("v" + i, i));
        }
        Path base = tempDir.resolve("rb");
        RebuildCoordinator coordinator = new RebuildCoordinator(base);
        Snapshot snapshot = source.readSnapshot(0);
        coordinator.startOrResume(snapshot);
        coordinator.commitSwap();

        // 开始重建（以漂移前快照为基准），影子库应用部分增量后“进程中断”（不 commitSwap）
        RebuildCoordinator rebuilding = new RebuildCoordinator(base);
        rebuilding.startOrResume(snapshot);
        // 快照高水位之后到达的源端变更，重建期间通过影子增量通道追赶
        source.put("k5", TestFixtures.row("v5", 5));
        // 直接对影子库应用 hw 之后的变更（幂等通道）
        ChangeEvent k5 = new ChangeEvent(OpType.INSERT, "k5",
                new RowData(java.util.Map.of("id", "k5", "name", "v5", "age", 5), 1, 6),
                new SourcePosition(6), 1);
        rebuilding.applyShadow(k5);
        long shadowBeforeCrash = rebuilding.shadowLastAppliedCommitSeq();
        rebuilding.abort(); // 进程中断：影子目录保留

        // 重启后续传：复用影子库，重复应用 k5 必须幂等
        RebuildCoordinator resumed = new RebuildCoordinator(base);
        assertTrue(resumed.isRebuilding(), "未完成的影子库必须被识别为可续传重建");
        assertEquals(shadowBeforeCrash, resumed.shadowLastAppliedCommitSeq());
        assertEquals(com.github.lechandonga.cdcsyncjava.target.ApplyOutcome.DUPLICATE_SKIPPED,
                resumed.applyShadow(k5), "重放未确认/重复数据必须幂等");

        // 读取方在整个中断-续传期间都只看到旧库
        assertEquals(4, resumed.current().allLiveRows().size());
        assertNull(resumed.current().get("k5"));

        resumed.commitSwap();
        assertEquals(5, resumed.current().allLiveRows().size());
        assertEquals("v5", resumed.current().get("k5").values().get("name"));
    }
}
