package com.github.lechandonga.cdcsyncjava.tests;

import com.github.lechandonga.cdcsyncjava.config.SyncConfig;
import com.github.lechandonga.cdcsyncjava.engine.SyncEngine;
import com.github.lechandonga.cdcsyncjava.error.RejectReason;
import com.github.lechandonga.cdcsyncjava.error.SkippedEvent;
import com.github.lechandonga.cdcsyncjava.model.FieldType;
import com.github.lechandonga.cdcsyncjava.schema.SchemaChange;
import com.github.lechandonga.cdcsyncjava.source.LocalMutableSource;
import com.github.lechandonga.cdcsyncjava.target.LocalTarget;
import com.github.lechandonga.cdcsyncjava.target.RebuildSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;

import static com.github.lechandonga.cdcsyncjava.tests.support.SyncTestSupport.newEngine;
import static com.github.lechandonga.cdcsyncjava.tests.support.SyncTestSupport.newSource;
import static com.github.lechandonga.cdcsyncjava.tests.support.SyncTestSupport.newTarget;
import static com.github.lechandonga.cdcsyncjava.tests.support.SyncTestSupport.row;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 在线全量重建：
 * - 重建过程中读取方始终看到完整的旧活动表，绝不看到半成品影子表；
 * - commit 原子切换后读到与源端 as-of P 一致的新表（含字段演进后的投影）；
 * - 重建可被中断（abort/崩溃），影子表废弃，活动表不受污染，且可安全重试。
 */
class OnlineRebuildTest {

    @TempDir
    Path dir;

    @Test
    void rebuildSwitchesAtomicallyAndReadersNeverSeeHalfState() {
        LocalMutableSource source = newSource();
        for (int i = 0; i < 100; i++) {
            source.upsert("k" + i, row("n" + i, i));
        }
        LocalTarget target = newTarget(dir);
        SyncEngine engine = newEngine(source, target, SyncConfig.defaults());
        engine.syncOnce();

        // 模式演进后，旧表仍是旧形态；重建后应呈现投影后的新形态
        source.evolve(new SchemaChange.RenameField("age", "years"));
        source.upsert("k0", Map.of("name", "n0-new", "years", 999L));
        engine.syncOnce();

        // 重建：重建期间对读方采样，任何时刻都必须能读到完整 100 行旧/新表
        int[] observedDuring = new int[1];
        Thread reader = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                int size = target.activeRows().size();
                // 切换前 100 行旧表，切换后 100 行新表，绝不应出现中间数量
                if (size != 100) {
                    observedDuring[0] = size;
                    return;
                }
            }
        });
        reader.start();
        engine.rebuildFull();
        reader.interrupt();

        assertEquals(0, observedDuring[0] == 100 ? 0 : observedDuring[0],
                "读方不应看到半成品行数（实际观测到 " + observedDuring[0] + "）");

        Map<String, Object> rebuilt = target.get("k0").orElseThrow();
        assertEquals(999L, rebuilt.get("years"));
        assertFalse(rebuilt.containsKey("age"));
        assertEquals(100, target.activeRows().size());
        // 重建计数持久化
        assertEquals(1L, target.checkpoint().rebuilds());
        engine.close();
    }

    @Test
    void interruptedRebuildLeavesActiveTableUntouchedAndCanRetry() {
        LocalMutableSource source = newSource();
        source.upsert("k1", row("a", 1L));
        source.upsert("k2", row("b", 2L));
        LocalTarget target = newTarget(dir);
        SyncEngine engine = newEngine(source, target, SyncConfig.defaults());
        engine.syncOnce();

        // 手动开始一次重建，写入部分影子数据后 abort（模拟中断）
        RebuildSession session = target.beginRebuild(engine.skippedLedger(), "rebuild-int-1");
        session.addSnapshotRow(new com.github.lechandonga.cdcsyncjava.model.SnapshotRow(
                "k9", 0L, 1, row("phantom", 9L)));
        session.abort();

        // 活动表不受污染，读不到影子行
        assertEquals(new TreeMap<>(row("a", 1L)), new TreeMap<>(target.get("k1").orElseThrow()));
        assertFalse(target.get("k9").isPresent(), "影子表半成品不得泄漏到活动表");
        assertEquals(0L, target.checkpoint().rebuilds());

        // 台账可定位到中断记录
        SkippedEvent abortRecord = engine.skippedEvents().stream()
                .filter(s -> s.reason() == RejectReason.REBUILD_ABORTED)
                .findFirst().orElseThrow();
        assertEquals("rebuild-int-1", abortRecord.eventId());

        // 可安全重试，且暂存区已被清理
        assertFalse(Files.exists(target.baseDir().resolve("rebuild-staging")));
        engine.rebuildFull();
        assertEquals(1L, target.checkpoint().rebuilds());
        assertEquals(2, target.activeRows().size());
        engine.close();
    }

    @Test
    void crashedRebuildStagingIsCleanedOnRestartAndRebuildSucceeds() {
        LocalMutableSource source = newSource();
        source.upsert("k1", row("a", 1L));
        LocalTarget target1 = newTarget(dir);
        SyncEngine engine1 = newEngine(source, target1, SyncConfig.defaults());
        engine1.syncOnce();
        // 留下一个未提交的暂存区后直接“崩溃”（不 abort）
        RebuildSession session = target1.beginRebuild(engine1.skippedLedger(), "rebuild-crash");
        session.addSnapshotRow(new com.github.lechandonga.cdcsyncjava.model.SnapshotRow(
                "kz", 0L, 1, row("z", 99L)));
        engine1.close();
        assertTrue(Files.exists(dir.resolve("rebuild-staging")));

        // 重启：暂存区应被自动废弃，活动表完好，随后可正常重建
        LocalTarget target2 = newTarget(dir);
        assertFalse(Files.exists(target2.baseDir().resolve("rebuild-staging")),
                "未完成暂存区必须在重启时清理");
        assertTrue(target2.get("k1").isPresent());
        assertFalse(target2.get("kz").isPresent());

        SyncEngine engine2 = newEngine(source, target2, SyncConfig.defaults());
        engine2.rebuildFull();
        assertEquals(1L, target2.checkpoint().rebuilds());
        assertEquals(new TreeMap<>(row("a", 1L)), new TreeMap<>(target2.get("k1").orElseThrow()));
        engine2.close();
    }
}
