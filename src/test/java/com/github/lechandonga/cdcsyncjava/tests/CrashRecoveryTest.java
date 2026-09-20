package com.github.lechandonga.cdcsyncjava.tests;

import com.github.lechandonga.cdcsyncjava.config.SyncConfig;
import com.github.lechandonga.cdcsyncjava.engine.SyncEngine;
import com.github.lechandonga.cdcsyncjava.model.ChangeEvent;
import com.github.lechandonga.cdcsyncjava.source.LocalMutableSource;
import com.github.lechandonga.cdcsyncjava.target.LocalTarget;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.TreeMap;

import static com.github.lechandonga.cdcsyncjava.tests.support.SyncTestSupport.newEngine;
import static com.github.lechandonga.cdcsyncjava.tests.support.SyncTestSupport.newSource;
import static com.github.lechandonga.cdcsyncjava.tests.support.SyncTestSupport.newTarget;
import static com.github.lechandonga.cdcsyncjava.tests.support.SyncTestSupport.row;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 崩溃恢复 / 断点续传：
 * - 已确认数据在“进程重启”后不丢失；
 * - 已写 WAL 但未提交位点的数据，重启后不被当作已处理（位点回退），
 *   源端重读时由幂等拦截，最终恰好一致；
 * - 快照完成标志在重启后保持。
 */
class CrashRecoveryTest {

    @TempDir
    Path dir;

    @Test
    void restartKeepsConfirmedDataAndCheckpoint() {
        LocalMutableSource source = newSource();
        source.upsert("k1", row("a", 1L));
        source.upsert("k2", row("b", 2L));

        LocalTarget target1 = newTarget(dir);
        SyncEngine engine1 = newEngine(source, target1, SyncConfig.defaults());
        engine1.syncOnce();
        long confirmed = target1.checkpoint().position();
        assertTrue(target1.checkpoint().snapshotCompleted());
        engine1.close();

        // 模拟进程重启：同目录新建目标端与引擎
        LocalTarget target2 = newTarget(dir);
        SyncEngine engine2 = newEngine(source, target2, SyncConfig.defaults());

        assertEquals(confirmed, target2.checkpoint().position(), "位点必须恢复");
        assertTrue(target2.checkpoint().snapshotCompleted(), "快照完成标志必须恢复");
        assertEquals(new TreeMap<>(row("a", 1L)), new TreeMap<>(target2.get("k1").orElseThrow()),
                "已确认数据不得丢失");
        assertEquals(new TreeMap<>(row("b", 2L)), new TreeMap<>(target2.get("k2").orElseThrow()));

        // 重启后继续同步新变更（断点续传）
        source.upsert("k1", row("a2", 3L));
        long expected = source.currentPosition();
        engine2.syncOnce();
        assertEquals(expected, target2.checkpoint().position());
        assertEquals(row("a2", 3L), target2.get("k1").orElseThrow());
        engine2.close();
    }

    @Test
    void crashAfterWalButBeforeCheckpointDoesNotLoseOrPrematurelyCommit() {
        LocalMutableSource source = newSource();
        source.upsert("k1", row("a", 1L));

        LocalTarget target1 = newTarget(dir);
        SyncEngine engine1 = newEngine(source, target1,
                new SyncConfig(10, 1, 100, 1000, SyncConfig.BufferPolicy.BLOCK));
        engine1.syncOnce(); // 快照完成，位点 = 0
        long snapshotPos = target1.checkpoint().position();

        // 新变更：直接写 WAL（模拟“数据已落盘、位点尚未提交”的崩溃窗口）
        ChangeEvent uncommitted = new ChangeEvent(
                "unc-1", "k9", snapshotPos + 1, 1,
                com.github.lechandonga.cdcsyncjava.model.ChangeType.UPSERT,
                row("ghost", 9L));
        target1.applyDurable(List.of(uncommitted), "crash-window", engine1.skippedLedger());
        // 不调用 commitCheckpoint —— 模拟在提交前崩溃
        assertEquals(snapshotPos, target1.checkpoint().position(),
                "未确认数据不得把位点前移");
        engine1.close();

        // 重启：位点必须仍停在旧值，未确认状态被回滚，k9 不得被当作已处理
        LocalTarget target2 = newTarget(dir);
        assertEquals(snapshotPos, target2.checkpoint().position(),
                "重启后不得把未确认位点当作已处理");
        assertFalse(target2.get("k9").isPresent(),
                "未确认数据在重启回滚后不得残留在活动表");

        // 源端从旧位点重读该 seq（同 eventId 或新投递），幂等重新应用并确认，最终一致
        SyncEngine engine2 = newEngine(source, target2, SyncConfig.defaults());
        source.upsert("k9", row("ghost", 9L));
        long expected = source.currentPosition();
        engine2.syncOnce();
        assertEquals(expected, target2.checkpoint().position());
        assertEquals(row("ghost", 9L), target2.get("k9").orElseThrow(),
                "重读后未确认数据被恰好应用一次");
        engine2.close();
    }

    @Test
    void crashDuringSnapshotIsSafelyResumed() {
        LocalMutableSource source = newSource();
        for (int i = 0; i < 50; i++) {
            source.upsert("k" + i, row("n" + i, i));
        }
        LocalTarget target1 = newTarget(dir);
        SyncEngine engine1 = newEngine(source, target1, SyncConfig.defaults());
        // 只完成部分快照数据（直接落部分行，不标记完成），随后崩溃
        var snap = source.snapshotAt(source.currentPosition());
        for (int i = 0; i < 20; i++) {
            var r = snap.rows().get(i);
            target1.applyDurable(List.of(new ChangeEvent(
                    "snap-k" + i + "-" + r.lastSeq(), r.key(), r.lastSeq(),
                    r.schemaVersion(), com.github.lechandonga.cdcsyncjava.model.ChangeType.UPSERT,
                    r.data())), "partial-snapshot", engine1.skippedLedger());
        }
        engine1.close();
        assertFalse(newTarget(dir).checkpoint().snapshotCompleted());

        // 重启后重跑快照：已落盘的 20 行幂等跳过，其余补齐，最终完整
        LocalTarget target2 = newTarget(dir);
        SyncEngine engine2 = newEngine(source, target2, SyncConfig.defaults());
        engine2.syncOnce();
        assertTrue(target2.checkpoint().snapshotCompleted());
        for (int i = 0; i < 50; i++) {
            assertTrue(target2.get("k" + i).isPresent(), "k" + i + " 应在重跑快照后存在");
        }
        engine2.close();
    }
}
