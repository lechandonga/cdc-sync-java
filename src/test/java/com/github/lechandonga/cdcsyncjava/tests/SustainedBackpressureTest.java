package com.github.lechandonga.cdcsyncjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.lechandonga.cdcsyncjava.engine.BackpressurePolicy;
import com.github.lechandonga.cdcsyncjava.engine.SyncConfig;
import com.github.lechandonga.cdcsyncjava.engine.SyncEngine;
import com.github.lechandonga.cdcsyncjava.engine.SyncException;
import com.github.lechandonga.cdcsyncjava.model.RejectReason;
import com.github.lechandonga.cdcsyncjava.source.InMemorySourceDatabase;
import com.github.lechandonga.cdcsyncjava.support.SlowTargetStore;
import com.github.lechandonga.cdcsyncjava.support.TestFixtures;
import com.github.lechandonga.cdcsyncjava.target.FileTargetStore;

/**
 * 目标端长时间落后场景：
 * <ul>
 *   <li>等待式背压必须在<b>整个落后过程中</b>持续有效——同步不能在落后状态下自行终止，
 *       缓冲占用始终有界，不丢批次、不重复应用，已确认位点绝不跑到未落稳数据前面；</li>
 *   <li>积压与背压（缓冲深度、源端落后量、背压等待次数/时长）可观测；</li>
 *   <li>“暂时处理不过来”（靠背压等待最终收敛）与“真的不可恢复”（TARGET_UNRECOVERABLE
 *       终止同步并抛出可区分原因）必须能分清。</li>
 * </ul>
 */
class SustainedBackpressureTest {

    @TempDir
    Path tempDir;

    private SyncConfig config(Path stateDir) {
        // 容量 1 + BLOCK：落后时 reader 持续等待，绝无 QUEUE_FULL 终止
        return SyncConfig.builder(stateDir)
                .batchSize(2).parallelism(1).bufferCapacity(1)
                .backpressurePolicy(BackpressurePolicy.BLOCK).pollTimeoutMillis(2).build();
    }

    @Test
    void waitingBackpressureHoldsThroughEntireLagPeriodAndEventuallyConverges() throws Exception {
        InMemorySourceDatabase source = TestFixtures.newSource();
        int total = 120;
        // 每条 DML 注入延迟，制造长时间持续落后
        SlowTargetStore slow = new SlowTargetStore(
                new FileTargetStore(tempDir.resolve("target")), 4);
        SyncEngine engine = new SyncEngine(source, slow, config(tempDir.resolve("state")));

        // 先启动引擎（空源），再由写入线程在增量阶段快速灌入：慢消费 + 快生产形成持续落后
        Thread runner = new Thread(() -> engine.run(total));
        runner.start();
        Thread writer = new Thread(() -> {
            for (int i = 1; i <= total; i++) {
                source.put("k" + i, TestFixtures.row("n" + i, i));
            }
        });
        writer.start();

        // 落后期间持续采样：积压有界、进度不超前落稳数据、源端确实落后过
        AtomicLong lagSamples = new AtomicLong();
        AtomicBoolean progressAheadOfData = new AtomicBoolean(false);
        AtomicBoolean bufferBounded = new AtomicBoolean(true);
        AtomicLong observedMaxLag = new AtomicLong();
        for (int s = 0; s < 400; s++) {
            Thread.sleep(2);
            if (!runner.isAlive()) {
                break;
            }
            if (engine.sourceLag() > 0) {
                lagSamples.incrementAndGet();
            }
            observedMaxLag.accumulateAndGet(engine.sourceLag(), Math::max);
            // 已确认位点绝不能超过目标端实际 fsync 的位点
            if (engine.committedPosition() > slow.lastAppliedCommitSeq()) {
                progressAheadOfData.set(true);
            }
            if (engine.backlogBatches() > 1) {
                bufferBounded.set(false);
            }
        }
        writer.join(5_000);
        engine.awaitReached(total, 30_000);
        runner.join(30_000);

        assertEquals(total, engine.committedPosition(), "落后再久也必须收敛，不得自行终止");
        assertTrue(lagSamples.get() >= 5,
                "采样期内必须多次观测到源端落后，实际落后样本=" + lagSamples.get());
        assertTrue(observedMaxLag.get() >= 1, "应观测到积压，maxLag=" + observedMaxLag.get());
        assertTrue(engine.backpressureWaits() >= 1,
                "等待式背压必须实际命中过，等待次数=" + engine.backpressureWaits());
        assertTrue(engine.backpressureWaitMillis() >= 1, "应记录背压累计等待时长");
        assertTrue(engine.maxBacklogBatches() >= 1, "峰值积压应可观测");
        assertTrue(bufferBounded.get(), "积压必须始终被容量限制，不能无界增长");
        assertTrue(!progressAheadOfData.get(), "已确认进度绝不能跑到未落稳数据前面");

        // 不丢、不重：最终行数恰好 total，统计无拒绝
        assertEquals(total, slow.allLiveRows().size());
        assertEquals(0L, engine.stats().rejected());
        for (int i = 1; i <= total; i++) {
            assertEquals("n" + i, slow.get("k" + i).values().get("name"), "k" + i);
        }
    }

    @Test
    void unrecoverableTargetFailureIsDistinctFromTransientBackpressure() throws Exception {
        InMemorySourceDatabase source = TestFixtures.newSource();
        source.put("seed", TestFixtures.row("seed", 1));
        SlowTargetStore slow = new SlowTargetStore(
                new FileTargetStore(tempDir.resolve("target2")), 1);
        // 应用若干条后注入一次真正不可恢复的故障
        slow.injectUnrecoverableAfter(8);
        SyncEngine engine = new SyncEngine(source, slow, config(tempDir.resolve("state2")));

        long goal = 100;
        Thread writer = new Thread(() -> {
            for (int i = 2; i <= goal; i++) {
                source.put("k" + i, TestFixtures.row("n" + i, i));
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        writer.setDaemon(true);
        writer.start();

        SyncException failure = assertThrows(SyncException.class, () -> engine.run(goal));
        assertEquals(RejectReason.TARGET_UNRECOVERABLE, failure.reason(),
                "不可恢复失败必须区别于 QUEUE_FULL 这类暂时背压");
        // 同步确实因失败终止，而不是继续在落后状态空转
        assertTrue(engine.committedPosition() < goal,
                "不可恢复失败时同步必须终止，committed=" + engine.committedPosition());
    }
}
