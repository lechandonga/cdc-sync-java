package com.github.lechandonga.cdcsyncjava.tests;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.lechandonga.cdcsyncjava.engine.BackpressurePolicy;
import com.github.lechandonga.cdcsyncjava.engine.BoundedEventQueue;
import com.github.lechandonga.cdcsyncjava.engine.SyncConfig;
import com.github.lechandonga.cdcsyncjava.engine.SyncEngine;
import com.github.lechandonga.cdcsyncjava.engine.SyncException;
import com.github.lechandonga.cdcsyncjava.model.ChangeBatch;
import com.github.lechandonga.cdcsyncjava.model.RejectReason;
import com.github.lechandonga.cdcsyncjava.model.SourcePosition;
import com.github.lechandonga.cdcsyncjava.source.InMemorySourceDatabase;
import com.github.lechandonga.cdcsyncjava.support.TestFixtures;
import com.github.lechandonga.cdcsyncjava.target.FileTargetStore;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 有界缓冲与背压测试：
 * REJECT 策略下缓冲耗尽必须以可区分原因 QUEUE_FULL 拒绝，而不是无界占用内存；
 * BLOCK 策略下持续拉取会被限流，消费排空后恢复同步并最终收敛。
 */
class BackpressureTest {

    @TempDir
    Path tempDir;

    private ChangeBatch batch(long id, long to) {
        return new ChangeBatch(id, new SourcePosition(to - 1), new SourcePosition(to),
                java.util.List.of(), java.util.List.of());
    }

    @Test
    void rejectPolicyThrowsQueueFullWhenBufferExhausted() throws Exception {
        BoundedEventQueue queue = new BoundedEventQueue(2, BackpressurePolicy.REJECT, 10);
        queue.put(batch(1, 1));
        queue.put(batch(2, 2));
        SyncException e = assertThrows(SyncException.class, () -> queue.put(batch(3, 3)));
        assertEquals(RejectReason.QUEUE_FULL, e.reason(), "缓冲耗尽必须给出可区分原因");
        assertTrue(e.getMessage().contains("batchId=3"), "拒绝信息必须含批次标识: " + e.getMessage());

        // 消费排空后可继续投递（可恢复的限流）
        assertNotNull(queue.poll(10));
        queue.put(batch(4, 4));
        assertEquals(2, queue.size());
    }

    @Test
    void blockPolicyLimitsReaderWhenTargetFallsBehindAndRecovers() throws Exception {
        InMemorySourceDatabase source = TestFixtures.newSource();
        // 极小缓冲 + BLOCK：目标端落后时 reader 被阻塞，内存占用始终有界
        FileTargetStore target = new FileTargetStore(tempDir.resolve("target"));
        SyncConfig config = SyncConfig.builder(tempDir.resolve("state"))
                .batchSize(1).parallelism(1).bufferCapacity(1)
                .backpressurePolicy(BackpressurePolicy.BLOCK).pollTimeoutMillis(2).build();
        SyncEngine engine = new SyncEngine(source, target, config);

        long total = 100;
        Thread runner = new Thread(() -> engine.run(total));
        runner.start();
        for (int i = 1; i <= total; i++) {
            source.put("k" + i, TestFixtures.row("n" + i, i));
        }
        engine.awaitReached(total, 15_000);
        runner.join(15_000);
        assertEquals(total, engine.committedPosition());
        assertEquals(total, target.allLiveRows().size());
        assertEquals(0, engine.stats().rejected(), "BLOCK 策略下不应产生拒绝");
    }

    @Test
    void rejectPolicyAtEngineLevelIsReportedWithDistinguishableReason() throws Exception {
        InMemorySourceDatabase source = TestFixtures.newSource();
        for (int i = 1; i <= 20; i++) {
            source.put("k" + i, TestFixtures.row("n" + i, i));
        }
        FileTargetStore target = new FileTargetStore(tempDir.resolve("target2"));
        // 容量 1 + REJECT：快照后首个批次可能成功，随后缓冲满即拒绝
        SyncConfig config = SyncConfig.builder(tempDir.resolve("state2"))
                .batchSize(1).parallelism(1).bufferCapacity(1)
                .backpressurePolicy(BackpressurePolicy.REJECT).pollTimeoutMillis(2).build();
        SyncEngine engine = new SyncEngine(source, target, config);

        AtomicBoolean sawQueueFull = new AtomicBoolean(false);
        Thread runner = new Thread(() -> {
            try {
                engine.run(20);
            } catch (SyncException e) {
                if (e.reason() == RejectReason.QUEUE_FULL) {
                    sawQueueFull.set(true);
                } else {
                    throw e;
                }
            }
        });
        runner.start();
        runner.join(10_000);
        // 缓冲耗尽必须表现为 QUEUE_FULL（可区分），而不是 OOM/无界堆积/静默丢数据
        assertTrue(sawQueueFull.get() || engine.committedPosition() >= 20,
                "要么显式 QUEUE_FULL 拒绝，要么全部完成");
    }
}
