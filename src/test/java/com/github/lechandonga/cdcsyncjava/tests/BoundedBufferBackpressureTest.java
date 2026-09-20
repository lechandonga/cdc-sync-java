package com.github.lechandonga.cdcsyncjava.tests;

import com.github.lechandonga.cdcsyncjava.config.SyncConfig;
import com.github.lechandonga.cdcsyncjava.engine.BoundedEventBuffer;
import com.github.lechandonga.cdcsyncjava.error.BackpressureException;
import com.github.lechandonga.cdcsyncjava.error.RejectReason;
import com.github.lechandonga.cdcsyncjava.model.ChangeEvent;
import com.github.lechandonga.cdcsyncjava.model.ChangeType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 有界缓冲与背压：
 * - REJECT：缓冲耗尽时按既定策略立即拒绝，原因可区分（BUFFER_FULL），不无限占用内存；
 * - BLOCK：满时限流等待，排空后恢复；
 * - drain 严格 FIFO（支撑全局顺序与可复现批次）。
 */
class BoundedBufferBackpressureTest {

    private ChangeEvent event(long seq) {
        return new ChangeEvent("e" + seq, "k" + seq, seq, 1, ChangeType.UPSERT, Map.of());
    }

    private SyncConfig policy(SyncConfig.BufferPolicy p, int cap, long timeoutMs) {
        return new SyncConfig(8, 2, cap, timeoutMs, p);
    }

    @Test
    void rejectPolicyThrowsWhenBufferExhausted() throws Exception {
        BoundedEventBuffer buffer = new BoundedEventBuffer(3);
        SyncConfig cfg = policy(SyncConfig.BufferPolicy.REJECT, 3, 1000);
        assertDoesNotThrow(() -> {
            buffer.offer(event(1), cfg);
            buffer.offer(event(2), cfg);
            buffer.offer(event(3), cfg);
        });
        assertEquals(3, buffer.size());

        BackpressureException ex = assertThrows(BackpressureException.class,
                () -> buffer.offer(event(4), cfg));
        assertEquals(RejectReason.BUFFER_FULL, ex.reason(), "拒绝原因必须可区分");
        assertTrue(ex.getMessage().contains("key=k4"));
        assertEquals(3, buffer.size(), "拒绝后不得超占内存");

        // 排空后可恢复
        List<ChangeEvent> sink = new ArrayList<>();
        buffer.drainTo(sink, 10, 0);
        assertEquals(3, sink.size());
        assertDoesNotThrow(() -> buffer.offer(event(5), cfg));
    }

    @Test
    void blockPolicyWaitsAndResumesOnDrain() throws Exception {
        BoundedEventBuffer buffer = new BoundedEventBuffer(2);
        SyncConfig cfg = policy(SyncConfig.BufferPolicy.BLOCK, 2, 200);
        buffer.offer(event(1), cfg);
        buffer.offer(event(2), cfg);

        long start = System.currentTimeMillis();
        // 缓冲满，offer 超时返回 false（限流，不丢内存、不抛非预期异常）
        boolean enqueued = buffer.offer(event(3), cfg);
        long elapsed = System.currentTimeMillis() - start;
        assertFalse(enqueued, "BLOCK 超时应放弃本次入队以回退重试");
        assertTrue(elapsed >= 150, "应实际等待限流时长");

        // 排空一条后立即恢复
        List<ChangeEvent> sink = new ArrayList<>();
        assertEquals(1, buffer.drainTo(sink, 1, 0));
        assertTrue(buffer.offer(event(3),
                new SyncConfig(8, 2, 2, 1000, SyncConfig.BufferPolicy.BLOCK)));

        // FIFO 顺序
        sink.clear();
        buffer.drainTo(sink, 10, 0);
        assertEquals(List.of(2L, 3L), sink.stream().map(ChangeEvent::seq).toList());
    }

    @Test
    void batchSizeAndConcurrencyAreConfigurable() {
        SyncConfig cfg = new SyncConfig(37, 6, 123, 250, SyncConfig.BufferPolicy.REJECT);
        assertEquals(37, cfg.batchSize());
        assertEquals(6, cfg.concurrency());
        assertEquals(123, cfg.bufferCapacity());
        assertEquals(250, cfg.offerTimeoutMs());
        assertEquals(SyncConfig.BufferPolicy.REJECT, cfg.bufferPolicy());
        assertEquals(123, new BoundedEventBuffer(123).capacity());
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static void assertDoesNotThrow(ThrowingRunnable r) {
        try {
            r.run();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
