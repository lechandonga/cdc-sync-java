package com.github.lechandonga.cdcsyncjava.engine;

import com.github.lechandonga.cdcsyncjava.config.SyncConfig;
import com.github.lechandonga.cdcsyncjava.error.BackpressureException;
import com.github.lechandonga.cdcsyncjava.model.ChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 有界事件缓冲，防止目标端持续落后时无界占用内存。
 * <ul>
 *   <li>{@code BLOCK}：缓冲满时限流等待（最多 offerTimeoutMs），等待仍失败则放弃本批读取并重试；</li>
 *   <li>{@code REJECT}：缓冲满时立即抛 {@link BackpressureException}，给出可区分的拒绝原因。</li>
 * </ul>
 * 内部为 FIFO（{@link ArrayBlockingQueue}），事件本身按全局 seq 生成，drain 保持入队顺序。
 */
public final class BoundedEventBuffer {

    private static final Logger log = LoggerFactory.getLogger(BoundedEventBuffer.class);

    private final ArrayBlockingQueue<ChangeEvent> queue;

    public BoundedEventBuffer(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("buffer capacity must be positive: " + capacity);
        }
        this.queue = new ArrayBlockingQueue<>(capacity);
    }

    /**
     * 放入一个事件。
     *
     * @return true 已入队；false 在 BLOCK 策略下表示超时未入队（调用方应回退本轮读取）
     */
    public boolean offer(ChangeEvent event, SyncConfig config) throws InterruptedException {
        if (config.bufferPolicy() == SyncConfig.BufferPolicy.REJECT) {
            boolean ok = queue.offer(event);
            if (!ok) {
                log.warn("[buffer] 背压拒绝 event={} key={} seq={} capacity={} size={}",
                        event.eventId(), event.key(), event.seq(),
                        capacity(), queue.size());
                throw new BackpressureException(
                        "有界缓冲已满 (capacity=" + capacity() + ", size=" + queue.size()
                                + ")，按 REJECT 策略拒绝事件 key=" + event.key()
                                + " seq=" + event.seq());
            }
            return true;
        }
        boolean ok = queue.offer(event, config.offerTimeoutMs(), TimeUnit.MILLISECONDS);
        if (!ok) {
            log.warn("[buffer] 限流等待 {}ms 仍满，回退本轮读取 size={} capacity={} seq={}",
                    config.offerTimeoutMs(), queue.size(), capacity(), event.seq());
        }
        return ok;
    }

    /** 按 FIFO 顺序最多取出 max 条；无事件时短暂等待以减少空转。 */
    public int drainTo(List<ChangeEvent> sink, int max, long pollTimeoutMs) throws InterruptedException {
        int taken = 0;
        while (taken < max) {
            long wait = taken == 0 ? pollTimeoutMs : 0L;
            ChangeEvent e = wait > 0 ? queue.poll(wait, TimeUnit.MILLISECONDS) : queue.poll();
            if (e == null) {
                break;
            }
            sink.add(e);
            taken++;
        }
        return taken;
    }

    public int size() {
        return queue.size();
    }

    public int capacity() {
        return queue.remainingCapacity() + queue.size();
    }

    public void clear() {
        queue.clear();
    }
}
