package com.github.lechandonga.cdcsyncjava.engine;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.github.lechandonga.cdcsyncjava.model.ChangeBatch;
import com.github.lechandonga.cdcsyncjava.model.RejectReason;

/**
 * 有界事件缓冲，杜绝目标端持续落后时无界占用内存。
 * <ul>
 *   <li>{@link BackpressurePolicy#BLOCK}（等待式背压）：缓冲满时读取方<b>持续等待</b>直到有空位，
 *       目标端落后多久就限流多久，同步不会因为暂时处理不过来而自行终止；
 *       等待期间批次保留在源端读取位点上，不丢批次；等待次数 / 累计时长 / 最大积压可观测；
 *       与 {@link RejectReason#QUEUE_FULL}（立即拒绝、可恢复）和
 *       {@link RejectReason#TARGET_UNRECOVERABLE}（真的不可恢复）三者可区分。</li>
 *   <li>{@link BackpressurePolicy#REJECT}：立即拒绝本次投递并抛 QUEUE_FULL（可恢复的限流）。</li>
 * </ul>
 */
public class BoundedEventQueue {

    private final BlockingQueue<ChangeBatch> queue;
    private final BackpressurePolicy policy;
    private final long offerTimeoutMillis;
    /** 读取方因缓冲满而被迫等待的次数（背压命中次数）。 */
    private final AtomicLong backpressureWaits = new AtomicLong();
    /** 读取方累计背压等待毫秒数。 */
    private final AtomicLong backpressureWaitMillis = new AtomicLong();
    /** 历史峰值积压批次数。 */
    private final AtomicLong maxOccupancy = new AtomicLong();

    public BoundedEventQueue(int capacity, BackpressurePolicy policy, long offerTimeoutMillis) {
        if (capacity < 1) {
            throw new IllegalArgumentException("缓冲容量必须 >= 1: " + capacity);
        }
        this.queue = new ArrayBlockingQueue<>(capacity);
        this.policy = policy;
        this.offerTimeoutMillis = offerTimeoutMillis;
    }

    /**
     * 放入一个批次。
     * <p>REJECT：满则立即抛 QUEUE_FULL；BLOCK：满则持续等待（响应中断），直到空位出现，
     * 等待期间不丢失批次、不推进读取位点。</p>
     */
    public void put(ChangeBatch batch) throws InterruptedException {
        if (policy == BackpressurePolicy.REJECT) {
            boolean accepted = queue.offer(batch);
            if (!accepted) {
                throw queueFull(batch);
            }
            observeOccupancy();
            return;
        }
        // BLOCK：先试一次，满了才进入持续等待，避免无积压时的统计噪音
        if (queue.offer(batch)) {
            observeOccupancy();
            return;
        }
        backpressureWaits.incrementAndGet();
        long startNanos = System.nanoTime();
        try {
            // 无限等待：目标端落后期间同步必须活着，不能因为“暂时处理不过来”而终止；
            // offerTimeoutMillis 仅作为等待分片，超时后继续等，使等待可被中断/观测
            while (!queue.offer(batch, Math.max(1, offerTimeoutMillis), TimeUnit.MILLISECONDS)) {
                backpressureWaitMillis.addAndGet(Math.max(1, offerTimeoutMillis));
            }
        } finally {
            long waited = (System.nanoTime() - startNanos) / 1_000_000L;
            if (waited > 0) {
                backpressureWaitMillis.addAndGet(waited);
            }
        }
        observeOccupancy();
    }

    private SyncException queueFull(ChangeBatch batch) {
        return new SyncException(RejectReason.QUEUE_FULL,
                "有界缓冲已满（容量 " + capacity() + "，积压 " + queue.size()
                        + "），按 " + policy + " 策略拒绝批次 batchId=" + batch.batchId()
                        + " 位点区间 (" + batch.from().commitSeq() + ","
                        + batch.to().commitSeq() + "]");
    }

    private void observeOccupancy() {
        maxOccupancy.accumulateAndGet(queue.size(), Math::max);
    }

    /** 取出下一批次，超时返回 null。 */
    public ChangeBatch poll(long timeoutMillis) throws InterruptedException {
        return queue.poll(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    /** 当前积压批次数。 */
    public int size() {
        return queue.size();
    }

    /** 背压命中次数（读取方因缓冲满被迫等待的次数）。 */
    public long backpressureWaits() {
        return backpressureWaits.get();
    }

    /** 背压累计等待毫秒数。 */
    public long backpressureWaitMillis() {
        return backpressureWaitMillis.get();
    }

    /** 历史峰值积压批次数。 */
    public long maxOccupancy() {
        return maxOccupancy.get();
    }

    /** 容量。 */
    public int capacity() {
        // ArrayBlockingQueue 保留容量信息
        return queue.size() + queue.remainingCapacity();
    }

    /** 清空（崩溃恢复 / 停止时调用，未确认数据不保留在内存）。 */
    public void clear() {
        queue.clear();
    }
}
