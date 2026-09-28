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
 *   <li>{@link BackpressurePolicy#BLOCK}：等待式背压，在空位出现前持续阻塞拉取线程
 *       （仅响应中断，不设超时），因此目标端长时间落后时同步会一直限流等待而不会自行终止，
 *       也不会丢批次；积压始终可通过 {@link #size()} / {@link #capacity()} 观测。</li>
 *   <li>{@link BackpressurePolicy#REJECT}：立即拒绝本次投递并抛 QUEUE_FULL。
 *       QUEUE_FULL 表示“暂时处理不过来”的可恢复限流，区别于目标端
 *       UNRECOVERABLE_TARGET_FAILURE 这类真失败。</li>
 * </ul>
 */
public class BoundedEventQueue {

    private final BlockingQueue<ChangeBatch> queue;
    private final BackpressurePolicy policy;
    private final AtomicLong totalOffered = new AtomicLong();
    private final AtomicLong totalRejected = new AtomicLong();

    public BoundedEventQueue(int capacity, BackpressurePolicy policy, long offerTimeoutMillis) {
        if (capacity < 1) {
            throw new IllegalArgumentException("缓冲容量必须 >= 1: " + capacity);
        }
        this.queue = new ArrayBlockingQueue<>(capacity);
        this.policy = policy;
    }

    /** 放入一个批次；满时按策略持续等待（BLOCK，可中断）或立即抛 QUEUE_FULL（REJECT）。 */
    public void put(ChangeBatch batch) throws InterruptedException {
        totalOffered.incrementAndGet();
        switch (policy) {
            case REJECT -> {
                if (!queue.offer(batch)) {
                    totalRejected.incrementAndGet();
                    throw queueFull(batch);
                }
            }
            case BLOCK ->
                // 持续等待空位：目标端落后多久就限流多久，绝不超时自杀、绝不丢批次
                queue.put(batch);
        }
    }

    private SyncException queueFull(ChangeBatch batch) {
        return new SyncException(RejectReason.QUEUE_FULL,
                "有界缓冲已满（容量 " + capacity() + "，积压 " + queue.size()
                        + "），按 " + policy + " 策略拒绝批次 batchId=" + batch.batchId()
                        + " 位点区间 (" + batch.from().commitSeq() + ","
                        + batch.to().commitSeq() + "]；属可恢复限流，目标端排空后可继续");
    }

    /** 取出下一批次，超时返回 null。 */
    public ChangeBatch poll(long timeoutMillis) throws InterruptedException {
        return queue.poll(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    /** 当前积压批次数。 */
    public int size() {
        return queue.size();
    }

    /** 容量。 */
    public int capacity() {
        // ArrayBlockingQueue 保留容量信息
        return queue.size() + queue.remainingCapacity();
    }

    /** 累计尝试投递批次数（含被拒绝的）。 */
    public long totalOffered() {
        return totalOffered.get();
    }

    /** 累计因背压拒绝的批次数（REJECT 策略下增长）。 */
    public long totalRejected() {
        return totalRejected.get();
    }

    /** 清空（崩溃恢复 / 停止时调用，未确认数据不保留在内存）。 */
    public void clear() {
        queue.clear();
    }
}
