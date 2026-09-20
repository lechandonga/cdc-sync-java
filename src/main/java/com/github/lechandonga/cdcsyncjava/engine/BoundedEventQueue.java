package com.github.lechandonga.cdcsyncjava.engine;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.github.lechandonga.cdcsyncjava.model.ChangeBatch;
import com.github.lechandonga.cdcsyncjava.model.RejectReason;

/**
 * 有界事件缓冲，杜绝目标端持续落后时无界占用内存。
 * <ul>
 *   <li>{@link BackpressurePolicy#BLOCK}：限时等待空位，超时仍满则抛 QUEUE_FULL（可恢复的限流）</li>
 *   <li>{@link BackpressurePolicy#REJECT}：立即拒绝本次投递并抛 QUEUE_FULL</li>
 * </ul>
 */
public class BoundedEventQueue {

    private final BlockingQueue<ChangeBatch> queue;
    private final BackpressurePolicy policy;
    private final long offerTimeoutMillis;

    public BoundedEventQueue(int capacity, BackpressurePolicy policy, long offerTimeoutMillis) {
        if (capacity < 1) {
            throw new IllegalArgumentException("缓冲容量必须 >= 1: " + capacity);
        }
        this.queue = new ArrayBlockingQueue<>(capacity);
        this.policy = policy;
        this.offerTimeoutMillis = offerTimeoutMillis;
    }

    /** 放入一个批次；满时按策略等待或立即抛 QUEUE_FULL。 */
    public void put(ChangeBatch batch) throws InterruptedException {
        boolean accepted = switch (policy) {
            case REJECT -> queue.offer(batch);
            case BLOCK -> queue.offer(batch, offerTimeoutMillis, TimeUnit.MILLISECONDS);
        };
        if (!accepted) {
            throw new SyncException(RejectReason.QUEUE_FULL,
                    "有界缓冲已满（容量 " + capacity() + "，积压 " + queue.size()
                            + "），按 " + policy + " 策略拒绝批次 batchId=" + batch.batchId()
                            + " 位点区间 (" + batch.from().commitSeq() + ","
                            + batch.to().commitSeq() + "]");
        }
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

    /** 清空（崩溃恢复 / 停止时调用，未确认数据不保留在内存）。 */
    public void clear() {
        queue.clear();
    }
}
