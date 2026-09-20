package com.github.lechandonga.cdcsyncjava.engine;

import com.github.lechandonga.cdcsyncjava.config.SyncConfig;
import com.github.lechandonga.cdcsyncjava.error.BackpressureException;
import com.github.lechandonga.cdcsyncjava.error.SkippedLedger;
import com.github.lechandonga.cdcsyncjava.error.SyncException;
import com.github.lechandonga.cdcsyncjava.model.ChangeEvent;
import com.github.lechandonga.cdcsyncjava.source.SourceAdapter;
import com.github.lechandonga.cdcsyncjava.target.TargetAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 增量阶段协调器。
 *
 * <p>单生产者拉取线程（源端按全局 seq 顺序读取，背压时阻塞或拒绝）→
 * 有界缓冲 → 应用线程按批 drain（同批保证全局顺序）→ 按键分区并行应用 →
 * <b>整批应用成功后才把位点提交到本批最大 seq</b>。
 *
 * <p>故障语义：
 * <ul>
 *   <li>崩溃于“已写 WAL、未提交位点”：重启时 WAL 重放、位点仍停留在旧值，
 *       源端从旧位点重读这批，重复事件被 (key,seq) 幂等拦截，不重复计数；</li>
 *   <li>某批应用异常：不提交位点，已落盘部分无害，重新拉取整批重试（至少一次投递）。</li>
 * </ul>
 */
public final class IncrementalCoordinator {

    private static final Logger log = LoggerFactory.getLogger(IncrementalCoordinator.class);

    private final SourceAdapter source;
    private final TargetAdapter target;
    private final ParallelKeyApplier applier;
    private final BoundedEventBuffer buffer;
    private final SkippedLedger ledger;
    private final SyncConfig config;
    private final AtomicLong batchSeq = new AtomicLong();

    public IncrementalCoordinator(SourceAdapter source, TargetAdapter target,
                                  ParallelKeyApplier applier, BoundedEventBuffer buffer,
                                  SkippedLedger ledger, SyncConfig config) {
        this.source = source;
        this.target = target;
        this.applier = applier;
        this.buffer = buffer;
        this.ledger = ledger;
        this.config = config;
    }

    /** 处理当前所有可读增量（单线程循环：拉取-应用-提交），返回处理后的最新位点。 */
    public long drainOnce() {
        long from = target.checkpoint().position();
        long sourceNow = source.currentPosition();
        if (sourceNow <= from) {
            log.debug("[incremental] 无可处理增量，位点={}", from);
            return from;
        }
        long total = 0;
        int emptyReads = 0;
        while (true) {
            long position = target.checkpoint().position();
            if (source.currentPosition() <= position) {
                break;
            }

            List<ChangeEvent> batch = new ArrayList<>(config.batchSize());
            int n;
            try {
                // 首批等待片刻以支持 runToPosition 的连续运行，后续立即排空
                long wait = (total == 0) ? 50L : 0L;
                n = buffer.drainTo(batch, config.batchSize(), wait);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }

            if (n == 0) {
                // 缓冲为空：从源端拉取一段灌入缓冲（受背压约束）
                pumpOnce(position);
                if (buffer.size() == 0) {
                    if (++emptyReads > 1) {
                        break;
                    }
                    continue;
                }
                emptyReads = 0;
                continue;
            }
            emptyReads = 0;

            long maxSeq = batch.getLast().seq();
            String batchId = "incr-b" + batchSeq.incrementAndGet();
            log.info("[incremental] batch={} 大小={} seq范围=[{}..{}] 缓冲余量={}",
                    batchId, n, batch.getFirst().seq(), maxSeq,
                    buffer.capacity() - buffer.size());

            // 应用失败则不提交位点：已落盘部分由幂等保证安全，上层/重启重新拉取
            applier.apply(batch, batchId, target, source.schemaManager(), ledger);
            target.commitCheckpoint(maxSeq);
            total += n;
        }
        log.info("[incremental] 本轮结束，确认位点={}，处理事件数={}", target.checkpoint().position(), total);
        return target.checkpoint().position();
    }

    /** 从源端读取至多 batchSize 条并按顺序灌入有界缓冲；返回入队条数。 */
    private int pumpOnce(long fromExclusiveSeq) {
        List<ChangeEvent> fetched = source.readChanges(fromExclusiveSeq, config.batchSize());
        int enqueued = 0;
        for (ChangeEvent e : fetched) {
            try {
                if (buffer.offer(e, config)) {
                    enqueued++;
                } else {
                    // BLOCK 超时：停止本轮灌入，稍后重试，源端位点未动不会丢事件
                    log.warn("[incremental] seq={} 限流未入队，稍后重试（位点保持={}）",
                            e.seq(), fromExclusiveSeq);
                    break;
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (BackpressureException be) {
                // REJECT 策略：按既定策略拒绝，错误上抛（位点不前移），可区分原因
                throw be;
            }
        }
        return enqueued;
    }

    /** 阻塞运行直到源端位点达到 targetPosition（含），用于并发写入场景的确定性收敛。 */
    public long runToPosition(long targetPosition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 60_000L;
        while (true) {
            long pos = drainOnce();
            if (pos >= targetPosition && source.currentPosition() <= pos) {
                return pos;
            }
            if (System.currentTimeMillis() > deadline) {
                throw new SyncException(null,
                        "等待源端位点 " + targetPosition + " 超时（当前确认=" + pos + "）");
            }
            Thread.sleep(2L);
        }
    }
}
