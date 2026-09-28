package com.github.lechandonga.cdcsyncjava.engine;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lechandonga.cdcsyncjava.model.ChangeBatch;
import com.github.lechandonga.cdcsyncjava.model.SourcePosition;
import com.github.lechandonga.cdcsyncjava.source.LocalSourceDatabase;
import com.github.lechandonga.cdcsyncjava.target.ApplyOutcome;
import com.github.lechandonga.cdcsyncjava.target.RebuildCoordinator;

/**
 * 在线全量重建驱动器：在常规增量同步持续运行（读取方始终看到完整 current）的同时，
 * 对同一数据源另起一条“快照 + 增量追赶”流水线构建影子库，追平后只切换一次生效。
 *
 * <p>正确性要点：</p>
 * <ul>
 *   <li>重建期间读取方始终读 current，影子半成品不可见；最终由 {@link RebuildCoordinator#commitSwap()}
 *       原子 rename + 引擎批次边界屏障共同保证“只切换一次”，读取方绝无中间态；</li>
 *   <li>快照只覆盖到 highWatermark，其后的全部增量（含结构变化 DDL）由本驱动器从源端日志
 *       按序追平；DDL 带源端版本号幂等登记，DML 按 commitSeq 幂等；</li>
 *   <li>追赶进度在每个完整批次 fsync 后持久化进 shadow manifest，中途被打断保留影子目录，
 *       重试严格从已确认位点之后继续，重复多少次都不会把状态弄乱；</li>
 *   <li>切换时把运行中引擎的底层目标存储一次性热替换为新库句柄并对齐位点：
 *       切换前的在途批次落旧库（切换后按幂等跳过），切换后的批次落新库，不重、不丢。</li>
 * </ul>
 */
public class OnlineRebuildService {

    private static final Logger log = LoggerFactory.getLogger(OnlineRebuildService.class);

    private final LocalSourceDatabase source;
    private final RebuildCoordinator coordinator;
    private final SyncEngine liveEngine;
    private final int batchSize;
    private final long pollTimeoutMillis;

    private final AtomicLong shadowApplied = new AtomicLong();
    private final AtomicLong shadowDuplicates = new AtomicLong();

    public OnlineRebuildService(LocalSourceDatabase source, RebuildCoordinator coordinator,
                                SyncEngine liveEngine, int batchSize, long pollTimeoutMillis) {
        this.source = source;
        this.coordinator = coordinator;
        this.liveEngine = liveEngine;
        this.batchSize = Math.max(1, batchSize);
        this.pollTimeoutMillis = Math.max(1, pollTimeoutMillis);
    }

    /**
     * 执行一次在线重建直到追平 {@code catchUpTo} 并原子切换。
     * 可在被中断后用同一磁盘目录的新实例重试：未完成影子库会被识别并从 manifest 位点续传。
     *
     * @param catchUpTo 切换前影子库必须追平的源端位点
     */
    public void rebuildAndSwitch(long catchUpTo) throws InterruptedException {
        // 新重建拍快照；若目录里已有未完成影子库则传入快照被忽略，直接续传
        coordinator.startOrResume(source.readSnapshot(0), source);
        long from = coordinator.caughtUpTo();
        log.info("在线重建追赶开始：snapshotHighWatermark={} 追赶位点={} 目标={}",
                coordinator.snapshotHighWatermark(), from, catchUpTo);

        while (coordinator.isRebuilding()) {
            if (from >= catchUpTo) {
                break;
            }
            var opt = source.nextBatch(new SourcePosition(from), batchSize,
                    Duration.ofMillis(pollTimeoutMillis));
            if (opt.isEmpty()) {
                // 源端暂时无新提交：等待式等待，重建不自行终止
                continue;
            }
            ChangeBatch batch = opt.get();
            applyBatchToShadow(batch);
            // 整批应用完成（影子 WAL 已逐条 fsync）后才推进持久化追赶位点
            coordinator.saveCatchUpProgress(batch.to().commitSeq());
            from = batch.to().commitSeq();
        }

        if (!coordinator.isRebuilding()) {
            throw new SyncException(com.github.lechandonga.cdcsyncjava.model.RejectReason.REBUILD_NOT_ACTIVE,
                    "重建在追赶完成前已失效（影子库丢失），拒绝切换");
        }
        long shadowTail = coordinator.shadowLastAppliedCommitSeq();
        if (shadowTail < catchUpTo) {
            throw new SyncException(com.github.lechandonga.cdcsyncjava.model.RejectReason.ILLEGAL_STATE,
                    "影子库位点 " + shadowTail + " 未追平目标位点 " + catchUpTo + "，拒绝切换");
        }

        // 1) 影子数据 fsync 后原子 rename：current->staging, shadow->current
        coordinator.commitSwap();
        // 2) 运行中引擎在批次边界热替换到底层新库，只切换一次，读取方无中间态
        liveEngine.requestOnlineSwitch(coordinator.committedStore(), shadowTail);
        log.info("在线重建切换完成：新库位点={} 影子应用={} 幂等跳过={}",
                shadowTail, shadowApplied.get(), shadowDuplicates.get());
    }

    private void applyBatchToShadow(ChangeBatch batch) {
        // DDL 以 barrier 先于同批 DML 登记；带源端版本号，断点重放整批时幂等跳过
        for (ChangeBatch.SchemaChangeAt ddl : batch.schemaChanges()) {
            coordinator.registerShadowSchemaChange(ddl.change(), ddl.schemaVersionAfter());
        }
        for (var event : batch.events()) {
            ApplyOutcome outcome = coordinator.applyShadow(event);
            if (outcome == ApplyOutcome.APPLIED) {
                shadowApplied.incrementAndGet();
            } else if (outcome == ApplyOutcome.DUPLICATE_SKIPPED) {
                shadowDuplicates.incrementAndGet();
            }
        }
    }

    public long shadowApplied() {
        return shadowApplied.get();
    }

    public long shadowDuplicates() {
        return shadowDuplicates.get();
    }
}
