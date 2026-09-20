package com.github.lechandonga.cdcsyncjava.engine;

import com.github.lechandonga.cdcsyncjava.config.SyncConfig;
import com.github.lechandonga.cdcsyncjava.error.RejectReason;
import com.github.lechandonga.cdcsyncjava.error.SkippedEvent;
import com.github.lechandonga.cdcsyncjava.error.SkippedLedger;
import com.github.lechandonga.cdcsyncjava.error.SyncException;
import com.github.lechandonga.cdcsyncjava.model.ChangeEvent;
import com.github.lechandonga.cdcsyncjava.model.Snapshot;
import com.github.lechandonga.cdcsyncjava.model.SnapshotRow;
import com.github.lechandonga.cdcsyncjava.source.SourceAdapter;
import com.github.lechandonga.cdcsyncjava.target.RebuildSession;
import com.github.lechandonga.cdcsyncjava.target.TargetAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

/**
 * 同步引擎门面。
 *
 * <p>职责：编排“一致性快照 → 增量捕获”的衔接、断点续传、至少一次投递下的幂等应用、
 * 有界缓冲背压，以及不影响读取方的在线全量重建。
 *
 * <p>进程“重启”语义：以同一持久化目录重新构造 {@code LocalTarget} 并重建本引擎即可，
 * 位点与数据自动恢复。
 */
public final class SyncEngine implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SyncEngine.class);

    private final SourceAdapter source;
    private final TargetAdapter target;
    private final SyncConfig config;
    private final SkippedLedger ledger;
    private final ParallelKeyApplier applier;
    private final BoundedEventBuffer buffer;
    private final SnapshotCoordinator snapshotCoordinator;
    private final IncrementalCoordinator incrementalCoordinator;
    /**
     * 同步操作与全量重建互斥：重建提交把活动表整体替换，若与一轮增量提交交错，
     * 可能用 as-of P 的新表覆盖边界之后刚写入的增量。串行化后语义清晰、结果可解释。
     */
    private final Object coordinationLock = new Object();

    public SyncEngine(SourceAdapter source, TargetAdapter target, SyncConfig config) {
        this.source = source;
        this.target = target;
        this.config = config;
        this.ledger = new SkippedLedger();
        this.applier = new ParallelKeyApplier(config.concurrency());
        this.buffer = new BoundedEventBuffer(config.bufferCapacity());
        this.snapshotCoordinator = new SnapshotCoordinator(source, target, applier, config, ledger);
        this.incrementalCoordinator = new IncrementalCoordinator(
                source, target, applier, buffer, ledger, config);
        log.info("[engine] 初始化完成 batchSize={} concurrency={} bufferCapacity={} policy={}",
                config.batchSize(), config.concurrency(), config.bufferCapacity(),
                config.bufferPolicy());
    }

    /** 确保快照完成，并把当前所有可读增量排空一次（断点续传时自动从已提交位点继续）。 */
    public void syncOnce() {
        synchronized (coordinationLock) {
            snapshotCoordinator.ensureSnapshot();
            incrementalCoordinator.drainOnce();
        }
    }

    /** 同步直到源端指定位点被确认。 */
    public void runToPosition(long position) {
        synchronized (coordinationLock) {
            snapshotCoordinator.ensureSnapshot();
            try {
                incrementalCoordinator.runToPosition(position);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SyncException(null, "runToPosition 被中断", e);
            }
        }
    }

    /**
     * 在线全量重建：影子表装载 as-of P 快照并补放 seq&lt;=P 的全部变更，
     * commit 原子换表。读方在切换前始终读旧活动表，任何时刻不见半成品；
     * 过程可被中断（abort/崩溃），暂存区被废弃后可安全重试。
     */
    public void rebuildFull() {
        synchronized (coordinationLock) {
            doRebuildFull();
        }
    }

    private void doRebuildFull() {
        String rebuildId = "rebuild-" + UUID.randomUUID();
        log.info("[engine] 开始在线全量重建 {}", rebuildId);

        RebuildSession session = target.beginRebuild(ledger, rebuildId);
        try {
            long p = source.currentPosition();

            // 1) 影子表装载一致性快照（幂等：seq=lastSeq）
            Snapshot snapshot = source.snapshotAt(p);
            for (SnapshotRow row : snapshot.rows()) {
                session.addSnapshotRow(row);
            }

            // 2) 补放历史变更日志（按全局顺序投影后应用），得到一致的重建结果
            List<ChangeEvent> all = source.readChanges(-1L, Integer.MAX_VALUE);
            for (ChangeEvent e : all) {
                if (e.seq() > p) {
                    break;
                }
                ChangeEvent projected = source.schemaManager().project(e);
                session.applyChange(projected);
            }

            // 3) 原子切换（与正常增量写互斥），位点置 P
            session.commit(p);
            log.info("[engine] 全量重建 {} 完成，活动表已原子切换至位点 {}", rebuildId, p);
        } catch (RuntimeException ex) {
            log.error("[engine] 全量重建 {} 失败，废弃影子表，活动表保持不变: {}", rebuildId, ex.toString());
            if (session.active()) {
                session.abort();
            }
            if (!(ex instanceof SyncException)) {
                throw new SyncException(RejectReason.REBUILD_ABORTED,
                        "全量重建失败: " + ex.getMessage(), ex);
            }
            throw ex;
        }
    }

    public SkippedLedger skippedLedger() {
        return ledger;
    }

    public List<SkippedEvent> skippedEvents() {
        return ledger.all();
    }

    public TargetAdapter target() {
        return target;
    }

    @Override
    public void close() {
        applier.close();
        target.close();
    }
}
