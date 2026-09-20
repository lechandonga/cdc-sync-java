package com.github.lechandonga.cdcsyncjava.engine;

import com.github.lechandonga.cdcsyncjava.config.SyncConfig;
import com.github.lechandonga.cdcsyncjava.error.SkippedLedger;
import com.github.lechandonga.cdcsyncjava.model.ChangeEvent;
import com.github.lechandonga.cdcsyncjava.model.ChangeType;
import com.github.lechandonga.cdcsyncjava.model.Snapshot;
import com.github.lechandonga.cdcsyncjava.model.SnapshotRow;
import com.github.lechandonga.cdcsyncjava.source.SourceAdapter;
import com.github.lechandonga.cdcsyncjava.target.TargetAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 快照阶段协调器。
 *
 * <p>衔接协议（无重复、无丢失、无错序）：
 * <ol>
 *   <li>记录源端当前最新提交位点 P；</li>
 *   <li>装载严格 as-of P 的一致性快照视图（快照期间产生的变更 seq &gt; P，不进入快照）；</li>
 *   <li>快照行被编码为 seq = lastSeq（&le; P）的幂等 UPSERT，经分区应用器写入；</li>
 *   <li>写入成功后位点原子推进到 P 并标记 snapshotCompleted；</li>
 *   <li>增量阶段从 seq &gt; P 开始；快照与增量若在边界重叠（重试场景），
 *       由目标端 (key,seq) 幂等表拦截，绝不重复生效。</li>
 * </ol>
 * 本过程可安全重入：位点已显示快照完成时直接跳过。
 */
public final class SnapshotCoordinator {

    private static final Logger log = LoggerFactory.getLogger(SnapshotCoordinator.class);

    private final SourceAdapter source;
    private final TargetAdapter target;
    private final ParallelKeyApplier applier;
    private final SyncConfig config;
    private final SkippedLedger ledger;

    public SnapshotCoordinator(SourceAdapter source, TargetAdapter target,
                               ParallelKeyApplier applier, SyncConfig config,
                               SkippedLedger ledger) {
        this.source = source;
        this.target = target;
        this.applier = applier;
        this.config = config;
        this.ledger = ledger;
    }

    /** 若位点显示快照已完成则跳过；否则执行。返回是否实际执行了快照。 */
    public boolean ensureSnapshot() {
        if (target.checkpoint().snapshotCompleted()) {
            log.info("[snapshot] 快照此前已完成（位点={}），跳过", target.checkpoint().position());
            return false;
        }

        // 1) 一致性位点
        long p = source.currentPosition();
        log.info("[snapshot] 开始一致性快照，边界位点 P={}", p);

        // 2) as-of P 视图（源端在日志锁内回放，天然包含截至 P 的全部提交）
        Snapshot snapshot = source.snapshotAt(p);
        log.info("[snapshot] 快照视图包含 {} 行（截至 seq<={}）", snapshot.rows().size(), p);

        // 3) 分批写入：把快照行编码为 seq=lastSeq 的事件，与增量共享同一幂等域
        List<ChangeEvent> batch = new ArrayList<>(config.batchSize());
        int batchNo = 0;
        for (SnapshotRow row : snapshot.rows()) {
            batch.add(new ChangeEvent(
                    "snap-" + row.key() + "-" + row.lastSeq(),
                    row.key(), row.lastSeq(), row.schemaVersion(),
                    ChangeType.UPSERT, Map.copyOf(row.data())));
            if (batch.size() >= config.batchSize()) {
                applier.apply(new ArrayList<>(batch), "snapshot-b" + (++batchNo),
                        target, source.schemaManager(), ledger);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            applier.apply(new ArrayList<>(batch), "snapshot-b" + (++batchNo),
                    target, source.schemaManager(), ledger);
        }

        // 4) 数据全部落盘后，位点原子推进到 P（崩溃在这之前 → 重启重跑快照，幂等）
        target.completeSnapshot(p);
        log.info("[snapshot] 快照阶段完成并确认 P={}，后续增量从 seq>{} 接管", p, p);
        return true;
    }
}
