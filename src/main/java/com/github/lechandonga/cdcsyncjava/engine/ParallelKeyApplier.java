package com.github.lechandonga.cdcsyncjava.engine;

import com.github.lechandonga.cdcsyncjava.error.SkippedEvent;
import com.github.lechandonga.cdcsyncjava.error.SkippedLedger;
import com.github.lechandonga.cdcsyncjava.error.SyncException;
import com.github.lechandonga.cdcsyncjava.model.ChangeEvent;
import com.github.lechandonga.cdcsyncjava.schema.SchemaManager;
import com.github.lechandonga.cdcsyncjava.target.TargetAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * 按键分区并行应用器。
 *
 * <p>处理流程（以批次为单位）：
 * <ol>
 *   <li>整批先做模式投影（旧版本→当前版本）与负载校验：
 *       非法事件只记入 {@link SkippedLedger}，绝不落盘、不污染数据；</li>
 *   <li>合法事件按 key 哈希到固定分区，分区内单线程串行（保证同键按源端 seq 顺序生效），
 *       分区间并行（不同键并行处理）；</li>
 *   <li>等待全部分区完成后方法才返回：调用方据此在整批确认后提交位点，
 *       任一分区异常则整批视为失败（目标端写入幂等，可安全整批重试）。</li>
 * </ol>
 */
public final class ParallelKeyApplier {

    private static final Logger log = LoggerFactory.getLogger(ParallelKeyApplier.class);

    private final int concurrency;
    private final ExecutorService[] partitions;

    public ParallelKeyApplier(int concurrency) {
        if (concurrency <= 0) {
            throw new IllegalArgumentException("concurrency must be positive: " + concurrency);
        }
        this.concurrency = concurrency;
        this.partitions = new ExecutorService[concurrency];
        for (int i = 0; i < concurrency; i++) {
            final int idx = i;
            partitions[i] = Executors.newSingleThreadExecutor(
                    r -> {
                        Thread t = new Thread(r, "cdc-applier-p" + idx);
                        t.setDaemon(true);
                        return t;
                    });
        }
    }

    public int concurrency() {
        return concurrency;
    }

    /**
     * 应用一个批次（调用方保证 events 按全局 seq 有序）。
     */
    public void apply(List<ChangeEvent> events, String batchId, TargetAdapter target,
                      SchemaManager schemaManager, SkippedLedger ledger) {
        if (events.isEmpty()) {
            return;
        }

        // ---- 1) 整批投影 + 校验，剔除非法事件（定位到具体记录与原因） ----
        Map<Integer, List<ChangeEvent>> byPartition = new LinkedHashMap<>();
        for (ChangeEvent raw : events) {
            ChangeEvent projected;
            try {
                projected = schemaManager.project(raw);
            } catch (SyncException ex) {
                log.warn("[applier] 拒绝事件 batch={} event={} key={} seq={} reason={} detail={}",
                        batchId, raw.eventId(), raw.key(), raw.seq(), ex.reason(), ex.getMessage());
                ledger.record(SkippedEvent.of(raw.eventId(), raw.key(), raw.seq(), batchId,
                        ex.reason(), ex.getMessage()));
                continue;
            }
            byPartition
                    .computeIfAbsent(partitionOf(projected.key()), k -> new ArrayList<>())
                    .add(projected);
        }

        if (byPartition.isEmpty()) {
            log.info("[applier] batch={} 全部 {} 条事件被拒绝/跳过，未发生写入",
                    batchId, events.size());
            return;
        }

        // ---- 2) 分区并行应用，分区内保持顺序 ----
        List<Future<?>> futures = new ArrayList<>();
        for (Map.Entry<Integer, List<ChangeEvent>> entry : byPartition.entrySet()) {
            List<ChangeEvent> partEvents = entry.getValue();
            futures.add(partitions[entry.getKey()].submit(() -> {
                // 目标端 applyDurable 对 (key,seq) 幂等，重复/重试不产生重复数据
                target.applyDurable(partEvents, batchId + "-p" + entry.getKey(), ledger);
            }));
        }

        // ---- 3) 等待整批所有分区完成（失败则整批抛出，调用方不提交位点） ----
        SyncException firstError = null;
        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (Exception ex) {
                Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                log.error("[applier] batch={} 分区应用失败，整批不确认: {}", batchId, cause.toString());
                if (firstError == null) {
                    firstError = (cause instanceof SyncException se)
                            ? se
                            : new SyncException(null,
                                    "批次 " + batchId + " 应用失败: " + cause.getMessage(), cause);
                }
            }
        }
        if (firstError != null) {
            throw firstError;
        }
        log.debug("[applier] batch={} 已应用合法事件（位点尚未提交）", batchId);
    }

    private int partitionOf(String key) {
        return Math.floorMod(key.hashCode(), concurrency);
    }

    public void close() {
        for (ExecutorService es : partitions) {
            es.shutdown();
            try {
                es.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
