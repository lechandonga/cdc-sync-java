package com.github.lechandonga.cdcsyncjava.target;

import com.github.lechandonga.cdcsyncjava.error.SkippedLedger;

import java.nio.file.Path;
import com.github.lechandonga.cdcsyncjava.model.ChangeEvent;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** 目标端适配器：幂等写入、位点提交、原子全量重建。 */
public interface TargetAdapter {

    /**
     * 持久化应用一批已校验事件（WAL + 内存表），按 (key, seq) 幂等去重；
     * 不推进位点（位点须在批次确认后由 commitCheckpoint 提交）。
     */
    void applyDurable(List<ChangeEvent> events, String batchId, SkippedLedger ledger);

    /** 位点提交（原子落盘）。 */
    void commitCheckpoint(long position);

    /** 标记快照阶段完成（位点推进到 P）。 */
    void completeSnapshot(long position);

    Checkpoint checkpoint();

    Optional<Map<String, Object>> get(String key);

    Map<String, Map<String, Object>> activeRows();

    RebuildSession beginRebuild(SkippedLedger ledger, String rebuildId);

    /** 持久化目录（重启恢复时以同一目录重新构造）。 */
    Path baseDir();

    void close();
}
