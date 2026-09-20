package com.github.lechandonga.cdcsyncjava.source;

import com.github.lechandonga.cdcsyncjava.model.ChangeEvent;
import com.github.lechandonga.cdcsyncjava.model.Snapshot;
import com.github.lechandonga.cdcsyncjava.schema.SchemaManager;

import java.util.List;

/** 源端适配器：可变本地数据源，提供一致性快照与增量日志读取。 */
public interface SourceAdapter {

    /** 当前最新提交位点（全局序号）。 */
    long currentPosition();

    /** 取位点 P 时刻的一致性视图（只包含 seq &lt;= P 的状态）。 */
    Snapshot snapshotAt(long position);

    /** 读取序号大于 fromExclusiveSeq 的最多 limit 条变更（按提交顺序）。 */
    List<ChangeEvent> readChanges(long fromExclusiveSeq, int limit);

    SchemaManager schemaManager();
}
