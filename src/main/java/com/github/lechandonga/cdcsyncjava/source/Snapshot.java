package com.github.lechandonga.cdcsyncjava.source;

import com.github.lechandonga.cdcsyncjava.model.RowData;
import com.github.lechandonga.cdcsyncjava.model.SourcePosition;
import com.github.lechandonga.cdcsyncjava.model.TableSchema;

import java.util.List;

/**
 * 一致性快照：某一时刻的 schema、全部行以及高水位。
 * 高水位表示快照数据已包含所有 commitSeq <= highWatermark 的提交，
 * 后续增量日志只需要从 highWatermark 之后读取。
 */
public record Snapshot(TableSchema schema, SourcePosition highWatermark, List<RowData> rows) {
}
