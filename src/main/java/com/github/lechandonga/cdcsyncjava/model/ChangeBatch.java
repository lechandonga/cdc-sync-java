package com.github.lechandonga.cdcsyncjava.model;

import java.util.List;

/**
 * 一次从源端日志拉取的变更批次，可能包含 DDL 与 DML，严格保持源端顺序。
 *
 * @param batchId 源端单调递增的批次序号
 * @param from 本批次起始位点（不含），即上一批最后一条的位点
 * @param to 本批次最后一条事件位点
 * @param schemaChanges 结构变更（按顺序）
 * @param events 数据变更（按顺序）
 */
public record ChangeBatch(long batchId, SourcePosition from, SourcePosition to,
                          List<SchemaChangeAt> schemaChanges, List<ChangeEvent> events) {

    /** 带位点与版本的结构变更。 */
    public record SchemaChangeAt(SourcePosition position, int schemaVersionAfter,
                                 SchemaChange change) {
    }
}
