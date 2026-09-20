package com.github.lechandonga.cdcsyncjava.model;

/**
 * 一条源端变更事件（DML）。
 *
 * @param op 操作类型
 * @param key 主键值（字符串形式）
 * @param after 变更后的行；DELETE 时为 null
 * @param position 位点
 * @param schemaVersion 事件产生时的 schema 版本
 */
public record ChangeEvent(OpType op, String key, RowData after, SourcePosition position,
                          int schemaVersion) {
}
