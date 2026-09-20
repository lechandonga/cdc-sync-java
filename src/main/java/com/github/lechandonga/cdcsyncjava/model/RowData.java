package com.github.lechandonga.cdcsyncjava.model;

import java.util.Map;

/**
 * 源端一行数据的逻辑视图。
 * values 中包含主键列（key 也在 values 里）。
 *
 * @param values 列名 -> 值
 * @param schemaVersion 该行所属 schema 版本
 * @param commitSeq 源端提交序号（全局单调，用于去重与排序）
 */
public record RowData(Map<String, Object> values, int schemaVersion, long commitSeq) {
}
