package com.github.lechandonga.cdcsyncjava.model;

/**
 * 一条被跳过的变更记录，必须可定位到具体记录与原因。
 *
 * @param batchId 所属批次
 * @param position 位点
 * @param key 主键（DDL 时为 null）
 * @param reason 原因
 * @param detail 人类可读说明
 */
public record SkipRecord(long batchId, SourcePosition position, String key,
                         RejectReason reason, String detail) {
}
