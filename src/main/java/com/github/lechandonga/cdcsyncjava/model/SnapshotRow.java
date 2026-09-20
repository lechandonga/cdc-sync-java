package com.github.lechandonga.cdcsyncjava.model;

import java.util.Map;

/**
 * 一致性快照中的一行：内容是截至快照位点 P 时该行的最终状态，
 * lastSeq 是产生该状态的最后一次变更序号（&lt;= P），用于与增量幂等衔接。
 */
public record SnapshotRow(String key, long lastSeq, int schemaVersion, Map<String, Object> data) {
}
