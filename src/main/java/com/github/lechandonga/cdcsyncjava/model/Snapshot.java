package com.github.lechandonga.cdcsyncjava.model;

import java.util.List;

/** 一致性快照：位点 P + 截至 P 的行集合。 */
public record Snapshot(long position, List<SnapshotRow> rows) {
}
