package com.github.lechandonga.cdcsyncjava.engine;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import com.github.lechandonga.cdcsyncjava.model.SkipRecord;

/**
 * 同步统计与跳过明细：应用 / 重复 / 拒绝分别计数，
 * 每条跳过项可定位到批次、位点、键与可区分原因。
 */
public class SyncStats {

    private final AtomicLong applied = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();
    private final AtomicLong schemaChanges = new AtomicLong();
    private final List<SkipRecord> skipRecords = new CopyOnWriteArrayList<>();

    public long applied() {
        return applied.get();
    }

    public long duplicates() {
        return duplicates.get();
    }

    public long rejected() {
        return rejected.get();
    }

    public long schemaChanges() {
        return schemaChanges.get();
    }

    public List<SkipRecord> skipRecords() {
        return List.copyOf(skipRecords);
    }

    void recordApplied() {
        applied.incrementAndGet();
    }

    void recordSchemaChange() {
        schemaChanges.incrementAndGet();
    }

    void recordDuplicate(SkipRecord record) {
        duplicates.incrementAndGet();
        skipRecords.add(record);
    }

    void recordRejected(SkipRecord record) {
        rejected.incrementAndGet();
        skipRecords.add(record);
    }
}
