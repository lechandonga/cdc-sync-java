package com.github.lechandonga.cdcsyncjava.source;

import com.github.lechandonga.cdcsyncjava.model.ChangeEvent;
import com.github.lechandonga.cdcsyncjava.model.ChangeType;
import com.github.lechandonga.cdcsyncjava.model.FieldDef;
import com.github.lechandonga.cdcsyncjava.model.SchemaDef;
import com.github.lechandonga.cdcsyncjava.model.Snapshot;
import com.github.lechandonga.cdcsyncjava.model.SnapshotRow;
import com.github.lechandonga.cdcsyncjava.schema.SchemaChange;
import com.github.lechandonga.cdcsyncjava.schema.SchemaManager;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 内置内存可变数据源：
 * <ul>
 *   <li>所有写入追加到只增变更日志，在锁内分配全局单调提交序号（seq）；</li>
 *   <li>一致性快照通过在同一把日志锁内回放日志到位点 P 得到严格的 as-of P 视图；</li>
 *   <li>快照期间产生的变更 seq 大于 P，不会进入快照，但会被随后增量（seq &gt; P）覆盖。</li>
 * </ul>
 */
public final class LocalMutableSource implements SourceAdapter {

    private final Object logLock = new Object();
    private final List<ChangeEvent> log = new ArrayList<>();
    private final AtomicLong seqGen = new AtomicLong();
    private final AtomicLong eventIdGen = new AtomicLong();
    private final SchemaManager schemaManager;

    public LocalMutableSource(List<FieldDef> initialFields) {
        LinkedHashMap<String, FieldDef> fields = new LinkedHashMap<>();
        for (FieldDef f : initialFields) {
            fields.put(f.name(), f);
        }
        this.schemaManager = new SchemaManager(new SchemaDef(1, fields));
    }

    @Override
    public SchemaManager schemaManager() {
        return schemaManager;
    }

    // ==================== 写入入口 ====================

    public long upsert(String key, Map<String, Object> data) {
        Map<String, Object> payload = new LinkedHashMap<>(data);
        // 先按当前模式校验（SchemaDef 不可变），非法写入在源端即被拒绝，且不会消耗提交序号
        schemaManager.validate(schemaManager.current(), payload, "local-upsert:" + key);
        // 序号分配与日志追加必须原子：保证日志顺序严格等于全局提交序号顺序，
        // 并发写入下也不会出现“先分配的序号后入日志”导致的错序/漏读
        synchronized (logLock) {
            int version = schemaManager.current().version();
            long seq = seqGen.getAndIncrement();
            log.add(new ChangeEvent("evt-" + eventIdGen.getAndIncrement(), key, seq, version,
                    ChangeType.UPSERT, payload));
            return seq;
        }
    }

    public long delete(String key) {
        synchronized (logLock) {
            int version = schemaManager.current().version();
            long seq = seqGen.getAndIncrement();
            log.add(new ChangeEvent("evt-" + eventIdGen.getAndIncrement(), key, seq, version,
                    ChangeType.DELETE, Map.of()));
            return seq;
        }
    }

    public void evolve(SchemaChange change) {
        // SchemaManager 对不兼容演进抛出异常，且版本历史不变
        schemaManager.evolve(change);
    }

    // ==================== 读取 ====================

    @Override
    public long currentPosition() {
        synchronized (logLock) {
            return log.isEmpty() ? -1L : log.getLast().seq();
        }
    }

    @Override
    public Snapshot snapshotAt(long position) {
        // 在日志锁内回放，得到严格的 as-of P 视图：按提交顺序，每个键最后一次变更生效
        Map<String, SnapshotRow> view = new LinkedHashMap<>();
        synchronized (logLock) {
            for (ChangeEvent e : log) {
                if (e.seq() > position) {
                    break;
                }
                if (e.type() == ChangeType.DELETE) {
                    view.remove(e.key());
                } else {
                    view.put(e.key(), new SnapshotRow(
                            e.key(), e.seq(), e.schemaVersion(), new LinkedHashMap<>(e.data())));
                }
            }
        }
        return new Snapshot(position, new ArrayList<>(view.values()));
    }

    @Override
    public List<ChangeEvent> readChanges(long fromExclusiveSeq, int limit) {
        List<ChangeEvent> out = new ArrayList<>();
        synchronized (logLock) {
            for (ChangeEvent e : log) {
                if (e.seq() <= fromExclusiveSeq) {
                    continue;
                }
                out.add(e);
                if (out.size() >= limit) {
                    break;
                }
            }
        }
        return out;
    }
}
