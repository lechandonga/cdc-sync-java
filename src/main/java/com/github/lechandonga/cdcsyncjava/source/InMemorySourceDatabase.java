package com.github.lechandonga.cdcsyncjava.source;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.github.lechandonga.cdcsyncjava.engine.SchemaRegistry;
import com.github.lechandonga.cdcsyncjava.model.ChangeBatch;
import com.github.lechandonga.cdcsyncjava.model.ChangeEvent;
import com.github.lechandonga.cdcsyncjava.model.ColumnDef;
import com.github.lechandonga.cdcsyncjava.model.ColumnType;
import com.github.lechandonga.cdcsyncjava.model.OpType;
import com.github.lechandonga.cdcsyncjava.model.RowData;
import com.github.lechandonga.cdcsyncjava.model.SchemaChange;
import com.github.lechandonga.cdcsyncjava.model.SourcePosition;
import com.github.lechandonga.cdcsyncjava.model.TableSchema;

/**
 * 基于内存的可变源端数据源，模拟真实数据库。
 *
 * <p>所有提交（DML / DDL）在同一把提交锁下分配全局单调 commitSeq 并追加到日志，
 * 因此快照与增量读取共用同一线性化历史：快照在锁内取 highWatermark 与行镜像，
 * 快照之后的日志读取严格从 highWatermark 开始，衔接处无重复、无丢失、无错序。</p>
 */
public class InMemorySourceDatabase implements LocalSourceDatabase {

    /** 日志条目：DML 或 DDL，携带提交序号。 */
    private sealed interface LogEntry permits DmlEntry, DdlEntry {
        long seq();
    }

    private record DmlEntry(long seq, ChangeEvent event) implements LogEntry {
    }

    private record DdlEntry(long seq, SchemaChange change, int schemaVersionAfter)
            implements LogEntry {
    }

    private final Object commitLock = new Object();
    private final SchemaRegistry registry = new SchemaRegistry(true);
    /** 提交日志，下标 seq-1。 */
    private final List<LogEntry> log = new ArrayList<>();
    /** 物化的当前行状态（含行内最新 schema 版本）。 */
    private final Map<String, RowData> rows = new LinkedHashMap<>();
    private long batchSeqGenerator = 0L;
    private volatile boolean initialized = false;

    public InMemorySourceDatabase() {
    }

    @Override
    public void initialize(String keyColumn, List<ColumnDef> columns) {
        synchronized (commitLock) {
            if (initialized) {
                throw new IllegalStateException("源端已经初始化");
            }
            registry.init(new TableSchema(1, keyColumn, columns));
            initialized = true;
        }
    }

    @Override
    public TableSchema currentSchema() {
        synchronized (commitLock) {
            return registry.current();
        }
    }

    @Override
    public TableSchema schemaAt(int version) {
        synchronized (commitLock) {
            return registry.at(version);
        }
    }

    @Override
    public SchemaChange schemaChangeTo(int version) {
        synchronized (commitLock) {
            if (version < 2 || version > registry.currentVersion()) {
                throw new IllegalArgumentException("非法版本: " + version);
            }
            for (LogEntry e : log) {
                if (e instanceof DdlEntry d && d.schemaVersionAfter() == version) {
                    return d.change();
                }
            }
            throw new IllegalStateException("未找到产生 v" + version + " 的 DDL");
        }
    }

    @Override
    public long put(String key, Map<String, Object> values) {
        Objects.requireNonNull(key, "key");
        synchronized (commitLock) {
            requireInit();
            TableSchema schema = registry.current();
            Map<String, Object> copy = validateAndCopy(values, schema);
            copy.put(schema.keyColumn(), key);
            long seq = log.size() + 1L;
            RowData row = new RowData(copy, schema.version(), seq);
            OpType op = rows.containsKey(key) ? OpType.UPDATE : OpType.INSERT;
            rows.put(key, row);
            ChangeEvent event = new ChangeEvent(op, key, row, new SourcePosition(seq),
                    schema.version());
            log.add(new DmlEntry(seq, event));
            commitLock.notifyAll();
            return seq;
        }
    }

    @Override
    public long delete(String key) {
        Objects.requireNonNull(key, "key");
        synchronized (commitLock) {
            requireInit();
            TableSchema schema = registry.current();
            long seq = log.size() + 1L;
            rows.remove(key);
            ChangeEvent event = new ChangeEvent(OpType.DELETE, key, null,
                    new SourcePosition(seq), schema.version());
            log.add(new DmlEntry(seq, event));
            commitLock.notifyAll();
            return seq;
        }
    }

    @Override
    public long evolve(SchemaChange change) {
        Objects.requireNonNull(change, "change");
        synchronized (commitLock) {
            requireInit();
            TableSchema next = registry.register(change);
            long seq = log.size() + 1L;
            log.add(new DdlEntry(seq, change, next.version()));
            // 存量行保留其产生时的 schema 版本，由下游按版本翻译，源端不就地改写
            commitLock.notifyAll();
            return seq;
        }
    }

    @Override
    public long latestCommitSeq() {
        synchronized (commitLock) {
            return (long) log.size();
        }
    }

    @Override
    public Snapshot readSnapshot(long fromWatermark) {
        synchronized (commitLock) {
            requireInit();
            // fromWatermark != 0 仅用于重建续传时复现同一快照上界；
            // 普通快照固定取当前最新 highWatermark。
            long highWatermark = fromWatermark > 0 ? fromWatermark : log.size();
            if (highWatermark > log.size()) {
                throw new IllegalArgumentException(
                        "请求的高水位超过当前提交: " + highWatermark + " > " + log.size());
            }
            TableSchema schemaAtWatermark = registry.at(schemaVersionAt(highWatermark));
            List<RowData> snapshotRows = new ArrayList<>();
            for (RowData row : rows.values()) {
                // 快照点镜像：复制不可变值，保证快照读取期间并发提交不影响快照内容
                snapshotRows.add(new RowData(new LinkedHashMap<>(row.values()),
                        row.schemaVersion(), row.commitSeq()));
            }
            return new Snapshot(schemaAtWatermark, new SourcePosition(highWatermark),
                    List.copyOf(snapshotRows));
        }
    }

    @Override
    public Optional<ChangeBatch> nextBatch(SourcePosition after, int maxBatchSize,
                                           Duration waitTimeout) {
        Objects.requireNonNull(after, "after");
        long deadlineNanos = System.nanoTime() + waitTimeout.toNanos();
        synchronized (commitLock) {
            requireInit();
            while (log.size() <= after.commitSeq()) {
                long remaining = deadlineNanos - System.nanoTime();
                if (remaining <= 0) {
                    return Optional.empty();
                }
                try {
                    commitLock.wait(Duration.ofNanos(remaining).toMillis(),
                            (int) (remaining % 1_000_000));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            List<ChangeBatch.SchemaChangeAt> ddl = new ArrayList<>();
            List<ChangeEvent> events = new ArrayList<>();
            long end = Math.min(log.size(), after.commitSeq() + Math.max(1, maxBatchSize));
            int schemaVersion = schemaVersionAt(after.commitSeq());
            for (long s = after.commitSeq() + 1; s <= end; s++) {
                LogEntry entry = log.get((int) s - 1);
                if (entry instanceof DmlEntry dml) {
                    events.add(dml.event());
                } else if (entry instanceof DdlEntry d) {
                    schemaVersion = d.schemaVersionAfter();
                    ddl.add(new ChangeBatch.SchemaChangeAt(new SourcePosition(d.seq()),
                            d.schemaVersionAfter(), d.change()));
                }
            }
            long batchId = ++batchSeqGenerator;
            return Optional.of(new ChangeBatch(batchId, after, new SourcePosition(end),
                    List.copyOf(ddl), List.copyOf(events)));
        }
    }

    private int schemaVersionAt(long seq) {
        int version = 1;
        for (long s = 1; s <= seq && s <= log.size(); s++) {
            LogEntry e = log.get((int) s - 1);
            if (e instanceof DdlEntry d) {
                version = d.schemaVersionAfter();
            }
        }
        return version;
    }

    private void requireInit() {
        if (!initialized) {
            throw new IllegalStateException("源端尚未 initialize");
        }
    }

    private Map<String, Object> validateAndCopy(Map<String, Object> values, TableSchema schema) {
        Objects.requireNonNull(values, "values");
        Map<String, Object> copy = new LinkedHashMap<>();
        for (ColumnDef c : schema.columns()) {
            if (c.name().equals(schema.keyColumn())) {
                continue;
            }
            Object v = values.get(c.name());
            if (v == null) {
                if (!c.nullable()) {
                    throw new IllegalArgumentException("非空列缺少值: " + c.name());
                }
                copy.put(c.name(), null);
                continue;
            }
            copy.put(c.name(), coerce(c, v));
        }
        return copy;
    }

    private Object coerce(ColumnDef c, Object v) {
        if (c.type() == ColumnType.STRING) {
            return v.toString();
        }
        if (c.type() == ColumnType.INTEGER) {
            if (v instanceof Number n) {
                return n.intValue();
            }
            return Integer.parseInt(v.toString());
        }
        if (c.type() == ColumnType.BOOLEAN) {
            if (v instanceof Boolean b) {
                return b;
            }
            return Boolean.parseBoolean(v.toString());
        }
        throw new IllegalStateException("未知列类型: " + c.type());
    }
}
