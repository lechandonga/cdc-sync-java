package com.github.lechandonga.cdcsyncjava.target;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.github.lechandonga.cdcsyncjava.engine.SchemaRegistry;
import com.github.lechandonga.cdcsyncjava.engine.SyncException;
import com.github.lechandonga.cdcsyncjava.model.ChangeEvent;
import com.github.lechandonga.cdcsyncjava.model.ColumnDef;
import com.github.lechandonga.cdcsyncjava.model.ColumnType;
import com.github.lechandonga.cdcsyncjava.model.EvolutionType;
import com.github.lechandonga.cdcsyncjava.model.OpType;
import com.github.lechandonga.cdcsyncjava.model.RejectReason;
import com.github.lechandonga.cdcsyncjava.model.RowData;
import com.github.lechandonga.cdcsyncjava.model.SchemaChange;
import com.github.lechandonga.cdcsyncjava.model.TableSchema;

/**
 * 基于目录文件的目标端实现。
 *
 * <p>持久化模型（write-ahead 风格）：所有已确认数据先顺序写入 {@code target-wal.log}
 * 并 {@code force(true)} 刷盘，再更新内存位点；checkpoint 只可能 <= WAL 最大位点，
 * 因此崩溃恢复后不会丢失已确认数据，也不会把未确认数据当作已处理
 * （未刷盘的尾部记录在恢复时被截断，重放时按幂等规则跳过）。</p>
 *
 * <p>WAL 行格式（字段 URL 编码，{@code |} 分隔，null 写作 {@code \0}）：
 * <pre>
 * I|keyCol|name:type:nullable(,...)           初始 schema（仅首行一次）
 * S|newVersion|evoType|src|tgt|colType|null   schema 演进
 * D|op|key|commitSeq|schemaVersion|k=v(;...)  数据变更（DELETE 值部分为空）
 * </pre>
 * 每行写入后 flush + fsync，保证崩溃可见性。</p>
 */
public class FileTargetStore implements LocalTargetStore {

    private static final String WAL_NAME = "target-wal.log";
    private static final String NULL_MARK = "\0";

    private final Path walFile;
    private final SchemaRegistry registry = new SchemaRegistry();
    private final Map<String, TargetRow> rows = new LinkedHashMap<>();
    /** 每个键已应用的最大位点：幂等判断必须按键做，不能用全局水位（不同键并行落盘顺序交错）。 */
    private final Map<String, Long> perKeyLastSeq = new LinkedHashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private FileChannel channel;
    private boolean initialized = false;
    private long lastAppliedSeq = 0L;

    public FileTargetStore(Path dataDirectory) {
        try {
            Files.createDirectories(dataDirectory);
            this.walFile = dataDirectory.resolve(WAL_NAME);
            openAndRecover();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void initialize(TableSchema initialSchema) {
        lock.writeLock().lock();
        try {
            if (initialized) {
                return;
            }
            registry.init(initialSchema);
            List<String> valid = List.of();
            writeLine(encodeInitial(initialSchema));
            initialized = true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public int registerSchemaChange(SchemaChange change) {
        lock.writeLock().lock();
        try {
            ensureInit();
            TableSchema next = registry.register(change);
            // 演进校验通过后先持久化再对外生效；register 失败无副作用、不污染已有数据
            writeLine(encodeChange(next.version(), change));
            return next.version();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public ApplyOutcome apply(ChangeEvent event) {
        lock.writeLock().lock();
        try {
            ensureInit();
            long seq = event.position().commitSeq();
            // 幂等按键判断：不同键并行落盘顺序交错，不能用全局水位比较
            Long keySeq = perKeyLastSeq.get(event.key());
            if (keySeq != null && seq <= keySeq) {
                // 至少一次投递：重复批次 / 批次重试 / 重启重放在此幂等拦截
                return ApplyOutcome.DUPLICATE_SKIPPED;
            }
            int eventSchema = event.schemaVersion();
            if (eventSchema > registry.currentVersion()) {
                throw new SyncException(RejectReason.UNKNOWN_SCHEMA_VERSION,
                        "数据事件位点 " + seq + " 的 schema 版本 v" + eventSchema
                                + " 尚未登记（当前 v" + registry.currentVersion()
                                + "）；批次内 DDL 必须先于 DML 生效");
            }
            TableSchema currentSchema = registry.current();
            Map<String, Object> translated = null;
            if (event.op() != OpType.DELETE) {
                RowData after = event.after();
                if (after == null) {
                    throw new SyncException(RejectReason.ILLEGAL_STATE,
                            "非 DELETE 事件缺少 after 行，位点 " + seq);
                }
                translated = registry.translate(after.values(), after.schemaVersion(),
                        currentSchema.version());
                translated.put(currentSchema.keyColumn(), event.key());
            }
            // 先持久化（fsync）再更新内存位点，保证“数据先于位点”
            writeLine(encodeData(event, currentSchema.version(), translated));
            // DELETE 保留墓碑（不 remove），保证位点可追溯、重放可解释
            rows.put(event.key(), new TargetRow(event.key(),
                    translated == null ? Map.of() : translated,
                    currentSchema.version(), seq, event.op() == OpType.DELETE));
            perKeyLastSeq.put(event.key(), seq);
            lastAppliedSeq = Math.max(lastAppliedSeq, seq);
            return ApplyOutcome.APPLIED;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public TargetRow get(String key) {
        lock.readLock().lock();
        try {
            return rows.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Collection<TargetRow> allLiveRows() {
        lock.readLock().lock();
        try {
            List<TargetRow> live = new ArrayList<>();
            for (TargetRow r : rows.values()) {
                if (!r.deleted()) {
                    live.add(r);
                }
            }
            return List.copyOf(live);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Collection<TargetRow> allRows() {
        lock.readLock().lock();
        try {
            return List.copyOf(rows.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public long lastAppliedCommitSeq() {
        lock.readLock().lock();
        try {
            return lastAppliedSeq;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public int currentSchemaVersion() {
        lock.readLock().lock();
        try {
            ensureInit();
            return registry.currentVersion();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public TableSchema schemaAt(int version) {
        lock.readLock().lock();
        try {
            return registry.at(version);
        } finally {
            lock.readLock().unlock();
        }
    }

    private void ensureInit() {
        if (!initialized) {
            throw new SyncException(RejectReason.ILLEGAL_STATE, "目标端尚未 initialize");
        }
    }

    // ---------------- 持久化与崩溃恢复 ----------------

    private void openAndRecover() throws IOException {
        boolean exists = Files.exists(walFile) && Files.size(walFile) > 0;
        channel = FileChannel.open(walFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.READ);
        if (!exists) {
            return;
        }
        ByteBuffer all = ByteBuffer.wrap(Files.readAllBytes(walFile));
        String content = StandardCharsets.UTF_8.decode(all).toString();
        String[] rawLines = content.split("\n", -1);
        List<String> validLines = new ArrayList<>();
        for (String raw : rawLines) {
            if (raw.isEmpty()) {
                continue;
            }
            try {
                replay(raw);
            } catch (RuntimeException e) {
                // 崩溃可能留下半行 / 未 fsync 尾部：在第一条坏记录处截断
                break;
            }
            validLines.add(raw);
        }
        if (validLines.size() < countNonEmpty(rawLines)) {
            channel.position(0);
            StringBuilder sb = new StringBuilder();
            for (String l : validLines) {
                sb.append(l).append('\n');
            }
            byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
            channel.write(ByteBuffer.wrap(bytes));
            channel.truncate(bytes.length);
            channel.force(true);
        }
        channel.position(channel.size());
        if (initialized) {
            // 已在 replay 中注册
        }
    }

    private static int countNonEmpty(String[] lines) {
        int n = 0;
        for (String l : lines) {
            if (!l.isEmpty()) {
                n++;
            }
        }
        return n;
    }

    private void replay(String raw) {
        String[] p = split(raw);
        switch (p[0]) {
            case "I" -> {
                if (initialized) {
                    throw new IllegalStateException("重复的初始 schema 行");
                }
                registry.init(decodeInitial(p));
                initialized = true;
            }
            case "S" -> {
                if (!initialized) {
                    throw new IllegalStateException("初始 schema 缺失");
                }
                registry.register(decodeChange(p));
            }
            case "D" -> {
                if (!initialized) {
                    throw new IllegalStateException("初始 schema 缺失");
                }
                replayData(p);
            }
            default -> throw new IllegalStateException("未知 WAL 记录类型: " + p[0]);
        }
    }

    private void replayData(String[] p) {
        OpType op = OpType.valueOf(p[1]);
        String key = dec(p[2]);
        long seq = Long.parseLong(p[3]);
        int schemaVersion = Integer.parseInt(p[4]);
        // WAL 中不同键的记录因并行落盘可能在文件层交错；全局不要求单调。
        // 同一键必须单调（同键串行保证），若出现同键回退，说明尾部不完整，触发截断。
        Long keySeq = perKeyLastSeq.get(key);
        if (keySeq != null && seq <= keySeq) {
            throw new IllegalStateException("同键位点回退，WAL 尾部不完整: key=" + key
                    + " seq=" + seq + " <= " + keySeq);
        }
        TableSchema schema = registry.at(schemaVersion);
        Map<String, Object> values = parseValues(p.length > 5 ? p[5] : "", schema);
        rows.put(key, new TargetRow(key, values, schemaVersion, seq, op == OpType.DELETE));
        perKeyLastSeq.put(key, seq);
        lastAppliedSeq = Math.max(lastAppliedSeq, seq);
    }

    private void writeLine(String line) {
        try {
            byte[] bytes = (line + "\n").getBytes(StandardCharsets.UTF_8);
            channel.write(ByteBuffer.wrap(bytes));
            channel.force(true);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // ---------------- 编解码 ----------------

    private String encodeInitial(TableSchema s) {
        StringBuilder sb = new StringBuilder("I|").append(enc(s.keyColumn())).append('|');
        boolean first = true;
        for (ColumnDef c : s.columns()) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            sb.append(enc(c.name())).append(':').append(c.type().name()).append(':')
                    .append(c.nullable());
        }
        return sb.toString();
    }

    private TableSchema decodeInitial(String[] p) {
        String key = dec(p[1]);
        List<ColumnDef> cols = new ArrayList<>();
        if (p.length > 2 && !p[2].isEmpty()) {
            for (String item : p[2].split(",")) {
                String[] t = item.split(":");
                cols.add(new ColumnDef(dec(t[0]), ColumnType.valueOf(t[1]),
                        Boolean.parseBoolean(t[2])));
            }
        }
        return new TableSchema(1, key, cols);
    }

    private String encodeChange(int newVersion, SchemaChange c) {
        return "S|" + newVersion + '|' + c.type() + '|'
                + enc(orNull(c.sourceColumn())) + '|'
                + enc(orNull(c.targetColumn())) + '|'
                + (c.newColumnType() == null ? NULL_MARK : c.newColumnType().name()) + '|'
                + c.newColumnNullable();
    }

    private SchemaChange decodeChange(String[] p) {
        int version = Integer.parseInt(p[1]);
        EvolutionType type = EvolutionType.valueOf(p[2]);
        String src = NULL_MARK.equals(p[3]) ? null : dec(p[3]);
        String tgt = NULL_MARK.equals(p[4]) ? null : dec(p[4]);
        ColumnType colType = NULL_MARK.equals(p[5]) ? null : ColumnType.valueOf(p[5]);
        boolean nullable = Boolean.parseBoolean(p[6]);
        if (registry.currentVersion() + 1 != version) {
            throw new IllegalStateException("schema 版本不连续，期望 v"
                    + (registry.currentVersion() + 1) + " 实际 v" + version);
        }
        return new SchemaChange(type, src, tgt, colType, nullable);
    }

    private String encodeData(ChangeEvent event, int storedSchemaVersion,
                              Map<String, Object> values) {
        StringBuilder sb = new StringBuilder("D|").append(event.op()).append('|')
                .append(enc(event.key())).append('|')
                .append(event.position().commitSeq()).append('|')
                .append(storedSchemaVersion).append('|');
        if (values != null) {
            boolean first = true;
            for (Map.Entry<String, Object> e : values.entrySet()) {
                if (!first) {
                    sb.append(';');
                }
                first = false;
                sb.append(enc(e.getKey())).append('=')
                        .append(e.getValue() == null ? NULL_MARK : enc(String.valueOf(e.getValue())));
            }
        }
        return sb.toString();
    }

    private Map<String, Object> parseValues(String s, TableSchema schema) {
        Map<String, Object> map = new LinkedHashMap<>();
        if (s.isEmpty()) {
            return map;
        }
        for (String pair : s.split(";")) {
            int eq = pair.indexOf('=');
            String k = dec(pair.substring(0, eq));
            String raw = pair.substring(eq + 1);
            Object v;
            if (NULL_MARK.equals(raw)) {
                v = null;
            } else {
                String strVal = dec(raw);
                ColumnDef cd = schema.findColumn(k);
                v = coerceFromString(cd, strVal);
            }
            map.put(k, v);
        }
        return map;
    }

    private Object coerceFromString(ColumnDef cd, String s) {
        if (cd == null) {
            return s;
        }
        return switch (cd.type()) {
            case STRING -> s;
            case INTEGER -> Integer.valueOf(s);
            case BOOLEAN -> Boolean.valueOf(s);
        };
    }

    private static String orNull(String s) {
        return s == null ? NULL_MARK : s;
    }

    static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8).replace("+", "%20");
    }

    static String dec(String s) {
        return URLDecoder.decode(s, StandardCharsets.UTF_8);
    }

    private static String[] split(String line) {
        List<String> out = new ArrayList<>();
        int start = 0;
        for (int i = 0; i < line.length(); i++) {
            if (line.charAt(i) == '|') {
                out.add(line.substring(start, i));
                start = i + 1;
            }
        }
        out.add(line.substring(start));
        return out.toArray(String[]::new);
    }
}
