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
 * <p>WAL 行格式（字段 URL 编码，{@code |} 分隔，null 写作 {@code %00}）：
 * <pre>
 * I|keyCol|name:type:nullable(,...)           初始 schema（仅首行一次）
 * S|newVersion|evoType|src|tgt|colType|null   schema 演进
 * D|op|key|commitSeq|schemaVersion|k=v(;...)  增量数据（按 commitSeq 严格幂等）
 * P|op|key|commitSeq|schemaVersion|k=v(;...)  快照镜像行（按键去重，业务 seq 可不按文件序）
 * </pre>
 * 每行写入后 flush + fsync，保证崩溃可见性。</p>
 */
public class FileTargetStore implements LocalTargetStore {

    private static final String WAL_NAME = "target-wal.log";
    /**
     * null 的文本标记。必须是“只可能由引擎自己写出、不可能来自用户值”的串：
     * 所有用户值都经过 URL 编码（{@code %} -> {@code %25}），因此原始 {@code %00}
     * 不可能出现在用户数据中。早期版本使用原始 NUL 字节，会在崩溃恢复按行截断重写时
     * 被 String 行分割吞掉后续内容，故改为纯文本标记。
     */
    private static final String NULL_MARK = "%00";

    private final Path walFile;
    private SchemaRegistry registry = new SchemaRegistry();
    private final Map<String, TargetRow> rows = new LinkedHashMap<>();
    /** 每个键已应用的最大位点：幂等判断必须按键做，不能用全局水位（不同键并行落盘顺序交错）。 */
    private final Map<String, Long> perKeyLastSeq = new LinkedHashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private FileChannel channel;
    private boolean initialized = false;
    private long lastAppliedSeq = 0L;

    public FileTargetStore(Path dataDirectory) {
        this(dataDirectory, false);
    }

    /**
     * @param strict 兼容参数：初始记录损坏在任意模式下都以 TARGET_STATE_CORRUPT 拒绝；
     *               该参数仅为显式表达调用方意图而保留
     */
    public FileTargetStore(Path dataDirectory, boolean strict) {
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
    public boolean initialized() {
        lock.readLock().lock();
        try {
            return initialized;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public LocalTargetStore reopenStrict() {
        // 原地严格重开：在同一实例上重置内存状态并重新恢复，避免引擎持有的引用漂移；
        // 初始记录损坏时 openAndRecover 直接抛 TARGET_STATE_CORRUPT，不静默截断
        lock.writeLock().lock();
        try {
            try {
                if (channel != null) {
                    channel.force(true);
                    channel.close();
                }
            } catch (IOException ignored) {
                // 关闭旧句柄失败也继续用新句柄恢复
            }
            registry = new SchemaRegistry();
            rows.clear();
            perKeyLastSeq.clear();
            initialized = false;
            lastAppliedSeq = 0L;
            try {
                channel = FileChannel.open(walFile, StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE, StandardOpenOption.READ);
                recoverInPlace();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return this;
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
    public int registerSchemaChangeAt(SchemaChange change, int schemaVersionAfter) {
        lock.writeLock().lock();
        try {
            ensureInit();
            if (schemaVersionAfter <= registry.currentVersion()) {
                // 重放已登记版本：只做幂等校验，不再重复追加 WAL（该版本行此前已 fsync）
                return registry.registerAt(change, schemaVersionAfter);
            }
            int version = registry.registerAt(change, schemaVersionAfter);
            writeLine(encodeChange(version, change));
            return version;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public ApplyOutcome apply(ChangeEvent event) {
        return writeData(event, false);
    }

    @Override
    public ApplyOutcome applySnapshotRow(ChangeEvent event, long snapshotHighWatermark) {
        // snapshotHighWatermark 体现“快照只确认到高水位”的语义；全局确认位点由引擎
        // checkpoint 负责，target 内部 lastAppliedSeq 只反映 WAL 已含数据的最大业务 seq
        return writeData(event, true);
    }

    private ApplyOutcome writeData(ChangeEvent event, boolean snapshot) {
        lock.writeLock().lock();
        try {
            ensureInit();
            long seq = event.position().commitSeq();
            String key = event.key();
            if (snapshot) {
                // 快照镜像：同一物理键已存活即幂等，不按业务 seq 比较
                TargetRow existing = rows.get(key);
                if (existing != null && !existing.deleted()) {
                    return ApplyOutcome.DUPLICATE_SKIPPED;
                }
            } else {
                // 增量：同一键按 commitSeq 严格幂等（并行落盘交错时也不能用全局水位）
                Long keySeq = perKeyLastSeq.get(key);
                if (keySeq != null && seq <= keySeq) {
                    return ApplyOutcome.DUPLICATE_SKIPPED;
                }
            }
            int eventSchema = event.schemaVersion();
            if (eventSchema > registry.currentVersion()) {
                throw new SyncException(RejectReason.UNKNOWN_SCHEMA_VERSION,
                        (snapshot ? "快照行" : "数据事件") + "位点 " + seq
                                + " 的 schema 版本 v" + eventSchema + " 尚未登记（当前 v"
                                + registry.currentVersion() + "）");
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
                translated.put(currentSchema.keyColumn(), key);
            }
            // 先持久化（fsync）再更新内存，保证“数据先于位点”；快照行带 P 标记
            writeLine(encodeData(event, currentSchema.version(), translated, snapshot));
            rows.put(key, new TargetRow(key,
                    translated == null ? Map.of() : translated,
                    currentSchema.version(), seq, event.op() == OpType.DELETE));
            perKeyLastSeq.put(key, seq);
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
        recoverInPlace();
    }

    /** 假设 {@link #channel} 已打开，在隔离状态中重放 WAL 并一次性生效（供构造与原地重开复用）。 */
    private void recoverInPlace() throws IOException {
        // 恢复必须在隔离状态中完成：先把全部行重放进临时 registry/maps，
        // 期间遇到坏尾只截断文件、绝不污染对外可见状态；重放全部成功后一次性生效。
        SchemaRegistry recoveryRegistry = new SchemaRegistry();
        Map<String, TargetRow> recoveryRows = new LinkedHashMap<>();
        Map<String, Long> recoveryKeySeq = new LinkedHashMap<>();
        long[] recoveryLastSeq = {0L};
        boolean[] recoveryInit = {false};

        ByteBuffer all;
        try {
            all = ByteBuffer.wrap(Files.readAllBytes(walFile));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        String content = StandardCharsets.UTF_8.decode(all).toString();
        String[] rawLines = content.split("\n", -1);
        List<String> validLines = new ArrayList<>();
        for (String raw : rawLines) {
            if (raw.isEmpty()) {
                continue;
            }
            try {
                replayInto(raw, recoveryRegistry, recoveryRows, recoveryKeySeq,
                        recoveryLastSeq, recoveryInit);
            } catch (RuntimeException e) {
                if (!recoveryInit[0]) {
                    // 首条记录（初始 schema）损坏：初始记录一旦 fsync 就不会因崩溃残缺，
                    // 损坏只能说明现场不可信。任何模式下都拒绝启动，绝不静默截断重写
                    throw new SyncException(RejectReason.TARGET_STATE_CORRUPT,
                            "目标 WAL 初始记录损坏或无法识别，拒绝启动: " + walFile
                                    + "（首条坏记录: " + raw + "）", e);
                }
                // 已完成初始化后崩溃留下的未 fsync 数据尾部 / 半行：在此处安全截断
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
            // 必须先截断到新长度、再定位到“截断后的真实文件尾”；
            // 顺序反了会把 position 留在旧大小，后续追加写出稀疏空洞，
            // 重开时空洞中的 NUL 被当作空行从而吞掉其后的全部记录
            channel.truncate(bytes.length);
            channel.force(true);
            channel.position(bytes.length);
        } else {
            channel.position(channel.size());
        }
        // 全部重放/截断完成后，隔离状态一次性替换对外状态
        registry.adoptRecoveredVersions(recoveryRegistry.versions());
        this.rows.putAll(recoveryRows);
        this.perKeyLastSeq.putAll(recoveryKeySeq);
        this.lastAppliedSeq = recoveryLastSeq[0];
        this.initialized = recoveryInit[0];
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

    private void replayInto(String raw, SchemaRegistry reg, Map<String, TargetRow> rowMap,
                            Map<String, Long> keySeqMap, long[] lastSeqHolder,
                            boolean[] initHolder) {
        String[] p = split(raw);
        switch (p[0]) {
            case "I" -> {
                if (initHolder[0]) {
                    throw new IllegalStateException("重复的初始 schema 行");
                }
                reg.init(decodeInitial(p));
                initHolder[0] = true;
            }
            case "S" -> {
                if (!initHolder[0]) {
                    throw new IllegalStateException("初始 schema 缺失");
                }
                reg.register(decodeChangeWith(reg, p));
            }
            case "D", "P" -> {
                if (!initHolder[0]) {
                    throw new IllegalStateException("初始 schema 缺失");
                }
                replayDataInto(p, p[0].equals("P"), reg, rowMap, keySeqMap, lastSeqHolder);
            }
            default -> throw new IllegalStateException("未知 WAL 记录类型: " + p[0]);
        }
    }

    private void replayDataInto(String[] p, boolean snapshot, SchemaRegistry reg,
                                Map<String, TargetRow> rowMap, Map<String, Long> keySeqMap,
                                long[] lastSeqHolder) {
        OpType op = OpType.valueOf(p[1]);
        String key = dec(p[2]);
        long seq = Long.parseLong(p[3]);
        int schemaVersion = Integer.parseInt(p[4]);
        TableSchema schema = reg.at(schemaVersion);
        Map<String, Object> values = parseValues(p.length > 5 ? p[5] : "", schema);
        boolean deleted = op == OpType.DELETE;
        Long keySeq = keySeqMap.get(key);
        if (snapshot) {
            // 快照镜像行：同一物理键重复出现一律以后到的完整镜像为准（快照续跑重复落盘），
            // 不做 seq 单调/回退校验——快照行携带的业务 seq 不保证与文件顺序一致
            rowMap.put(key, new TargetRow(key, values, schemaVersion, seq, deleted));
            keySeqMap.put(key, keySeq == null ? seq : Math.max(keySeq, seq));
            lastSeqHolder[0] = Math.max(lastSeqHolder[0], seq);
            return;
        }
        // 增量行：同键必须严格按 commitSeq 单调
        if (keySeq != null) {
            if (seq < keySeq && !deleted) {
                throw new IllegalStateException("同键位点回退，WAL 尾部不完整: key=" + key
                        + " seq=" + seq + " < " + keySeq);
            }
            if (seq == keySeq) {
                return; // 重复批次/重启重放：幂等
            }
        }
        rowMap.put(key, new TargetRow(key, values, schemaVersion, seq, deleted));
        keySeqMap.put(key, seq);
        lastSeqHolder[0] = Math.max(lastSeqHolder[0], seq);
    }

    private void writeLine(String line) {
        try {
            byte[] bytes = (line + "\n").getBytes(StandardCharsets.UTF_8);
            // 追加位置永远以“文件实际字节长度”为准，不能依据逻辑位点：
            // 快照行携带的是该行自己的 commitSeq（可能远大于文件当前行数），
            // 用逻辑位点定位 channel 会在文件中部写出空洞/覆写已有记录
            long physicalEnd = channel.size();
            if (channel.position() != physicalEnd) {
                channel.position(physicalEnd);
            }
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

    private SchemaChange decodeChangeWith(SchemaRegistry reg, String[] p) {
        int version = Integer.parseInt(p[1]);
        EvolutionType type = EvolutionType.valueOf(p[2]);
        String src = NULL_MARK.equals(p[3]) ? null : dec(p[3]);
        String tgt = NULL_MARK.equals(p[4]) ? null : dec(p[4]);
        ColumnType colType = NULL_MARK.equals(p[5]) ? null : ColumnType.valueOf(p[5]);
        boolean nullable = Boolean.parseBoolean(p[6]);
        if (reg.currentVersion() + 1 != version) {
            throw new IllegalStateException("schema 版本不连续，期望 v"
                    + (reg.currentVersion() + 1) + " 实际 v" + version);
        }
        return new SchemaChange(type, src, tgt, colType, nullable);
    }

    private String encodeData(ChangeEvent event, int storedSchemaVersion,
                              Map<String, Object> values, boolean snapshot) {
        // P = 快照镜像行（恢复按键去重，不做 seq 单调校验）；D = 增量事件（严格按 seq 幂等）
        StringBuilder sb = new StringBuilder(snapshot ? "P|" : "D|").append(event.op()).append('|')
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
