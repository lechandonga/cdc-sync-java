package com.github.lechandonga.cdcsyncjava.target;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.cdcsyncjava.error.RejectReason;
import com.github.lechandonga.cdcsyncjava.error.SkippedEvent;
import com.github.lechandonga.cdcsyncjava.error.SkippedLedger;
import com.github.lechandonga.cdcsyncjava.model.ChangeEvent;
import com.github.lechandonga.cdcsyncjava.model.ChangeType;
import com.github.lechandonga.cdcsyncjava.model.SnapshotRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/**
 * 内置本地目标端（崩溃安全的本地文件存储）。
 *
 * <p>写入协议（在同一把锁内）：先 append WAL（flush + fsync）→ 更新内存表；
 * 位点单独以 temp+原子 rename 落盘，因此崩溃恢复时：
 * <ul>
 *   <li>WAL 有、位点未推进 → 重放后跳过位点之后的事件（不会把未确认数据算作已处理）；</li>
 *   <li>位点已推进 → 对应数据必已在 WAL/表中（不会丢失已确认数据）。</li>
 * </ul>
 * 重放与重复投递均由 (key,lastSeq) 幂等表拦截，重复数据只记台账、不重复生效。
 *
 * <p>全量重建使用单独的 staging 暂存区，commit 时在写锁内原子换表并压缩 WAL，
 * 未提交的暂存区在构造时被清理，可安全重试；读取方任何时刻只见旧表或新表。
 */
public final class LocalTarget implements TargetAdapter {

    private static final Logger log = LoggerFactory.getLogger(LocalTarget.class);

    private final Path baseDir;
    private final Path tableFile;
    private final Path walFile;
    private final Path checkpointFile;
    private final Path stagingDir;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Object writeLock = new Object();

    private final Map<String, RowState> table = new LinkedHashMap<>();
    private final List<ChangeEvent> wal = new ArrayList<>();
    private volatile Checkpoint checkpoint;

    private record RowState(long lastSeq, int schemaVersion, boolean deleted,
                            Map<String, Object> data) {
    }

    public LocalTarget(Path baseDir) {
        this.baseDir = baseDir;
        this.tableFile = baseDir.resolve("table.json");
        this.walFile = baseDir.resolve("wal.jsonl");
        this.checkpointFile = baseDir.resolve("checkpoint.json");
        this.stagingDir = baseDir.resolve("rebuild-staging");
        this.checkpoint = Checkpoint.initial();
        try {
            start();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // ==================== 启动恢复 ====================

    private void start() throws IOException {
        Files.createDirectories(baseDir);

        // 1) 清理上次未完成（未 commit）的重建暂存区 —— 读方不受影响
        if (Files.isDirectory(stagingDir)) {
            log.info("[target] 检测到未完成的重建暂存区，废弃以便安全重试: {}", stagingDir);
            deleteRecursively(stagingDir);
        }

        // 2) 载入活动表快照
        if (Files.exists(tableFile)) {
            TableSnapshot snap = mapper.readValue(Files.readAllBytes(tableFile), TableSnapshot.class);
            if (snap.rows != null) {
                for (PersistedRow r : snap.rows) {
                    table.put(r.key(), r.state());
                }
            }
        }

        // 3) 重放 WAL（幂等：按 key/seq 去重）
        if (Files.exists(walFile)) {
            try (BufferedReader br = Files.newBufferedReader(walFile, StandardCharsets.UTF_8)) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.isBlank()) {
                        continue;
                    }
                    ChangeEvent e = decodeEvent(line);
                    applyToMemory(e, null, null);
                    wal.add(e);
                }
            }
        }

        // 4) 载入位点（数据先于位点：超过已确认位点的事件只重放、不算已处理）
        if (Files.exists(checkpointFile)) {
            checkpoint = mapper.readValue(Files.readAllBytes(checkpointFile), Checkpoint.class);
        }
        long confirmed = checkpoint.position();
        int dropped = 0;
        wal.removeIf(e -> e.seq() > confirmed);
        // 内存表中超过确认位点的状态必须回退：从已确认事件重建表
        if (confirmed >= 0) {
            // 重新以“已确认”为界构建内存状态
            Map<String, RowState> rebuilt = new LinkedHashMap<>();
            // table.json 本身即一次已确认提交点（commit 时写），保留其中 <= confirmed 的行
            for (Map.Entry<String, RowState> e : table.entrySet()) {
                if (e.getValue().lastSeq() <= confirmed) {
                    rebuilt.put(e.getKey(), e.getValue());
                }
            }
            for (ChangeEvent e : wal) {
                if (e.seq() <= confirmed) {
                    replayInto(rebuilt, e);
                }
            }
            dropped = table.size() - rebuilt.size();
            table.clear();
            table.putAll(rebuilt);
        }
        log.info("[target] 恢复完成: 已确认位点={}, 快照完成={}, 行数={}, WAL={}{}",
                checkpoint.position(), checkpoint.snapshotCompleted(),
                table.size(), wal.size(),
                dropped > 0 ? ", 回滚未确认行数=" + dropped : "");
    }

    private void replayInto(Map<String, RowState> target, ChangeEvent e) {
        if (e.type() == ChangeType.DELETE) {
            RowState cur = target.get(e.key());
            if (cur == null || e.seq() > cur.lastSeq()) {
                target.put(e.key(), new RowState(e.seq(), e.schemaVersion(), true, Map.of()));
            }
        } else {
            RowState cur = target.get(e.key());
            if (cur == null || e.seq() > cur.lastSeq()) {
                target.put(e.key(), new RowState(e.seq(), e.schemaVersion(), false,
                        new LinkedHashMap<>(e.data())));
            }
        }
    }

    // ==================== 写入 ====================

    @Override
    public void applyDurable(List<ChangeEvent> events, String batchId, SkippedLedger ledger) {
        synchronized (writeLock) {
            for (ChangeEvent e : events) {
                boolean applied = applyToMemory(e, batchId, ledger);
                if (applied) {
                    appendWal(e);
                }
            }
        }
    }

    /** @return true 表示实际生效（需要落 WAL）；false 表示幂等/过期跳过 */
    private boolean applyToMemory(ChangeEvent e, String batchId, SkippedLedger ledger) {
        RowState cur = table.get(e.key());
        if (cur != null && e.seq() < cur.lastSeq()) {
            logSkip(ledger, e, batchId, RejectReason.OBSOLETE_ORDER,
                    "当前已应用 seq=" + cur.lastSeq() + "，事件 seq=" + e.seq());
            return false;
        }
        if (cur != null && e.seq() == cur.lastSeq()) {
            logSkip(ledger, e, batchId, RejectReason.ALREADY_APPLIED,
                    "seq=" + e.seq() + " 已对该键生效，重复投递被幂等拦截");
            return false;
        }
        replayInto(table, e);
        return true;
    }

    private void logSkip(SkippedLedger ledger, ChangeEvent e, String batchId,
                         RejectReason reason, String detail) {
        log.warn("[target] 跳过事件 batch={} event={} key={} seq={} reason={} detail={}",
                batchId, e.eventId(), e.key(), e.seq(), reason, detail);
        if (ledger != null) {
            ledger.record(SkippedEvent.of(e.eventId(), e.key(), e.seq(), batchId, reason, detail));
        }
    }

    private void appendWal(ChangeEvent e) {
        try {
            String line = mapper.writeValueAsString(eventMap(e)) + System.lineSeparator();
            Files.writeString(walFile, line, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            fsync(walFile);
            wal.add(e);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    // ==================== 位点 ====================

    @Override
    public void commitCheckpoint(long position) {
        synchronized (writeLock) {
            try {
                Checkpoint next = new Checkpoint(position, checkpoint.snapshotCompleted(),
                        checkpoint.rebuilds());
                persistAtomic(checkpointFile, mapper.writeValueAsBytes(next));
                checkpoint = next;
                log.info("[target] 位点已提交 position={} (snapshotCompleted={})",
                        position, next.snapshotCompleted());
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }
    }

    @Override
    public void completeSnapshot(long position) {
        synchronized (writeLock) {
            try {
                Checkpoint next = new Checkpoint(position, true, checkpoint.rebuilds());
                persistAtomic(checkpointFile, mapper.writeValueAsBytes(next));
                checkpoint = next;
                log.info("[target] 位点已提交 position={} (snapshotCompleted=true)", position);
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }
    }

    @Override
    public Checkpoint checkpoint() {
        return checkpoint;
    }

    // ==================== 读 ====================

    @Override
    public Optional<Map<String, Object>> get(String key) {
        synchronized (writeLock) {
            RowState r = table.get(key);
            if (r == null || r.deleted()) {
                return Optional.empty();
            }
            return Optional.of(new LinkedHashMap<>(r.data()));
        }
    }

    @Override
    public Map<String, Map<String, Object>> activeRows() {
        synchronized (writeLock) {
            Map<String, Map<String, Object>> out = new TreeMap<>();
            for (Map.Entry<String, RowState> e : table.entrySet()) {
                if (!e.getValue().deleted()) {
                    out.put(e.getKey(), new LinkedHashMap<>(e.getValue().data()));
                }
            }
            return out;
        }
    }

    @Override
    public Path baseDir() {
        return baseDir;
    }

    // ==================== 全量重建（原子换表） ====================

    @Override
    public RebuildSession beginRebuild(SkippedLedger ledger, String rebuildId) {
        synchronized (writeLock) {
            if (Files.isDirectory(stagingDir)) {
                try {
                    deleteRecursively(stagingDir);
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            }
            try {
                Files.createDirectories(stagingDir);
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
            log.info("[rebuild={}] 重建开始（影子暂存区），读方仍见活动表", rebuildId);
            return new StagingSession(ledger, rebuildId);
        }
    }

    /** 暂存重建会话：全部写入隔离在 staging 目录与独立内存表中。 */
    private final class StagingSession implements RebuildSession {

        private final SkippedLedger ledger;
        private final String rebuildId;
        private final Path stageTable;
        private final Path stageWal;
        private final Map<String, RowState> stageState = new LinkedHashMap<>();
        private volatile boolean finished;

        StagingSession(SkippedLedger ledger, String rebuildId) {
            this.ledger = ledger;
            this.rebuildId = rebuildId;
            this.stageTable = stagingDir.resolve("table.json");
            this.stageWal = stagingDir.resolve("wal.jsonl");
        }

        @Override
        public void addSnapshotRow(SnapshotRow row) {
            synchronized (LocalTarget.this.writeLock) {
                ensureActive();
                stageState.put(row.key(), new RowState(row.lastSeq(), row.schemaVersion(), false,
                        new LinkedHashMap<>(row.data())));
            }
        }

        @Override
        public void applyChange(ChangeEvent projectedEvent) {
            synchronized (LocalTarget.this.writeLock) {
                ensureActive();
                RowState cur = stageState.get(projectedEvent.key());
                if (cur != null && projectedEvent.seq() <= cur.lastSeq()) {
                    logSkip(ledger, projectedEvent, rebuildId,
                            projectedEvent.seq() == cur.lastSeq()
                                    ? RejectReason.ALREADY_APPLIED : RejectReason.OBSOLETE_ORDER,
                            "重建影子表中该键已应用 seq=" + cur.lastSeq());
                    return;
                }
                replayInto(stageState, projectedEvent);
            }
        }

        @Override
        public void commit(long newPosition) {
            synchronized (LocalTarget.this.writeLock) {
                ensureActive();
                try {
                    // 1) 影子表落盘（此时读方完全不受影响）
                    List<PersistedRow> rows = new ArrayList<>();
                    for (Map.Entry<String, RowState> e : stageState.entrySet()) {
                        rows.add(new PersistedRow(e.getKey(), e.getValue()));
                    }
                    persistAtomic(stageTable, mapper.writeValueAsBytes(new TableSnapshot(rows)));

                    // 2) 原子换表 + WAL 压缩为该位点之后的事件
                    Path oldTable = baseDir.resolve("table.old.json");
                    Files.deleteIfExists(oldTable);
                    if (Files.exists(tableFile)) {
                        Files.move(tableFile, oldTable, StandardCopyOption.ATOMIC_MOVE);
                        Files.move(stageTable, tableFile, StandardCopyOption.REPLACE_EXISTING,
                                StandardCopyOption.ATOMIC_MOVE);
                    } else {
                        // 全新目标端此前只有 WAL、尚无表快照，直接把影子表提升为活动表
                        Files.move(stageTable, tableFile, StandardCopyOption.REPLACE_EXISTING,
                                StandardCopyOption.ATOMIC_MOVE);
                    }

                    List<ChangeEvent> remaining = wal.stream()
                            .filter(e -> e.seq() > newPosition).toList();
                    Files.writeString(walFile, "", StandardCharsets.UTF_8,
                            StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
                    for (ChangeEvent e : remaining) {
                        Files.writeString(walFile,
                                mapper.writeValueAsString(eventMap(e)) + System.lineSeparator(),
                                StandardCharsets.UTF_8, StandardOpenOption.APPEND);
                    }
                    fsync(walFile);

                    table.clear();
                    table.putAll(stageState);
                    wal.clear();
                    wal.addAll(remaining);

                    // 3) 位点原子提交（数据已就位）
                    Checkpoint next = new Checkpoint(newPosition, true, checkpoint.rebuilds() + 1);
                    persistAtomic(checkpointFile, mapper.writeValueAsBytes(next));
                    checkpoint = next;

                    finished = true;
                    Files.deleteIfExists(oldTable);
                    deleteRecursively(stagingDir);
                    log.info("[rebuild={}] 重建提交完成，原子切换活动表 position={} 行数={}",
                            rebuildId, newPosition, table.size());
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            }
        }

        @Override
        public void abort() {
            synchronized (LocalTarget.this.writeLock) {
                if (finished) {
                    return;
                }
                try {
                    deleteRecursively(stagingDir);
                    finished = true;
                    log.warn("[rebuild={}] 重建被中断，影子表已废弃，活动表未受影响", rebuildId);
                    if (ledger != null) {
                        ledger.record(SkippedEvent.of(rebuildId, "<snapshot>", -1L, rebuildId,
                                RejectReason.REBUILD_ABORTED, "全量重建被中断，可安全重试"));
                    }
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            }
        }

        @Override
        public boolean active() {
            return !finished;
        }

        private void ensureActive() {
            if (finished) {
                throw new IllegalStateException("重建会话 " + rebuildId + " 已结束");
            }
        }
    }

    @Override
    public void close() {
        // 全部写入均已 fsync，无需额外操作
    }

    // ==================== 序列化辅助 ====================

    private record PersistedRow(String key, RowState state) {
    }

    private record TableSnapshot(List<PersistedRow> rows) {
    }

    private Map<String, Object> eventMap(ChangeEvent e) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("eventId", e.eventId());
        m.put("key", e.key());
        m.put("seq", e.seq());
        m.put("schemaVersion", e.schemaVersion());
        m.put("type", e.type().name());
        m.put("data", e.data());
        return m;
    }

    private ChangeEvent decodeEvent(String line) throws IOException {
        Map<String, Object> m = mapper.readValue(line, new TypeReference<Map<String, Object>>() {
        });
        @SuppressWarnings("unchecked")
        Map<String, Object> rawData = (Map<String, Object>) m.getOrDefault("data", Map.of());
        // Jackson 会把 JSON 整数反序列化为 Integer，归一为 Long 以保持与写入时一致的值类型
        Map<String, Object> data = new LinkedHashMap<>();
        if (rawData != null) {
            for (Map.Entry<String, Object> e : rawData.entrySet()) {
                Object v = e.getValue();
                if (v instanceof Integer || v instanceof Short || v instanceof Byte) {
                    v = ((Number) v).longValue();
                }
                data.put(e.getKey(), v);
            }
        }
        long seq = ((Number) Objects.requireNonNull(m.get("seq"))).longValue();
        int sv = ((Number) Objects.requireNonNull(m.get("schemaVersion"))).intValue();
        return new ChangeEvent((String) m.get("eventId"), (String) m.get("key"), seq, sv,
                ChangeType.valueOf((String) m.get("type")), data);
    }

    private static void persistAtomic(Path file, byte[] content) throws IOException {
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        Files.write(tmp, content, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
            ch.force(true);
        }
        Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private static void fsync(Path file) {
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException ignore) {
            // 文件可能尚不存在时忽略
        }
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (var paths = Files.walk(root)) {
            paths.sorted(java.util.Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException ex) {
                            throw new UncheckedIOException(ex);
                        }
                    });
        }
    }
}
