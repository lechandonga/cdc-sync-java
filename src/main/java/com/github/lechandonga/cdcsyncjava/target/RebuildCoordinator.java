package com.github.lechandonga.cdcsyncjava.target;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Collection;

import com.github.lechandonga.cdcsyncjava.engine.SyncException;
import com.github.lechandonga.cdcsyncjava.model.ChangeEvent;
import com.github.lechandonga.cdcsyncjava.model.RejectReason;
import com.github.lechandonga.cdcsyncjava.model.SchemaChange;
import com.github.lechandonga.cdcsyncjava.source.LocalSourceDatabase;
import com.github.lechandonga.cdcsyncjava.source.Snapshot;

/**
 * 全量重建协调器。
 *
 * <p>目录布局：{@code current/} 为读取方始终可见的在线库；{@code shadow/} 为重建影子库；
 * {@code shadow-manifest.properties} 记录重建快照的 highWatermark，用于中断后续传。</p>
 *
 * <ul>
 *   <li>重建期间所有读取仍落在 current，绝不接触半成品 shadow；</li>
 *   <li>影子库通过快照 + 增量追赶构建，应用全部按 commitSeq 幂等，可安全中断重试；</li>
 *   <li>{@link #commitSwap()} 先 fsync 影子数据，再用原子 rename 交换目录，读取方瞬间看到新库。</li>
 * </ul>
 */
public class RebuildCoordinator {

    private static final String CURRENT_DIR = "current";
    private static final String SHADOW_DIR = "shadow";
    private static final String STAGING_DIR = "shadow.staging";
    private static final String MANIFEST = "shadow-manifest.properties";

    private final Path baseDirectory;
    private final Path currentDir;
    private final Path shadowDir;
    private final Path manifest;
    private FileTargetStore current;
    private FileTargetStore shadow;
    private long snapshotHighWatermark = -1L;
    /** 影子增量追赶已确认位点；中断前持久化到 manifest，重启严格从其后续传。 */
    private long caughtUpTo = -1L;

    public RebuildCoordinator(Path baseDirectory) {
        try {
            Files.createDirectories(baseDirectory);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.baseDirectory = baseDirectory;
        this.currentDir = baseDirectory.resolve(CURRENT_DIR);
        this.shadowDir = baseDirectory.resolve(SHADOW_DIR);
        this.manifest = baseDirectory.resolve(MANIFEST);
        if (Files.exists(currentDir)) {
            this.current = new FileTargetStore(currentDir);
        }
        // 构造时自动识别“进程中断后遗留的未完成影子库”，isRebuilding() 立即可见
        if (Files.exists(shadowDir) && Files.exists(manifest)) {
            readManifest();
            this.shadow = new FileTargetStore(shadowDir);
        }
    }

    /** 读取方当前可见的存储，始终是完整状态；重建未初始化时抛异常。 */
    public synchronized LocalTargetStore current() {
        if (current == null) {
            throw new SyncException(RejectReason.REBUILD_NOT_ACTIVE,
                    "尚无已提交的目标库，请先完成一次重建");
        }
        return current;
    }

    /**
     * 以快照开始一次重建；若存在未完成的影子库与同一快照 manifest，则直接续传，
     * 忽略传入快照并复用历史 highWatermark 与追赶位点。
     */
    public synchronized void startOrResume(Snapshot snapshot) {
        startOrResume(snapshot, null);
    }

    /**
     * 以快照开始一次重建，并由源端补齐快照覆盖的全部历史结构演进。
     *
     * <p>快照中的行可能携带旧 schemaVersion（快照前发生过 DDL），影子库必须先按序登记
     * v1..快照版本 的全部 schema，才能翻译这些旧版本行；否则重建期间带结构变化时
     * 第一条旧版本行就会因 UNKNOWN_SCHEMA_VERSION 失败。</p>
     */
    public synchronized void startOrResume(Snapshot snapshot, LocalSourceDatabase source) {
        if (shadow != null) {
            // 构造函数已从磁盘恢复未完成影子库：幂等的 resume，不重复加载
            return;
        }
        // 全新重建：清理可能残留的半成品
        deleteRecursively(shadowDir);
        deleteRecursively(baseDirectory.resolve(STAGING_DIR));
        this.shadow = new FileTargetStore(shadowDir);
        int snapshotVersion = snapshot.schema().version();
        shadow.initialize(source != null ? source.schemaAt(1) : snapshot.schema());
        // 快照覆盖的历史结构演进必须与主流水线一致地按序登记（幂等）
        if (source != null) {
            for (int v = 2; v <= snapshotVersion; v++) {
                shadow.registerSchemaChangeAt(source.schemaChangeTo(v), v);
            }
        }
        long loadHw = snapshot.highWatermark().commitSeq();
        for (var row : snapshot.rows()) {
            String keyColumn = shadow.schemaAt(row.schemaVersion()).keyColumn();
            ChangeEvent insert = new ChangeEvent(
                    com.github.lechandonga.cdcsyncjava.model.OpType.INSERT,
                    String.valueOf(row.values().get(keyColumn)),
                    row,
                    new com.github.lechandonga.cdcsyncjava.model.SourcePosition(row.commitSeq()),
                    row.schemaVersion());
            shadow.applySnapshotRow(insert, loadHw);
        }
        this.snapshotHighWatermark = snapshot.highWatermark().commitSeq();
        this.caughtUpTo = this.snapshotHighWatermark;
        writeManifest(this.snapshotHighWatermark, this.caughtUpTo);
    }

    /** 向影子库应用一条增量（DDL 之外的 DML）；幂等。 */
    public synchronized ApplyOutcome applyShadow(ChangeEvent event) {
        requireShadow();
        return shadow.apply(event);
    }

    /**
     * 向影子库登记一次结构演进（带源端声明版本，支持断点重放幂等）。
     * @return 登记 / 已存在的版本号
     */
    public synchronized int registerShadowSchemaChange(SchemaChange change,
                                                       int schemaVersionAfter) {
        requireShadow();
        return shadow.registerSchemaChangeAt(change, schemaVersionAfter);
    }

    /** 影子库当前已确认位点。 */
    public synchronized long shadowLastAppliedCommitSeq() {
        requireShadow();
        return shadow.lastAppliedCommitSeq();
    }

    /** 重建是否进行中。 */
    public synchronized boolean isRebuilding() {
        return shadow != null;
    }

    /** 快照 highWatermark（增量追赶起点）。 */
    public synchronized long snapshotHighWatermark() {
        return snapshotHighWatermark;
    }

    /** 影子增量追赶已确认位点（持久化；重启严格从其后续传）。 */
    public synchronized long caughtUpTo() {
        return caughtUpTo;
    }

    /**
     * 持久化一次追赶进度：调用方保证所有 commitSeq ≤ position 的影子事件都已 fsync。
     * 中断后重启的重建严格从该位点之后继续，绝不重复推进结构版本。
     */
    public synchronized void saveCatchUpProgress(long position) {
        requireShadow();
        if (position <= caughtUpTo) {
            return;
        }
        this.caughtUpTo = position;
        writeManifest(snapshotHighWatermark, caughtUpTo);
    }

    /**
     * 完成重建：影子数据全部 fsync 后原子切换目录。
     * 切换步骤：current -> staging(备份)，shadow -> current，staging 删除。
     * 全程读取方只会看到旧 current 或新 current，不会看到半成品。
     */
    public synchronized void commitSwap() {
        requireShadow();
        try {
            Path staging = baseDirectory.resolve(STAGING_DIR);
            deleteRecursively(staging);
            Path oldCurrent = currentDir;
            if (Files.exists(oldCurrent)) {
                Files.move(oldCurrent, staging, StandardCopyOption.ATOMIC_MOVE);
            }
            try {
                Files.move(shadowDir, oldCurrent, StandardCopyOption.ATOMIC_MOVE);
                fsyncDirectory(baseDirectory);
            } catch (IOException swapFailure) {
                // 回滚：恢复旧 current，保证读取方永远有完整库
                if (Files.exists(staging) && !Files.exists(oldCurrent)) {
                    Files.move(staging, oldCurrent, StandardCopyOption.ATOMIC_MOVE);
                }
                throw swapFailure;
            }
            deleteRecursively(staging);
            Files.deleteIfExists(manifest);
            // shadow 句柄指向的目录已 rename 为 current：在该路径上打开新句柄交给在线引擎，
            // 切换一次生效，新句柄读到的就是影子库刚 fsync 的完整内容
            this.current = new FileTargetStore(currentDir);
            this.shadow = null;
            this.snapshotHighWatermark = -1L;
            this.caughtUpTo = -1L;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 切换完成后新库的存储句柄（供在线引擎热替换）；重建未提交时返回 null。 */
    public synchronized FileTargetStore committedStore() {
        return shadow == null ? current : null;
    }

    /** 影子库存储句柄（在线重建追赶期间直接应用批次用）。 */
    public synchronized FileTargetStore shadowStore() {
        requireShadow();
        return shadow;
    }

    /** 中断重建：保留影子目录与 manifest，供下次 startOrResume 安全续传。 */
    public synchronized void abort() {
        this.shadow = null;
    }

    /** 影子库行（测试校验用）。 */
    public synchronized Collection<TargetRow> shadowLiveRows() {
        requireShadow();
        return shadow.allLiveRows();
    }

    private void requireShadow() {
        if (shadow == null) {
            throw new SyncException(RejectReason.REBUILD_NOT_ACTIVE, "重建未开始或已提交");
        }
    }

    private void writeManifest(long highWatermark, long caughtUp) {
        String content = "highWatermark=" + highWatermark + "\n"
                + "caughtUpTo=" + caughtUp + "\n";
        com.github.lechandonga.cdcsyncjava.engine.CheckpointStore
                .writeAtomically(manifest, content);
    }

    private void readManifest() {
        try {
            long hw = -1L;
            long caughtUp = -1L;
            for (String line : Files.readAllLines(manifest, StandardCharsets.UTF_8)) {
                if (line.startsWith("highWatermark=")) {
                    hw = Long.parseLong(line.substring("highWatermark=".length()).trim());
                } else if (line.startsWith("caughtUpTo=")) {
                    caughtUp = Long.parseLong(line.substring("caughtUpTo=".length()).trim());
                }
            }
            if (hw < 0) {
                throw new IllegalStateException("manifest 缺少 highWatermark: " + manifest);
            }
            this.snapshotHighWatermark = hw;
            // 兼容旧版 manifest（只有 highWatermark）：追赶位点从快照高水位开始
            this.caughtUpTo = caughtUp >= 0 ? caughtUp : hw;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void fsyncDirectory(Path dir) throws IOException {
        try (FileChannel ch = FileChannel.open(dir)) {
            ch.force(true);
        }
    }

    private static void deleteRecursively(Path path) {
        if (!Files.exists(path)) {
            return;
        }
        try (var stream = Files.walk(path)) {
            stream.sorted((a, b) -> b.compareTo(a)).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
