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
    private final Path manifestFile;
    private FileTargetStore current;
    private FileTargetStore shadow;
    private long snapshotHighWatermark = -1L;

    public RebuildCoordinator(Path baseDirectory) {
        try {
            Files.createDirectories(baseDirectory);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.baseDirectory = baseDirectory;
        this.currentDir = baseDirectory.resolve(CURRENT_DIR);
        this.shadowDir = baseDirectory.resolve(SHADOW_DIR);
        this.manifestFile = baseDirectory.resolve(MANIFEST);
        if (Files.exists(currentDir)) {
            this.current = new FileTargetStore(currentDir);
        }
        // 构造时自动识别“进程中断后遗留的未完成影子库”，isRebuilding() 立即可见
        if (Files.exists(shadowDir) && Files.exists(manifestFile)) {
            this.snapshotHighWatermark = readManifestHighWatermark();
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
     * 忽略传入快照并复用历史 highWatermark。
     */
    public synchronized void startOrResume(Snapshot snapshot) {
        if (shadow != null) {
            // 构造函数已从磁盘恢复未完成影子库：幂等的 resume，不重复加载
            return;
        }
        // 全新重建：清理可能残留的半成品
        deleteRecursively(shadowDir);
        deleteRecursively(baseDirectory.resolve(STAGING_DIR));
        this.shadow = new FileTargetStore(shadowDir);
        this.shadow.initialize(snapshot.schema());
        for (var row : snapshot.rows()) {
            ChangeEvent insert = new ChangeEvent(
                    com.github.lechandonga.cdcsyncjava.model.OpType.INSERT,
                    String.valueOf(row.values().get(snapshot.schema().keyColumn())),
                    row,
                    new com.github.lechandonga.cdcsyncjava.model.SourcePosition(row.commitSeq()),
                    row.schemaVersion());
            shadow.apply(insert);
        }
        this.snapshotHighWatermark = snapshot.highWatermark().commitSeq();
        writeManifest(this.snapshotHighWatermark);
    }

    /** 向影子库应用一条增量（DDL 之外的 DML）；幂等。 */
    public synchronized ApplyOutcome applyShadow(ChangeEvent event) {
        requireShadow();
        return shadow.apply(event);
    }

    /** 向影子库登记一次结构演进。 */
    public synchronized int registerShadowSchemaChange(
            com.github.lechandonga.cdcsyncjava.model.SchemaChange change) {
        requireShadow();
        return shadow.registerSchemaChange(change);
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
            Files.deleteIfExists(manifestFile);
            this.current = new FileTargetStore(currentDir);
            this.shadow = null;
            this.snapshotHighWatermark = -1L;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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

    private void writeManifest(long highWatermark) {
        String content = "highWatermark=" + highWatermark + "\n";
        com.github.lechandonga.cdcsyncjava.engine.CheckpointStore
                .writeAtomically(manifestFile, content);
    }

    private long readManifestHighWatermark() {
        try {
            for (String line : Files.readAllLines(manifestFile, StandardCharsets.UTF_8)) {
                if (line.startsWith("highWatermark=")) {
                    return Long.parseLong(line.substring("highWatermark=".length()).trim());
                }
            }
            throw new IllegalStateException("manifest 缺少 highWatermark: " + manifestFile);
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
