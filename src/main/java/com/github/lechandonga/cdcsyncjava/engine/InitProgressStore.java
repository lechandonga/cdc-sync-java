package com.github.lechandonga.cdcsyncjava.engine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * 首次初始化进度标记。
 *
 * <p>快照阶段开始前先把快照 highWatermark 原子写入 {@code init-progress.properties}，
 * 快照阶段最后一个完整批次提交（checkpoint 落盘）后再删除该标记。因此：</p>
 * <ul>
 *   <li>标记存在 = 上一次初始化在快照阶段被中断，重启必须从记录的 highWatermark 续跑，
 *       绝不能把它当成已完成初始化跳过，也不会每次启动都从零重来（目标端幂等去重）；</li>
 *   <li>标记不存在 = 要么尚未开始初始化（目标端为空），要么初始化已完成（checkpoint 存在）。</li>
 * </ul>
 *
 * <p>写入走 {@link CheckpointStore#writeAtomically}（临时文件 fsync + 原子 rename），
 * 进程在任意时刻被杀，标记要么不存在、要么内容完整。</p>
 */
public class InitProgressStore {

    private static final String FILE_NAME = "init-progress.properties";

    private final Path file;

    public InitProgressStore(Path stateDirectory) {
        try {
            Files.createDirectories(stateDirectory);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.file = stateDirectory.resolve(FILE_NAME);
    }

    /** 是否存在未完成的初始化标记。 */
    public boolean exists() {
        return Files.exists(file);
    }

    /** 读取未完成初始化的快照 highWatermark；标记不存在或损坏抛异常。 */
    public long loadHighWatermark() {
        try {
            for (String line : Files.readAllLines(file)) {
                String trimmed = line.trim();
                if (trimmed.startsWith("snapshotHighWatermark=")) {
                    return Long.parseLong(
                            trimmed.substring("snapshotHighWatermark=".length()).trim());
                }
            }
            throw new SyncException(com.github.lechandonga.cdcsyncjava.model.RejectReason.ILLEGAL_STATE,
                    "初始化进度标记缺少 snapshotHighWatermark: " + file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 标记初始化（快照阶段）开始，记录快照 highWatermark。 */
    public void begin(long snapshotHighWatermark) {
        CheckpointStore.writeAtomically(file,
                "snapshotHighWatermark=" + snapshotHighWatermark + "\n");
    }

    /** 快照阶段已完整提交，清除标记。 */
    public void complete() {
        try {
            Files.deleteIfExists(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
