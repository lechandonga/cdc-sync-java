package com.github.lechandonga.cdcsyncjava.engine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * 快照初始化阶段的持久化标记：快照开始前写入 {@code init.state=IN_PROGRESS}，
 * 快照全部行落盘并提交 highWatermark 位点后删除。
 *
 * <p>进程在快照阶段中途终止时，标记遗留到下次启动：引擎据此识别“未完成的初始化”，
 * 从快照阶段幂等续跑，而不会误判为空库从头来过，也不会误判为已完成跳过初始化。</p>
 *
 * <p>标记采用 {@link CheckpointStore#writeAtomically} 的原子写（临时文件 + fsync + rename），
 * 自身只会出现“存在 / 不存在”两种状态，不存在半行问题。</p>
 */
public class InitStateStore {

    public static final String FILE_NAME = "init.state";

    private final Path file;

    public InitStateStore(Path stateDirectory) {
        try {
            Files.createDirectories(stateDirectory);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.file = stateDirectory.resolve(FILE_NAME);
    }

    /** 标记初始化开始（幂等：重复写入内容相同）。 */
    public void markInProgress() {
        CheckpointStore.writeAtomically(file, "state=IN_PROGRESS\n");
    }

    /** 初始化完整完成后清除标记。 */
    public void clear() {
        try {
            Files.deleteIfExists(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 是否存在未完成的初始化标记。 */
    public boolean isInProgress() {
        return Files.exists(file);
    }
}
