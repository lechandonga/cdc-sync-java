package com.github.lechandonga.cdcsyncjava.engine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

import com.github.lechandonga.cdcsyncjava.model.SourcePosition;

/**
 * 位点持久化：临时文件 + fsync + 原子 rename。
 *
 * <p>提交协议：数据（目标 WAL）先 fsync，位点后提交。因此任意时刻
 * 位点 &lt;= 已持久化数据位点：崩溃后重放 (checkpoint, wal-tail] 是安全的，
 * 重复部分由目标端按 commitSeq 幂等跳过。</p>
 */
public class CheckpointStore {

    private final Path file;

    public CheckpointStore(Path stateDirectory) {
        try {
            Files.createDirectories(stateDirectory);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.file = stateDirectory.resolve("checkpoint.properties");
    }

    /** 读取已提交位点；不存在返回 ZERO。 */
    public SourcePosition load() {
        try {
            if (!Files.exists(file) || Files.size(file) == 0) {
                return SourcePosition.ZERO;
            }
            for (String line : Files.readAllLines(file)) {
                String trimmed = line.trim();
                if (trimmed.startsWith("commitSeq=")) {
                    return new SourcePosition(Long.parseLong(trimmed.substring("commitSeq=".length())));
                }
            }
            return SourcePosition.ZERO;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 原子提交位点（临时文件 fsync，再原子 rename，再 fsync 目录）。 */
    public void store(SourcePosition position) {
        writeAtomically(file, "commitSeq=" + position.commitSeq() + "\n");
    }

    /** 清除位点（全量重建场景）。 */
    public void clear() {
        try {
            Files.deleteIfExists(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 原子写文件（临时文件 fsync + 原子 rename + fsync 目录）。 */
    public static void writeAtomically(Path target, String content) {
        try {
            Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
            try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                ch.write(java.nio.ByteBuffer.wrap(content.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
                ch.force(true);
            }
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
            try (FileChannel dir = FileChannel.open(target.getParent())) {
                dir.force(true);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
