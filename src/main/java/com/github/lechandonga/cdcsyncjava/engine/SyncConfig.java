package com.github.lechandonga.cdcsyncjava.engine;

import java.nio.file.Path;

/**
 * 同步引擎配置：批量大小、并发度、缓冲容量等均可配置。
 */
public final class SyncConfig {

    private final Path stateDirectory;
    private final int batchSize;
    private final int parallelism;
    private final int bufferCapacity;
    private final BackpressurePolicy backpressurePolicy;
    private final long pollTimeoutMillis;

    public SyncConfig(Path stateDirectory, int batchSize, int parallelism, int bufferCapacity,
                      BackpressurePolicy backpressurePolicy, long pollTimeoutMillis) {
        this.stateDirectory = stateDirectory;
        this.batchSize = batchSize;
        this.parallelism = parallelism;
        this.bufferCapacity = bufferCapacity;
        this.backpressurePolicy = backpressurePolicy;
        this.pollTimeoutMillis = pollTimeoutMillis;
    }

    public Path stateDirectory() {
        return stateDirectory;
    }

    public int batchSize() {
        return batchSize;
    }

    public int parallelism() {
        return parallelism;
    }

    public int bufferCapacity() {
        return bufferCapacity;
    }

    public BackpressurePolicy backpressurePolicy() {
        return backpressurePolicy;
    }

    public long pollTimeoutMillis() {
        return pollTimeoutMillis;
    }

    /** 合理默认值构建器。 */
    public static Builder builder(Path stateDirectory) {
        return new Builder(stateDirectory);
    }

    /** 配置构建器。 */
    public static final class Builder {
        private final Path stateDirectory;
        private int batchSize = 100;
        private int parallelism = 4;
        private int bufferCapacity = 256;
        private BackpressurePolicy backpressurePolicy = BackpressurePolicy.BLOCK;
        private long pollTimeoutMillis = 100L;

        public Builder(Path stateDirectory) {
            this.stateDirectory = stateDirectory;
        }

        public Builder batchSize(int v) {
            this.batchSize = v;
            return this;
        }

        public Builder parallelism(int v) {
            this.parallelism = v;
            return this;
        }

        public Builder bufferCapacity(int v) {
            this.bufferCapacity = v;
            return this;
        }

        public Builder backpressurePolicy(BackpressurePolicy v) {
            this.backpressurePolicy = v;
            return this;
        }

        public Builder pollTimeoutMillis(long v) {
            this.pollTimeoutMillis = v;
            return this;
        }

        public SyncConfig build() {
            return new SyncConfig(stateDirectory, batchSize, parallelism, bufferCapacity,
                    backpressurePolicy, pollTimeoutMillis);
        }
    }
}
