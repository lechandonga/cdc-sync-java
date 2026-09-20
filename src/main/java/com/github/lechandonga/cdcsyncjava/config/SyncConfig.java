package com.github.lechandonga.cdcsyncjava.config;

/** 同步引擎可调参数：批量大小、并发度、有界缓冲与背压策略。 */
public final class SyncConfig {

    public enum BufferPolicy { BLOCK, REJECT }

    private final int batchSize;
    private final int concurrency;
    private final int bufferCapacity;
    private final long offerTimeoutMs;
    private final BufferPolicy bufferPolicy;

    public SyncConfig(int batchSize, int concurrency, int bufferCapacity,
                      long offerTimeoutMs, BufferPolicy bufferPolicy) {
        this.batchSize = batchSize;
        this.concurrency = concurrency;
        this.bufferCapacity = bufferCapacity;
        this.offerTimeoutMs = offerTimeoutMs;
        this.bufferPolicy = bufferPolicy;
    }

    public static SyncConfig defaults() {
        return new SyncConfig(100, 4, 1000, 1000, BufferPolicy.BLOCK);
    }

    public int batchSize() { return batchSize; }
    public int concurrency() { return concurrency; }
    public int bufferCapacity() { return bufferCapacity; }
    public long offerTimeoutMs() { return offerTimeoutMs; }
    public BufferPolicy bufferPolicy() { return bufferPolicy; }
}
