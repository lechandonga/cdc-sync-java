package com.github.lechandonga.cdcsyncjava.source;

/** 快照读取器：建立一致性快照。 */
public interface SnapshotReader {

    /**
     * 建立一致性快照。
     *
     * @param fromWatermark 高水位上界（0 表示当前最新）；重建续传时传入快照自身高水位以保证一致
     */
    Snapshot readSnapshot(long fromWatermark);
}
