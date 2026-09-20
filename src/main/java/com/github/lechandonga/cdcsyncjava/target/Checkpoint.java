package com.github.lechandonga.cdcsyncjava.target;

/**
 * 位点：快照完成标志 + 已提交的源端全局序号。
 * 初始 (-1, false)，快照完成后变为 (P, true)。
 */
public record Checkpoint(long position, boolean snapshotCompleted, long rebuilds) {

    public static Checkpoint initial() {
        return new Checkpoint(-1L, false, 0L);
    }
}
