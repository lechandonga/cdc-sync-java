package com.github.lechandonga.cdcsyncjava.target;

import com.github.lechandonga.cdcsyncjava.model.ChangeEvent;
import com.github.lechandonga.cdcsyncjava.model.SnapshotRow;

/**
 * 全量重建会话：所有写入只影响影子表，读取方始终看到旧活动表；
 * commit 原子切换，abort/崩溃后影子表被废弃、可安全重试。
 */
public interface RebuildSession {

    void addSnapshotRow(SnapshotRow row);

    void applyChange(ChangeEvent projectedEvent);

    /**
     * 原子切换为新表并推进位点：仅接受 seq <= newPosition 的事件，
     * 切换期间与正常增量互斥，读取方只见旧表或新表，永不看到半成品。
     */
    void commit(long newPosition);

    void abort();

    boolean active();
}
