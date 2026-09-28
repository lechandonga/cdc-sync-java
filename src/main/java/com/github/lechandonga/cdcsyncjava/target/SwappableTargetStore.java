package com.github.lechandonga.cdcsyncjava.target;

import java.util.Collection;

import com.github.lechandonga.cdcsyncjava.engine.SyncException;
import com.github.lechandonga.cdcsyncjava.model.ChangeEvent;
import com.github.lechandonga.cdcsyncjava.model.RejectReason;
import com.github.lechandonga.cdcsyncjava.model.SchemaChange;
import com.github.lechandonga.cdcsyncjava.model.TableSchema;

/**
 * 可在同步运行期间原子替换底层实现的目标存储代理。
 *
 * <p>在线全量重建完成时，影子目录被原子 rename 为 current，底层 {@link FileTargetStore}
 * 指向的文件句柄随之失效；同步引擎始终通过本代理访问目标端，在批次边界的切换屏障处
 * 调用 {@link #swapTo} 一次性替换句柄，切换前后的写入分别只落在旧库 / 新库，绝不跨库。</p>
 */
public class SwappableTargetStore implements LocalTargetStore {

    private volatile LocalTargetStore delegate;

    public SwappableTargetStore(LocalTargetStore initial) {
        this.delegate = initial;
    }

    /** 当前底层存储（读取方/测试可拿到底层视图）。 */
    public LocalTargetStore currentDelegate() {
        return delegate;
    }

    /** 原子替换底层存储（由同步引擎在切换屏障、无在途批次时调用）。 */
    public void swapTo(LocalTargetStore next) {
        if (next == null) {
            throw new SyncException(RejectReason.ILLEGAL_STATE, "swapTo 目标不能为 null");
        }
        this.delegate = next;
    }

    @Override
    public void initialize(TableSchema initialSchema) {
        delegate.initialize(initialSchema);
    }

    @Override
    public boolean initialized() {
        return delegate.initialized();
    }

    @Override
    public LocalTargetStore reopenStrict() {
        // 严格重开底层文件存储并在代理内原子替换，保持引擎持有的代理引用不变
        LocalTargetStore reopened = delegate.reopenStrict();
        this.delegate = reopened;
        return this;
    }

    @Override
    public int registerSchemaChange(SchemaChange change) {
        return delegate.registerSchemaChange(change);
    }

    @Override
    public int registerSchemaChangeAt(SchemaChange change, int schemaVersionAfter) {
        return delegate.registerSchemaChangeAt(change, schemaVersionAfter);
    }

    @Override
    public ApplyOutcome apply(ChangeEvent event) {
        return delegate.apply(event);
    }

    @Override
    public ApplyOutcome applySnapshotRow(ChangeEvent event, long snapshotHighWatermark) {
        return delegate.applySnapshotRow(event, snapshotHighWatermark);
    }

    @Override
    public TargetRow get(String key) {
        return delegate.get(key);
    }

    @Override
    public Collection<TargetRow> allLiveRows() {
        return delegate.allLiveRows();
    }

    @Override
    public Collection<TargetRow> allRows() {
        return delegate.allRows();
    }

    @Override
    public long lastAppliedCommitSeq() {
        return delegate.lastAppliedCommitSeq();
    }

    @Override
    public int currentSchemaVersion() {
        return delegate.currentSchemaVersion();
    }

    @Override
    public TableSchema schemaAt(int version) {
        return delegate.schemaAt(version);
    }
}
