package com.github.lechandonga.cdcsyncjava.support;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

import com.github.lechandonga.cdcsyncjava.engine.SyncException;
import com.github.lechandonga.cdcsyncjava.model.ChangeEvent;
import com.github.lechandonga.cdcsyncjava.model.RejectReason;
import com.github.lechandonga.cdcsyncjava.model.SchemaChange;
import com.github.lechandonga.cdcsyncjava.model.TableSchema;
import com.github.lechandonga.cdcsyncjava.target.ApplyOutcome;
import com.github.lechandonga.cdcsyncjava.target.LocalTargetStore;
import com.github.lechandonga.cdcsyncjava.target.TargetRow;

/**
 * 测试用目标端包装器：
 * <ul>
 *   <li>每次 DML 应用注入固定延迟，模拟目标端长时间处理不过来（持续落后）；</li>
 *   <li>可在若干次应用后注入一次“不可恢复失败”（TARGET_UNRECOVERABLE），
 *       用来区分“暂时背压”与“真的不可恢复”。</li>
 * </ul>
 * 其余方法全部委托底层真实存储，幂等/持久化语义不变。
 */
public class SlowTargetStore implements LocalTargetStore {

    private final LocalTargetStore delegate;
    private final long applyDelayMillis;
    private final AtomicLong appliedCount = new AtomicLong();
    private volatile long failAfterApplies = Long.MAX_VALUE;
    private volatile boolean armed = true;

    public SlowTargetStore(LocalTargetStore delegate, long applyDelayMillis) {
        this.delegate = delegate;
        this.applyDelayMillis = applyDelayMillis;
    }

    /** 在成功应用 n 条之后，下一条应用抛出 TARGET_UNRECOVERABLE（只触发一次）。 */
    public void injectUnrecoverableAfter(long n) {
        this.failAfterApplies = n;
    }

    public long appliedCount() {
        return appliedCount.get();
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
        delegate.reopenStrict();
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
        if (armed && appliedCount.get() >= failAfterApplies) {
            armed = false;
            throw new SyncException(RejectReason.TARGET_UNRECOVERABLE,
                    "模拟目标端不可恢复失败（磁盘损坏），位点 " + event.position().commitSeq());
        }
        if (applyDelayMillis > 0) {
            try {
                Thread.sleep(applyDelayMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        ApplyOutcome outcome = delegate.apply(event);
        if (outcome == ApplyOutcome.APPLIED) {
            appliedCount.incrementAndGet();
        }
        return outcome;
    }

    @Override
    public ApplyOutcome applySnapshotRow(ChangeEvent event, long snapshotHighWatermark) {
        // 快照行不走慢/故障注入（背压场景关注增量阶段）
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
