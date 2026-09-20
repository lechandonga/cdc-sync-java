package com.github.lechandonga.cdcsyncjava.engine;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lechandonga.cdcsyncjava.model.ChangeBatch;
import com.github.lechandonga.cdcsyncjava.model.ChangeEvent;
import com.github.lechandonga.cdcsyncjava.model.OpType;
import com.github.lechandonga.cdcsyncjava.model.RejectReason;
import com.github.lechandonga.cdcsyncjava.model.RowData;
import com.github.lechandonga.cdcsyncjava.model.SkipRecord;
import com.github.lechandonga.cdcsyncjava.model.SourcePosition;
import com.github.lechandonga.cdcsyncjava.source.LocalSourceDatabase;
import com.github.lechandonga.cdcsyncjava.source.Snapshot;
import com.github.lechandonga.cdcsyncjava.target.ApplyOutcome;
import com.github.lechandonga.cdcsyncjava.target.LocalTargetStore;

/**
 * 增量同步引擎。
 *
 * <p>阶段一（快照）：目标端为空库时，读取源端一致性快照（schema + 行 + highWatermark），
 * 在目标端登记 schema 并按 commitSeq 幂等写入所有行；快照期间并发产生的变更其 commitSeq
 * 全部大于 highWatermark，阶段二严格从 highWatermark 之后读取日志，因此衔接处不重、不丢、
 * 不乱序。</p>
 *
 * <p>阶段二（增量）：读取线程按批次拉源端日志并投入有界缓冲（背压可 BLOCK/REJECT）；
 * 协调线程取出批次，批次内 DDL 以 barrier 方式先于 DML 顺序登记，DML 按键哈希分发到固定
 * worker：同一键始终落在同一 worker 串行执行（保证源端提交顺序），不同键并行。</p>
 *
 * <p>提交协议：目标 WAL 先 fsync，全部 worker 完成本批次后再提交 checkpoint，
 * 崩溃后从 checkpoint 重放，未确认数据重放为幂等跳过，已确认数据不丢。</p>
 */
public class SyncEngine {

    private static final Logger log = LoggerFactory.getLogger(SyncEngine.class);

    private final LocalSourceDatabase source;
    private final LocalTargetStore target;
    private final SyncConfig config;
    private final CheckpointStore checkpointStore;
    private final SyncStats stats = new SyncStats();

    private final AtomicLong committed = new AtomicLong();
    private final AtomicLong requestedCatchUp = new AtomicLong(Long.MAX_VALUE);
    private volatile boolean running = false;
    private volatile boolean crashed = false;
    private volatile Throwable fatalError;
    private ExecutorService workers;
    private Thread coordinatorThread;
    private Thread readerThread;
    private BoundedEventQueue queue;
    private volatile long readerFrom = -1L;
    /** reader 到达 catchUpTo 并正常结束后计数，杜绝 coordinator 误判终止。 */
    private CountDownLatch readerDone;

    public SyncEngine(LocalSourceDatabase source, LocalTargetStore target, SyncConfig config) {
        this.source = source;
        this.target = target;
        this.config = config;
        this.checkpointStore = new CheckpointStore(config.stateDirectory());
    }

    /**
     * 启动同步并阻塞运行，直到已确认位点达到 catchUpTo 后自行优雅收尾返回；
     * 传 {@link Long#MAX_VALUE} 表示持续运行直到 {@link #stop()}。
     * 空库：快照 -> 增量；已有数据：从 checkpoint 续传增量。
     */
    public void run(long catchUpTo) {
        synchronized (this) {
            if (running) {
                throw new SyncException(RejectReason.ILLEGAL_STATE, "引擎已在运行");
            }
            running = true;
            crashed = false;
            fatalError = null;
            requestedCatchUp.set(catchUpTo);
        }
        SourcePosition checkpoint = checkpointStore.load();
        long startFrom;
        if (target.lastAppliedCommitSeq() == 0 && checkpoint.commitSeq() == 0) {
            startFrom = runSnapshotPhase();
        } else {
            // 重启续传：checkpoint <= 目标 WAL 位点，缺口由目标端幂等补齐
            startFrom = checkpoint.commitSeq();
            committed.set(Math.max(checkpoint.commitSeq(), target.lastAppliedCommitSeq()));
            log.info("从 checkpoint 续传，checkpoint={} 目标已确认={}",
                    checkpoint.commitSeq(), target.lastAppliedCommitSeq());
        }
        runIncremental(startFrom);
    }

    /** 快照阶段，返回增量起始位点（= 快照 highWatermark）。 */
    private long runSnapshotPhase() {
        Snapshot snapshot = source.readSnapshot(0);
        long hw = snapshot.highWatermark().commitSeq();
        int schemaVersion = snapshot.schema().version();
        log.info("快照阶段开始：schema=v{} 行数={} highWatermark={}",
                schemaVersion, snapshot.rows().size(), hw);
        target.initialize(source.schemaAt(1));
        // 把快照前的全部历史演进按序登记，目标端才能翻译携带旧版本号的快照行
        for (int v = 2; v <= schemaVersion; v++) {
            com.github.lechandonga.cdcsyncjava.model.SchemaChange change =
                    source.schemaChangeTo(v);
            target.registerSchemaChange(change);
            stats.recordSchemaChange();
        }
        for (RowData row : snapshot.rows()) {
            applySnapshotRow(row);
        }
        checkpointStore.store(new SourcePosition(hw));
        committed.set(hw);
        log.info("快照阶段完成并提交位点 highWatermark={}，切换增量", hw);
        return hw;
    }

    private void applySnapshotRow(RowData row) {
        // 快照中的行可能携带旧 schemaVersion（快照前发生过 DDL），目标端按登记版本翻译
        String key = String.valueOf(
                row.values().get(source.schemaAt(row.schemaVersion()).keyColumn()));
        ChangeEvent insert = new ChangeEvent(OpType.INSERT, key, row,
                new SourcePosition(row.commitSeq()), row.schemaVersion());
        ApplyOutcome outcome = target.apply(insert);
        if (outcome == ApplyOutcome.APPLIED) {
            stats.recordApplied();
        } else if (outcome == ApplyOutcome.DUPLICATE_SKIPPED) {
            stats.recordDuplicate(new SkipRecord(0, insert.position(), insert.key(),
                    RejectReason.ALREADY_APPLIED, "快照行位点已应用（重试/重启）"));
        }
    }

    /** 增量阶段。 */
    private void runIncremental(long startFrom) {
        long catchUpTo = requestedCatchUp.get();
        int parallelism = Math.max(1, config.parallelism());
        this.queue = new BoundedEventQueue(config.bufferCapacity(), config.backpressurePolicy(),
                Math.max(10, config.pollTimeoutMillis()));
        this.workers = Executors.newFixedThreadPool(parallelism, r -> {
            Thread t = new Thread(r, "cdc-worker");
            t.setDaemon(true);
            return t;
        });
        this.readerFrom = startFrom;
        this.readerDone = new CountDownLatch(1);

        readerThread = new Thread(this::readerLoop, "cdc-reader");
        coordinatorThread = new Thread(this::coordinatorLoop, "cdc-coordinator");
        readerThread.setDaemon(true);
        coordinatorThread.setDaemon(true);
        readerThread.start();
        coordinatorThread.start();
        try {
            coordinatorThread.join();
            // coordinator 到达 catchUp 后 reader 可能仍在 wait 源端新数据，唤醒它
            readerThread.interrupt();
            readerThread.join(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        shutdownWorkersQuietly();
        if (fatalError != null && !crashed) {
            throw fatalError instanceof SyncException se ? se
                    : new SyncException(RejectReason.ILLEGAL_STATE, fatalError.getMessage(),
                            fatalError);
        }
        log.info("引擎停止，最终已提交位点={} catchUpTo={}", committed.get(), catchUpTo);
    }

    private void readerLoop() {
        long from = readerFrom;
        try {
            while (running && !Thread.currentThread().isInterrupted()) {
                var opt = source.nextBatch(new SourcePosition(from), config.batchSize(),
                        Duration.ofMillis(Math.max(1, config.pollTimeoutMillis())));
                if (opt.isEmpty()) {
                    if (from >= requestedCatchUp.get()) {
                        return;
                    }
                    continue;
                }
                ChangeBatch batch = opt.get();
                queue.put(batch); // 背压点：BLOCK 限流 / REJECT 抛 QUEUE_FULL
                from = batch.to().commitSeq();
                readerFrom = from;
                if (from >= requestedCatchUp.get()) {
                    return;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            fatalError = e;
            log.error("读取线程结束 reason={} {}",
                    e instanceof SyncException se ? se.reason() : RejectReason.ILLEGAL_STATE,
                    e.getMessage());
        } finally {
            readerDone.countDown();
        }
    }

    private void coordinatorLoop() {
        try {
            while (running && !Thread.currentThread().isInterrupted()) {
                ChangeBatch batch = queue.poll(Math.max(1, config.pollTimeoutMillis()));
                if (batch != null) {
                    processBatch(batch);
                    // 只有 reader 已正常结束、队列排空且位点达标，才允许收尾
                    if (committed.get() >= requestedCatchUp.get()
                            && readerDone.getCount() == 0 && queue.size() == 0) {
                        running = false;
                        return;
                    }
                    continue;
                }
                if (fatalError != null) {
                    running = false;
                    return;
                }
                // 队列暂时为空但 reader 仍在等待源端新提交时，继续轮询，绝不提前退出
                if (readerDone.getCount() == 0 && queue.size() == 0
                        && committed.get() >= requestedCatchUp.get()) {
                    running = false;
                    return;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            fatalError = e;
            running = false;
            log.error("协调线程结束: {}", e.getMessage(), e);
        }
    }

    /**
     * 处理一个批次：
     * 1) DDL 按序在协调线程应用（barrier：此前事件已全部完成）；
     * 2) DML 按键哈希分区，同 key 串行、不同 key 并行；
     * 3) 全部成功后提交位点到 batch.to()。
     */
    private void processBatch(ChangeBatch batch) throws InterruptedException {
        for (ChangeBatch.SchemaChangeAt ddl : batch.schemaChanges()) {
            applySchemaChange(batch, ddl);
        }
        List<ChangeEvent> events = batch.events();
        int stripes = Math.max(1, config.parallelism());
        if (!events.isEmpty()) {
            CountDownLatch done = new CountDownLatch(stripes);
            // worker 异常必须回传协调线程：任何一条失败都不得提交本批次位点
            final java.util.concurrent.atomic.AtomicReference<RuntimeException> firstError =
                    new java.util.concurrent.atomic.AtomicReference<>();
            for (int stripe = 0; stripe < stripes; stripe++) {
                final int s = stripe;
                workers.execute(() -> {
                    try {
                        for (ChangeEvent e : events) {
                            if (Math.floorMod(e.key().hashCode(), stripes) == s) {
                                applyOne(batch, e);
                            }
                        }
                    } catch (RuntimeException ex) {
                        firstError.compareAndSet(null, ex);
                    } finally {
                        done.countDown();
                    }
                });
            }
            done.await();
            RuntimeException error = firstError.get();
            if (error != null) {
                running = false;
                fatalError = error;
                throw error;
            }
        }
        long to = batch.to().commitSeq();
        // 位点提交严格晚于数据 fsync：这是崩溃恢复的重放下界
        checkpointStore.store(new SourcePosition(to));
        committed.set(to);
        log.debug("批次完成 batchId={} 区间=({}] DML={} 已提交={}", batch.batchId(),
                batch.from().commitSeq(), to, events.size(), to);
    }

    private void applySchemaChange(ChangeBatch batch, ChangeBatch.SchemaChangeAt ddl) {
        try {
            int newVersion = target.registerSchemaChange(ddl.change());
            stats.recordSchemaChange();
            log.info("结构演进已登记 batchId={} 位点={} {} -> v{}", batch.batchId(),
                    ddl.position().commitSeq(), ddl.change().type(), newVersion);
        } catch (SyncException e) {
            stats.recordRejected(new SkipRecord(batch.batchId(), ddl.position(), null,
                    e.reason(), "DDL 被拒绝: " + e.getMessage()));
            throw e;
        }
    }

    private void applyOne(ChangeBatch batch, ChangeEvent event) {
        try {
            ApplyOutcome outcome = target.apply(event);
            switch (outcome) {
                case APPLIED -> {
                    stats.recordApplied();
                    log.debug("应用成功 batchId={} 位点={} key={} op={}", batch.batchId(),
                            event.position().commitSeq(), event.key(), event.op());
                }
                case DUPLICATE_SKIPPED -> stats.recordDuplicate(new SkipRecord(batch.batchId(),
                        event.position(), event.key(), RejectReason.ALREADY_APPLIED,
                        "位点 <= 已确认位点（重复投递/批次重试/重启重放）"));
                case REJECTED -> stats.recordRejected(new SkipRecord(batch.batchId(),
                        event.position(), event.key(), RejectReason.ILLEGAL_STATE,
                        "目标端拒绝但未说明原因"));
            }
        } catch (SyncException e) {
            stats.recordRejected(new SkipRecord(batch.batchId(), event.position(),
                    event.key(), e.reason(), e.getMessage()));
            throw e;
        }
    }

    /** 阻塞等待已确认位点达到 position（达到后 run 会自行收尾）。 */
    public void awaitReached(long position, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (committed.get() < position && System.currentTimeMillis() < deadline) {
            if (fatalError != null && !crashed) {
                throw fatalError instanceof SyncException se ? se
                        : new SyncException(RejectReason.ILLEGAL_STATE, fatalError.getMessage(),
                                fatalError);
            }
            Thread.sleep(5);
        }
        if (committed.get() < position) {
            throw new SyncException(RejectReason.ILLEGAL_STATE,
                    "等待位点 " + position + " 超时，当前 " + committed.get());
        }
    }

    /** 优雅停止：停止拉取，等待已分发工作完成，run 返回。 */
    public void stop() {
        running = false;
        if (readerThread != null) {
            readerThread.interrupt();
        }
        if (coordinatorThread != null) {
            coordinatorThread.interrupt();
        }
        shutdownWorkersQuietly();
    }

    private void shutdownWorkersQuietly() {
        if (workers != null) {
            workers.shutdown();
            try {
                workers.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * 模拟进程崩溃：立即中断全部线程、丢弃内存缓冲，既不提交位点也不优雅收尾。
     * 目标 WAL 中已 fsync 的数据保留；checkpoint 停在最后一个完整批次。
     */
    public void simulateCrash() {
        crashed = true;
        running = false;
        if (workers != null) {
            workers.shutdownNow();
        }
        if (queue != null) {
            queue.clear();
        }
        if (readerThread != null) {
            readerThread.interrupt();
        }
        if (coordinatorThread != null) {
            coordinatorThread.interrupt();
        }
    }

    /** 当前已提交位点。 */
    public long committedPosition() {
        return committed.get();
    }

    /** 统计信息。 */
    public SyncStats stats() {
        return stats;
    }
}
