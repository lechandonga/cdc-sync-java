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
    private LocalTargetStore target;
    private final SyncConfig config;
    private final CheckpointStore checkpointStore;
    private final InitProgressStore initProgressStore;
    private final SyncStats stats = new SyncStats();

    private final AtomicLong committed = new AtomicLong();
    private final AtomicLong requestedCatchUp = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong readerPosition = new AtomicLong(0L);
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

    /**
     * 测试钩子：快照阶段每应用完一行后回调；若 {@code running} 已被置为 false
     * （例如钩子内调用 simulateCrash），快照阶段立即中断并抛出，模拟“初始化未跑完进程终止”。
     * 生产环境保持 null，无任何行为影响。
     */
    public volatile Runnable snapshotRowHook;

    /** 在线重建的一次性原子切换请求；协调线程在批次边界执行切换屏障。 */
    private volatile SwitchRequest pendingSwitch;

    private record SwitchRequest(com.github.lechandonga.cdcsyncjava.target.LocalTargetStore next,
                                 long newCheckpoint, CountDownLatch finished) {
    }

    public SyncEngine(LocalSourceDatabase source, LocalTargetStore target, SyncConfig config) {
        this.source = source;
        this.target = target;
        this.config = config;
        this.checkpointStore = new CheckpointStore(config.stateDirectory());
        this.initProgressStore = new InitProgressStore(config.stateDirectory());
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
        if (initProgressStore.exists()) {
            // 上一次初始化在快照阶段被终止：严格重开目标端（初始记录损坏即 TARGET_STATE_CORRUPT，
            // 不静默截断），再从记录的快照高水位续跑；未完成的初始化不会被当成已完成跳过，
            // 重复重启都从此处安全续传
            target = target.reopenStrict();
            long resumeHw = initProgressStore.loadHighWatermark();
            log.info("检测到未完成的初始化标记，从快照高水位 {} 续跑", resumeHw);
            startFrom = runSnapshotPhase(resumeHw, true);
        } else if (checkpoint.commitSeq() > 0 && target.lastAppliedCommitSeq() == 0) {
            // 有已提交位点、目标端却没有任何已落稳数据：现场对不上，拒绝启动
            throw new SyncException(RejectReason.STATE_CHECKPOINT_AHEAD_OF_DATA,
                    "checkpoint 位点 " + checkpoint.commitSeq()
                            + " 已提交但目标端没有任何已落稳数据：已确认进度跑到数据前面，拒绝启动");
        } else if (!target.initialized() && checkpoint.commitSeq() == 0) {
            startFrom = runSnapshotPhase(0L, false);
        } else {
            // 启动现场校验：位点跑到实际数据前面属于对不上，拒绝启动而不是污染数据
            validateOnDiskState(checkpoint);
            // 重启续传：checkpoint <= 目标 WAL 位点，缺口由目标端幂等补齐
            startFrom = checkpoint.commitSeq();
            committed.set(Math.max(checkpoint.commitSeq(), target.lastAppliedCommitSeq()));
            log.info("从 checkpoint 续传，checkpoint={} 目标已确认={}",
                    checkpoint.commitSeq(), target.lastAppliedCommitSeq());
        }
        runIncremental(startFrom);
    }

    /**
     * 启动现场校验：checkpoint 不得超过目标端已持久化位点，目标端必须已初始化。
     * 对不上时给出可区分原因并拒绝启动，不写任何状态。
     */
    private void validateOnDiskState(SourcePosition checkpoint) {
        if (!target.initialized()) {
            throw new SyncException(RejectReason.INITIALIZATION_INCOMPLETE,
                    "目标端尚未初始化且无未完成初始化标记，现场无法解释；拒绝启动");
        }
        long targetSeq = target.lastAppliedCommitSeq();
        if (checkpoint.commitSeq() > targetSeq) {
            throw new SyncException(RejectReason.STATE_CHECKPOINT_AHEAD_OF_DATA,
                    "checkpoint 位点 " + checkpoint.commitSeq()
                            + " 超过目标端已持久化位点 " + targetSeq
                            + "：已确认进度跑到实际未落稳的数据前面，拒绝启动");
        }
    }

    /**
     * 快照阶段，返回增量起始位点（= 快照 highWatermark）。
     *
     * @param resumeHighWatermark 续跑时传入上一次快照的高水位（复现同一快照上界）；0 表示全新快照
     * @param resume 是否为初始化中断后的续跑
     */
    private long runSnapshotPhase(long resumeHighWatermark, boolean resume) {
        // 快照上界在任何目标写入之前先落盘：再次被杀仍能从同一上界续跑，保证初始化不丢不重
        Snapshot snapshot = source.readSnapshot(resumeHighWatermark);
        long hw = snapshot.highWatermark().commitSeq();
        int schemaVersion = snapshot.schema().version();
        if (!resume) {
            initProgressStore.begin(hw);
        }
        log.info("快照阶段开始{}：schema=v{} 行数={} highWatermark={}",
                resume ? "（续跑）" : "", schemaVersion, snapshot.rows().size(), hw);
        if (!target.initialized()) {
            target.initialize(source.schemaAt(1));
        }
        // 把快照前的全部历史演进按序登记，目标端才能翻译携带旧版本号的快照行；
        // 续跑时已登记的 DDL 按版本幂等跳过（断点可能正落在某条 DDL 之后）
        for (int v = 2; v <= schemaVersion; v++) {
            com.github.lechandonga.cdcsyncjava.model.SchemaChange change =
                    source.schemaChangeTo(v);
            int registered = target.registerSchemaChangeAt(change, v);
            if (registered == v) {
                stats.recordSchemaChange();
            }
        }
        for (RowData row : snapshot.rows()) {
            applySnapshotRow(row, hw);
            // 测试钩子：可在快照应用中途确定性地模拟进程终止
            if (snapshotRowHook != null) {
                snapshotRowHook.run();
            }
            if (!running) {
                throw new SyncException(RejectReason.INITIALIZATION_INCOMPLETE,
                        "快照阶段被中断（模拟进程终止），已写入行与初始化标记均保留在磁盘");
            }
        }
        // 先提交位点（数据此时已全部 fsync），再清除初始化标记
        checkpointStore.store(new SourcePosition(hw));
        committed.set(hw);
        initProgressStore.complete();
        log.info("快照阶段完成并提交位点 highWatermark={}，切换增量", hw);
        return hw;
    }

    private void applySnapshotRow(RowData row, long snapshotHighWatermark) {
        // 快照中的行可能携带旧 schemaVersion（快照前发生过 DDL），目标端按登记版本翻译
        String key = String.valueOf(
                row.values().get(source.schemaAt(row.schemaVersion()).keyColumn()));
        ChangeEvent insert = new ChangeEvent(OpType.INSERT, key, row,
                new SourcePosition(row.commitSeq()), row.schemaVersion());
        // 快照行按键物化幂等（非按业务 seq）：续跑时同一物理行重复落盘安全跳过
        ApplyOutcome outcome = target.applySnapshotRow(insert, snapshotHighWatermark);
        if (outcome == ApplyOutcome.APPLIED) {
            stats.recordApplied();
        } else if (outcome == ApplyOutcome.DUPLICATE_SKIPPED) {
            stats.recordDuplicate(new SkipRecord(0, insert.position(), insert.key(),
                    RejectReason.ALREADY_APPLIED, "快照行该键已物化（断点续跑）"));
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
                readerPosition.set(batch.to().commitSeq());
                queue.put(batch); // 背压点：BLOCK 持续等待（可恢复）/ REJECT 抛 QUEUE_FULL
                from = batch.to().commitSeq();
                readerFrom = from;
                if (from >= requestedCatchUp.get()) {
                    return;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            // 停止 / 崩溃打断源端等待时源端适配器会包成 RuntimeException(InterruptedException)，
            // 这不是同步失败；只有“真的不可恢复 / 可恢复的 QUEUE_FULL”才上抛
            boolean interruptedByStop = (e.getCause() instanceof InterruptedException) && !running;
            if (interruptedByStop) {
                return;
            }
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
                    // 批次边界执行在线重建的一次性原子切换（此前在途批次均已提交）
                    if (pendingSwitch != null) {
                        performSwitch();
                    }
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
                // 在线切换不能依赖“恰好有新批次”：引擎已追平（队列空）时也要在屏障点切换
                if (pendingSwitch != null) {
                    performSwitch();
                    continue;
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
            // 按源端声明版本幂等登记：正常运行登记新版本；崩溃/切换后整批重放时跳过已登记 DDL
            int newVersion = target.registerSchemaChangeAt(ddl.change(), ddl.schemaVersionAfter());
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
        failPendingSwitch();
        if (readerThread != null) {
            readerThread.interrupt();
        }
        if (coordinatorThread != null) {
            coordinatorThread.interrupt();
        }
        shutdownWorkersQuietly();
    }

    private void failPendingSwitch() {
        SwitchRequest req = pendingSwitch;
        if (req != null) {
            pendingSwitch = null;
            req.finished().countDown();
        }
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
        failPendingSwitch();
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

    /**
     * 读取线程已从源端拉取到的位点（已确认进度绝不可能超过它，也不会超过已落稳数据）。
     */
    public long readerPosition() {
        return readerPosition.get();
    }

    /** 当前内存缓冲积压批次数（背压可观测）。 */
    public int backlogBatches() {
        return queue == null ? 0 : queue.size();
    }

    /** 源端最新位点相对已确认位点的落后量（积压规模，随源端写入实时变化）。 */
    public long sourceLag() {
        return Math.max(0L, source.latestCommitSeq() - committed.get());
    }

    /** 背压命中次数：读取方因缓冲满被迫持续等待的次数。 */
    public long backpressureWaits() {
        return queue == null ? 0L : queue.backpressureWaits();
    }

    /** 背压累计等待毫秒数。 */
    public long backpressureWaitMillis() {
        return queue == null ? 0L : queue.backpressureWaitMillis();
    }

    /** 历史峰值积压批次数。 */
    public long maxBacklogBatches() {
        return queue == null ? 0L : queue.maxOccupancy();
    }

    /**
     * 在线重建原子切换：请求协调线程在下一个批次边界（无在途批次、本批已提交）暂停应用、
     * 原子替换底层目标存储并把已确认位点对齐到新库位点，然后恢复同步。
     * 整个过程切换一次生效，读取方（经可热切换存储）只会看到完整旧库或完整新库。
     *
     * <p>本方法会阻塞直到切换完成；要求引擎使用 {@code SwappableTargetStore} 且采用
     * 等待式（BLOCK）背压——REJECT 策略下切换窗口的新批次可能被拒绝，故不支持。</p>
     */
    public void requestOnlineSwitch(
            com.github.lechandonga.cdcsyncjava.target.LocalTargetStore next,
            long newCheckpoint) throws InterruptedException {
        if (!(target instanceof com.github.lechandonga.cdcsyncjava.target.SwappableTargetStore)) {
            throw new SyncException(RejectReason.ILLEGAL_STATE,
                    "在线切换要求目标端为 SwappableTargetStore");
        }
        if (config.backpressurePolicy() != BackpressurePolicy.BLOCK) {
            throw new SyncException(RejectReason.ILLEGAL_STATE,
                    "在线重建切换仅支持 BLOCK 等待式背压，当前策略 "
                            + config.backpressurePolicy());
        }
        CountDownLatch finished = new CountDownLatch(1);
        pendingSwitch = new SwitchRequest(next, newCheckpoint, finished);
        while (finished.getCount() != 0) {
            if (fatalError != null && !crashed) {
                throw fatalError instanceof SyncException se ? se
                        : new SyncException(RejectReason.ILLEGAL_STATE,
                                "切换屏障期间引擎发生不可恢复失败", fatalError);
            }
            Thread.sleep(2);
        }
    }

    /** 协调线程在批次边界执行的一次性切换（无在途批次，本批数据已 fsync + checkpoint）。 */
    private void performSwitch() {
        SwitchRequest req = pendingSwitch;
        if (req == null) {
            return;
        }
        try {
            ((com.github.lechandonga.cdcsyncjava.target.SwappableTargetStore) target)
                    .swapTo(req.next());
            // 新库位点即新的已确认上界：不回退（避免重复推进），也不越过新库已落稳数据
            long aligned = Math.max(committed.get(), req.newCheckpoint());
            checkpointStore.store(new SourcePosition(aligned));
            committed.set(aligned);
            readerFrom = Math.max(readerFrom, aligned);
            log.info("在线重建原子切换完成：新库位点={} 已确认位点={}", req.newCheckpoint(), aligned);
        } finally {
            pendingSwitch = null;
            req.finished().countDown();
        }
    }

    /** 统计信息。 */
    public SyncStats stats() {
        return stats;
    }
}
