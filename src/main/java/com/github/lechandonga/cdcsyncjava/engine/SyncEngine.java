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
import com.github.lechandonga.cdcsyncjava.model.SchemaChange;
import com.github.lechandonga.cdcsyncjava.model.SkipRecord;
import com.github.lechandonga.cdcsyncjava.model.SourcePosition;
import com.github.lechandonga.cdcsyncjava.source.LocalSourceDatabase;
import com.github.lechandonga.cdcsyncjava.source.Snapshot;
import com.github.lechandonga.cdcsyncjava.target.ApplyOutcome;
import com.github.lechandonga.cdcsyncjava.target.LocalTargetStore;
import com.github.lechandonga.cdcsyncjava.target.RebuildCoordinator;

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
    private final InitStateStore initStateStore;
    private final SyncStats stats = new SyncStats();

    /** 在线重建协调器（可选）：绑定后引擎的在线写入全部指向其 current()，切换瞬间改路由。 */
    private volatile RebuildCoordinator rebuildCoordinator;
    /** 重建影子追赶进度（DDL 也推进）；达到 rebuildTargetSeq 后在批次提交点原子切换。 */
    private volatile long rebuildShadowSeq = -1L;
    /** 本次重建约定的追赶上界；-1 表示当前无重建。 */
    private volatile long rebuildTargetSeq = -1L;

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
        this.initStateStore = new InitStateStore(config.stateDirectory());
    }

    /**
     * 绑定在线重建协调器（必须在 {@link #run} 之前调用）。绑定后：
     * 引擎在线写入落到 {@code coordinator.current()}；若磁盘上存在未完成影子库，
     * 启动时自动续传该重建；运行中 {@link #requestOnlineRebuild(long)} 可就地发起新重建。
     */
    public SyncEngine withRebuildCoordinator(RebuildCoordinator coordinator) {
        if (running) {
            throw new SyncException(RejectReason.ILLEGAL_STATE, "引擎运行中不能绑定重建协调器");
        }
        this.rebuildCoordinator = coordinator;
        return this;
    }

    /** 当前在线写入与读取的目标库；绑定重建协调器时为其 current，否则为构造目标库。 */
    private LocalTargetStore activeTarget() {
        RebuildCoordinator c = rebuildCoordinator;
        return c != null ? c.current() : target;
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
        // 启动现场校验：未完成的初始化可续跑；现场自相矛盾时拒绝启动，绝不污染已有数据
        LocalTargetStore active = activeTarget();
        SourcePosition checkpoint = checkpointStore.load();
        boolean initInProgress = initStateStore.isInProgress();
        if (initInProgress && active.isInitialized()
                && checkpoint.commitSeq() > 0) {
            running = false;
            throw new SyncException(RejectReason.INIT_STATE_MISMATCH,
                    "初始化标记 IN_PROGRESS 但目标库已初始化且 checkpoint="
                            + checkpoint.commitSeq()
                            + "，现场状态对不上；为避免污染已有数据拒绝启动");
        }
        if (initInProgress && !active.isInitialized()) {
            running = false;
            throw new SyncException(RejectReason.INIT_STATE_MISMATCH,
                    "初始化标记 IN_PROGRESS 但目标 WAL 中没有初始 schema，初始化现场不完整，"
                            + "拒绝启动（需人工核查后清空状态重建）");
        }
        long startFrom;
        if (initInProgress) {
            // 快照阶段中途终止：WAL 已含初始 schema 与部分行，从快照阶段幂等续跑
            log.warn("检测到未完成的初始化标记，从快照阶段幂等续跑 checkpoint={}",
                    checkpoint.commitSeq());
            startFrom = runSnapshotPhase();
        } else if (active.lastAppliedCommitSeq() == 0 && checkpoint.commitSeq() == 0) {
            startFrom = runSnapshotPhase();
        } else {
            // 重启续传：checkpoint <= 目标 WAL 位点，缺口由目标端幂等补齐
            startFrom = checkpoint.commitSeq();
            committed.set(Math.max(checkpoint.commitSeq(), active.lastAppliedCommitSeq()));
            log.info("从 checkpoint 续传，checkpoint={} 目标已确认={}",
                    checkpoint.commitSeq(), active.lastAppliedCommitSeq());
        }
        resumePendingRebuild(checkpoint.commitSeq());
        try {
            runIncremental(startFrom);
        } catch (SnapshotInterruptedException e) {
            running = false;
            log.warn("快照阶段中断退出（crashed={}），现场保留，重启后续跑", crashed);
        }
    }

    /** 快照阶段被中断的内部标记：不是同步失败，而是“留现场、下次续跑”。 */
    private static final class SnapshotInterruptedException extends RuntimeException {
    }

    /**
     * 若重建协调器磁盘上存在未完成影子库（进程曾在重建中终止），恢复重建追赶上下文：
     * 影子库由协调器构造时自行恢复，此处只恢复内存中的高水位/目标/进度。
     */
    private void resumePendingRebuild(long checkpointSeq) {
        RebuildCoordinator c = rebuildCoordinator;
        if (c == null || !c.isRebuilding()) {
            return;
        }
        long hw = c.snapshotHighWatermark();
        long target = c.rebuildTargetSeq();
        if (target < 0) {
            // 进程在“影子已建、追赶目标尚未落 manifest”窗口终止：无法安全确定切换上界，
            // 按现场不一致拒绝，避免在错误位点切换
            throw new SyncException(RejectReason.INIT_STATE_MISMATCH,
                    "未完成重建缺少追赶上界 manifest（highWatermark=" + hw + "），拒绝续传");
        }
        this.rebuildTargetSeq = target;
        // 影子进度从其持久化现场恢复；checkpoint 在影子 fan-out 之后，故 >= 影子进度
        this.rebuildShadowSeq = Math.max(hw, c.shadowProgressSeq());
        log.warn("重建中断后续传：highWatermark={} 影子进度={} 追赶上界={}",
                hw, rebuildShadowSeq, rebuildTargetSeq);
    }

    /** 快照阶段，返回增量起始位点（= 快照 highWatermark）。可在中断后幂等续跑。 */
    private long runSnapshotPhase() {
        Snapshot snapshot = source.readSnapshot(0);
        long hw = snapshot.highWatermark().commitSeq();
        int schemaVersion = snapshot.schema().version();
        initStateStore.markInProgress();
        LocalTargetStore active = activeTarget();
        if (!active.isInitialized()) {
            log.info("快照阶段开始：schema=v{} 行数={} highWatermark={}",
                    schemaVersion, snapshot.rows().size(), hw);
            active.initialize(source.schemaAt(1));
        } else {
            log.warn("快照阶段续跑：目标库已有初始 schema，历史 DDL 与行按幂等规则补登记/补写");
        }
        // 把快照前的全部历史演进按序登记，目标端才能翻译携带旧版本号的快照行
        for (int v = 2; v <= schemaVersion; v++) {
            if (active.currentSchemaVersion() < v) {
                SchemaChange change = source.schemaChangeTo(v);
                active.registerSchemaChange(change);
                stats.recordSchemaChange();
            }
        }
        int rowIndex = 0;
        java.util.function.IntPredicate hook = config.snapshotInterruptHook();
        for (RowData row : snapshot.rows()) {
            if (!running) {
                // 快照阶段被 simulateCrash/stop 中断：标记与已落盘行保留，下次启动续跑
                log.warn("快照阶段在第 {}/{} 行被中断，保留现场供续跑", rowIndex,
                        snapshot.rows().size());
                throw new SnapshotInterruptedException();
            }
            applySnapshotRow(row);
            rowIndex++;
            if (hook != null && hook.test(rowIndex - 1)) {
                // 测试钩子：行落盘后立刻模拟进程崩溃（WAL 已 fsync、初始化标记仍在）
                log.warn("快照中断钩子在第 {} 行触发，模拟进程崩溃", rowIndex);
                crashed = true;
                running = false;
                throw new SnapshotInterruptedException();
            }
        }
        checkpointStore.store(new SourcePosition(hw));
        committed.set(hw);
        // 全部行落盘且位点提交后才清除初始化标记：任何更早的崩溃都只会留下可续跑现场
        initStateStore.clear();
        log.info("快照阶段完成并提交位点 highWatermark={}，切换增量", hw);
        return hw;
    }

    private void applySnapshotRow(RowData row) {
        LocalTargetStore active = activeTarget();
        // 快照中的行可能携带旧 schemaVersion（快照前发生过 DDL），目标端按登记版本翻译
        String key = String.valueOf(
                row.values().get(source.schemaAt(row.schemaVersion()).keyColumn()));
        ChangeEvent insert = new ChangeEvent(OpType.INSERT, key, row,
                new SourcePosition(row.commitSeq()), row.schemaVersion());
        ApplyOutcome outcome = active.apply(insert);
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
     *
     * <p>在线重建进行中时，同一协调线程在当前库提交之后、位点提交之前把本批变更
     * fan-out 到影子库（严格串行、按源端顺序、按位点幂等）。影子追平约定上界后在此
     * 提交点执行唯一一次原子切换：之后的写入立即落到新 current。任何更早的崩溃都只留下
     * “未完成影子”，重启续跑，读取方始终只看到某个完整库。</p>
     */
    private void processBatch(ChangeBatch batch) throws InterruptedException {
        RebuildCoordinator coordinator = rebuildCoordinator;
        boolean rebuilding = coordinator != null && coordinator.isRebuilding();
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
        if (rebuilding) {
            fanOutToShadow(coordinator, batch, to);
        }
        // 位点提交严格晚于数据 fsync（含影子 fan-out）：这是崩溃恢复的重放下界
        checkpointStore.store(new SourcePosition(to));
        committed.set(to);
        log.debug("批次完成 batchId={} 区间=({}] DML={} 已提交={}", batch.batchId(),
                batch.from().commitSeq(), to, events.size(), to);
    }

    /**
     * 把当前批次中 highWatermark 之后的变更按源端顺序补到影子库（协调线程串行执行）。
     * DDL 与 DML 均按位点幂等；影子进度追平本次重建约定上界后执行唯一一次原子切换。
     */
    private void fanOutToShadow(RebuildCoordinator coordinator, ChangeBatch batch, long to) {
        long hw = coordinator.snapshotHighWatermark();
        // 合并 DDL 与 DML 到一条按位点有序的流：源端日志中二者共用全局 commitSeq
        List<Object> ordered = new java.util.ArrayList<>(
                batch.schemaChanges().size() + batch.events().size());
        ordered.addAll(batch.schemaChanges());
        ordered.addAll(batch.events());
        ordered.sort(java.util.Comparator.comparingLong(o -> {
            if (o instanceof ChangeBatch.SchemaChangeAt d) {
                return d.position().commitSeq();
            }
            return ((ChangeEvent) o).position().commitSeq();
        }));
        for (Object o : ordered) {
            if (o instanceof ChangeBatch.SchemaChangeAt d) {
                long seq = d.position().commitSeq();
                if (seq > hw) {
                    coordinator.registerShadowSchemaChange(d.change(), seq);
                    rebuildShadowSeq = Math.max(rebuildShadowSeq, seq);
                }
            } else {
                ChangeEvent e = (ChangeEvent) o;
                long seq = e.position().commitSeq();
                if (seq > hw) {
                    ApplyOutcome outcome = coordinator.applyShadow(e);
                    if (outcome == ApplyOutcome.APPLIED) {
                        stats.recordApplied();
                    } else if (outcome == ApplyOutcome.DUPLICATE_SKIPPED) {
                        stats.recordDuplicate(new SkipRecord(batch.batchId(), e.position(),
                                e.key(), RejectReason.ALREADY_APPLIED,
                                "影子库重建 fan-out 重复位点（重试/重启续传）"));
                    }
                    rebuildShadowSeq = Math.max(rebuildShadowSeq, seq);
                }
            }
        }
        // 切换判定点：本批已把 (HW, to] 补进影子。切换必须同时满足：
        //  1) to >= 约定的追赶上界（下界）；
        //  2) to >= 此刻源端最新提交 —— 影子已追到实时尾部。
        // 若判定后仍有更新提交进来，它们走切换后的常规流水线直接落到新 current，
        // 既不丢也不重复；整个重建仍只发生一次切换。
        if (rebuildTargetSeq >= 0 && to >= rebuildTargetSeq
                && to >= source.latestCommitSeq()
                && rebuildShadowSeq >= rebuildTargetSeq) {
            long seqAtSwap = committed.get();
            coordinator.commitSwap();
            rebuildTargetSeq = -1L;
            rebuildShadowSeq = -1L;
            // 切换后当前库（原影子）可能领先于旧 current：位点对齐到实际落库进度，
            // 保证“已确认位点不超过已落稳数据”在切换后依然成立
            committed.set(Math.max(seqAtSwap, coordinator.current().lastAppliedCommitSeq()));
            log.info("在线重建原子切换完成（唯一一次），切换位点={} 当前已确认={}",
                    seqAtSwap, committed.get());
        }
    }

    /**
     * 同步持续运行期间就地发起一次全量重建（不停同步）。
     *
     * @param targetSeq 重建追赶上界：影子必须追平到该源端位点才允许切换；
     *                  调用方通常传“发起时源端最新位点”，重建期间写入的增量会自动追平
     */
    public void requestOnlineRebuild(long targetSeq) {
        RebuildCoordinator coordinator = rebuildCoordinator;
        if (coordinator == null) {
            throw new SyncException(RejectReason.REBUILD_NOT_ACTIVE,
                    "引擎未绑定重建协调器，无法发起在线重建");
        }
        synchronized (coordinator) {
            if (coordinator.isRebuilding()) {
                throw new SyncException(RejectReason.REBUILD_NOT_ACTIVE,
                        "已有重建进行中（targetSeq=" + rebuildTargetSeq + "），不能重复发起");
            }
            // 重建快照取此刻源端一致性视图；快照并发写入的 commitSeq 必然 > 快照 HW
            Snapshot snapshot = source.readSnapshot(0);
            List<SchemaChange> history = new java.util.ArrayList<>();
            for (int v = 2; v <= snapshot.schema().version(); v++) {
                history.add(source.schemaChangeTo(v));
            }
            coordinator.startOrResume(snapshot, source.schemaAt(1), history);
            long effectiveTarget = Math.max(targetSeq, snapshot.highWatermark().commitSeq());
            coordinator.setRebuildTargetSeq(effectiveTarget);
            this.rebuildTargetSeq = effectiveTarget;
            // 影子初始只含快照（进度 = HW）。当前在线库通常已同步得更远；
            // fan-out 从 HW 之后开始，必须等到确实追过 HW 的批次才可能切换，
            // 因此初始进度取 HW，绝不能取在线库已确认位点。
            this.rebuildShadowSeq = snapshot.highWatermark().commitSeq();
            log.info("在线重建已发起：快照 highWatermark={} 追赶上界={} 影子初始进度={}",
                    snapshot.highWatermark().commitSeq(), effectiveTarget, rebuildShadowSeq);
        }
    }

    private void applySchemaChange(ChangeBatch batch, ChangeBatch.SchemaChangeAt ddl) {
        try {
            int newVersion = activeTarget().registerSchemaChange(ddl.change());
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
            ApplyOutcome outcome = activeTarget().apply(event);
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

    /** 引擎是否仍在运行（目标端长期落后、等待式背压期间必须持续为 true）。 */
    public boolean isRunning() {
        return running;
    }

    /** 读取线程已拉取并交付缓冲的最新位点。 */
    public long readerPosition() {
        return readerFrom;
    }

    /** 内存缓冲当前积压批次数（落后期间的可观测指标）。 */
    public int backlogBatches() {
        BoundedEventQueue q = queue;
        return q == null ? 0 : q.size();
    }

    /** 内存缓冲容量。 */
    public int bufferCapacity() {
        BoundedEventQueue q = queue;
        return q == null ? config.bufferCapacity() : q.capacity();
    }

    /** 读取线程累计尝试投递的批次数。 */
    public long totalBatchesOffered() {
        BoundedEventQueue q = queue;
        return q == null ? 0 : q.totalOffered();
    }

    /** 读取线程因 REJECT 背压累计被拒绝的批次数（可恢复限流计数）。 */
    public long totalBatchesRejected() {
        BoundedEventQueue q = queue;
        return q == null ? 0 : q.totalRejected();
    }

    /**
     * 源端最新位点与已确认位点的差值（事件积压量）。
     * 目标端落后期间此值持续为正，等待式背压应让同步存活而非终止。
     */
    public long lagSeq() {
        return Math.max(0L, source.latestCommitSeq() - committed.get());
    }

    /** 统计信息。 */
    public SyncStats stats() {
        return stats;
    }
}
