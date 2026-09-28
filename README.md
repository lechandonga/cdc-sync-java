# CDC Sync Java Bootstrap — 本地可变数据源增量同步引擎

一套以内置源端 / 目标端适配器模拟真实同步的增量同步引擎，保证全量快照与增量变更在
**并发写入、模式演进、进程故障**下收敛到一致且可解释的结果，并支持**在线全量重建、
初始化中断续跑、目标端持续落后下的等待式背压**等贴近线上的场景，全部可在本地复现验证。

## 模块结构

| 包 | 职责 |
|---|---|
| `model` | 表结构、行、变更事件 / 批次、位点、结构演进、拒绝原因、跳过记录等不可变模型 |
| `source` | `LocalSourceDatabase` 及其内存实现：提交日志 + 一致性快照 + 增量日志读取 |
| `target` | `LocalTargetStore` 文件 WAL 实现（fsync 持久化、按位点幂等、删除墓碑）；`RebuildCoordinator` 影子重建与原子切换；`SwappableTargetStore` 运行期热替换 |
| `engine` | `SyncEngine` 快照→增量流水线、`SchemaRegistry` 版本登记与翻译、`BoundedEventQueue` 有界背压、`CheckpointStore` 原子位点、`InitProgressStore` 初始化进度标记、`OnlineRebuildService` 在线重建驱动、`SyncStats` 可定位统计 |

## 关键语义如何保证

1. **快照与增量衔接（不重 / 不丢 / 不乱序）**
   - 源端所有 DML/DDL 在同一把提交锁下分配全局单调 `commitSeq` 并追加日志；
   - 快照在锁内确定 `highWatermark` 与行镜像；增量严格从 `highWatermark` 之后读取；
   - 快照期间的并发提交 `commitSeq` 必然大于 highWatermark，由增量阶段完整覆盖；
   - 快照阶段会把 v1..快照版本的历史 DDL 全部登记到目标端，快照行携带的旧版本号可被翻译。

2. **按键保序 + 不同键并行（确定可复现）**
   - 每批 DML 按 `hash(key) mod parallelism` 分区，同一键始终由同一 worker 串行应用；
   - 目标端幂等判断按**每键位点**（不是全局水位），杜绝并行交错导致的误判；
   - 同键多次更新最终值严格等于源端最后一次提交。

3. **模式演进（新增 / 重命名 / 删除 / 拒绝改型）**
   - DDL 在协调线程以 barrier 方式先于同批 DML 登记；
   - 旧结构事件按登记的逐版本列映射翻译（新增补 null、改名平移、删除移除）；
   - 不兼容演进（重名新增、未知列改名/删除、名字冲突、列改型、新增非空列）抛出带
     `RejectReason` 的异常，校验失败不写任何状态、不污染已有数据。

4. **至少一次投递的幂等应用**
   - 目标端以 `commitSeq` 去重：重复批次、批次重试、重启重放返回 `DUPLICATE_SKIPPED`；
   - 每条跳过通过 `SkipRecord(batchId, position, key, reason, detail)` 可定位到具体记录。

5. **位点一致性与崩溃恢复**
   - 提交协议：目标 WAL 先 `force(true)` fsync，批次全部成功后才原子写 checkpoint；
   - 因此 checkpoint ≤ 已持久化数据位点；崩溃后重放缺口，重复部分幂等跳过；
   - WAL 区分两类数据记录：`D`（增量事件，按键 commitSeq 严格幂等、同键 seq 回退即判坏尾）
     与 `P`（快照镜像行，按“该键是否已物化”幂等——快照行携带的是该行自己的提交序号，
     可能大于快照高水位，不能按 seq 比较）；初始化中断续跑时重复的快照行安全跳过、不重写；
   - WAL 恢复先在隔离内存状态中重放（坏尾截断绝不污染对外视图），成功后一次性生效；
     追加位置严格对齐文件实际字节尾，避免空洞覆盖；
   - `SyncEngine.simulateCrash()` 立即中断线程并丢弃内存队列，模拟进程故障。

6. **有界缓冲与背压**
   - 容量固定；`BLOCK`（落后期间**持续等待**的限流，见第 8 节）与 `REJECT`（立即抛 `QUEUE_FULL`）两策略；
   - 批量大小、并发度、缓冲容量、超时均可在 `SyncConfig` 配置。

7. **在线全量重建**
   - 重建写入 `shadow/`，读取方始终读 `current/`；manifest 记录快照 highWatermark **与增量追赶位点 caughtUpTo**；
   - 影子库按序登记快照覆盖的全部历史结构版本（v1..快照版本），旧版本快照行可翻译；
   - 重建开始后源端继续写入的增量（含 DDL）由 `OnlineRebuildService` 从日志按序追平，
     DDL 带版本号幂等登记、DML 按 commitSeq 幂等，追赶进度逐批 fsync 进 manifest；
   - 可随时中断（保留影子目录），重启后识别并从 caughtUpTo 续传，重复应用幂等，重复多次状态不乱；
   - `commitSwap()` 以目录原子 rename 切换，`SyncEngine` 在**批次边界屏障**处一次性热替换底层存储
     （`SwappableTargetStore`）并对齐位点，读取方只会看到旧库或新库，绝无半成品，且只切换一次。

8. **等待式背压（目标端持续落后）**
   - `BLOCK` 策略下缓冲满时读取方持续等待（按超时分片轮询、响应中断），
     目标端落后多久同步就存活多久，**不会因“暂时处理不过来”而自行终止**；
   - 等待期间批次停留在读取位点、缓冲占用始终有界，不丢批次；协调侧按批次提交，不重复应用；
   - 已确认位点严格不超过目标端实际 fsync 位点（提交协议保证），进度不会跑到未落稳数据前面；
   - 可区分三类情况：等待式背压（最终收敛）、`QUEUE_FULL`（REJECT 下可恢复限流）、
     `TARGET_UNRECOVERABLE`（真的不可恢复，终止同步并上抛）；
   - 积压与背压可观测：`backlogBatches()`、`maxBacklogBatches()`、`sourceLag()`、
     `readerPosition()`、`backpressureWaits()`、`backpressureWaitMillis()`。

9. **初始化中断续跑与启动现场校验**
   - 快照阶段开始前先把快照 highWatermark 原子写入 `init-progress.properties`，
     快照最后一个完整批次提交后再删除：标记存在即“上次初始化未完成”，重启从该高水位续跑，
     快照行按 commitSeq 幂等、历史 DDL 按版本号幂等登记，**无论重启多少次都从断点继续到完成，
     不会每次失败，也不会把半成品当成已完成跳过**；
   - 启动时现场对不上则拒绝启动并给出可区分原因，不写任何状态、不污染已有数据：
     `STATE_CHECKPOINT_AHEAD_OF_DATA`（位点超前于目标端实际数据）、
     `TARGET_STATE_CORRUPT`（目标 WAL 初始记录损坏/无法识别）。

## 适用边界

- 内置源端 / 目标端为本地文件 + 内存模拟，用于语义验证；接入真实数据库时需提供等价适配器
  （提交日志线性化、一致性快照、按 commitSeq 幂等应用、原子目录 rename）。
- 在线原子切换依赖文件系统的**同目录原子 rename**；`OnlineRebuildService` 的引擎热替换要求
  `SyncEngine` 使用 `SwappableTargetStore` 且采用 `BLOCK` 背压（REJECT 下切换窗口可能拒批，故拒绝执行）。
- 结构演进仅支持新增可空列 / 重命名 / 删除列；列改型与新增非空列一律拒绝（不做隐式转换、不回填默认值）。
- 在线重建是单实例单影子库：同一时刻只有一次重建；并发发起重建由 `RebuildCoordinator` 串行化为续传。
- 背压只限制内存占用，不做落盘溢写；源端日志需保留到影子库追平位点（重建期间不能截断源端历史）。

## 日志可观测性

关键日志均带 **位点 / 批次 / 键**：
- 批次完成：`batchId / 区间 (from,to] / DML 数 / 已提交位点`
- DML 应用：`batchId / 位点 / key / op`
- 结构演进：`batchId / 位点 / 类型 / 新版本号`
- 背压：`容量 / 积压 / batchId / 位点区间 / 等待次数与累计时长`
- 在线重建：`快照高水位 / 追赶位点 / 切换后新库位点`
- 初始化：`检测到未完成标记 / 续跑高水位 / 完成并清除标记`

## 测试

`src/test/java/.../tests` 覆盖 23 个用例：
快照-增量衔接、同键并发顺序、重复投递幂等、模式演进（含快照并发 DDL）、
不兼容 DDL 流式拒绝、崩溃恢复、完整进程重启、跳过记录定位、有界背压（BLOCK/REJECT）、
影子全量重建中断续传，以及本次新增的三类线上场景：

| 新增测试类 | 覆盖行为 |
|---|---|
| `OnlineRebuildConcurrencyTest` | 同步持续运行时在线重建 + 重建期并发 DML/DDL；读取方全程只见完整库；只切换一次；结果与独立“正常全量+增量”逐键一致；重建中途打断后从 manifest 位点续传完成 |
| `InitializationRecoveryTest` | 初始化期含结构变化时快照阶段被杀，多次重启从断点续跑到完成，不把半成品当完成跳过；位点超前数据（`STATE_CHECKPOINT_AHEAD_OF_DATA`）与损坏 WAL（`TARGET_STATE_CORRUPT`）拒绝启动且不污染数据 |
| `SustainedBackpressureTest` | 目标端长时间落后时 BLOCK 背压全程有效、同步不自行终止、积压有界、进度不超前落稳数据、积压/背压指标可观测；不可恢复失败以 `TARGET_UNRECOVERABLE` 区别于暂时背压 |

```bash
mvn test
```

