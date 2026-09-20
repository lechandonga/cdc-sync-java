# 本地可变数据源增量同步引擎

内置「源端适配器」与「目标端适配器」，在不依赖外部数据库/消息队列的前提下，模拟一条真实的
**一致性快照 → 增量捕获（CDC）** 同步链路。目标端使用本地文件（WAL + 原子位点 + 原子换表）
保证进程故障后可恢复，因此可以通过“同一目录重新构造引擎”真实模拟进程重启。

## 包结构

| 包 | 关键类型 | 职责 |
|----|----------|------|
| `model` | `ChangeEvent` `Snapshot` `SnapshotRow` `SchemaDef` `FieldDef` | 变更事件、快照、模式等领域模型 |
| `source` | `SourceAdapter` / `LocalMutableSource` | 只增变更日志、全局提交序号、as-of P 一致性快照 |
| `target` | `TargetAdapter` / `LocalTarget` / `Checkpoint` / `RebuildSession` | WAL 幂等写入、原子位点、崩溃重放、原子全量重建 |
| `schema` | `SchemaManager` `SchemaChange` | 模式版本历史、兼容演进校验、旧版本事件投影 |
| `engine` | `SyncEngine` `SnapshotCoordinator` `IncrementalCoordinator` `ParallelKeyApplier` `BoundedEventBuffer` | 快照/增量编排、按键分区并行、有界缓冲与背压 |
| `error` | `RejectReason` `SkippedEvent` `SkippedLedger` `*Exception` | 可区分拒绝原因、可定位的跳过台账 |
| `config` | `SyncConfig` | 批量大小、并发度、缓冲容量、背压策略与超时 |

## 关键设计如何对应需求

1. **快照 → 增量无衔接缺口**
   - `LocalMutableSource` 的所有写入在同一把日志锁内“分配全局序号 + 追加日志”，
     故日志顺序严格等于提交序号顺序（并发写入也不会错序）。
   - 快照取边界位点 P 并在日志锁内回放得到严格 as-of P 视图；快照期间产生的变更 seq>P
     不进入快照，但由增量阶段（seq>P）完整覆盖。
   - 快照行编码为 `seq=lastSeq(≤P)` 的幂等 UPSERT；快照与增量边界重叠时由目标端
     `(key,lastSeq)` 幂等表拦截，不重复、不丢失、不错序。

2. **按键保序、异键并行、结果确定**
   - `ParallelKeyApplier` 先整批做模式投影/校验，再按 `hash(key)` 分到固定单线程分区：
     同键严格按 seq 串行，异键并行；整批成功后位点才推进，结果确定、可复现。

3. **模式演进**
   - 支持新增字段、重命名（保留位置与类型）、删除字段；`SchemaManager` 按版本链把旧事件
     投影到当前字段名（重命名链可传递，删除字段丢弃，新增字段在旧事件中缺失）。
   - 不兼容演进（重名 / 源不存在 / 目标已存在 / 类型不匹配）抛 `SchemaEvolutionException`，
     带可区分 `RejectReason`，且版本历史保持不变，不污染已有数据。

4. **至少一次投递的幂等应用**
   - 目标端以 `(key,lastSeq)` 去重：`seq==lastSeq` → `ALREADY_APPLIED`，`seq<lastSeq`
     → `OBSOLETE_ORDER`；重复投递、批次重试、进程重启都不会产生重复数据或错误计数。
   - 每条跳过事件以 `SkippedEvent(eventId,key,seq,batchId,reason,detail)` 进入
     `SkippedLedger`，可定位到具体记录与原因。

5. **断点续传 / 崩溃恢复**
   - 写入顺序：先 append WAL（fsync）→ 更新内存表；位点文件 temp + 原子 rename。
     数据永远先于位点落盘。
   - 重启：表快照 + WAL 重放恢复；超过已确认位点的事件被回滚（不算已处理），
     源端从旧位点重读时被幂等拦截——已确认不丢、未确认不冒认。

6. **有界缓冲与背压**
   - `BoundedEventBuffer`（`ArrayBlockingQueue`）容量有界；策略 `BLOCK`（限流等待，超时回退
     本轮读取稍后重试，不丢事件）或 `REJECT`（立即抛 `BackpressureException`，原因
     `BUFFER_FULL`）。批量大小、并发度、缓冲容量均可在 `SyncConfig` 配置。

7. **不影响读取方的在线全量重建**
   - `beginRebuild` 创建隔离的影子暂存区；提交时在目标写锁内“原子换表 + 压缩 WAL +
     原子提交位点”，读方任何时刻只见旧表或新表。abort 或崩溃遗留的暂存区在重启时清理，
     可安全重试；位点中持久化 `rebuilds` 计数。

日志在关键点包含**位点（position）、批次（batchId）、键（key）/序号（seq）**信息，
便于解释与排障。

## 测试

以需求点组织，共 26 个用例：

- `SnapshotIncrementalHandoffTest`：快照/增量衔接 + 快照期间并发写入完整覆盖
- `IdempotentDeliveryTest`：重复投递、批次重试、过期乱序、非法负载拒绝原因
- `SchemaEvolutionTest`：新增/重命名/删除投影、重命名链、不兼容演进拒绝且不污染
- `CrashRecoveryTest`：进程重启、WAL 已写位点未提、快照中断重跑
- `BoundedBufferBackpressureTest`：REJECT / BLOCK、FIFO、参数可配置
- `OnlineRebuildTest`：原子切换、读方不见半成品、中断/崩溃后可安全重试
- `ConcurrentKeyConflictTest`：多写线程同键保序、确定性可复现、upsert/delete 混合

运行：`mvn test`
