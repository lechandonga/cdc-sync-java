# CDC Sync Java Bootstrap — 本地可变数据源增量同步引擎

一套以内置源端 / 目标端适配器模拟真实同步的增量同步引擎，保证全量快照与增量变更在
**并发写入、模式演进、进程故障**下收敛到一致且可解释的结果。

## 模块结构

| 包 | 职责 |
|---|---|
| `model` | 表结构、行、变更事件 / 批次、位点、结构演进、拒绝原因、跳过记录等不可变模型 |
| `source` | `LocalSourceDatabase` 及其内存实现：提交日志 + 一致性快照 + 增量日志读取 |
| `target` | `LocalTargetStore` 文件 WAL 实现（fsync 持久化、按位点幂等、删除墓碑）；`RebuildCoordinator` 影子重建与原子切换 |
| `engine` | `SyncEngine` 快照→增量流水线、`SchemaRegistry` 版本登记与翻译、`BoundedEventQueue` 有界背压、`CheckpointStore` 原子位点、`SyncStats` 可定位统计 |

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
   - WAL 恢复时对未 fsync 的尾部 / 半行做截断，未确认数据不会被当作已处理；
   - `SyncEngine.simulateCrash()` 立即中断线程并丢弃内存队列，模拟进程故障。

6. **有界缓冲与背压**
   - 容量固定；`BLOCK`（限时等待的限流）与 `REJECT`（立即抛 `QUEUE_FULL`）两策略；
   - 批量大小、并发度、缓冲容量、超时均可在 `SyncConfig` 配置。

7. **在线全量重建**
   - 重建写入 `shadow/`，读取方始终读 `current/`；manifest 记录快照 highWatermark；
   - 可随时中断（保留影子目录），重启后识别并续传，重复应用幂等；
   - `commitSwap()` 以目录原子 rename 切换，读取方只会看到旧库或新库，绝无半成品。

## 日志可观测性

关键日志均带 **位点 / 批次 / 键**：
- 批次完成：`batchId / 区间 (from,to] / DML 数 / 已提交位点`
- DML 应用：`batchId / 位点 / key / op`
- 结构演进：`batchId / 位点 / 类型 / 新版本号`
- 背压拒绝：`容量 / 积压 / batchId / 位点区间`

## 测试

`src/test/java/.../tests` 覆盖 16 个用例：
快照-增量衔接、同键并发顺序、重复投递幂等、模式演进（含快照并发 DDL）、
不兼容 DDL 流式拒绝、崩溃恢复、完整进程重启、跳过记录定位、有界背压（BLOCK/REJECT）、
影子全量重建中断续传。

```bash
mvn test
```
