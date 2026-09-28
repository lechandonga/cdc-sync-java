package com.github.lechandonga.cdcsyncjava.target;

import java.nio.file.Path;
import java.util.Collection;

import com.github.lechandonga.cdcsyncjava.model.ChangeEvent;
import com.github.lechandonga.cdcsyncjava.model.SchemaChange;
import com.github.lechandonga.cdcsyncjava.model.TableSchema;

/**
 * 内置目标端存储适配器。
 * 写入带位点，按位点幂等；结构演进先登记 schema，旧结构事件按登记的映射关系翻译。
 */
public interface LocalTargetStore {

    /** 初始化目标端（空库时写入第一个 schema 版本）。 */
    void initialize(TableSchema initialSchema);

    /** 目标端是否已写入初始 schema（空目录返回 false；用于启动时识别未完成的初始化）。 */
    boolean initialized();

    /**
     * 在标记“初始化未完成”时，以严格模式（初始记录损坏即抛 TARGET_STATE_CORRUPT，
     * 不静默截断）在同一目录重新打开并替换当前底层存储；非文件型实现可直接返回 this。
     */
    LocalTargetStore reopenStrict();

    /**
     * 登记一次结构演进，产生新的 schema 版本。
     * 不兼容时抛出带 RejectReason 的异常，且不得改变已有数据。
     *
     * @return 演进后的新版本号
     */
    int registerSchemaChange(SchemaChange change);

    /**
     * 按源端声明的“演进后版本号”幂等登记：重放已登记且结构一致的 DDL 时跳过；
     * 结构冲突 / 版本缺口时拒绝。用于初始化与在线重建中断后的批次重放。
     *
     * @return 登记 / 已存在的版本号
     */
    int registerSchemaChangeAt(SchemaChange change, int schemaVersionAfter);

    /** 应用一条数据变更（按 commitSeq 幂等）。 */
    ApplyOutcome apply(ChangeEvent event);

    /**
     * 应用一条快照镜像行。与 {@link #apply} 的区别：
     * <ul>
     *   <li>幂等判据是“该键是否已在目标端物化”（同一物理行重复落盘即跳过），
     *       不比较业务 commitSeq —— 快照行携带的是该行自己的提交序号，可能大于
     *       快照高水位（如最后一次更新的键），按 seq 比较会误判为新更新；</li>
     *   <li>不抬高全局已确认位点（{@code lastAppliedCommitSeq} 仍由快照高水位 /
     *       增量批次位点推进），避免“已确认进度跑到未落稳数据前面”。</li>
     * </ul>
     */
    ApplyOutcome applySnapshotRow(ChangeEvent event, long snapshotHighWatermark);

    /** 读取一行当前状态（不存在返回 null）。 */
    TargetRow get(String key);

    /** 读取全部存活（未删除）行。 */
    Collection<TargetRow> allLiveRows();

    /** 全部行（含删除墓碑）。 */
    Collection<TargetRow> allRows();

    /** 已持久化确认的最大位点（空库为 0）。 */
    long lastAppliedCommitSeq();

    /** 当前 schema 版本。 */
    int currentSchemaVersion();

    /** 取某个版本的 schema（不存在返回 null）。 */
    TableSchema schemaAt(int version);
}
