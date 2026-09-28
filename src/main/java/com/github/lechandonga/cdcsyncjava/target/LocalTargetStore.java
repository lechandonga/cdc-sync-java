package com.github.lechandonga.cdcsyncjava.target;

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

    /** 是否已完成初始 schema 写入（WAL 中存在 I 记录；崩溃后由 WAL 恢复判定）。 */
    boolean isInitialized();

    /**
     * 登记一次结构演进，产生新的 schema 版本。
     * 不兼容时抛出带 RejectReason 的异常，且不得改变已有数据。
     *
     * @return 演进后的新版本号
     */
    int registerSchemaChange(SchemaChange change);

    /** 应用一条数据变更（按 commitSeq 幂等）。 */
    ApplyOutcome apply(ChangeEvent event);

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
