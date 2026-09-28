package com.github.lechandonga.cdcsyncjava.model;

/**
 * 可区分的拒绝 / 跳过原因。
 * 每一种失败都必须能映射到这里的某个常量，便于定位到具体记录。
 */
public enum RejectReason {
    /** 位点对应的变更已经应用过（至少一次投递下的重复数据/批次重试/重启重放）。 */
    ALREADY_APPLIED,
    /** 事件位点落后于当前已提交位点。 */
    OBSOLETE_POSITION,
    /** 数据事件携带的 schema 版本在目标端不存在。 */
    UNKNOWN_SCHEMA_VERSION,
    /** 新增列与已有列重名。 */
    INCOMPATIBLE_ADD_DUPLICATE_COLUMN,
    /** 重命名的源列不存在。 */
    INCOMPATIBLE_RENAME_UNKNOWN_COLUMN,
    /** 重命名的目标列名与已有列冲突。 */
    INCOMPATIBLE_RENAME_NAME_CONFLICT,
    /** 删除的列不存在。 */
    INCOMPATIBLE_DROP_UNKNOWN_COLUMN,
    /** 不支持的类型变更（本引擎不允许列改型）。 */
    INCOMPATIBLE_TYPE_CHANGE,
    /** 有界缓冲耗尽，按 REJECT 策略拒绝本次拉取。 */
    QUEUE_FULL,
    /** 快照读取失败。 */
    SNAPSHOT_FAILURE,
    /** 全量重建尚未开始或状态非法。 */
    REBUILD_NOT_ACTIVE,
    /** 启动时发现上一次初始化未完成（自动续跑；多次重启都不会被判为已完成）。 */
    INITIALIZATION_INCOMPLETE,
    /** 现场对不上：目标端没有任何数据，却存在已提交位点（位点跑到数据前面）。 */
    STATE_CHECKPOINT_AHEAD_OF_DATA,
    /** 现场对不上：目标端存储损坏/无法识别，拒绝启动以免污染已有数据。 */
    TARGET_STATE_CORRUPT,
    /** 目标端应用变更时发生不可恢复失败（区别于 QUEUE_FULL 这类可恢复的暂时背压）。 */
    TARGET_UNRECOVERABLE,
    /** 其它非法状态。 */
    ILLEGAL_STATE
}
