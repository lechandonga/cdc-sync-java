package com.github.lechandonga.cdcsyncjava.error;

/**
 * 可区分的拒绝 / 跳过原因。
 */
public enum RejectReason {

    // ---- 幂等 / 顺序类跳过（非错误） ----
    ALREADY_APPLIED("事件已应用，幂等跳过"),
    OBSOLETE_ORDER("事件序号落后于该键已应用序号，过期跳过"),

    // ---- 模式 / 数据类拒绝 ----
    UNKNOWN_SCHEMA_VERSION("事件引用了未知的模式版本"),
    FIELD_TYPE_MISMATCH("字段值类型与模式声明不匹配"),
    UNDECLARED_FIELD("事件携带了当前模式中不存在的字段"),
    RENAME_TARGET_EXISTS("重命名目标字段已存在"),
    RENAME_SOURCE_MISSING("重命名源字段不存在"),
    INCOMPATIBLE_TYPE_CHANGE("字段类型变更不兼容"),
    SCHEMA_VERSION_CONFLICT("模式版本冲突（不能回退或重复）"),

    // ---- 背压类拒绝 ----
    BUFFER_FULL("有界缓冲已满，按拒绝策略丢弃本轮写入"),

    // ---- 重建类 ----
    REBUILD_ABORTED("全量重建被中断，影子表已废弃");

    private final String description;

    RejectReason(String description) {
        this.description = description;
    }

    public String description() {
        return description;
    }
}
