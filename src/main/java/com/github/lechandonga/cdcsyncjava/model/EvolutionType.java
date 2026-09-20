package com.github.lechandonga.cdcsyncjava.model;

/** 源端结构演进类型。 */
public enum EvolutionType {
    ADD_COLUMN,
    RENAME_COLUMN,
    DROP_COLUMN,
    /** 列改型（例如 STRING -> INTEGER）：源端可登记，目标端按规则判定为不兼容并拒绝。 */
    ALTER_TYPE
}
