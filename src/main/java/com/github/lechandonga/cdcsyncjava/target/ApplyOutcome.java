package com.github.lechandonga.cdcsyncjava.target;

/** 单条变更应用到目标端的结果。 */
public enum ApplyOutcome {
    /** 首次应用成功。 */
    APPLIED,
    /** 重复投递，位点已应用，幂等跳过。 */
    DUPLICATE_SKIPPED,
    /** 因给定原因拒绝（不写入）。 */
    REJECTED
}
