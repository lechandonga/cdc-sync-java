package com.github.lechandonga.cdcsyncjava.engine;

/** 缓冲耗尽时的背压策略。 */
public enum BackpressurePolicy {
    /** 阻塞拉取线程直到有空位（限流）。 */
    BLOCK,
    /** 立即拒绝本次拉取并抛出可区分异常。 */
    REJECT
}
