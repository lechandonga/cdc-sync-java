package com.github.lechandonga.cdcsyncjava.source;

import java.time.Duration;
import java.util.Optional;

import com.github.lechandonga.cdcsyncjava.model.ChangeBatch;
import com.github.lechandonga.cdcsyncjava.model.SourcePosition;

/** 增量日志读取器：从位点之后拉取变更。 */
public interface LogReader {

    /**
     * 拉取一批变更。
     *
     * @param after 位点（不含），只返回 commitSeq 严格大于该位点的变更
     * @param maxBatchSize 批次最大事件数
     * @param waitTimeout 无数据时最长等待时间
     * @return 变更批次；等待超时仍无数据返回 empty
     */
    Optional<ChangeBatch> nextBatch(SourcePosition after, int maxBatchSize, Duration waitTimeout);
}
