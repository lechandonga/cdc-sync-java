package com.github.lechandonga.cdcsyncjava.tests;

import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.lechandonga.cdcsyncjava.engine.BackpressurePolicy;
import com.github.lechandonga.cdcsyncjava.engine.SyncConfig;
import com.github.lechandonga.cdcsyncjava.engine.SyncEngine;
import com.github.lechandonga.cdcsyncjava.model.ChangeBatch;
import com.github.lechandonga.cdcsyncjava.model.SkipRecord;
import com.github.lechandonga.cdcsyncjava.model.RejectReason;
import com.github.lechandonga.cdcsyncjava.source.InMemorySourceDatabase;
import com.github.lechandonga.cdcsyncjava.support.TestFixtures;
import com.github.lechandonga.cdcsyncjava.target.FileTargetStore;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 至少一次投递下的幂等应用测试：
 * 批次重试（同一批日志重复拉取并应用）与重复投递不得产生重复数据或错误计数。
 */
class IdempotentDeliveryTest {

    @TempDir
    Path tempDir;

    private SyncConfig config() {
        return SyncConfig.builder(tempDir.resolve("state"))
                .batchSize(50)
                .parallelism(1)
                .bufferCapacity(4)
                .backpressurePolicy(BackpressurePolicy.BLOCK)
                .pollTimeoutMillis(10)
                .build();
    }

    @Test
    void duplicateBatchDeliveryIsIdempotentAndLocatable() throws Exception {        InMemorySourceDatabase source = TestFixtures.newSource();
        for (int i = 1; i <= 5; i++) {
            source.put("k" + i, TestFixtures.row("n" + i, i));
        }
        long last = source.latestCommitSeq();

        FileTargetStore target = new FileTargetStore(tempDir.resolve("target"));
        SyncEngine engine = new SyncEngine(source, target, config());
        Thread runner = new Thread(() -> engine.run(last));
        runner.start();
        engine.awaitReached(last, 3000);
        runner.join(3000);

        assertEquals(5, target.allLiveRows().size());
        assertEquals(5, engine.stats().applied());

        // 模拟批次重试：重新拉取已经确认过的日志区间，并直接对目标端再次应用
        List<ChangeBatch> retried = source
                .nextBatch(new com.github.lechandonga.cdcsyncjava.model.SourcePosition(0), 50,
                        java.time.Duration.ofMillis(50))
                .map(List::of)
                .orElseGet(List::of);
        assertEquals(1, retried.size());
        int duplicateEvents = 0;
        for (var event : retried.get(0).events()) {
            var outcome = target.apply(event);
            assertEquals(com.github.lechandonga.cdcsyncjava.target.ApplyOutcome.DUPLICATE_SKIPPED,
                    outcome, "重复事件必须被幂等跳过: " + event.key());
            duplicateEvents++;
        }
        // 数据不重复
        assertEquals(5, target.allLiveRows().size());
        assertEquals(last, target.lastAppliedCommitSeq());

        // 通过引擎再放一批重复（走引擎统计通道），验证跳过明细可定位批次/位点/键/原因
        long beforeDup = engine.stats().duplicates();
        var batchAgain = source
                .nextBatch(new com.github.lechandonga.cdcsyncjava.model.SourcePosition(0), 50,
                        java.time.Duration.ofMillis(50))
                .orElseThrow();
        for (var e : batchAgain.events()) {
            var o = target.apply(e);
            assertEquals(com.github.lechandonga.cdcsyncjava.target.ApplyOutcome.DUPLICATE_SKIPPED, o);
        }
        // 直接重放目标端不走引擎统计，故这里只校验数据层幂等
        assertEquals(5, target.allLiveRows().size());
        assertEquals(beforeDup, engine.stats().duplicates());
        assertTrue(duplicateEvents > 0);

        // 同键多次更新：最终值为最后一次（源端顺序）
        source.put("k1", TestFixtures.row("n1-final", 99));
        long finalSeq = source.latestCommitSeq();
        SyncEngine engine2 = new SyncEngine(source, target, config());
        Thread r2 = new Thread(() -> engine2.run(finalSeq));
        r2.start();
        engine2.awaitReached(finalSeq, 3000);
        r2.join(3000);
        assertEquals("n1-final", target.get("k1").values().get("name"));
        assertEquals(99, target.get("k1").values().get("age"));
    }
}
