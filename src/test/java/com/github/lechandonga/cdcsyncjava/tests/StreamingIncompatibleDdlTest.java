package com.github.lechandonga.cdcsyncjava.tests;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.lechandonga.cdcsyncjava.engine.BackpressurePolicy;
import com.github.lechandonga.cdcsyncjava.engine.SyncConfig;
import com.github.lechandonga.cdcsyncjava.engine.SyncEngine;
import com.github.lechandonga.cdcsyncjava.engine.SyncException;
import com.github.lechandonga.cdcsyncjava.model.RejectReason;
import com.github.lechandonga.cdcsyncjava.model.ColumnType;
import com.github.lechandonga.cdcsyncjava.model.SchemaChange;
import com.github.lechandonga.cdcsyncjava.source.InMemorySourceDatabase;
import com.github.lechandonga.cdcsyncjava.support.TestFixtures;
import com.github.lechandonga.cdcsyncjava.target.FileTargetStore;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 增量流中的不兼容结构演进：
 * 引擎必须以可区分原因拒绝，已同步数据完好，位点停在 DDL 之前，进程修复后可续传。
 */
class StreamingIncompatibleDdlTest {

    @TempDir
    Path tempDir;

    @Test
    void incompatibleDdlInStreamFailsWithReasonAndLeavesDataIntact() throws Exception {
        InMemorySourceDatabase source = TestFixtures.newSource();
        source.put("k1", TestFixtures.row("alice", 10));
        source.put("k2", TestFixtures.row("bob", 20));
        FileTargetStore target = new FileTargetStore(tempDir.resolve("target"));
        SyncConfig cfg = SyncConfig.builder(tempDir.resolve("state"))
                .batchSize(2).parallelism(2).bufferCapacity(4)
                .backpressurePolicy(BackpressurePolicy.BLOCK).pollTimeoutMillis(2).build();
        SyncEngine engine = new SyncEngine(source, target, cfg);

        // 初始 2 条先同步完
        Thread runner = new Thread(() -> engine.run(2));
        runner.start();
        engine.awaitReached(2, 3000);
        runner.join(3000);

        // 源端接受一次列改型（数据库自身能力），但目标端按策略判定为不兼容并拒绝
        source.evolve(SchemaChange.alterType("age", ColumnType.BOOLEAN));
        source.put("k3", TestFixtures.row("carol", 30));
        long finalSeq = source.latestCommitSeq();

        SyncException failure = assertThrows(SyncException.class, () -> engine.run(finalSeq));
        assertEquals(RejectReason.INCOMPATIBLE_TYPE_CHANGE, failure.reason());
        assertTrue(failure.getMessage().contains("age"), "拒绝原因必须说明具体列: "
                + failure.getMessage());

        // 已有数据完好；坏 DDL 之后的 k3 未被写入；schema 版本未推进；checkpoint 未越过 DDL
        assertEquals("alice", target.get("k1").values().get("name"));
        assertEquals("bob", target.get("k2").values().get("name"));
        assertNull(target.get("k3"));
        assertEquals(1, target.currentSchemaVersion());
        assertEquals(2, engine.committedPosition());
        assertEquals(1, engine.stats().rejected());
    }
}
