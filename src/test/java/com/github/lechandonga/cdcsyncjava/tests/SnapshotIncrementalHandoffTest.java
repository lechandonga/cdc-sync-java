package com.github.lechandonga.cdcsyncjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.lechandonga.cdcsyncjava.engine.BackpressurePolicy;
import com.github.lechandonga.cdcsyncjava.engine.SyncConfig;
import com.github.lechandonga.cdcsyncjava.engine.SyncEngine;
import com.github.lechandonga.cdcsyncjava.source.InMemorySourceDatabase;
import com.github.lechandonga.cdcsyncjava.support.TestFixtures;
import com.github.lechandonga.cdcsyncjava.target.FileTargetStore;
import com.github.lechandonga.cdcsyncjava.target.TargetRow;

/**
 * 快照与增量衔接测试：
 * 快照点之前的数据由快照覆盖，快照进行期间 / 之后的变更由增量完整覆盖；
 * 衔接阶段不得重复、丢失或错序。
 */
class SnapshotIncrementalHandoffTest {

    @TempDir
    Path tempDir;

    private SyncConfig config() {
        return SyncConfig.builder(tempDir.resolve("state"))
                .batchSize(2)
                .parallelism(4)
                .bufferCapacity(8)
                .backpressurePolicy(BackpressurePolicy.BLOCK)
                .pollTimeoutMillis(20)
                .build();
    }

    @Test
    void snapshotRowsAndIncrementalChangesConverge() throws Exception {
        InMemorySourceDatabase source = TestFixtures.newSource();
        source.put("k1", TestFixtures.row("alice", 10));
        source.put("k2", TestFixtures.row("bob", 20));
        long afterInitial = source.latestCommitSeq();

        FileTargetStore target = new FileTargetStore(tempDir.resolve("target"));
        SyncEngine engine = new SyncEngine(source, target, config());

        long targetWatermark = source.latestCommitSeq() + 3;
        // 引擎在另一线程跑快照，快照可能正在进行时制造并发写入
        Thread runner = new Thread(() -> engine.run(targetWatermark));
        runner.start();

        // 快照进行期间/之后的变更必须被增量阶段完整覆盖
        source.put("k1", TestFixtures.row("alice2", 11));
        source.put("k3", TestFixtures.row("carol", 30));
        source.delete("k2");

        engine.awaitReached(targetWatermark, 3000);
        runner.join(3000);

        assertEquals(targetWatermark, engine.committedPosition());
        TargetRow k1 = target.get("k1");
        assertEquals("alice2", k1.values().get("name"));
        assertEquals(11, k1.values().get("age"));
        TargetRow k2 = target.get("k2");
        // k2 被删除：若删除落在增量阶段则留下墓碑；若落在快照之前/期间则直接不存在。
        // 两种情况下它都不得是存活行。
        assertTrue(k2 == null || k2.deleted(), "k2 必须已删除或不存在");
        assertEquals(2, target.allLiveRows().size());
        assertEquals("carol", target.get("k3").values().get("name"));
        // 无重复计数：所有变更恰好应用一次
        assertEquals(0, engine.stats().duplicates());
        assertEquals(0, engine.stats().rejected());
    }

}
