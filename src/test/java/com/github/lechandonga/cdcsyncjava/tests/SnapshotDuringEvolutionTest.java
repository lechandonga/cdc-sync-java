package com.github.lechandonga.cdcsyncjava.tests;

import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.lechandonga.cdcsyncjava.engine.BackpressurePolicy;
import com.github.lechandonga.cdcsyncjava.engine.SyncConfig;
import com.github.lechandonga.cdcsyncjava.engine.SyncEngine;
import com.github.lechandonga.cdcsyncjava.model.ColumnType;
import com.github.lechandonga.cdcsyncjava.model.SchemaChange;
import com.github.lechandonga.cdcsyncjava.source.InMemorySourceDatabase;
import com.github.lechandonga.cdcsyncjava.support.TestFixtures;
import com.github.lechandonga.cdcsyncjava.target.FileTargetStore;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 快照与结构演进并发：
 * 无论快照建立在 DDL 之前还是之后，快照行携带各自产生时的 schema 版本，
 * 目标端按登记的历史版本翻译，最终所有行在最新 schema 下值确定、无错乱。
 */
class SnapshotDuringEvolutionTest {

    @TempDir
    Path tempDir;

    @Test
    void evolutionAroundSnapshotConverges() throws Exception {
        for (int iter = 0; iter < 10; iter++) {
            InMemorySourceDatabase source = TestFixtures.newSource();
            source.put("k1", TestFixtures.row("a", 1));
            Path dir = tempDir.resolve("iter-" + iter);
            FileTargetStore target = new FileTargetStore(dir.resolve("target"));
            SyncEngine engine = new SyncEngine(source, target,
                    SyncConfig.builder(dir.resolve("state")).batchSize(2).parallelism(3)
                            .bufferCapacity(4).backpressurePolicy(BackpressurePolicy.BLOCK)
                            .pollTimeoutMillis(1).build());

            Thread runner = new Thread(() -> engine.run(5));
            runner.start();
            // 与快照阶段竞速：新增列 + 旧结构/新结构写入交织
            source.evolve(SchemaChange.addColumn("email", ColumnType.STRING, true));
            source.put("k1", Map.of("name", "a2", "age", 2, "email", "a@x"));
            source.put("k2", Map.of("name", "b", "age", 3)); // 未给 email：补 null
            source.put("k2", Map.of("name", "b2", "age", 4, "email", "b@x"));

            engine.awaitReached(5, 5000);
            runner.join(5000);

            assertEquals(2, target.currentSchemaVersion(), "iter " + iter);
            assertEquals("a@x", target.get("k1").values().get("email"), "iter " + iter);
            assertEquals("b@x", target.get("k2").values().get("email"), "iter " + iter);
            assertEquals(4, target.get("k2").values().get("age"), "iter " + iter);
            assertEquals(0, engine.stats().rejected(), "iter " + iter);
        }
    }
}
