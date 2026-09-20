package com.github.lechandonga.cdcsyncjava.tests;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.lechandonga.cdcsyncjava.engine.BackpressurePolicy;
import com.github.lechandonga.cdcsyncjava.engine.SyncConfig;
import com.github.lechandonga.cdcsyncjava.engine.SyncEngine;
import com.github.lechandonga.cdcsyncjava.source.InMemorySourceDatabase;
import com.github.lechandonga.cdcsyncjava.support.TestFixtures;
import com.github.lechandonga.cdcsyncjava.target.FileTargetStore;
import com.github.lechandonga.cdcsyncjava.target.TargetRow;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 按键保序与并发测试：
 * 同一键的多次变更必须严格按源端提交顺序生效（最终值确定可复现）；
 * 不同键的处理互不阻塞；重复运行多次结果一致。
 */
class KeyOrderingConcurrencyTest {

    @TempDir
    Path tempDir;

    private SyncConfig config(Path stateDir, int parallelism) {
        return SyncConfig.builder(stateDir)
                .batchSize(10)
                .parallelism(parallelism)
                .bufferCapacity(16)
                .backpressurePolicy(BackpressurePolicy.BLOCK)
                .pollTimeoutMillis(5)
                .build();
    }

    @Test
    void sameKeyUpdatesApplyInCommitOrder() throws Exception {
        for (int iteration = 0; iteration < 5; iteration++) {
            InMemorySourceDatabase source = TestFixtures.newSource();
            Path dir = tempDir.resolve("run-" + iteration);
            FileTargetStore target = new FileTargetStore(dir.resolve("target"));
            SyncEngine engine = new SyncEngine(source, target, config(dir.resolve("state"), 8));

            int keys = 20;
            int updatesPerKey = 30;
            long expected = (long) keys * updatesPerKey;
            Thread runner = new Thread(() -> engine.run(expected));
            runner.start();

            // 交错提交：k1,k2,...,kn,k1,... 同一键多次变更
            for (int u = 0; u < updatesPerKey; u++) {
                for (int k = 1; k <= keys; k++) {
                    source.put("k" + k, TestFixtures.row("k" + k + "-v" + u, u));
                }
            }
            engine.awaitReached(expected, 10_000);
            runner.join(10_000);

            assertEquals(expected, engine.committedPosition());
            assertEquals(0, engine.stats().rejected());
            for (int k = 1; k <= keys; k++) {
                TargetRow row = target.get("k" + k);
                // 同键最后一次提交必须胜出：顺序不能被并行打乱
                assertEquals(updatesPerKey - 1, row.values().get("age"),
                        "iteration " + iteration + " key k" + k);
                assertEquals("k" + k + "-v" + (updatesPerKey - 1), row.values().get("name"));
            }
        }
    }
}
