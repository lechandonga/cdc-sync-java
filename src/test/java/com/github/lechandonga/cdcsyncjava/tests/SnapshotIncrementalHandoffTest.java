package com.github.lechandonga.cdcsyncjava.tests;

import com.github.lechandonga.cdcsyncjava.config.SyncConfig;
import com.github.lechandonga.cdcsyncjava.engine.SyncEngine;
import com.github.lechandonga.cdcsyncjava.model.ChangeEvent;
import com.github.lechandonga.cdcsyncjava.source.LocalMutableSource;
import com.github.lechandonga.cdcsyncjava.target.LocalTarget;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.github.lechandonga.cdcsyncjava.tests.support.SyncTestSupport.newEngine;
import static com.github.lechandonga.cdcsyncjava.tests.support.SyncTestSupport.newSource;
import static com.github.lechandonga.cdcsyncjava.tests.support.SyncTestSupport.newTarget;
import static com.github.lechandonga.cdcsyncjava.tests.support.SyncTestSupport.row;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 快照 → 增量衔接：
 * 1) 快照装载全量基线；
 * 2) 快照进行期间并发写入的变更必须被后续增量完整覆盖（无丢失）；
 * 3) 衔接边界无重复、无错序；
 * 4) 同一键多次变更按源端提交顺序收敛。
 */
class SnapshotIncrementalHandoffTest {

    @TempDir
    Path dir;

    @Test
    void snapshotBaselineThenIncrementalConverge() {
        LocalMutableSource source = newSource();
        source.upsert("k1", row("alice", 10L));
        source.upsert("k2", row("bob", 20L));

        LocalTarget target = newTarget(dir);
        SyncConfig config = new SyncConfig(1, 1, 10, 500, SyncConfig.BufferPolicy.BLOCK);
        SyncEngine engine = newEngine(source, target, config);

        engine.syncOnce(); // 快照 + 当前增量
        assertEquals(row("alice", 10L), target.get("k1").orElseThrow());
        assertEquals(row("bob", 20L), target.get("k2").orElseThrow());
        assertTrue(target.checkpoint().snapshotCompleted());
        assertEquals(source.currentPosition(), target.checkpoint().position());

        // 增量变更（同键多次，顺序敏感）
        source.upsert("k1", row("alice", 11L));
        source.delete("k2");
        source.upsert("k1", row("alice2", 12L));
        long expectedPos = source.currentPosition();
        engine.syncOnce();

        assertEquals(row("alice2", 12L), target.get("k1").orElseThrow(),
                "同一键多次变更必须按提交顺序生效");
        assertTrue(target.get("k2").isEmpty(), "删除必须生效");
        assertEquals(expectedPos, target.checkpoint().position());
        assertTrue(engine.skippedEvents().isEmpty(), "正常衔接不应有跳过");
        engine.close();
    }

    @Test
    void changesDuringSnapshotAreCoveredByIncremental() throws Exception {
        // 源端先有基线数据，另一个线程在快照执行期间持续并发写入；
        // 最终 runToPosition 必须完整收敛，丢失/错序即失败。
        LocalMutableSource source = newSource();
        for (int i = 0; i < 200; i++) {
            source.upsert("base" + i, row("n" + i, i));
        }
        LocalTarget target = newTarget(dir);
        SyncConfig config = new SyncConfig(16, 4, 64, 1000, SyncConfig.BufferPolicy.BLOCK);
        SyncEngine engine = newEngine(source, target, config);

        AtomicLong lastSeq = new AtomicLong();
        CountDownLatch stop = new CountDownLatch(1);
        List<Thread> writers = new ArrayList<>();
        int writerCount = 4;
        CountDownLatch started = new CountDownLatch(writerCount);

        for (int w = 0; w < writerCount; w++) {
            final int wid = w;
            Thread t = new Thread(() -> {
                started.countDown();
                int j = 0;
                while (stop.getCount() > 0) {
                    // 每个 writer 独占自己的键，同键多次写入检验顺序
                    String key = "hot" + wid;
                    long age = j++;
                    long seq = source.upsert(key, row("w" + wid + "-" + j, age));
                    lastSeq.updateAndGet(cur -> Math.max(cur, seq));
                    if (j >= 200) {
                        break;
                    }
                }
            }, "writer-" + w);
            writers.add(t);
            t.start();
        }

        started.await();
        // 快照在写入仍在进行时启动
        engine.syncOnce();

        for (Thread t : writers) {
            t.join(5000);
        }
        stop.countDown();

        long expected = source.currentPosition();
        engine.runToPosition(expected);

        // 最终：所有 hot 键 age 应为各自最后一次写入值（199）
        for (int wi = 0; wi < writerCount; wi++) {
            final int w = wi;
            Map<String, Object> got = target.get("hot" + w).orElseThrow(
                    () -> new AssertionError("hot" + w + " 丢失"));
            assertEquals(199L, ((Number) got.get("age")).longValue(),
                    "hot" + w + " 最终状态错误（错序或丢失）");
            assertEquals("w" + w + "-200", got.get("name"));
        }
        for (int i = 0; i < 200; i++) {
            assertTrue(target.get("base" + i).isPresent(), "基线 base" + i + " 丢失");
        }
        assertEquals(source.currentPosition(), target.checkpoint().position());
        engine.close();
    }
}
