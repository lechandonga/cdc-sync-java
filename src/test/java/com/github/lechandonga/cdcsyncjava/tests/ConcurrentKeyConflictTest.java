package com.github.lechandonga.cdcsyncjava.tests;

import com.github.lechandonga.cdcsyncjava.config.SyncConfig;
import com.github.lechandonga.cdcsyncjava.engine.SyncEngine;
import com.github.lechandonga.cdcsyncjava.source.LocalMutableSource;
import com.github.lechandonga.cdcsyncjava.target.LocalTarget;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static com.github.lechandonga.cdcsyncjava.tests.support.SyncTestSupport.newEngine;
import static com.github.lechandonga.cdcsyncjava.tests.support.SyncTestSupport.newSource;
import static com.github.lechandonga.cdcsyncjava.tests.support.SyncTestSupport.newTarget;
import static com.github.lechandonga.cdcsyncjava.tests.support.SyncTestSupport.row;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发键冲突：
 * - 多个写线程对不同键并行写入、对相同键顺序追加；
 * - 同一键的多次变更必须严格按源端提交序号生效（最终值可确定、可复现）；
 * - 重复运行多次结果一致（确定性），且目标端行数/位点精确收敛。
 */
class ConcurrentKeyConflictTest {

    @TempDir
    Path dir;

    @Test
    void manyWritersDeterministicConvergence() throws Exception {
        LocalMutableSource source = newSource();
        LocalTarget target = newTarget(dir);
        SyncConfig config = new SyncConfig(8, 4, 32, 500, SyncConfig.BufferPolicy.BLOCK);
        SyncEngine engine = newEngine(source, target, config);

        int writers = 8;
        int perWriter = 250;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(writers);
        AtomicLong maxSeq = new AtomicLong();

        for (int w = 0; w < writers; w++) {
            final String key = "k" + w;
            Thread t = new Thread(() -> {
                try {
                    start.await();
                    for (int j = 0; j < perWriter; j++) {
                        long seq = source.upsert(key, row("v" + j, j));
                        maxSeq.updateAndGet(cur -> Math.max(cur, seq));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            }, "writer-" + w);
            t.start();
        }

        // 快照在并发写入开始的同时启动，制造快照/增量衔接竞争
        start.countDown();
        engine.syncOnce();
        done.await();

        long finalSourcePos = source.currentPosition();
        engine.runToPosition(finalSourcePos);

        // 每个键最终值必须是最后一次提交（age = perWriter-1），且名称匹配
        for (int wi = 0; wi < writers; wi++) {
            final int w = wi;
            Map<String, Object> got = target.get("k" + w).orElseThrow(
                    () -> new AssertionError("k" + w + " 丢失"));
            assertEquals(perWriter - 1L, ((Number) got.get("age")).longValue(),
                    "k" + w + " 未按提交顺序收敛");
            assertEquals("v" + (perWriter - 1), got.get("name"));
        }
        assertEquals(writers, target.activeRows().size(), "不得产生重复行");
        assertEquals(finalSourcePos, target.checkpoint().position());
        engine.close();
    }

    @RepeatedTest(5)
    void convergenceIsReproducibleAcrossRuns() {
        // 相同写入序列、重复跑 5 次，结果必须逐字节一致（确定性/可复现）
        LocalMutableSource source = newSource();
        for (int i = 0; i < 300; i++) {
            source.upsert("key" + (i % 10), row("n" + i, i));
        }
        long finalPos = source.currentPosition();

        LocalTarget target = newTarget(dir);
        SyncEngine engine = newEngine(source, target,
                new SyncConfig(13, 3, 50, 200, SyncConfig.BufferPolicy.BLOCK));
        engine.syncOnce();
        engine.runToPosition(finalPos);

        var expected = new TreeMap<String, TreeMap<String, Object>>();
        for (Map.Entry<String, Map<String, Object>> e : target.activeRows().entrySet()) {
            expected.put(e.getKey(), new TreeMap<>(e.getValue()));
        }
        assertEquals(10, expected.size());
        for (var e : expected.entrySet()) {
            int idx = Integer.parseInt(e.getKey().substring(3));
            // 每个键的最后一次写入：最大 i 满足 i%10==idx，即 290+idx
            int last = 290 + idx;
            assertEquals(last, ((Number) e.getValue().get("age")).longValue());
            assertEquals("n" + last, e.getValue().get("name"));
        }
        engine.close();
    }

    @Test
    void mixedUpsertDeleteConvergenceIsExact() throws Exception {
        LocalMutableSource source = newSource();
        for (int i = 0; i < 100; i++) {
            source.upsert("d" + i, row("init", i));
        }
        LocalTarget target = newTarget(dir);
        SyncEngine engine = newEngine(source, target, SyncConfig.defaults());
        engine.syncOnce();

        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(2);
        // 线程 A：删除偶数键；线程 B：更新奇数键
        Thread deleter = new Thread(() -> {
            try {
                start.await();
                for (int i = 0; i < 100; i += 2) {
                    source.delete("d" + i);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                done.countDown();
            }
        });
        Thread updater = new Thread(() -> {
            try {
                start.await();
                for (int i = 1; i < 100; i += 2) {
                    source.upsert("d" + i, row("updated", i + 1000L));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                done.countDown();
            }
        });
        deleter.start();
        updater.start();
        start.countDown();
        done.await();

        engine.runToPosition(source.currentPosition());
        for (int i = 0; i < 100; i++) {
            if (i % 2 == 0) {
                assertTrue(target.get("d" + i).isEmpty(), "偶数键 d" + i + " 应已删除");
            } else {
                Map<String, Object> got = target.get("d" + i).orElseThrow();
                assertEquals("updated", got.get("name"));
                assertEquals(i + 1000L, ((Number) got.get("age")).longValue());
            }
        }
        assertEquals(50, target.activeRows().size());
        engine.close();
    }
}
