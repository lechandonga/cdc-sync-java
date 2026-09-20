package com.github.lechandonga.cdcsyncjava.tests;

import com.github.lechandonga.cdcsyncjava.config.SyncConfig;
import com.github.lechandonga.cdcsyncjava.engine.ParallelKeyApplier;
import com.github.lechandonga.cdcsyncjava.error.RejectReason;
import com.github.lechandonga.cdcsyncjava.error.SkippedEvent;
import com.github.lechandonga.cdcsyncjava.error.SkippedLedger;
import com.github.lechandonga.cdcsyncjava.model.ChangeEvent;
import com.github.lechandonga.cdcsyncjava.model.ChangeType;
import com.github.lechandonga.cdcsyncjava.model.FieldDef;
import com.github.lechandonga.cdcsyncjava.model.SchemaDef;
import com.github.lechandonga.cdcsyncjava.schema.SchemaManager;
import com.github.lechandonga.cdcsyncjava.source.LocalMutableSource;
import com.github.lechandonga.cdcsyncjava.target.LocalTarget;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.github.lechandonga.cdcsyncjava.tests.support.SyncTestSupport.newSource;
import static com.github.lechandonga.cdcsyncjava.tests.support.SyncTestSupport.newTarget;
import static com.github.lechandonga.cdcsyncjava.tests.support.SyncTestSupport.row;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 至少一次投递下的幂等应用：
 * - 同一批次重复投递 / 批次重试，不产生重复数据或错误状态；
 * - 跳过事件可定位到具体记录（eventId/key/seq）与原因；
 * - 乱序到达的过期事件（seq 倒退）被安全跳过。
 */
class IdempotentDeliveryTest {

    @TempDir
    Path dir;

    private ChangeEvent upsert(long seq, String key, Map<String, Object> data) {
        return new ChangeEvent("e" + seq, key, seq, 1, ChangeType.UPSERT, new LinkedHashMap<>(data));
    }

    @Test
    void duplicateBatchDeliveryIsIdempotent() {
        LocalMutableSource source = newSource();
        LocalTarget target = newTarget(dir);
        SkippedLedger ledger = new SkippedLedger();
        SchemaManager schemas = source.schemaManager();
        ParallelKeyApplier applier = new ParallelKeyApplier(2);

        List<ChangeEvent> batch = List.of(
                upsert(1, "k1", row("a", 1L)),
                upsert(2, "k1", row("a2", 2L)),
                upsert(3, "k2", row("b", 3L)));

        // 首次投递
        applier.apply(batch, "retry-batch", target, schemas, ledger);
        target.commitCheckpoint(3);

        // 批次重试：完全相同的事件再次投递（模拟至少一次）
        applier.apply(batch, "retry-batch", target, schemas, ledger);
        // 再来一次
        applier.apply(batch, "retry-batch", target, schemas, ledger);

        assertEquals(row("a2", 2L), target.get("k1").orElseThrow());
        assertEquals(row("b", 3L), target.get("k2").orElseThrow());
        assertEquals(3L, target.checkpoint().position(), "重复投递不得错误推进位点");

        // 6 次重复全部被幂等拦截：k1 的 seq=1 已被 seq=2 越过 => OBSOLETE_ORDER，
        // k1 seq=2 精确重复 => ALREADY_APPLIED，k2 seq=3 精确重复 => ALREADY_APPLIED
        List<SkippedEvent> skipped = ledger.all();
        assertEquals(6, skipped.size());
        long alreadyApplied = skipped.stream()
                .filter(s -> s.reason() == RejectReason.ALREADY_APPLIED).count();
        long obsolete = skipped.stream()
                .filter(s -> s.reason() == RejectReason.OBSOLETE_ORDER).count();
        assertEquals(4, alreadyApplied); // seq=2 与 seq=3 各重放两次
        assertEquals(2, obsolete);       // seq=1 各重放一次
        for (SkippedEvent s : skipped) {
            assertTrue(s.eventId().startsWith("e"));
            assertTrue(s.key().startsWith("k"));
            assertTrue(s.batchId().startsWith("retry-batch"));
        }
        // 目标表无重复（每键恰好一行）
        assertEquals(2, target.activeRows().size());
        applier.close();
    }

    @Test
    void staleOutOfOrderEventIsSkippedAndTraceable() {
        LocalMutableSource source = newSource();
        LocalTarget target = newTarget(dir);
        SkippedLedger ledger = new SkippedLedger();
        ParallelKeyApplier applier = new ParallelKeyApplier(1);

        applier.apply(List.of(upsert(1, "k1", row("new", 1L))), "b1", target,
                source.schemaManager(), ledger);
        target.commitCheckpoint(1);
        // 迟到的旧版本事件（seq=0 < 已应用 seq=1）
        applier.apply(List.of(upsert(0, "k1", row("old", 0L))), "b-stale", target,
                source.schemaManager(), ledger);

        assertEquals(row("new", 1L), target.get("k1").orElseThrow(), "旧事件不得覆盖新状态");
        SkippedEvent s = ledger.all().getFirst();
        assertEquals(RejectReason.OBSOLETE_ORDER, s.reason());
        assertEquals("e0", s.eventId());
        assertEquals("k1", s.key());
        assertEquals(0L, s.seq());
        assertTrue(s.batchId().startsWith("b-stale"), s.batchId());
        applier.close();
    }

    @Test
    void invalidPayloadEventsAreRejectedWithDistinctReasons() {
        SchemaDef schema = new SchemaDef(1, new LinkedHashMap<>(Map.of(
                "name", new FieldDef("name", com.github.lechandonga.cdcsyncjava.model.FieldType.STRING))));
        SchemaManager sm = new SchemaManager(schema);
        LocalTarget target = newTarget(dir);
        SkippedLedger ledger = new SkippedLedger();
        ParallelKeyApplier applier = new ParallelKeyApplier(1);

        // 未声明字段
        ChangeEvent undeclared = upsert(1, "k1", Map.of("ghost", "x"));
        // 类型不匹配（name 应为 String）
        ChangeEvent badType = upsert(2, "k2", Map.of("name", 123L));

        applier.apply(List.of(undeclared, badType), "bad-batch", target, sm, ledger);

        List<SkippedEvent> skipped = ledger.all();
        assertEquals(2, skipped.size());
        assertEquals(RejectReason.UNDECLARED_FIELD, skipped.get(0).reason());
        assertEquals(RejectReason.FIELD_TYPE_MISMATCH, skipped.get(1).reason());
        assertTrue(target.activeRows().isEmpty(), "非法事件不得落盘污染数据");
        applier.close();
    }
}
