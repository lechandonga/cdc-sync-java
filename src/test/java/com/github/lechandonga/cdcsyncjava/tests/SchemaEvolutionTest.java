package com.github.lechandonga.cdcsyncjava.tests;

import com.github.lechandonga.cdcsyncjava.config.SyncConfig;
import com.github.lechandonga.cdcsyncjava.engine.SyncEngine;
import com.github.lechandonga.cdcsyncjava.error.RejectReason;
import com.github.lechandonga.cdcsyncjava.error.SchemaEvolutionException;
import com.github.lechandonga.cdcsyncjava.error.SkippedEvent;
import com.github.lechandonga.cdcsyncjava.model.FieldType;
import com.github.lechandonga.cdcsyncjava.schema.SchemaChange;
import com.github.lechandonga.cdcsyncjava.source.LocalMutableSource;
import com.github.lechandonga.cdcsyncjava.target.LocalTarget;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static com.github.lechandonga.cdcsyncjava.tests.support.SyncTestSupport.newEngine;
import static com.github.lechandonga.cdcsyncjava.tests.support.SyncTestSupport.newSource;
import static com.github.lechandonga.cdcsyncjava.tests.support.SyncTestSupport.newTarget;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 源端结构演进：
 * - 新增字段、重命名、删除均可兼容投影，旧结构事件在过渡期不导致写入错乱；
 * - 不兼容演进（重名、源不存在、目标已存在）必须被拒绝、说明原因且不污染已有数据/版本历史。
 */
class SchemaEvolutionTest {

    @TempDir
    Path dir;

    @Test
    void addRenameAndRemoveFieldsProjectCorrectly() {
        LocalMutableSource source = newSource();
        source.upsert("k1", Map.of("name", "alice", "age", 10L)); // schema v1
        LocalTarget target = newTarget(dir);
        SyncEngine engine = newEngine(source, target, SyncConfig.defaults());
        engine.syncOnce();

        // 新增字段 email
        source.evolve(new SchemaChange.AddField("email", FieldType.STRING));
        source.upsert("k1", Map.of("name", "alice", "age", 11L, "email", "a@x")); // v2
        engine.syncOnce();
        assertEquals("a@x", target.get("k1").orElseThrow().get("email"));

        // 重命名 age -> years
        source.evolve(new SchemaChange.RenameField("age", "years"));
        // 过渡期：旧结构（仍带 age）的新事件不允许由源端制造；这里验证“历史旧版本事件”的投影
        source.upsert("k1", Map.of("name", "alice", "years", 12L, "email", "a@x")); // v3
        engine.syncOnce();
        Map<String, Object> got = target.get("k1").orElseThrow();
        assertEquals(12L, got.get("years"));
        assertFalse(got.containsKey("age"), "旧字段名 age 不应残留");

        // 删除 email
        source.evolve(new SchemaChange.RemoveField("email"));
        source.upsert("k1", Map.of("name", "alice", "years", 13L)); // v4
        engine.syncOnce();
        Map<String, Object> afterRemove = target.get("k1").orElseThrow();
        assertFalse(afterRemove.containsKey("email"), "删除的字段不得再写入");
        assertEquals(13L, afterRemove.get("years"));
        assertEquals(4, source.schemaManager().current().version());
        engine.close();
    }

    @Test
    void renameChainsPropagateOldEventsAcrossTransition() {
        // k1 在 v1 写入 age；v2 age->years；旧快照行（携带 age, schemaVersion=1）
        // 经引擎重放/投影后必须落到 years。
        LocalMutableSource source = newSource();
        source.upsert("k1", Map.of("name", "a", "age", 1L));
        source.evolve(new SchemaChange.RenameField("age", "years"));
        source.upsert("k2", Map.of("name", "b", "years", 2L));

        LocalTarget target = newTarget(dir);
        SyncEngine engine = newEngine(source, target, SyncConfig.defaults());
        engine.syncOnce();

        // k1 的 age 来自 v1 快照，投影到当前字段名 years
        assertEquals(1L, target.get("k1").orElseThrow().get("years"));
        assertEquals(2L, target.get("k2").orElseThrow().get("years"));
        assertFalse(target.get("k1").orElseThrow().containsKey("age"));
        engine.close();
    }

    @Test
    void incompatibleEvolutionsAreRejectedWithoutPollution() {
        LocalMutableSource source = newSource();
        int versionBefore = source.schemaManager().current().version();

        // 新增已存在字段
        SchemaEvolutionException dupAdd = assertThrows(SchemaEvolutionException.class,
                () -> source.evolve(new SchemaChange.AddField("age", FieldType.LONG)));
        assertEquals(RejectReason.SCHEMA_VERSION_CONFLICT, dupAdd.reason());

        // 重命名源不存在
        SchemaEvolutionException missingSource = assertThrows(SchemaEvolutionException.class,
                () -> source.evolve(new SchemaChange.RenameField("nope", "x")));
        assertEquals(RejectReason.RENAME_SOURCE_MISSING, missingSource.reason());

        // 先合法新增一个字段，再重命名到已存在目标
        source.evolve(new SchemaChange.AddField("nick", FieldType.STRING));
        SchemaEvolutionException targetExists = assertThrows(SchemaEvolutionException.class,
                () -> source.evolve(new SchemaChange.RenameField("nick", "name")));
        assertEquals(RejectReason.RENAME_TARGET_EXISTS, targetExists.reason());

        // 删除不存在字段
        SchemaEvolutionException removeMissing = assertThrows(SchemaEvolutionException.class,
                () -> source.evolve(new SchemaChange.RemoveField("nope")));
        assertEquals(RejectReason.SCHEMA_VERSION_CONFLICT, removeMissing.reason());

        // 只有一次合法演进（add nick），其余全部拒绝，历史不被污染
        assertEquals(versionBefore + 1, source.schemaManager().current().version());
        assertTrue(source.schemaManager().current().containsField("nick"));
        assertTrue(source.schemaManager().current().containsField("age"));
        assertEquals(2, source.schemaManager().history().size());
    }

    @Test
    void oldSchemaEventWithUndeclaredFieldIsRejectedAndLocated() {
        LocalMutableSource source = newSource();
        source.upsert("k1", Map.of("name", "a", "age", 1L));
        LocalTarget target = newTarget(dir);
        SyncEngine engine = newEngine(source, target, SyncConfig.defaults());
        engine.syncOnce();

        // 直接构造一个携带“当前模式不存在字段”的事件（模拟外部/损坏的旧结构事件）
        com.github.lechandonga.cdcsyncjava.model.ChangeEvent bad =
                new com.github.lechandonga.cdcsyncjava.model.ChangeEvent(
                        "bad-1", "k9", 999L, 1,
                        com.github.lechandonga.cdcsyncjava.model.ChangeType.UPSERT,
                        Map.of("name", "x", "age", 1L, "ghost", "boom"));
        new com.github.lechandonga.cdcsyncjava.engine.ParallelKeyApplier(1)
                .apply(List.of(bad), "legacy-batch", target, source.schemaManager(),
                        engine.skippedLedger());

        SkippedEvent s = engine.skippedEvents().getFirst();
        assertEquals(RejectReason.UNDECLARED_FIELD, s.reason());
        assertEquals("bad-1", s.eventId());
        assertEquals("k9", s.key());
        assertEquals(999L, s.seq());
        assertFalse(target.activeRows().containsKey("k9"), "非法旧结构事件不得写入");
        engine.close();
    }
}
