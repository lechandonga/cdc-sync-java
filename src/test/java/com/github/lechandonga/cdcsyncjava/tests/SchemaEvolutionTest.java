package com.github.lechandonga.cdcsyncjava.tests;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.lechandonga.cdcsyncjava.engine.BackpressurePolicy;
import com.github.lechandonga.cdcsyncjava.engine.SyncConfig;
import com.github.lechandonga.cdcsyncjava.engine.SyncEngine;
import com.github.lechandonga.cdcsyncjava.engine.SyncException;
import com.github.lechandonga.cdcsyncjava.model.ColumnDef;
import com.github.lechandonga.cdcsyncjava.model.ColumnType;
import com.github.lechandonga.cdcsyncjava.model.RejectReason;
import com.github.lechandonga.cdcsyncjava.model.SchemaChange;
import com.github.lechandonga.cdcsyncjava.source.InMemorySourceDatabase;
import com.github.lechandonga.cdcsyncjava.support.TestFixtures;
import com.github.lechandonga.cdcsyncjava.target.FileTargetStore;
import com.github.lechandonga.cdcsyncjava.target.TargetRow;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 源端结构演进测试：
 * 新增字段、字段重命名、字段删除在增量流中被正确登记并翻译旧结构数据；
 * 过渡期到达的旧 schema 变更不写入错乱；不兼容演进被拒绝、原因可区分、已有数据不被污染。
 */
class SchemaEvolutionTest {

    @TempDir
    Path tempDir;

    private SyncConfig config(Path dir) {
        return SyncConfig.builder(dir)
                .batchSize(5).parallelism(4).bufferCapacity(8)
                .backpressurePolicy(BackpressurePolicy.BLOCK).pollTimeoutMillis(5).build();
    }

    @Test
    void addRenameAndDropColumnsTranslateOldRows() throws Exception {
        InMemorySourceDatabase source = TestFixtures.newSource();
        source.put("k1", TestFixtures.row("alice", 10));
        source.put("k2", TestFixtures.row("bob", 20));

        FileTargetStore target = new FileTargetStore(tempDir.resolve("target"));
        SyncEngine engine = new SyncEngine(source, target, config(tempDir.resolve("state")));

        // 1) 新增可空列
        source.evolve(SchemaChange.addColumn("email", ColumnType.STRING, true));
        // 旧结构数据（v1 行，无 email 字段）在演进之后到达也必须正确补 null
        source.put("k3", Map.of("name", "carol", "age", 30));
        // 2) 重命名 age -> years
        source.evolve(SchemaChange.renameColumn("age", "years"));
        source.put("k1", Map.of("name", "alice2", "years", 11, "email", "a@x"));
        // 3) 删除 name
        source.evolve(SchemaChange.dropColumn("name"));
        source.put("k2", Map.of("years", 22, "email", "b@x"));
        long last = source.latestCommitSeq();

        Thread runner = new Thread(() -> engine.run(last));
        runner.start();
        engine.awaitReached(last, 5000);
        runner.join(5000);

        assertEquals(4, target.currentSchemaVersion());
        TargetRow k1 = target.get("k1");
        assertEquals("a@x", k1.values().get("email"));
        assertEquals(11, k1.values().get("years"));
        assertFalse(k1.values().containsKey("age"), "旧列名 age 不得残留在最新结构中");
        assertFalse(k1.values().containsKey("name"), "已删除列 name 不得出现");

        // 历史版本仍可查询（可解释）
        assertEquals("age", target.schemaAt(1).findColumn("age").name());
        assertEquals("years", target.schemaAt(3).findColumn("years").name());
        assertNull(target.schemaAt(4).findColumn("name"));
        assertEquals(0, engine.stats().rejected());
        assertEquals(3, engine.stats().schemaChanges());
    }

    @Test
    void incompatibleEvolutionsAreRejectedWithoutPollutingData() {
        InMemorySourceDatabase source = TestFixtures.newSource();
        FileTargetStore target = new FileTargetStore(tempDir.resolve("target"));
        target.initialize(source.currentSchema());
        // 先写入一行既有数据，后续拒绝演进不得影响它
        target.apply(new com.github.lechandonga.cdcsyncjava.model.ChangeEvent(
                com.github.lechandonga.cdcsyncjava.model.OpType.INSERT, "k1",
                new com.github.lechandonga.cdcsyncjava.model.RowData(
                        Map.of("id", "k1", "name", "alice", "age", 10), 1, 1),
                new com.github.lechandonga.cdcsyncjava.model.SourcePosition(1), 1));

        // 重命名不存在的列
        SyncException e1 = assertThrows(SyncException.class,
                () -> target.registerSchemaChange(SchemaChange.renameColumn("nope", "x")));
        assertEquals(RejectReason.INCOMPATIBLE_RENAME_UNKNOWN_COLUMN, e1.reason());

        // 重命名为已存在的列名
        SyncException e2 = assertThrows(SyncException.class,
                () -> target.registerSchemaChange(SchemaChange.renameColumn("name", "age")));
        assertEquals(RejectReason.INCOMPATIBLE_RENAME_NAME_CONFLICT, e2.reason());

        // 新增重名列
        SyncException e3 = assertThrows(SyncException.class,
                () -> target.registerSchemaChange(SchemaChange.addColumn("name", ColumnType.STRING, true)));
        assertEquals(RejectReason.INCOMPATIBLE_ADD_DUPLICATE_COLUMN, e3.reason());

        // 删除不存在的列
        SyncException e4 = assertThrows(SyncException.class,
                () -> target.registerSchemaChange(SchemaChange.dropColumn("ghost")));
        assertEquals(RejectReason.INCOMPATIBLE_DROP_UNKNOWN_COLUMN, e4.reason());

        // 新增非空列（无法回填历史行）
        SyncException e5 = assertThrows(SyncException.class,
                () -> target.registerSchemaChange(SchemaChange.addColumn("mandatory", ColumnType.STRING, false)));
        assertEquals(RejectReason.INCOMPATIBLE_TYPE_CHANGE, e5.reason());

        // 被拒绝后：版本不变、已有数据完好、存储仍可正常写入
        assertEquals(1, target.currentSchemaVersion());
        assertEquals("alice", target.get("k1").values().get("name"));
        target.apply(new com.github.lechandonga.cdcsyncjava.model.ChangeEvent(
                com.github.lechandonga.cdcsyncjava.model.OpType.UPDATE, "k2",
                new com.github.lechandonga.cdcsyncjava.model.RowData(
                        Map.of("id", "k2", "name", "zoe", "age", 40), 1, 2),
                new com.github.lechandonga.cdcsyncjava.model.SourcePosition(2), 1));
        assertEquals("zoe", target.get("k2").values().get("name"));
        assertNull(target.schemaAt(1).findColumn("mandatory"), "被拒绝的列不得登记");
    }
}
