package com.github.lechandonga.cdcsyncjava.schema;

import com.github.lechandonga.cdcsyncjava.error.RejectReason;
import com.github.lechandonga.cdcsyncjava.error.SchemaEvolutionException;
import com.github.lechandonga.cdcsyncjava.model.ChangeEvent;
import com.github.lechandonga.cdcsyncjava.model.ChangeType;
import com.github.lechandonga.cdcsyncjava.model.FieldDef;
import com.github.lechandonga.cdcsyncjava.model.FieldType;
import com.github.lechandonga.cdcsyncjava.model.SchemaDef;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 模式版本历史：负责演进校验，以及把旧版本事件投影到当前版本。
 *
 * <p>允许的兼容演进：新增字段、字段重命名、字段删除。
 * 不兼容的演进（字段重名、重命名源不存在、目标已存在等）抛出
 * {@link SchemaEvolutionException}，且已有版本历史保持不变（不污染）。
 */
public final class SchemaManager {

    /** 版本与其引入的演进操作（首版本为 null）。 */
    private record Version(SchemaDef def, SchemaChange change) {
    }

    private volatile List<Version> versions;

    public SchemaManager(SchemaDef initialSchema) {
        if (initialSchema == null) {
            throw new IllegalArgumentException("initialSchema required");
        }
        this.versions = List.of(new Version(initialSchema, null));
    }

    public SchemaDef current() {
        return versions.getLast().def();
    }

    /** @return 版本号 v（从 1 开始）对应的模式；未知版本返回 null */
    public SchemaDef version(int v) {
        List<Version> snap = versions;
        if (v < 1 || v > snap.size()) {
            return null;
        }
        return snap.get(v - 1).def();
    }

    public List<SchemaDef> history() {
        return versions.stream().map(Version::def).toList();
    }

    /** 应用一次兼容演进，返回新版本；不兼容则抛出异常且历史不变。 */
    public synchronized SchemaDef evolve(SchemaChange change) {
        if (change == null) {
            throw new SchemaEvolutionException(RejectReason.SCHEMA_VERSION_CONFLICT, "演进操作不能为空");
        }
        SchemaDef base = current();
        LinkedHashMap<String, FieldDef> next = new LinkedHashMap<>(base.fields());

        switch (change) {
            case SchemaChange.AddField add -> {
                if (next.containsKey(add.name())) {
                    throw new SchemaEvolutionException(RejectReason.SCHEMA_VERSION_CONFLICT,
                            "无法新增字段 '" + add.name() + "'：该字段在版本 " + base.version() + " 中已存在");
                }
                next.put(add.name(), new FieldDef(add.name(), add.type()));
            }
            case SchemaChange.RenameField rn -> {
                if (!next.containsKey(rn.from())) {
                    throw new SchemaEvolutionException(RejectReason.RENAME_SOURCE_MISSING,
                            "无法重命名：源字段 '" + rn.from() + "' 在版本 " + base.version() + " 中不存在");
                }
                if (next.containsKey(rn.to())) {
                    throw new SchemaEvolutionException(RejectReason.RENAME_TARGET_EXISTS,
                            "无法重命名 '" + rn.from() + "' -> '" + rn.to()
                                    + "'：目标字段在版本 " + base.version() + " 中已存在");
                }
                // 保留原位置与类型
                LinkedHashMap<String, FieldDef> rebuilt = new LinkedHashMap<>();
                for (Map.Entry<String, FieldDef> e : next.entrySet()) {
                    if (e.getKey().equals(rn.from())) {
                        rebuilt.put(rn.to(), new FieldDef(rn.to(), e.getValue().type()));
                    } else {
                        rebuilt.put(e.getKey(), e.getValue());
                    }
                }
                next = rebuilt;
            }
            case SchemaChange.RemoveField rm -> {
                if (!next.containsKey(rm.name())) {
                    throw new SchemaEvolutionException(RejectReason.SCHEMA_VERSION_CONFLICT,
                            "无法删除字段 '" + rm.name() + "'：该字段在版本 " + base.version() + " 中不存在");
                }
                next.remove(rm.name());
            }
        }

        SchemaDef def = new SchemaDef(base.version() + 1, next);
        versions = concat(versions, new Version(def, change));
        return def;
    }

    private static List<Version> concat(List<Version> old, Version added) {
        List<Version> copy = new java.util.ArrayList<>(old);
        copy.add(added);
        return List.copyOf(copy);
    }

    /**
     * 把任意历史版本的事件投影为当前模式下的事件。
     * 字段重命名按版本链传递；已删除字段被丢弃；新增字段在旧事件中缺失（保持缺失）。
     */
    public ChangeEvent project(ChangeEvent event) {
        int v = event.schemaVersion();
        SchemaDef src = version(v);
        if (src == null) {
            throw new SchemaEvolutionException(RejectReason.UNKNOWN_SCHEMA_VERSION,
                    "事件 " + event.eventId() + " 引用了未知的模式版本 " + v
                            + "（当前最新版本=" + current().version() + ")");
        }
        // 先按源版本校验负载合法性
        validate(src, event.data(), event.eventId());

        SchemaDef cur = current();
        if (v == cur.version()) {
            return event;
        }

        // oldName(at v) -> currentName
        Map<String, String> nameMap = new LinkedHashMap<>();
        for (String name : src.fields().keySet()) {
            nameMap.put(name, name);
        }
        List<Version> snap = versions;
        for (int k = v; k < cur.version(); k++) {
            SchemaChange c = snap.get(k).change(); // 版本 k+1 引入的变更
            if (c instanceof SchemaChange.RenameField rn) {
                nameMap.replaceAll((oldName, curName) -> curName.equals(rn.from()) ? rn.to() : curName);
            } else if (c instanceof SchemaChange.RemoveField rm) {
                nameMap.values().removeIf(rm.name()::equals);
            }
            // AddField：旧事件中本来就不存在，无需处理
        }

        Map<String, Object> projected = new LinkedHashMap<>();
        if (event.data() != null && event.type() == ChangeType.UPSERT) {
            for (Map.Entry<String, Object> e : event.data().entrySet()) {
                String newName = nameMap.get(e.getKey());
                if (newName != null && cur.containsField(newName)) {
                    projected.put(newName, e.getValue());
                }
                // 已删除字段被丢弃
            }
        }
        return new ChangeEvent(event.eventId(), event.key(), event.seq(), cur.version(),
                event.type(), projected);
    }

    /** 按模式校验负载：字段必须存在且值类型兼容。 */
    public void validate(SchemaDef schema, Map<String, Object> data, String eventId) {
        if (data == null) {
            return;
        }
        for (Map.Entry<String, Object> e : data.entrySet()) {
            FieldDef fd = schema.fields().get(e.getKey());
            if (fd == null) {
                throw new SchemaEvolutionException(RejectReason.UNDECLARED_FIELD,
                        "事件 " + eventId + " 携带了模式 v" + schema.version()
                                + " 中不存在的字段 '" + e.getKey() + "'");
            }
            if (!valueMatches(e.getValue(), fd.type())) {
                throw new SchemaEvolutionException(RejectReason.FIELD_TYPE_MISMATCH,
                        "事件 " + eventId + " 字段 '" + e.getKey() + "' 的值类型 "
                                + (e.getValue() == null ? "null" : e.getValue().getClass().getSimpleName())
                                + " 与声明类型 " + fd.type() + " 不匹配");
            }
        }
    }

    private static boolean valueMatches(Object value, FieldType type) {
        if (value == null) {
            return true;
        }
        return switch (type) {
            case STRING -> value instanceof String;
            case LONG -> value instanceof Long || value instanceof Integer
                    || value instanceof Short || value instanceof Byte;
            case DOUBLE -> value instanceof Number;
            case BOOLEAN -> value instanceof Boolean;
        };
    }

    public FieldDef field(SchemaDef schema, String name) {
        return schema.fields().get(name);
    }
}
