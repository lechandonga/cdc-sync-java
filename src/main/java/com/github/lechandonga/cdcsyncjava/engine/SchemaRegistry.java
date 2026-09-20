package com.github.lechandonga.cdcsyncjava.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.github.lechandonga.cdcsyncjava.model.ColumnDef;
import com.github.lechandonga.cdcsyncjava.model.EvolutionType;
import com.github.lechandonga.cdcsyncjava.model.RejectReason;
import com.github.lechandonga.cdcsyncjava.model.SchemaChange;
import com.github.lechandonga.cdcsyncjava.model.TableSchema;

/**
 * schema 版本登记处：保存全部历史版本，支持把旧版本行按列映射翻译到新版本。
 * 演进校验失败必须拒绝且不得污染已有版本。
 *
 * <p>翻译语义（仅支持三种演进，类型改型属于不兼容演进，直接拒绝）：</p>
 * <ul>
 *   <li>ADD_COLUMN：新列对旧数据补 null（非空新增列拒绝，因为历史行无法填充）</li>
 *   <li>RENAME_COLUMN：旧列名平移到新列名，值不变</li>
 *   <li>DROP_COLUMN：翻译时移除该列</li>
 * </ul>
 */
public class SchemaRegistry {

    private final List<TableSchema> versions = new ArrayList<>();
    /** 是否允许列改型：源端允许（模拟数据库自身行为），目标端默认拒绝（本引擎不做隐式转换）。 */
    private final boolean allowTypeChange;

    public SchemaRegistry() {
        this(false);
    }

    public SchemaRegistry(boolean allowTypeChange) {
        this.allowTypeChange = allowTypeChange;
    }

    /** 登记初始版本（版本号必须为 1）。 */
    public void init(TableSchema initial) {
        if (!versions.isEmpty()) {
            throw new SyncException(RejectReason.ILLEGAL_STATE, "schema 已经初始化，不能重复 init");
        }
        TableSchema v1 = initial.version() == 1 ? initial
                : new TableSchema(1, initial.keyColumn(), initial.columns());
        versions.add(v1);
    }

    /** 当前版本。 */
    public int currentVersion() {
        return versions.get(versions.size() - 1).version();
    }

    /** 当前 schema。 */
    public TableSchema current() {
        return versions.get(versions.size() - 1);
    }

    /** 取指定版本；不存在抛 UNKNOWN_SCHEMA_VERSION。 */
    public TableSchema at(int version) {
        for (TableSchema s : versions) {
            if (s.version() == version) {
                return s;
            }
        }
        throw new SyncException(RejectReason.UNKNOWN_SCHEMA_VERSION,
                "未知的 schema 版本: " + version + "，当前最高版本: "
                        + (versions.isEmpty() ? 0 : currentVersion()));
    }

    /**
     * 校验并登记演进，返回新版本 schema。
     * 不兼容时抛 SyncException，内部列表不变，已有版本与数据不被污染。
     */
    public synchronized TableSchema register(SchemaChange change) {
        TableSchema base = current();
        TableSchema next = validateAndEvolve(base, change);
        // 先在局部构造完成，再加入列表，保证失败时无副作用
        versions.add(next);
        return next;
    }

    private TableSchema validateAndEvolve(TableSchema base, SchemaChange change) {
        int newVersion = base.version() + 1;
        List<ColumnDef> columns = new ArrayList<>(base.columns());
        switch (change.type()) {
            case ADD_COLUMN -> {
                String name = change.targetColumn();
                if (base.hasColumn(name)) {
                    throw new SyncException(RejectReason.INCOMPATIBLE_ADD_DUPLICATE_COLUMN,
                            "新增列与已有列重名: " + name + "（schema v" + base.version() + "）");
                }
                if (!change.newColumnNullable()) {
                    throw new SyncException(RejectReason.INCOMPATIBLE_TYPE_CHANGE,
                            "新增非空列无法为历史行提供默认值，拒绝: " + name
                                    + "（仅允许新增可空列）");
                }
                columns.add(new ColumnDef(name, change.newColumnType(), true));
            }
            case RENAME_COLUMN -> {
                String from = change.sourceColumn();
                String to = change.targetColumn();
                ColumnDef existing = base.findColumn(from);
                if (existing == null) {
                    throw new SyncException(RejectReason.INCOMPATIBLE_RENAME_UNKNOWN_COLUMN,
                            "重命名的源列不存在: " + from + "（schema v" + base.version() + "）");
                }
                if (!from.equals(to) && base.hasColumn(to)) {
                    throw new SyncException(RejectReason.INCOMPATIBLE_RENAME_NAME_CONFLICT,
                            "重命名目标列与已有列冲突: " + to + "（schema v" + base.version() + "）");
                }
                for (int i = 0; i < columns.size(); i++) {
                    if (columns.get(i).name().equals(from)) {
                        columns.set(i, new ColumnDef(to, existing.type(), existing.nullable()));
                    }
                }
            }
            case DROP_COLUMN -> {
                String name = change.sourceColumn();
                if (!base.hasColumn(name)) {
                    throw new SyncException(RejectReason.INCOMPATIBLE_DROP_UNKNOWN_COLUMN,
                            "删除的列不存在: " + name + "（schema v" + base.version() + "）");
                }
                if (name.equals(base.keyColumn())) {
                    throw new SyncException(RejectReason.INCOMPATIBLE_DROP_UNKNOWN_COLUMN,
                            "禁止删除主键列: " + name);
                }
                columns.removeIf(c -> c.name().equals(name));
            }
            case ALTER_TYPE -> {
                if (!allowTypeChange) {
                    throw new SyncException(RejectReason.INCOMPATIBLE_TYPE_CHANGE,
                            "列改型属于不兼容演进（不做隐式类型转换），拒绝列 "
                                    + change.sourceColumn() + " -> " + change.newColumnType()
                                    + "（schema v" + base.version() + "）");
                }
                String name = change.sourceColumn();
                ColumnDef existing = base.findColumn(name);
                if (existing == null) {
                    throw new SyncException(RejectReason.INCOMPATIBLE_RENAME_UNKNOWN_COLUMN,
                            "改型的源列不存在: " + name);
                }
                for (int i = 0; i < columns.size(); i++) {
                    if (columns.get(i).name().equals(name)) {
                        columns.set(i, new ColumnDef(name, change.newColumnType(),
                                existing.nullable()));
                    }
                }
            }
            default -> throw new SyncException(RejectReason.INCOMPATIBLE_TYPE_CHANGE,
                    "不支持的演进类型: " + change.type());
        }
        return new TableSchema(newVersion, base.keyColumn(), columns);
    }

    /**
     * 将旧版本的一行值翻译到目标版本。
     * 逐版本应用：新增列补 null、重命名平移键、删除列移除。
     */
    public Map<String, Object> translate(Map<String, Object> values, int fromVersion,
                                         int toVersion) {
        if (fromVersion == toVersion) {
            return new LinkedHashMap<>(values);
        }
        if (fromVersion > toVersion || fromVersion < 1 || toVersion > currentVersion()) {
            throw new SyncException(RejectReason.UNKNOWN_SCHEMA_VERSION,
                    "无法在版本间翻译: v" + fromVersion + " -> v" + toVersion
                            + "（当前最高 v" + currentVersion() + "）");
        }
        Map<String, Object> result = new LinkedHashMap<>(values);
        for (int v = fromVersion; v < toVersion; v++) {
            // 仅用相邻两版的列名差异推导映射，保证可解释且与演进历史一致
            TableSchema before = at(v);
            TableSchema after = at(v + 1);
            Map<String, String> rename = new HashMap<>();
            java.util.Set<String> beforeNames = new java.util.HashSet<>();
            for (ColumnDef c : before.columns()) {
                beforeNames.add(c.name());
            }
            java.util.Set<String> afterNames = new java.util.HashSet<>();
            for (ColumnDef c : after.columns()) {
                afterNames.add(c.name());
            }
            // 位置对齐识别改名（同位置、同类型、旧名消失且新名出现）
            for (int i = 0; i < Math.min(before.columns().size(), after.columns().size()); i++) {
                String bn = before.columns().get(i).name();
                String an = after.columns().get(i).name();
                if (!bn.equals(an) && !afterNames.contains(bn) && !beforeNames.contains(an)
                        && before.columns().get(i).type() == after.columns().get(i).type()) {
                    rename.put(bn, an);
                }
            }
            Map<String, Object> stepped = new LinkedHashMap<>();
            for (ColumnDef c : after.columns()) {
                if (result.containsKey(c.name())) {
                    stepped.put(c.name(), result.get(c.name()));
                } else {
                    String oldName = null;
                    for (Map.Entry<String, String> e : rename.entrySet()) {
                        if (e.getValue().equals(c.name())) {
                            oldName = e.getKey();
                            break;
                        }
                    }
                    stepped.put(c.name(), oldName != null ? result.get(oldName) : null);
                }
            }
            result = stepped;
        }
        return result;
    }

    /** 已登记版本（按顺序，不可变视图）。 */
    public List<TableSchema> versions() {
        return Collections.unmodifiableList(new ArrayList<>(versions));
    }
}
