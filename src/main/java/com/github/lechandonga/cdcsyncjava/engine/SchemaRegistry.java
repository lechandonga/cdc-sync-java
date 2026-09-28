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

    /** WAL 恢复专用：把隔离重放得到的全部历史版本一次性采用。 */
    public void adoptRecoveredVersions(List<TableSchema> recovered) {
        if (!versions.isEmpty()) {
            throw new SyncException(RejectReason.ILLEGAL_STATE,
                    "只能对空 registry 采用恢复版本");
        }
        versions.addAll(recovered);
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

    /**
     * 按源端声明的“演进后版本号”幂等登记：
     * <ul>
     *   <li>该版本尚未登记（expectedVersion == current+1）：正常登记；</li>
     *   <li>该版本已经登记（初始化/重建中断后重放同一条 DDL）：若演进结果与已有版本
     *       结构完全一致则幂等跳过、不产生双登记/版本空洞；结构不一致说明现场冲突，拒绝；</li>
     *   <li>expectedVersion 超过 current+1：存在版本缺口，拒绝（必须严格连续登记）。</li>
     * </ul>
     * 返回登记 / 已存在的版本号。
     */
    public synchronized int registerAt(SchemaChange change, int expectedVersion) {
        int nextVersion = currentVersion() + 1;
        if (expectedVersion == nextVersion) {
            return register(change).version();
        }
        if (expectedVersion > nextVersion) {
            throw new SyncException(RejectReason.UNKNOWN_SCHEMA_VERSION,
                    "schema 演进出现版本缺口：期望登记 v" + nextVersion
                            + "，事件却声明 v" + expectedVersion + "（必须按序登记）");
        }
        // expectedVersion <= currentVersion：重放已登记版本，必须与历史演进结果结构一致
        TableSchema existing = at(expectedVersion);
        TableSchema base = expectedVersion == 1 ? existing : at(expectedVersion - 1);
        TableSchema candidate = expectedVersion == 1 ? existing
                : validateAndEvolve(base, change);
        if (!sameStructure(existing, candidate)) {
            throw new SyncException(RejectReason.TARGET_STATE_CORRUPT,
                    "重放的结构演进与已登记的 v" + expectedVersion
                            + " 结构冲突，拒绝污染已有 schema");
        }
        return expectedVersion;
    }

    /** 两个 schema 的结构（主键列 + 列名/类型/可空且顺序一致）是否相同。 */
    private static boolean sameStructure(TableSchema a, TableSchema b) {
        if (!a.keyColumn().equals(b.keyColumn())
                || a.columns().size() != b.columns().size()) {
            return false;
        }
        for (int i = 0; i < a.columns().size(); i++) {
            ColumnDef x = a.columns().get(i);
            ColumnDef y = b.columns().get(i);
            if (!x.name().equals(y.name()) || x.type() != y.type()
                    || x.nullable() != y.nullable()) {
                return false;
            }
        }
        return true;
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
