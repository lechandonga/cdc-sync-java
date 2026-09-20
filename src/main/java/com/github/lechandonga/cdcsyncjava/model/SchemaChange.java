package com.github.lechandonga.cdcsyncjava.model;

import java.util.Objects;

/**
 * 一次结构演进操作。不可变。
 * <ul>
 *   <li>ADD_COLUMN：使用 targetColumn 作为新列名，newColumnType/newColumnNullable 描述类型</li>
 *   <li>RENAME_COLUMN：sourceColumn -> targetColumn</li>
 *   <li>DROP_COLUMN：使用 sourceColumn</li>
 * </ul>
 */
public record SchemaChange(EvolutionType type, String sourceColumn, String targetColumn,
                          ColumnType newColumnType, boolean newColumnNullable) {

    public SchemaChange {
        Objects.requireNonNull(type, "type");
    }

    public static SchemaChange addColumn(String name, ColumnType type, boolean nullable) {
        return new SchemaChange(EvolutionType.ADD_COLUMN, null,
                Objects.requireNonNull(name), Objects.requireNonNull(type), nullable);
    }

    public static SchemaChange renameColumn(String from, String to) {
        return new SchemaChange(EvolutionType.RENAME_COLUMN,
                Objects.requireNonNull(from), Objects.requireNonNull(to), null, false);
    }

    public static SchemaChange dropColumn(String name) {
        return new SchemaChange(EvolutionType.DROP_COLUMN,
                Objects.requireNonNull(name), null, null, false);
    }

    /** 列改型：sourceColumn 的类型改为 newColumnType（本引擎目标端一律拒绝，用于演示不兼容演进）。 */
    public static SchemaChange alterType(String column, ColumnType newType) {
        return new SchemaChange(EvolutionType.ALTER_TYPE,
                Objects.requireNonNull(column), null,
                Objects.requireNonNull(newType), false);
    }
}
