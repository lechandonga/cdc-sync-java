package com.github.lechandonga.cdcsyncjava.model;

import java.util.List;
import java.util.Objects;

/**
 * 某一个 schema 版本的表结构。
 *
 * @param version 单调递增的版本号，从 1 开始
 * @param keyColumn 主键列名
 * @param columns 列定义（按声明顺序）
 */
public record TableSchema(int version, String keyColumn, List<ColumnDef> columns) {

    public TableSchema {
        Objects.requireNonNull(keyColumn, "keyColumn");
        columns = List.copyOf(columns);
    }

    /** 找到指定列定义，不存在返回 null。 */
    public ColumnDef findColumn(String name) {
        for (ColumnDef c : columns) {
            if (c.name().equals(name)) {
                return c;
            }
        }
        return null;
    }

    /** 是否包含指定列。 */
    public boolean hasColumn(String name) {
        return findColumn(name) != null;
    }
}
