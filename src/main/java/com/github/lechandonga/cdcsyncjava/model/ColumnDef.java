package com.github.lechandonga.cdcsyncjava.model;

/** 列定义：名称、类型、是否允许为空。 */
public record ColumnDef(String name, ColumnType type, boolean nullable) {
}
