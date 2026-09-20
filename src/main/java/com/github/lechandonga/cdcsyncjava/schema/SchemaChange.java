package com.github.lechandonga.cdcsyncjava.schema;

import com.github.lechandonga.cdcsyncjava.model.FieldType;

/**
 * 模式演进操作（仅允许兼容演进）。
 */
public sealed interface SchemaChange
        permits SchemaChange.AddField, SchemaChange.RenameField, SchemaChange.RemoveField {

    record AddField(String name, FieldType type) implements SchemaChange {
    }

    record RenameField(String from, String to) implements SchemaChange {
    }

    record RemoveField(String name) implements SchemaChange {
    }
}
