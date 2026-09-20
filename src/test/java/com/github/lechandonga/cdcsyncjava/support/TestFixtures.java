package com.github.lechandonga.cdcsyncjava.support;

import java.util.List;
import java.util.Map;

import com.github.lechandonga.cdcsyncjava.model.ColumnDef;
import com.github.lechandonga.cdcsyncjava.model.ColumnType;
import com.github.lechandonga.cdcsyncjava.source.InMemorySourceDatabase;

/** 测试共用构造工具：id + name + age 的初始表。 */
public final class TestFixtures {

    public static final String KEY = "id";

    private TestFixtures() {
    }

    public static InMemorySourceDatabase newSource() {
        InMemorySourceDatabase db = new InMemorySourceDatabase();
        db.initialize(KEY, List.of(
                new ColumnDef(KEY, ColumnType.STRING, false),
                new ColumnDef("name", ColumnType.STRING, true),
                new ColumnDef("age", ColumnType.INTEGER, true)));
        return db;
    }

    public static Map<String, Object> row(String name, Object age) {
        return Map.of("name", name, "age", age);
    }
}
