package com.github.lechandonga.cdcsyncjava.tests.support;

import com.github.lechandonga.cdcsyncjava.config.SyncConfig;
import com.github.lechandonga.cdcsyncjava.engine.SyncEngine;
import com.github.lechandonga.cdcsyncjava.model.FieldDef;
import com.github.lechandonga.cdcsyncjava.model.FieldType;
import com.github.lechandonga.cdcsyncjava.source.LocalMutableSource;
import com.github.lechandonga.cdcsyncjava.target.LocalTarget;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/** 测试辅助：构造带基础 schema 的源端与基于临时目录的引擎。 */
public final class SyncTestSupport {

    public static final List<FieldDef> BASE_FIELDS = List.of(
            new FieldDef("name", FieldType.STRING),
            new FieldDef("age", FieldType.LONG));

    private SyncTestSupport() {
    }

    public static LocalMutableSource newSource() {
        return new LocalMutableSource(BASE_FIELDS);
    }

    public static LocalTarget newTarget(Path dir) {
        return new LocalTarget(dir);
    }

    public static SyncEngine newEngine(LocalMutableSource source, LocalTarget target, SyncConfig config) {
        return new SyncEngine(source, target, config);
    }

    public static Map<String, Object> row(String name, long age) {
        return Map.of("name", name, "age", age);
    }
}
