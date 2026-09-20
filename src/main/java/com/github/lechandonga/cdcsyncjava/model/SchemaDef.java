package com.github.lechandonga.cdcsyncjava.model;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 某个版本的模式（schema）：版本号单调递增，字段保持声明顺序。
 */
public final class SchemaDef {

    private final int version;
    private final LinkedHashMap<String, FieldDef> fields;

    public SchemaDef(int version, LinkedHashMap<String, FieldDef> fields) {
        this.version = version;
        this.fields = new LinkedHashMap<>(fields);
    }

    public int version() {
        return version;
    }

    public Map<String, FieldDef> fields() {
        return new LinkedHashMap<>(fields);
    }

    public Optional<FieldDef> field(String name) {
        return Optional.ofNullable(fields.get(name));
    }

    public boolean containsField(String name) {
        return fields.containsKey(name);
    }
}
