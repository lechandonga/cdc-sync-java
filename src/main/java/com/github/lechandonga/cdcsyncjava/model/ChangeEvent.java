package com.github.lechandonga.cdcsyncjava.model;

import java.util.Map;

/**
 * 一条变更事件。
 *
 * @param seq           源端全局单调递增的提交序号（位点的核心组成部分）
 * @param schemaVersion 该事件写入时的源端模式版本
 * @param data          UPSERT 时的字段值；DELETE 通常为空
 */
public final class ChangeEvent {

    private final String eventId;
    private final String key;
    private final long seq;
    private final int schemaVersion;
    private final ChangeType type;
    private final Map<String, Object> data;

    public ChangeEvent(String eventId, String key, long seq, int schemaVersion,
                       ChangeType type, Map<String, Object> data) {
        this.eventId = eventId;
        this.key = key;
        this.seq = seq;
        this.schemaVersion = schemaVersion;
        this.type = type;
        this.data = data;
    }

    public String eventId() { return eventId; }
    public String key() { return key; }
    public long seq() { return seq; }
    public int schemaVersion() { return schemaVersion; }
    public ChangeType type() { return type; }
    public Map<String, Object> data() { return data; }

    @Override
    public String toString() {
        return "ChangeEvent{" + eventId + ", key=" + key + ", seq=" + seq
                + ", schemaVersion=" + schemaVersion + ", type=" + type + '}';
    }
}
