package com.github.lechandonga.cdcsyncjava.source;

import java.util.List;
import java.util.Map;

import com.github.lechandonga.cdcsyncjava.model.ColumnDef;
import com.github.lechandonga.cdcsyncjava.model.ColumnType;
import com.github.lechandonga.cdcsyncjava.model.SchemaChange;
import com.github.lechandonga.cdcsyncjava.model.TableSchema;

/**
 * 内置的可变源端数据源，模拟真实数据库。
 * 所有写入以全局递增 commitSeq 提交；快照与日志共用同一把提交锁保证衔接一致。
 */
public interface LocalSourceDatabase extends SnapshotReader, LogReader {

    /** 当前（最新）schema。 */
    TableSchema currentSchema();

    /** 指定版本的 schema（版本不存在抛异常）。 */
    TableSchema schemaAt(int version);

    /** 产生 version 版本（version >= 2）的那次结构演进内容。 */
    SchemaChange schemaChangeTo(int version);

    /** 初始化建表。 */
    void initialize(String keyColumn, List<ColumnDef> columns);

    /** 提交一行 insert/update，返回 commitSeq。 */
    long put(String key, Map<String, Object> values);

    /** 删除一行，返回 commitSeq。 */
    long delete(String key);

    /** 提交一次结构演进，返回 commitSeq。 */
    long evolve(SchemaChange change);

    /** 当前最新提交位点。 */
    long latestCommitSeq();
}
