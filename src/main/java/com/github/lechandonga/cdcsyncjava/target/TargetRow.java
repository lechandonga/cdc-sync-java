package com.github.lechandonga.cdcsyncjava.target;

import java.util.Map;

/**
 * 目标端一行物化状态。
 *
 * @param key 主键
 * @param values 按当前 schema 归一化后的列值
 * @param schemaVersion 写入时依据的 schema 版本
 * @param commitSeq 最后一次生效的源端位点
 * @param deleted 是否为删除墓碑
 */
public record TargetRow(String key, Map<String, Object> values, int schemaVersion,
                        long commitSeq, boolean deleted) {
}
