package com.github.lechandonga.cdcsyncjava.model;

/**
 * 源端位点。全局单调的提交序号即唯一位点。
 */
public record SourcePosition(long commitSeq) {

    public static final SourcePosition ZERO = new SourcePosition(0L);
}
