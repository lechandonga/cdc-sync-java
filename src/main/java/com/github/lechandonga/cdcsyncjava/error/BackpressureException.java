package com.github.lechandonga.cdcsyncjava.error;

/** 有界缓冲耗尽且策略为 REJECT 时抛出。 */
public class BackpressureException extends SyncException {

    public BackpressureException(String message) {
        super(RejectReason.BUFFER_FULL, message);
    }
}
