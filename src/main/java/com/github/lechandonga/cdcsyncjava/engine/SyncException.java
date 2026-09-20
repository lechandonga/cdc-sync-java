package com.github.lechandonga.cdcsyncjava.engine;

import com.github.lechandonga.cdcsyncjava.model.RejectReason;

/** 带可区分拒绝原因的同步异常。 */
public class SyncException extends RuntimeException {

    private final RejectReason reason;

    public SyncException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public SyncException(RejectReason reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = reason;
    }

    public RejectReason reason() {
        return reason;
    }
}
