package com.github.lechandonga.cdcsyncjava.error;

/** 同步引擎统一的受检语义运行时异常基类。 */
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
