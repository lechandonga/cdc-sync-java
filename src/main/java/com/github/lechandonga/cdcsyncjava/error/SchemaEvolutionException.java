package com.github.lechandonga.cdcsyncjava.error;

/** 模式演进 / 事件投影不兼容时抛出。 */
public class SchemaEvolutionException extends SyncException {

    public SchemaEvolutionException(RejectReason reason, String message) {
        super(reason, message);
    }
}
