package com.github.lechandonga.cdcsyncjava.error;

/**
 * 被跳过 / 拒绝的事件记录，可定位到具体记录（eventId/key/seq）与原因。
 */
public record SkippedEvent(String eventId,
                           String key,
                           long seq,
                           String batchId,
                           RejectReason reason,
                           String detail,
                           long timestamp) {

    public static SkippedEvent of(String eventId, String key, long seq, String batchId,
                                  RejectReason reason, String detail) {
        return new SkippedEvent(eventId, key, seq, batchId, reason, detail, System.currentTimeMillis());
    }
}
