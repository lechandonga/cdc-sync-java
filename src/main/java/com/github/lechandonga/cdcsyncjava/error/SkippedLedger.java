package com.github.lechandonga.cdcsyncjava.error;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** 线程安全的跳过事件台账（引擎与目标端共享）。 */
public final class SkippedLedger {

    private final CopyOnWriteArrayList<SkippedEvent> skipped = new CopyOnWriteArrayList<>();

    public void record(SkippedEvent s) {
        skipped.add(s);
    }

    public List<SkippedEvent> all() {
        return List.copyOf(skipped);
    }

    public int size() {
        return skipped.size();
    }

    public void clear() {
        skipped.clear();
    }
}
