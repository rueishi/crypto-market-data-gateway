package io.rueishi.marketdata.crypto.core.connector;

import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.core.snapshot.SnapshotContext;
import io.rueishi.marketdata.crypto.core.snapshot.SnapshotStrategy;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test snapshot strategy that records trigger calls and the latest supplied context.
 */
final class RecordingSnapshotStrategy implements SnapshotStrategy {
    final AtomicInteger triggerCount = new AtomicInteger();
    SnapshotContext lastContext;

    @Override
    public Mode mode() {
        return Mode.SUBSCRIBE_DRIVEN;
    }

    @Override
    public void triggerSnapshot(InstrumentConfig instrument, SnapshotContext ctx) {
        triggerCount.incrementAndGet();
        lastContext = ctx;
    }
}
