package io.rueishi.marketdata.crypto.core.connector;

import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import io.rueishi.marketdata.crypto.core.publisher.Publisher;
import java.util.concurrent.atomic.AtomicInteger;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.NanoClock;

/**
 * Test publisher that records control reset publication.
 */
final class RecordingPublisher implements Publisher {
    final AtomicInteger resetCount = new AtomicInteger();

    @Override
    public boolean publish(
            DirectBuffer buffer,
            int offset,
            int length,
            InstrumentCounters counters,
            NanoClock nanoClock) {
        return true;
    }

    @Override
    public void publishReset(
            int instrumentId,
            byte venueByte,
            byte bookDepthByte,
            byte templateIdByte,
            NanoClock nanoClock) {
        resetCount.incrementAndGet();
    }
}
