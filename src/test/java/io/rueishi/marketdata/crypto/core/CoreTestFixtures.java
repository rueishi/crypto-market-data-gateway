package io.rueishi.marketdata.crypto.core;

import io.rueishi.marketdata.crypto.core.config.TemplateId;
import io.rueishi.marketdata.crypto.core.encoding.SbeEncoder;
import io.rueishi.marketdata.crypto.core.observability.GatewayCounters;
import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import io.rueishi.marketdata.crypto.core.publisher.Publisher;
import java.nio.ByteBuffer;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;

/**
 * Shared test fixtures for core package unit tests.
 *
 * <p>This class provides small in-memory dependencies needed by context tests
 * without introducing production implementations owned by later task cards.</p>
 */
public final class CoreTestFixtures {
    private CoreTestFixtures() {
    }

    /**
     * Creates an in-memory Agrona counter manager sized for unit tests.
     *
     * @return a direct-buffer-backed counter manager
     */
    public static CountersManager newCountersManager() {
        int maxCounters = 256;
        return new CountersManager(
                new UnsafeBuffer(ByteBuffer.allocateDirect(CountersReader.METADATA_LENGTH * maxCounters)),
                new UnsafeBuffer(ByteBuffer.allocateDirect(CountersReader.COUNTER_LENGTH * maxCounters)));
    }

    /**
     * Creates a per-instrument counter bundle for context tests.
     *
     * @return per-instrument counters allocated through a temporary gateway facade
     */
    @SuppressWarnings("resource")
    public static InstrumentCounters newInstrumentCounters() {
        GatewayCounters gatewayCounters = new GatewayCounters(newCountersManager(), "gateway-1", "test", "STUB");
        return gatewayCounters.forInstrument(1001);
    }

    /**
     * Creates a small BOOK_LEVEL encoder for context tests.
     *
     * @return encoder with room for a few entries
     */
    public static SbeEncoder newEncoder() {
        return new SbeEncoder(1001, (byte) 1, (byte) 2, TemplateId.BOOK_LEVEL.byteValue(), 4, 0);
    }

    /**
     * Creates a no-op publisher that accepts messages without retaining buffers.
     *
     * @return no-op publisher implementation for dependency-carrier tests
     */
    public static Publisher noopPublisher() {
        return new Publisher() {
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
            }
        };
    }
}
