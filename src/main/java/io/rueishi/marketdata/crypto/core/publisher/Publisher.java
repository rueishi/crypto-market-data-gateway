package io.rueishi.marketdata.crypto.core.publisher;

import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.NanoClock;

/**
 * Downstream handoff contract for encoded gateway messages.
 *
 * <p>{@code Publisher} is called on the connector event-loop thread after a
 * message has been encoded into the connector's reusable direct buffer. Direct
 * tests may call it through {@code SbeEncoder.endMessage(...)}, while runtime
 * parser flow calls it through {@link io.rueishi.marketdata.crypto.core.parser.ParseContext#publishEncodedMessage(String)}
 * so retry, recovery, and handoff latency policy stays outside venue parsers.
 * Implementations must consume or copy the requested buffer region before
 * returning because the encoder can overwrite that same buffer on the next
 * message.</p>
 */
public interface Publisher {

    /**
     * Publishes one encoded message from the supplied buffer region.
     *
     * <p>The publisher must not retain {@code buffer} after this method returns.
     * Synchronous implementations must finish reading the region before
     * returning. Asynchronous implementations must copy the region into storage
     * they own before returning. Returning {@code false} signals backpressure;
     * retry and recovery policy remain the caller's responsibility.</p>
     *
     * @param buffer encoder-owned reusable direct buffer, valid only during this call
     * @param offset first byte of the encoded message within {@code buffer}
     * @param length number of encoded bytes to publish
     * @param counters per-instrument counters associated with this message
     * @param nanoClock caller-owned clock for publisher-stage timing
     * @return true if the message was delivered or queued, false if backpressure prevented delivery
     */
    boolean publish(DirectBuffer buffer, int offset, int length, InstrumentCounters counters, NanoClock nanoClock);

    /**
     * Publishes a zero-entry {@code BOOK_RESET} control message for a clean recovery or shutdown boundary.
     *
     * <p>This method is called outside normal parser message encoding and must
     * follow the same buffer ownership rule as {@link #publish(DirectBuffer, int,
     * int, InstrumentCounters, NanoClock)}. It does not update publisher-stage
     * latency counters because reset publication is a control-path operation.</p>
     *
     * @param instrumentId stable internal instrument id for the reset
     * @param venueByte immutable venue byte for the connector
     * @param bookDepthByte immutable depth byte for the connector
     * @param templateIdByte immutable template byte for the connector
     * @param nanoClock caller-owned clock used to stamp reset ingress time
     */
    void publishReset(int instrumentId, byte venueByte, byte bookDepthByte, byte templateIdByte, NanoClock nanoClock);
}
