package io.rueishi.marketdata.crypto.core.encoding;

import io.rueishi.marketdata.crypto.core.config.TemplateId;
import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import io.rueishi.marketdata.crypto.core.publisher.Publisher;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Encodes normalized market-data messages into a reusable off-heap binary buffer.
 *
 * <p>{@code SbeEncoder} is constructed once per connector after bootstrap has
 * resolved the configured instrument, venue, book depth, and {@link TemplateId}.
 * Venue parsers drive it in the event-loop flow by calling
 * {@link #beginMessage(byte, long, long, long, long, long)}, then
 * {@link #writeLevel(byte, byte, long, byte, long, byte)} or
 * {@link #writeOrder(long, byte, byte, byte, long, byte, long, byte)}, then
 * {@link #endMessage(Publisher, InstrumentCounters, NanoClock)}. Parser
 * contexts that own retry and recovery policy use {@link #finishMessage()} and
 * publish the returned byte region themselves.</p>
 *
 * <p>The encoder is not thread-safe and is intended to be owned exclusively by
 * one connector/event-loop thread. Its direct buffer is allocated once in the
 * constructor and reused for every message; entry count is patched at
 * {@code endMessage} so parsers can encode in a single pass without pre-counting
 * book entries.</p>
 */
public final class SbeEncoder {
    private final int instrumentId;
    private final byte venueByte;
    private final byte bookDepthByte;
    private final byte templateIdByte;
    private final int maxEntryCount;
    private final int entryLength;
    private final UnsafeBuffer buffer;

    private int entryCount;
    private int position;
    private boolean inProgress;
    private long currentIngressTimestamp;
    private long lastIngressTimestamp;
    private long reuseCount;

    /**
     * Creates an encoder for one connector/instrument/template combination.
     *
     * @param instrumentId stable configured internal id for this connector's instrument
     * @param venueByte immutable venue byte from the configured venue
     * @param bookDepthByte immutable book-depth byte from the configured venue
     * @param templateIdByte immutable template id byte from the configured venue
     * @param maxEntryCount maximum repeating-group entries per message
     * @param headroomBytes additional buffer capacity beyond the maximum encoded size
     * @throws IllegalArgumentException if the id, template, entry count, or headroom is invalid
     */
    public SbeEncoder(
            int instrumentId,
            byte venueByte,
            byte bookDepthByte,
            byte templateIdByte,
            int maxEntryCount,
            int headroomBytes) {
        if (instrumentId <= 0) {
            throw new IllegalArgumentException("instrumentId must be positive");
        }
        this.instrumentId = instrumentId;
        this.venueByte = venueByte;
        this.bookDepthByte = bookDepthByte;
        this.templateIdByte = templateIdByte;
        this.maxEntryCount = maxEntryCount;
        this.entryLength = EncodingConstants.entryLength(templateIdByte);
        this.buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(
                EncodingConstants.requiredCapacity(templateIdByte, maxEntryCount, headroomBytes)));
    }

    /**
     * Begins a new encoded message and writes the fixed header/body fields.
     *
     * <p>This method resets the internal entry counter to zero and writes a
     * placeholder {@code entryCount} into the header. It must be called only
     * when no message is already in progress; otherwise the reusable buffer
     * would be overwritten before the previous message was finalized.</p>
     *
     * @param eventType event type byte, such as BOOK_SNAPSHOT or BOOK_UPDATE
     * @param gatewayMessageSeq gateway-managed per-session message sequence
     * @param seq1 first venue-specific sequence value
     * @param seq2 second venue-specific sequence value
     * @param exchangeTimestamp exchange event time in epoch nanoseconds, or zero/-1 when absent
     * @param ingressTimestamp gateway ingress time in epoch nanoseconds
     * @throws IllegalStateException if another message is already in progress
     */
    public void beginMessage(
            byte eventType,
            long gatewayMessageSeq,
            long seq1,
            long seq2,
            long exchangeTimestamp,
            long ingressTimestamp) {
        if (inProgress) {
            throw new IllegalStateException("beginMessage called while a message is already in progress");
        }

        entryCount = 0;
        position = EncodingConstants.REPEATING_GROUP_OFFSET;
        inProgress = true;
        currentIngressTimestamp = ingressTimestamp;

        buffer.putShort(EncodingConstants.MAGIC_OFFSET, (short) EncodingConstants.MAGIC, EncodingConstants.BYTE_ORDER);
        buffer.putByte(EncodingConstants.VERSION_OFFSET, (byte) EncodingConstants.VERSION);
        buffer.putByte(EncodingConstants.TEMPLATE_ID_OFFSET, templateIdByte);
        buffer.putShort(
                EncodingConstants.BLOCK_LENGTH_OFFSET,
                (short) EncodingConstants.BODY_BLOCK_LENGTH,
                EncodingConstants.BYTE_ORDER);
        buffer.putShort(EncodingConstants.ENTRY_COUNT_OFFSET, (short) 0, EncodingConstants.BYTE_ORDER);

        buffer.putByte(EncodingConstants.EVENT_TYPE_OFFSET, eventType);
        buffer.putByte(EncodingConstants.VENUE_OFFSET, venueByte);
        buffer.putByte(EncodingConstants.BOOK_DEPTH_OFFSET, bookDepthByte);
        buffer.putInt(EncodingConstants.INSTRUMENT_ID_OFFSET, instrumentId, EncodingConstants.BYTE_ORDER);
        buffer.putLong(EncodingConstants.GATEWAY_MESSAGE_SEQ_OFFSET, gatewayMessageSeq, EncodingConstants.BYTE_ORDER);
        buffer.putLong(EncodingConstants.SEQ1_OFFSET, seq1, EncodingConstants.BYTE_ORDER);
        buffer.putLong(EncodingConstants.SEQ2_OFFSET, seq2, EncodingConstants.BYTE_ORDER);
        buffer.putLong(
                EncodingConstants.EXCHANGE_TIMESTAMP_OFFSET,
                exchangeTimestamp == 0 ? -1L : exchangeTimestamp,
                EncodingConstants.BYTE_ORDER);
        buffer.putLong(EncodingConstants.INGRESS_TIMESTAMP_OFFSET, ingressTimestamp, EncodingConstants.BYTE_ORDER);
    }

    /**
     * Writes one aggregated price-level entry for a {@link TemplateId#BOOK_LEVEL} message.
     *
     * <p>The method writes the 20-byte entry at the current repeating-group
     * position and advances the internal cursor. It fails if the encoder was
     * constructed for the ORDER_ENTRY template or if the configured entry limit
     * would be exceeded.</p>
     *
     * @param side side byte, such as BID=1 or ASK=2
     * @param action action byte, such as UPSERT=1 or DELETE=2
     * @param priceMantissa signed price mantissa
     * @param priceScale decimal scale for {@code priceMantissa}
     * @param qtyMantissa signed quantity mantissa
     * @param qtyScale decimal scale for {@code qtyMantissa}
     * @throws IllegalStateException if no message is in progress or the template is not BOOK_LEVEL
     * @throws IllegalArgumentException if the configured entry limit would be exceeded
     */
    public void writeLevel(byte side, byte action, long priceMantissa, byte priceScale, long qtyMantissa, byte qtyScale) {
        requireInProgress();
        if (templateIdByte != TemplateId.BOOK_LEVEL.byteValue()) {
            throw new IllegalStateException("writeLevel requires BOOK_LEVEL template");
        }
        requireEntryCapacity();

        int offset = position;
        buffer.putByte(offset, side);
        buffer.putByte(offset + 1, action);
        buffer.putByte(offset + 2, priceScale);
        buffer.putByte(offset + 3, qtyScale);
        buffer.putLong(offset + 4, priceMantissa, EncodingConstants.BYTE_ORDER);
        buffer.putLong(offset + 12, qtyMantissa, EncodingConstants.BYTE_ORDER);
        advanceEntry();
    }

    /**
     * Writes one individual order entry for a {@link TemplateId#ORDER_ENTRY} message.
     *
     * <p>The method writes the 29-byte entry at the current repeating-group
     * position and advances the internal cursor. It fails if the encoder was
     * constructed for the BOOK_LEVEL template or if the configured entry limit
     * would be exceeded.</p>
     *
     * @param orderId exchange-assigned stable order identifier
     * @param side side byte, such as BID=1 or ASK=2
     * @param action action byte, such as UPSERT=1 or DELETE=2
     * @param orderType order type byte, such as LIMIT=1
     * @param priceMantissa signed price mantissa
     * @param priceScale decimal scale for {@code priceMantissa}
     * @param qtyMantissa signed remaining quantity mantissa
     * @param qtyScale decimal scale for {@code qtyMantissa}
     * @throws IllegalStateException if no message is in progress or the template is not ORDER_ENTRY
     * @throws IllegalArgumentException if the configured entry limit would be exceeded
     */
    public void writeOrder(
            long orderId,
            byte side,
            byte action,
            byte orderType,
            long priceMantissa,
            byte priceScale,
            long qtyMantissa,
            byte qtyScale) {
        requireInProgress();
        if (templateIdByte != TemplateId.ORDER_ENTRY.byteValue()) {
            throw new IllegalStateException("writeOrder requires ORDER_ENTRY template");
        }
        requireEntryCapacity();

        int offset = position;
        buffer.putLong(offset, orderId, EncodingConstants.BYTE_ORDER);
        buffer.putByte(offset + 8, side);
        buffer.putByte(offset + 9, action);
        buffer.putByte(offset + 10, orderType);
        buffer.putByte(offset + 11, priceScale);
        buffer.putByte(offset + 12, qtyScale);
        buffer.putLong(offset + 13, priceMantissa, EncodingConstants.BYTE_ORDER);
        buffer.putLong(offset + 21, qtyMantissa, EncodingConstants.BYTE_ORDER);
        advanceEntry();
    }

    /**
     * Finalizes the current message, patches entry count, and hands the buffer to the publisher.
     *
     * <p>After this method returns, the buffer is available for reuse by the
     * next {@link #beginMessage(byte, long, long, long, long, long)} call. The
     * publisher must not retain the buffer reference. {@link #reuseCount()} is
     * incremented regardless of whether the publisher accepts the message.</p>
     *
     * @param publisher downstream publisher invoked synchronously on the caller thread
     * @param counters per-instrument counters supplied to publisher timing/backpressure code
     * @param nanoClock caller-owned clock supplied to the publisher
     * @return true if the publisher accepted the message, false if it reported backpressure
     * @throws NullPointerException if {@code publisher} is null
     * @throws IllegalStateException if no message is in progress
     */
    public boolean endMessage(Publisher publisher, InstrumentCounters counters, NanoClock nanoClock) {
        Objects.requireNonNull(publisher, "publisher");
        int length = finishMessage();
        return publisher.publish(buffer, 0, length, counters, nanoClock);
    }

    /**
     * Finalizes the current message and returns the encoded byte length for caller-owned publishing.
     *
     * <p>This method patches the exact {@code entryCount} into the binary
     * header, records the message ingress timestamp for handoff latency
     * calculation, increments the buffer reuse counter, and clears the
     * in-progress flag. It deliberately does not call a publisher or apply
     * retry/recovery policy; {@link io.rueishi.marketdata.crypto.core.parser.DefaultParseContext}
     * owns that higher-level behavior for parser-originated messages.</p>
     *
     * @return number of encoded bytes in the finalized message
     * @throws IllegalStateException if no message is in progress
     */
    public int finishMessage() {
        requireInProgress();
        buffer.putShort(EncodingConstants.ENTRY_COUNT_OFFSET, (short) entryCount, EncodingConstants.BYTE_ORDER);
        int length = position;
        lastIngressTimestamp = currentIngressTimestamp;
        currentIngressTimestamp = 0L;
        reuseCount++;
        inProgress = false;
        return length;
    }

    /**
     * Returns the ingress timestamp from the most recently finalized message.
     *
     * <p>{@link io.rueishi.marketdata.crypto.core.parser.DefaultParseContext}
     * reads this value immediately after {@link #finishMessage()} to update
     * ingress-to-handoff latency counters before the successful publish
     * handoff. The value is overwritten on each finalized message.</p>
     *
     * @return ingress timestamp supplied to the last completed {@link #beginMessage} call
     */
    public long lastIngressTimestamp() {
        return lastIngressTimestamp;
    }

    /**
     * Returns the number of finalized messages that have reused this encoder buffer.
     *
     * @return total calls to {@link #endMessage(Publisher, InstrumentCounters, NanoClock)}
     */
    public long reuseCount() {
        return reuseCount;
    }

    /**
     * Exposes the encoder-owned buffer for low-level tests.
     *
     * <p>Production callers should not depend on this accessor for message
     * handoff; {@link #endMessage(Publisher, InstrumentCounters, NanoClock)}
     * owns the publisher contract.</p>
     *
     * @return the reusable direct buffer owned by this encoder
     */
    public DirectBuffer buffer() {
        return buffer;
    }

    /**
     * Returns the allocated capacity of the reusable direct buffer.
     *
     * @return direct buffer capacity in bytes
     */
    public int capacity() {
        return buffer.capacity();
    }

    private void requireInProgress() {
        if (!inProgress) {
            throw new IllegalStateException("No message is in progress");
        }
    }

    private void requireEntryCapacity() {
        if (entryCount == maxEntryCount) {
            throw new IllegalArgumentException("entryCount exceeds configured maxEntryCount: " + maxEntryCount);
        }
    }

    private void advanceEntry() {
        entryCount++;
        position += entryLength;
    }
}
