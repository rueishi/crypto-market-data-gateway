package io.rueishi.marketdata.crypto.core.publisher;

import io.rueishi.marketdata.crypto.core.encoding.EncodingConstants;
import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Bounded in-memory {@link Publisher} for deterministic tests and local runtime capture.
 *
 * <p>{@code InMemoryPublisher} copies every published message into a
 * pre-allocated ring of byte-array slots before returning. That satisfies the
 * {@link Publisher} buffer ownership contract: it never retains the encoder's
 * reusable {@link DirectBuffer} reference, so later encoder writes cannot
 * corrupt already captured messages. It is thread-compatible for the single
 * event-loop-thread model but is not a production downstream adapter.</p>
 *
 * <p>The publisher always accepts hot-path messages and returns {@code true}.
 * {@link #publishReset(int, byte, byte, byte, NanoClock)} encodes a zero-entry
 * {@code BOOK_RESET} directly into the same capture ring for recovery and
 * shutdown tests, while deliberately not recording publisher-stage latency.</p>
 */
public final class InMemoryPublisher implements Publisher {
    private static final int DEFAULT_CAPACITY = 1024;
    private static final int DEFAULT_MAX_MESSAGE_BYTES = 300_000;

    private final byte[][] slots;
    private final int[] lengths;
    private final UnsafeBuffer resetEncodingBuffer = new UnsafeBuffer(new byte[EncodingConstants.MESSAGE_PREFIX_LENGTH]);
    private int writeIdx;
    private int totalCount;

    /**
     * Creates a publisher with the default ring capacity and message size.
     */
    public InMemoryPublisher() {
        this(DEFAULT_CAPACITY, DEFAULT_MAX_MESSAGE_BYTES);
    }

    /**
     * Creates a publisher with explicit bounded capture storage.
     *
     * @param capacity number of message slots in the capture ring
     * @param maxMessageBytes byte capacity of each slot
     * @throws IllegalArgumentException if either bound is not positive
     */
    public InMemoryPublisher(int capacity, int maxMessageBytes) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        if (maxMessageBytes < EncodingConstants.MESSAGE_PREFIX_LENGTH) {
            throw new IllegalArgumentException(
                    "maxMessageBytes must be at least " + EncodingConstants.MESSAGE_PREFIX_LENGTH);
        }
        this.slots = new byte[capacity][maxMessageBytes];
        this.lengths = new int[capacity];
    }

    /**
     * Copies one encoded message into the next ring slot before returning.
     *
     * <p>The method performs deterministic bounded work and never stores the
     * provided buffer reference. It records publisher-stage latency when counters
     * and a clock are supplied, then increments the message-published counter.
     * Test callers may pass {@code null} counters or clocks when they only need
     * capture behavior.</p>
     *
     * @param buffer encoder-owned reusable direct buffer
     * @param offset first byte to copy
     * @param length number of bytes to copy
     * @param counters per-instrument counters, or null for tests that do not track metrics
     * @param nanoClock caller-owned clock, or null when latency counters are not being tested
     * @return always {@code true}
     * @throws NullPointerException if {@code buffer} is null
     * @throws IllegalArgumentException if the requested region is empty, out of bounds, or too large for a slot
     */
    @Override
    public boolean publish(DirectBuffer buffer, int offset, int length, InstrumentCounters counters, NanoClock nanoClock) {
        validateRegion(buffer, offset, length);
        long startNanos = nanoClock == null ? 0L : nanoClock.nanoTime();
        capture(buffer, offset, length);
        if (counters != null) {
            counters.messagesPublished().increment();
            if (nanoClock != null) {
                PublisherLatencyStats.record(counters, nanoClock.nanoTime() - startNanos);
            }
        }
        return true;
    }

    /**
     * Encodes and captures a zero-entry {@code BOOK_RESET} control message.
     *
     * <p>The reset message has no repeating group, uses sequence fields of
     * {@code 0}, stores {@code -1} for exchange timestamp, and uses
     * {@code nanoClock.nanoTime()} for ingress timestamp. Publisher-stage
     * latency counters are intentionally not updated for this control path.</p>
     *
     * @param instrumentId stable internal instrument id
     * @param venueByte venue byte to encode
     * @param bookDepthByte book depth byte to encode
     * @param templateIdByte template id byte to encode
     * @param nanoClock clock used for reset ingress timestamp
     * @throws NullPointerException if {@code nanoClock} is null
     * @throws IllegalArgumentException if the reset message cannot fit in a capture slot
     */
    @Override
    public void publishReset(int instrumentId, byte venueByte, byte bookDepthByte, byte templateIdByte, NanoClock nanoClock) {
        long ingressTimestamp = nanoClock.nanoTime();
        resetEncodingBuffer.putShort(
                EncodingConstants.MAGIC_OFFSET,
                (short) EncodingConstants.MAGIC,
                EncodingConstants.BYTE_ORDER);
        resetEncodingBuffer.putByte(EncodingConstants.VERSION_OFFSET, (byte) EncodingConstants.VERSION);
        resetEncodingBuffer.putByte(EncodingConstants.TEMPLATE_ID_OFFSET, templateIdByte);
        resetEncodingBuffer.putShort(
                EncodingConstants.BLOCK_LENGTH_OFFSET,
                (short) EncodingConstants.BODY_BLOCK_LENGTH,
                EncodingConstants.BYTE_ORDER);
        resetEncodingBuffer.putShort(EncodingConstants.ENTRY_COUNT_OFFSET, (short) 0, EncodingConstants.BYTE_ORDER);

        resetEncodingBuffer.putByte(EncodingConstants.EVENT_TYPE_OFFSET, EncodingConstants.EVENT_TYPE_BOOK_RESET);
        resetEncodingBuffer.putByte(EncodingConstants.VENUE_OFFSET, venueByte);
        resetEncodingBuffer.putByte(EncodingConstants.BOOK_DEPTH_OFFSET, bookDepthByte);
        resetEncodingBuffer.putInt(EncodingConstants.INSTRUMENT_ID_OFFSET, instrumentId, ByteOrder.LITTLE_ENDIAN);
        resetEncodingBuffer.putLong(EncodingConstants.GATEWAY_MESSAGE_SEQ_OFFSET, 0L, EncodingConstants.BYTE_ORDER);
        resetEncodingBuffer.putLong(EncodingConstants.SEQ1_OFFSET, 0L, EncodingConstants.BYTE_ORDER);
        resetEncodingBuffer.putLong(EncodingConstants.SEQ2_OFFSET, 0L, EncodingConstants.BYTE_ORDER);
        resetEncodingBuffer.putLong(EncodingConstants.EXCHANGE_TIMESTAMP_OFFSET, -1L, EncodingConstants.BYTE_ORDER);
        resetEncodingBuffer.putLong(EncodingConstants.INGRESS_TIMESTAMP_OFFSET, ingressTimestamp, EncodingConstants.BYTE_ORDER);

        capture(resetEncodingBuffer, 0, EncodingConstants.MESSAGE_PREFIX_LENGTH);
    }

    /**
     * Returns the most recently captured message.
     *
     * @return a defensive copy of the latest message bytes
     * @throws IllegalStateException if no messages have been captured
     */
    public byte[] lastMessage() {
        if (totalCount == 0) {
            throw new IllegalStateException("No messages have been captured");
        }
        int slot = previousSlot();
        return Arrays.copyOf(slots[slot], lengths[slot]);
    }

    /**
     * Returns all currently retained messages in publication order.
     *
     * <p>When the ring has wrapped, only the most recent {@code capacity}
     * messages are retained and returned from oldest retained to newest.</p>
     *
     * @return defensive copies of retained messages
     */
    public List<byte[]> allMessages() {
        int retained = Math.min(totalCount, slots.length);
        int firstSlot = totalCount > slots.length ? writeIdx % slots.length : 0;
        List<byte[]> messages = new ArrayList<>(retained);
        for (int i = 0; i < retained; i++) {
            int slot = (firstSlot + i) % slots.length;
            messages.add(Arrays.copyOf(slots[slot], lengths[slot]));
        }
        return messages;
    }

    /**
     * Returns the total number of captured publish or reset messages since construction or reset.
     *
     * @return total captured message count
     */
    public int totalCount() {
        return totalCount;
    }

    /**
     * Clears capture state without reallocating the backing ring storage.
     */
    public void reset() {
        Arrays.fill(lengths, 0);
        writeIdx = 0;
        totalCount = 0;
    }

    /**
     * Returns the ring capacity in message slots.
     *
     * @return number of pre-allocated slots
     */
    public int capacity() {
        return slots.length;
    }

    /**
     * Returns the byte capacity of each message slot.
     *
     * @return maximum captured bytes per message
     */
    public int maxMessageBytes() {
        return slots[0].length;
    }

    private void capture(DirectBuffer buffer, int offset, int length) {
        if (length > maxMessageBytes()) {
            throw new IllegalArgumentException("length exceeds maxMessageBytes: " + length);
        }
        int slot = writeIdx % slots.length;
        buffer.getBytes(offset, slots[slot], 0, length);
        lengths[slot] = length;
        writeIdx++;
        totalCount++;
    }

    private void validateRegion(DirectBuffer buffer, int offset, int length) {
        if (buffer == null) {
            throw new NullPointerException("buffer");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset must not be negative");
        }
        if (length <= 0) {
            throw new IllegalArgumentException("length must be positive");
        }
        if (offset > buffer.capacity() - length) {
            throw new IllegalArgumentException("buffer region is out of bounds");
        }
    }

    private int previousSlot() {
        return (writeIdx - 1 + slots.length) % slots.length;
    }
}
