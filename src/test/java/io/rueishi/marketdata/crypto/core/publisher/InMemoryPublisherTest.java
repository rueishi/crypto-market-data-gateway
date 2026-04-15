package io.rueishi.marketdata.crypto.core.publisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.rueishi.marketdata.crypto.core.CoreTestFixtures;
import io.rueishi.marketdata.crypto.core.config.BookDepth;
import io.rueishi.marketdata.crypto.core.config.TemplateId;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.encoding.EncodingConstants;
import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link InMemoryPublisher}.
 *
 * <p>These tests verify the Phase 1 test publisher behavior: deterministic
 * bounded capture, defensive message copies, direct BOOK_RESET encoding, input
 * validation, and publisher-stage latency counter updates. The tests use
 * in-memory Agrona buffers and counters rather than real transport or connector
 * implementations.</p>
 */
class InMemoryPublisherTest {

    /**
     * Verifies that a published buffer region is copied into bounded capture storage.
     */
    @Test
    void capturesPublishedBufferRegion() {
        InMemoryPublisher publisher = new InMemoryPublisher(2, 64);
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(16));
        buffer.putByte(4, (byte) 0x11);
        buffer.putByte(5, (byte) 0x22);
        buffer.putByte(6, (byte) 0x33);

        assertThat(publisher.publish(buffer, 4, 3, null, null)).isTrue();

        assertThat(publisher.totalCount()).isEqualTo(1);
        assertThat(publisher.lastMessage()).containsExactly((byte) 0x11, (byte) 0x22, (byte) 0x33);
    }

    /**
     * Verifies that allMessages returns retained messages from oldest to newest after ring wrap.
     */
    @Test
    void returnsRetainedMessagesInPublicationOrderAfterWrap() {
        InMemoryPublisher publisher = new InMemoryPublisher(2, 64);
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[8]);

        publishOneByte(publisher, buffer, 1);
        publishOneByte(publisher, buffer, 2);
        publishOneByte(publisher, buffer, 3);

        assertThat(publisher.totalCount()).isEqualTo(3);
        assertThat(publisher.allMessages())
                .extracting(message -> message[0])
                .containsExactly((byte) 2, (byte) 3);
    }

    /**
     * Verifies that reset clears capture state while retaining configured capacity.
     */
    @Test
    void resetClearsCaptureStateWithoutChangingCapacity() {
        InMemoryPublisher publisher = new InMemoryPublisher(3, 64);
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[] {42});

        publisher.publish(buffer, 0, 1, null, null);
        publisher.reset();

        assertThat(publisher.totalCount()).isZero();
        assertThat(publisher.capacity()).isEqualTo(3);
        assertThat(publisher.maxMessageBytes()).isEqualTo(64);
        assertThatThrownBy(publisher::lastMessage)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No messages");
    }

    /**
     * Verifies that publishReset captures a schema v2 zero-entry BOOK_RESET without recording publisher latency.
     */
    @Test
    void publishReset_encodesCorrect59ByteBookReset() {
        InMemoryPublisher publisher = new InMemoryPublisher(2, 64);
        InstrumentCounters counters = CoreTestFixtures.newInstrumentCounters();

        publishReset(publisher);

        ByteBuffer decoded = ByteBuffer.wrap(publisher.lastMessage()).order(EncodingConstants.BYTE_ORDER);
        assertThat(publisher.lastMessage()).hasSize(EncodingConstants.BOOK_RESET_SIZE);
        assertThat(Short.toUnsignedInt(decoded.getShort(EncodingConstants.MAGIC_OFFSET))).isEqualTo(EncodingConstants.MAGIC);
        assertThat(Byte.toUnsignedInt(decoded.get(EncodingConstants.TEMPLATE_ID_OFFSET))).isEqualTo(TemplateId.BOOK_LEVEL.byteValue());
        assertThat(Short.toUnsignedInt(decoded.getShort(EncodingConstants.ENTRY_COUNT_OFFSET))).isZero();
        assertThat(Byte.toUnsignedInt(decoded.get(EncodingConstants.EVENT_TYPE_OFFSET)))
                .isEqualTo(EncodingConstants.EVENT_TYPE_BOOK_RESET);
        assertThat(Integer.toUnsignedLong(decoded.getInt(EncodingConstants.INSTRUMENT_ID_OFFSET))).isEqualTo(1001L);
        assertThat(decoded.getLong(EncodingConstants.GATEWAY_MESSAGE_SEQ_OFFSET)).isZero();
        assertThat(decoded.getLong(EncodingConstants.SEQ1_OFFSET)).isZero();
        assertThat(decoded.getLong(EncodingConstants.SEQ2_OFFSET)).isZero();
        assertThat(decoded.getLong(EncodingConstants.EXCHANGE_TIMESTAMP_OFFSET)).isEqualTo(EncodingConstants.NO_TIMESTAMP);
        assertThat(decoded.getLong(EncodingConstants.INGRESS_TIMESTAMP_OFFSET)).isEqualTo(123_456L);
        assertThat(decoded.getInt(EncodingConstants.CHECKSUM_OFFSET)).isEqualTo(EncodingConstants.NO_CHECKSUM);
        assertThat(counters.publisherLatencyLastNanos().get()).isZero();
    }

    /**
     * Verifies reset messages advertise the schema v2 body block length.
     */
    @Test
    void publishReset_blockLengthIs51() {
        InMemoryPublisher publisher = new InMemoryPublisher(1, 64);

        publishReset(publisher);

        ByteBuffer decoded = ByteBuffer.wrap(publisher.lastMessage()).order(EncodingConstants.BYTE_ORDER);
        assertThat(Short.toUnsignedInt(decoded.getShort(EncodingConstants.BLOCK_LENGTH_OFFSET)))
                .isEqualTo(EncodingConstants.BODY_BLOCK_LENGTH);
    }

    /**
     * Verifies reset messages explicitly write the schema v2 checksum placeholder at offset 55.
     */
    @Test
    void publishReset_checksumFieldIsZero() {
        InMemoryPublisher publisher = new InMemoryPublisher(1, 64);

        publishReset(publisher);

        ByteBuffer decoded = ByteBuffer.wrap(publisher.lastMessage()).order(EncodingConstants.BYTE_ORDER);
        assertThat(decoded.getInt(EncodingConstants.CHECKSUM_OFFSET)).isEqualTo(EncodingConstants.NO_CHECKSUM);
    }

    /**
     * Verifies BOOK_RESET is encoded as a zero-entry message with no repeating group.
     */
    @Test
    void publishReset_entryCountIsZero() {
        InMemoryPublisher publisher = new InMemoryPublisher(1, 64);

        publishReset(publisher);

        ByteBuffer decoded = ByteBuffer.wrap(publisher.lastMessage()).order(EncodingConstants.BYTE_ORDER);
        assertThat(Short.toUnsignedInt(decoded.getShort(EncodingConstants.ENTRY_COUNT_OFFSET))).isZero();
    }

    /**
     * Verifies reset messages carry schema version 2 in the header.
     */
    @Test
    void publishReset_schemaVersionIsTwo() {
        InMemoryPublisher publisher = new InMemoryPublisher(1, 64);

        publishReset(publisher);

        ByteBuffer decoded = ByteBuffer.wrap(publisher.lastMessage()).order(EncodingConstants.BYTE_ORDER);
        assertThat(Byte.toUnsignedInt(decoded.get(EncodingConstants.VERSION_OFFSET))).isEqualTo(EncodingConstants.VERSION);
        assertThat(EncodingConstants.VERSION).isEqualTo(2);
    }

    /**
     * Verifies malformed publish calls fail clearly instead of silently corrupting capture state.
     */
    @Test
    void rejectsInvalidPublishRegionsAndCapacity() {
        assertThatThrownBy(() -> new InMemoryPublisher(0, 64))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("capacity");
        assertThatThrownBy(() -> new InMemoryPublisher(1, EncodingConstants.MESSAGE_PREFIX_LENGTH - 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxMessageBytes");

        InMemoryPublisher publisher = new InMemoryPublisher(1, 64);
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[8]);

        assertThatThrownBy(() -> publisher.publish(buffer, 0, 0, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("length");
        assertThatThrownBy(() -> publisher.publish(buffer, 7, 2, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("out of bounds");
        assertThatThrownBy(() -> publisher.publish(buffer, 0, 65, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("out of bounds");
    }

    /**
     * Verifies that publish updates message count and publisher-stage latency counters when counters are supplied.
     */
    @Test
    void publishUpdatesMessageAndPublisherLatencyCounters() {
        InMemoryPublisher publisher = new InMemoryPublisher(1, 64);
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[] {1});
        InstrumentCounters counters = CoreTestFixtures.newInstrumentCounters();
        AtomicLong now = new AtomicLong(10);

        publisher.publish(buffer, 0, 1, counters, () -> now.getAndAdd(7));

        assertThat(counters.messagesPublished().get()).isEqualTo(1);
        assertThat(counters.publisherLatencyLastNanos().get()).isEqualTo(7);
        assertThat(counters.publisherLatencyMinNanos().get()).isEqualTo(7);
        assertThat(counters.publisherLatencyMaxNanos().get()).isEqualTo(7);
    }

    private static void publishOneByte(InMemoryPublisher publisher, UnsafeBuffer buffer, int value) {
        buffer.putByte(0, (byte) value);
        publisher.publish(buffer, 0, 1, null, null);
    }

    private static void publishReset(InMemoryPublisher publisher) {
        publisher.publishReset(
                1001,
                VenueEnum.COINBASE_L2.byteValue(),
                BookDepth.L2.byteValue(),
                TemplateId.BOOK_LEVEL.byteValue(),
                () -> 123_456L);
    }
}
