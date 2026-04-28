package io.rueishi.marketdata.crypto.core.encoding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.rueishi.marketdata.crypto.core.config.BookDepth;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Byte-layout tests for {@link SbeEncoder} using the schema v2 ORDER_EVENT template.
 *
 * <p>The tests encode through the production encoder and inspect captured bytes directly, with no mocked dependencies
 * beyond the in-test capturing publisher. They cover the L3 order-event wire offsets, reason-code mapping surface,
 * entry length, template mismatch handling, and the schema v2 header/body fields written by {@code beginMessage()}.</p>
 */
@Tag("unit")
class SbeEncoderOrderEventTest {
    private static final long ORDER_ID_HIGH = 0xd50ec98477a8460aL;
    private static final long ORDER_ID_LOW = 0xb95866f114b0de9bL;

    /**
     * Verifies a received order event writes every schema v2 field at its exact documented offset.
     */
    @Test
    void writeOrderEvent_received_correctBytesAtAllOffsets() {
        SbeEncoderBookLevelTest.CapturingPublisher publisher = encodeOrderEvent(
                EncodingConstants.REASON_RECEIVED,
                EncodingConstants.ORDER_TYPE_LIMIT,
                EncodingConstants.NO_TIMESTAMP,
                33_398L,
                (byte) 2,
                100_000_000L,
                (byte) 8,
                0L,
                (byte) 0);
        ByteBuffer buffer = wrapped(publisher);
        int base = EncodingConstants.REPEATING_GROUP_OFFSET;

        assertThat(buffer.getLong(base + EncodingConstants.ORDER_EVENT_ORDER_ID_HIGH_OFFSET)).isEqualTo(ORDER_ID_HIGH);
        assertThat(buffer.getLong(base + EncodingConstants.ORDER_EVENT_ORDER_ID_LOW_OFFSET)).isEqualTo(ORDER_ID_LOW);
        assertThat(buffer.get(base + EncodingConstants.ORDER_EVENT_SIDE_OFFSET)).isEqualTo(EncodingConstants.SIDE_BID);
        assertThat(buffer.get(base + EncodingConstants.ORDER_EVENT_ACTION_OFFSET)).isEqualTo(EncodingConstants.ACTION_UPSERT);
        assertThat(buffer.get(base + EncodingConstants.ORDER_EVENT_REASON_OFFSET)).isEqualTo(EncodingConstants.REASON_RECEIVED);
        assertThat(buffer.get(base + EncodingConstants.ORDER_EVENT_ORDER_TYPE_OFFSET))
                .isEqualTo(EncodingConstants.ORDER_TYPE_LIMIT);
        assertThat(buffer.get(base + EncodingConstants.ORDER_EVENT_PRICE_SCALE_OFFSET)).isEqualTo((byte) 2);
        assertThat(buffer.get(base + EncodingConstants.ORDER_EVENT_QTY_SCALE_OFFSET)).isEqualTo((byte) 8);
        assertThat(buffer.get(base + EncodingConstants.ORDER_EVENT_OLD_QTY_SCALE_OFFSET)).isEqualTo((byte) 0);
        assertThat(buffer.getLong(base + EncodingConstants.ORDER_EVENT_ORDER_TS_OFFSET))
                .isEqualTo(EncodingConstants.NO_TIMESTAMP);
        assertThat(buffer.getLong(base + EncodingConstants.ORDER_EVENT_PRICE_MANTISSA_OFFSET)).isEqualTo(33_398L);
        assertThat(buffer.getLong(base + EncodingConstants.ORDER_EVENT_QTY_MANTISSA_OFFSET)).isEqualTo(100_000_000L);
        assertThat(buffer.getLong(base + EncodingConstants.ORDER_EVENT_OLD_QTY_MANTISSA_OFFSET)).isZero();
    }

    /**
     * Verifies Coinbase open events are normalized to reason code 1 at the schema reason offset.
     */
    @Test
    void writeOrderEvent_open_reasonEncodedAtOffset18() {
        assertReason(EncodingConstants.REASON_OPEN);
    }

    /**
     * Verifies filled done events carry the filled reason code used by downstream order-state consumers.
     */
    @Test
    void writeOrderEvent_done_filled_reasonFilledFour() {
        assertReason(EncodingConstants.REASON_FILLED);
    }

    /**
     * Verifies cancelled done events carry the cancelled reason code distinct from fills.
     */
    @Test
    void writeOrderEvent_done_cancelled_reasonCancelledFive() {
        assertReason(EncodingConstants.REASON_CANCELLED);
    }

    /**
     * Verifies modify events write old quantity mantissa at offset 47 instead of overwriting current quantity.
     */
    @Test
    void writeOrderEvent_modified_oldQtyMantissaEncodedAtOffset47() {
        SbeEncoderBookLevelTest.CapturingPublisher publisher = encodeOrderEvent(
                EncodingConstants.REASON_MODIFIED,
                EncodingConstants.ORDER_TYPE_LIMIT,
                EncodingConstants.NO_TIMESTAMP,
                40_023L,
                (byte) 5,
                523_512L,
                (byte) 5,
                1_200_000L,
                (byte) 5);
        ByteBuffer buffer = wrapped(publisher);

        assertThat(buffer.getLong(
                EncodingConstants.REPEATING_GROUP_OFFSET + EncodingConstants.ORDER_EVENT_OLD_QTY_MANTISSA_OFFSET))
                .isEqualTo(1_200_000L);
    }

    /**
     * Verifies modify events write the old quantity scale at offset 22 beside the current quantity scale.
     */
    @Test
    void writeOrderEvent_modified_oldQtyScaleEncodedAtOffset22() {
        SbeEncoderBookLevelTest.CapturingPublisher publisher = encodeOrderEvent(
                EncodingConstants.REASON_MODIFIED,
                EncodingConstants.ORDER_TYPE_LIMIT,
                EncodingConstants.NO_TIMESTAMP,
                40_023L,
                (byte) 5,
                523_512L,
                (byte) 6,
                1_200_000L,
                (byte) 7);
        ByteBuffer buffer = wrapped(publisher);

        assertThat(buffer.get(
                EncodingConstants.REPEATING_GROUP_OFFSET + EncodingConstants.ORDER_EVENT_OLD_QTY_SCALE_OFFSET))
                .isEqualTo((byte) 7);
    }

    /**
     * Verifies activate/triggered events use the triggered reason code.
     */
    @Test
    void writeOrderEvent_triggered_reasonTriggeredTwo() {
        assertReason(EncodingConstants.REASON_TRIGGERED);
    }

    /**
     * Verifies the high half of a UUID-style order id is written at entry offset 0.
     */
    @Test
    void writeOrderEvent_orderIdHighEncodedAtOffset0() {
        ByteBuffer buffer = wrapped(encodeOrderEvent(EncodingConstants.REASON_RECEIVED));

        assertThat(buffer.getLong(
                EncodingConstants.REPEATING_GROUP_OFFSET + EncodingConstants.ORDER_EVENT_ORDER_ID_HIGH_OFFSET))
                .isEqualTo(ORDER_ID_HIGH);
    }

    /**
     * Verifies the low half of a UUID-style order id is written at entry offset 8.
     */
    @Test
    void writeOrderEvent_orderIdLowEncodedAtOffset8() {
        ByteBuffer buffer = wrapped(encodeOrderEvent(EncodingConstants.REASON_RECEIVED));

        assertThat(buffer.getLong(
                EncodingConstants.REPEATING_GROUP_OFFSET + EncodingConstants.ORDER_EVENT_ORDER_ID_LOW_OFFSET))
                .isEqualTo(ORDER_ID_LOW);
    }

    /**
     * Verifies absent order timestamps remain encoded as the -1 sentinel at offset 23.
     */
    @Test
    void writeOrderEvent_orderTimestampMinusOne_encodedAtOffset23() {
        ByteBuffer buffer = wrapped(encodeOrderEvent(EncodingConstants.REASON_RECEIVED));

        assertThat(buffer.getLong(
                EncodingConstants.REPEATING_GROUP_OFFSET + EncodingConstants.ORDER_EVENT_ORDER_TS_OFFSET))
                .isEqualTo(EncodingConstants.NO_TIMESTAMP);
    }

    /**
     * Verifies order events carry the caller-supplied action byte at offset 17.
     */
    @Test
    void writeOrderEvent_actionAlwaysUpsertAtOffset17() {
        ByteBuffer buffer = wrapped(encodeOrderEvent(EncodingConstants.REASON_RECEIVED));

        assertThat(buffer.get(EncodingConstants.REPEATING_GROUP_OFFSET + EncodingConstants.ORDER_EVENT_ACTION_OFFSET))
                .isEqualTo(EncodingConstants.ACTION_UPSERT);
    }

    /**
     * Verifies order-event writes fail clearly when the encoder was constructed for another template.
     */
    @Test
    void writeOrderEvent_wrongTemplate_throwsIllegalState() {
        SbeEncoder encoder = new SbeEncoder(
                2002,
                VenueEnum.COINBASE_L3.byteValue(),
                BookDepth.L3.byteValue(),
                EncodingConstants.TEMPLATE_ID_TRADE_EVENT,
                1,
                0);
        encoder.beginMessage(EncodingConstants.EVENT_TYPE_BOOK_TRADE, 1L, 1L, 1L, -1L, 1L);

        assertThatThrownBy(() -> encoder.writeOrderEvent(
                0L,
                0L,
                EncodingConstants.SIDE_BID,
                EncodingConstants.ACTION_UPSERT,
                EncodingConstants.REASON_RECEIVED,
                EncodingConstants.ORDER_TYPE_LIMIT,
                (byte) 0,
                (byte) 0,
                (byte) 0,
                0L,
                0L,
                0L,
                0L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("expected=2");
    }

    /**
     * Verifies one order-event entry extends the message by the full byte span implied by its final int64 field.
     */
    @Test
    void writeOrderEvent_entryLengthIs55Bytes() {
        SbeEncoderBookLevelTest.CapturingPublisher publisher = encodeOrderEvent(EncodingConstants.REASON_RECEIVED);

        assertThat(EncodingConstants.ORDER_EVENT_ENTRY_LENGTH).isEqualTo(55);
        assertThat(publisher.lastLength()).isEqualTo(
                EncodingConstants.REPEATING_GROUP_OFFSET + EncodingConstants.ORDER_EVENT_ENTRY_LENGTH);
    }

    /**
     * Verifies the schema v2 checksum placeholder is written as zero in the fixed body.
     */
    @Test
    void beginMessage_checksumFieldIsZeroAtOffset55() {
        ByteBuffer buffer = wrapped(encodeOrderEvent(EncodingConstants.REASON_RECEIVED));

        assertThat(buffer.getInt(EncodingConstants.BODY_CHECKSUM_OFFSET)).isEqualTo(EncodingConstants.NO_CHECKSUM);
    }

    /**
     * Verifies schema v2 messages advertise the 51-byte fixed body length in the header.
     */
    @Test
    void beginMessage_blockLengthIs51() {
        SbeDecoder decoded = new SbeDecoder(encodeOrderEvent(EncodingConstants.REASON_RECEIVED).lastMessage());

        assertThat(decoded.blockLength()).isEqualTo(51);
    }

    /**
     * Verifies schema v2 messages advertise version byte 2 in the header.
     */
    @Test
    void beginMessage_schemaVersionIs2() {
        SbeDecoder decoded = new SbeDecoder(encodeOrderEvent(EncodingConstants.REASON_RECEIVED).lastMessage());

        assertThat(decoded.version()).isEqualTo(2);
    }

    private static void assertReason(byte expectedReason) {
        ByteBuffer buffer = wrapped(encodeOrderEvent(expectedReason));

        assertThat(buffer.get(EncodingConstants.REPEATING_GROUP_OFFSET + EncodingConstants.ORDER_EVENT_REASON_OFFSET))
                .isEqualTo(expectedReason);
    }

    private static SbeEncoderBookLevelTest.CapturingPublisher encodeOrderEvent(byte reason) {
        return encodeOrderEvent(
                reason,
                EncodingConstants.ORDER_TYPE_LIMIT,
                EncodingConstants.NO_TIMESTAMP,
                33_398L,
                (byte) 2,
                100_000_000L,
                (byte) 8,
                0L,
                (byte) 0);
    }

    private static SbeEncoderBookLevelTest.CapturingPublisher encodeOrderEvent(
            byte reason,
            byte orderType,
            long orderTimestamp,
            long priceMantissa,
            byte priceScale,
            long qtyMantissa,
            byte qtyScale,
            long oldQtyMantissa,
            byte oldQtyScale) {
        SbeEncoderBookLevelTest.CapturingPublisher publisher = new SbeEncoderBookLevelTest.CapturingPublisher();
        SbeEncoder encoder = new SbeEncoder(
                2002,
                VenueEnum.COINBASE_L3.byteValue(),
                BookDepth.L3.byteValue(),
                EncodingConstants.TEMPLATE_ID_ORDER_EVENT,
                10,
                0);

        encoder.beginMessage(EncodingConstants.EVENT_TYPE_BOOK_UPDATE, 20L, 21L, 22L, 55L, 66L);
        encoder.writeOrderEvent(
                ORDER_ID_HIGH,
                ORDER_ID_LOW,
                EncodingConstants.SIDE_BID,
                EncodingConstants.ACTION_UPSERT,
                reason,
                orderType,
                priceScale,
                qtyScale,
                oldQtyScale,
                orderTimestamp,
                priceMantissa,
                qtyMantissa,
                oldQtyMantissa);
        assertThat(encoder.endMessage(publisher, null, () -> 0L)).isTrue();
        return publisher;
    }

    private static ByteBuffer wrapped(SbeEncoderBookLevelTest.CapturingPublisher publisher) {
        return ByteBuffer.wrap(publisher.lastMessage()).order(EncodingConstants.BYTE_ORDER);
    }
}
