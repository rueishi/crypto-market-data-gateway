package io.rueishi.marketdata.crypto.core.encoding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.rueishi.marketdata.crypto.core.config.BookDepth;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Byte-layout tests for {@link SbeEncoder} using the schema v2 TRADE_EVENT template.
 *
 * <p>The tests use the production encoder with a capturing publisher and inspect the emitted bytes directly. They cover
 * all trade-entry field offsets, fixed 51-byte entry sizing, entry count, and rejection when trade writes are attempted
 * through an encoder configured for another template.</p>
 */
@Tag("unit")
class SbeEncoderTradeEventTest {
    private static final long MAKER_ID_HIGH = 0x1111_2222_3333_4444L;
    private static final long MAKER_ID_LOW = 0x5555_6666_7777_8888L;
    private static final long TAKER_ID_HIGH = 0x9999_AAAA_BBBB_CCCCL;
    private static final long TAKER_ID_LOW = 0xDDDD_EEEE_FFFF_0001L;

    /**
     * Verifies the maker order id high half is written at trade entry offset 0.
     */
    @Test
    void writeTrade_makerOrderIdHigh_encodedAtOffset0() {
        assertLongAt(EncodingConstants.TRADE_EVENT_MAKER_ID_HIGH_OFFSET, MAKER_ID_HIGH);
    }

    /**
     * Verifies the maker order id low half is written at trade entry offset 8.
     */
    @Test
    void writeTrade_makerOrderIdLow_encodedAtOffset8() {
        assertLongAt(EncodingConstants.TRADE_EVENT_MAKER_ID_LOW_OFFSET, MAKER_ID_LOW);
    }

    /**
     * Verifies the taker order id high half is written at trade entry offset 16.
     */
    @Test
    void writeTrade_takerOrderIdHigh_encodedAtOffset16() {
        assertLongAt(EncodingConstants.TRADE_EVENT_TAKER_ID_HIGH_OFFSET, TAKER_ID_HIGH);
    }

    /**
     * Verifies the taker order id low half is written at trade entry offset 24.
     */
    @Test
    void writeTrade_takerOrderIdLow_encodedAtOffset24() {
        assertLongAt(EncodingConstants.TRADE_EVENT_TAKER_ID_LOW_OFFSET, TAKER_ID_LOW);
    }

    /**
     * Verifies the normalized taker side is written at trade entry offset 32.
     */
    @Test
    void writeTrade_side_takerSideAtOffset32() {
        ByteBuffer buffer = wrapped(encodeTrade());

        assertThat(buffer.get(EncodingConstants.REPEATING_GROUP_OFFSET + EncodingConstants.TRADE_EVENT_SIDE_OFFSET))
                .isEqualTo(EncodingConstants.SIDE_ASK);
    }

    /**
     * Verifies the trade price scale is written at trade entry offset 33.
     */
    @Test
    void writeTrade_priceScale_atOffset33() {
        ByteBuffer buffer = wrapped(encodeTrade());

        assertThat(buffer.get(EncodingConstants.REPEATING_GROUP_OFFSET + EncodingConstants.TRADE_EVENT_PRICE_SCALE_OFFSET))
                .isEqualTo((byte) 2);
    }

    /**
     * Verifies the trade quantity scale is written at trade entry offset 34.
     */
    @Test
    void writeTrade_qtyScale_atOffset34() {
        ByteBuffer buffer = wrapped(encodeTrade());

        assertThat(buffer.get(EncodingConstants.REPEATING_GROUP_OFFSET + EncodingConstants.TRADE_EVENT_QTY_SCALE_OFFSET))
                .isEqualTo((byte) 8);
    }

    /**
     * Verifies the trade price mantissa is written at trade entry offset 35.
     */
    @Test
    void writeTrade_priceMantissa_atOffset35() {
        assertLongAt(EncodingConstants.TRADE_EVENT_PRICE_MANTISSA_OFFSET, 55_123L);
    }

    /**
     * Verifies the trade quantity mantissa is written at trade entry offset 43.
     */
    @Test
    void writeTrade_qtyMantissa_atOffset43() {
        assertLongAt(EncodingConstants.TRADE_EVENT_QTY_MANTISSA_OFFSET, 900_000_000L);
    }

    /**
     * Verifies one trade entry extends the message by the fixed 51-byte schema v2 trade-entry length.
     */
    @Test
    void writeTrade_entryLengthIs51Bytes() {
        SbeEncoderBookLevelTest.CapturingPublisher publisher = encodeTrade();

        assertThat(EncodingConstants.TRADE_EVENT_ENTRY_LENGTH).isEqualTo(51);
        assertThat(publisher.lastLength()).isEqualTo(
                EncodingConstants.REPEATING_GROUP_OFFSET + EncodingConstants.TRADE_EVENT_ENTRY_LENGTH);
    }

    /**
     * Verifies a single trade write patches entry count to one at message finalization.
     */
    @Test
    void writeTrade_entryCountIsOne_afterSingleWrite() {
        SbeDecoder decoded = new SbeDecoder(encodeTrade().lastMessage());

        assertThat(decoded.entryCount()).isEqualTo(1);
    }

    /**
     * Verifies trade writes fail clearly when the encoder was constructed for another template.
     */
    @Test
    void writeTrade_wrongTemplate_throwsIllegalState() {
        SbeEncoder encoder = new SbeEncoder(
                2002,
                VenueEnum.COINBASE_L3.byteValue(),
                BookDepth.L3.byteValue(),
                EncodingConstants.TEMPLATE_ID_ORDER_EVENT,
                1,
                0);
        encoder.beginMessage(EncodingConstants.EVENT_TYPE_BOOK_UPDATE, 1L, 1L, 1L, -1L, 1L);

        assertThatThrownBy(() -> encoder.writeTrade(
                MAKER_ID_HIGH,
                MAKER_ID_LOW,
                TAKER_ID_HIGH,
                TAKER_ID_LOW,
                EncodingConstants.SIDE_ASK,
                (byte) 2,
                (byte) 8,
                55_123L,
                900_000_000L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("expected=3");
    }

    private static void assertLongAt(int tradeEntryOffset, long expectedValue) {
        ByteBuffer buffer = wrapped(encodeTrade());

        assertThat(buffer.getLong(EncodingConstants.REPEATING_GROUP_OFFSET + tradeEntryOffset)).isEqualTo(expectedValue);
    }

    private static SbeEncoderBookLevelTest.CapturingPublisher encodeTrade() {
        SbeEncoderBookLevelTest.CapturingPublisher publisher = new SbeEncoderBookLevelTest.CapturingPublisher();
        SbeEncoder encoder = new SbeEncoder(
                2002,
                VenueEnum.COINBASE_L3.byteValue(),
                BookDepth.L3.byteValue(),
                EncodingConstants.TEMPLATE_ID_TRADE_EVENT,
                1,
                0);

        encoder.beginMessage(EncodingConstants.EVENT_TYPE_BOOK_TRADE, 20L, 21L, 22L, 55L, 66L);
        encoder.writeTrade(
                MAKER_ID_HIGH,
                MAKER_ID_LOW,
                TAKER_ID_HIGH,
                TAKER_ID_LOW,
                EncodingConstants.SIDE_ASK,
                (byte) 2,
                (byte) 8,
                55_123L,
                900_000_000L);
        assertThat(encoder.endMessage(publisher, null, () -> 0L)).isTrue();
        return publisher;
    }

    private static ByteBuffer wrapped(SbeEncoderBookLevelTest.CapturingPublisher publisher) {
        return ByteBuffer.wrap(publisher.lastMessage()).order(EncodingConstants.BYTE_ORDER);
    }
}
