package io.rueishi.marketdata.crypto.core.encoding;

import java.nio.ByteBuffer;

/**
 * Test-only decoder for the gateway's SBE-style binary layout.
 *
 * <p>{@code SbeDecoder} wraps captured bytes from encoder tests and exposes
 * primitive field accessors for assertions. It deliberately lives under
 * {@code src/test/java}; production code must not import it. Repeating-group
 * offsets are derived from the encoded {@code blockLength} header field so the
 * tests verify the decoder rule owned by P1-003.</p>
 */
final class SbeDecoder {
    private final ByteBuffer buffer;

    /**
     * Wraps encoded message bytes for test decoding.
     *
     * @param encoded captured encoded message bytes
     */
    SbeDecoder(byte[] encoded) {
        this.buffer = ByteBuffer.wrap(encoded).order(EncodingConstants.BYTE_ORDER);
    }

    /** @return decoded uint16 magic field */
    int magic() {
        return Short.toUnsignedInt(buffer.getShort(EncodingConstants.MAGIC_OFFSET));
    }

    /** @return decoded uint8 schema version field */
    int version() {
        return Byte.toUnsignedInt(buffer.get(EncodingConstants.VERSION_OFFSET));
    }

    /** @return decoded uint8 template id field */
    int templateId() {
        return Byte.toUnsignedInt(buffer.get(EncodingConstants.TEMPLATE_ID_OFFSET));
    }

    /** @return decoded uint16 body block length field */
    int blockLength() {
        return Short.toUnsignedInt(buffer.getShort(EncodingConstants.BLOCK_LENGTH_OFFSET));
    }

    /** @return decoded uint16 repeating-group entry count field */
    int entryCount() {
        return Short.toUnsignedInt(buffer.getShort(EncodingConstants.ENTRY_COUNT_OFFSET));
    }

    /** @return decoded uint8 event type field */
    int eventType() {
        return Byte.toUnsignedInt(buffer.get(EncodingConstants.EVENT_TYPE_OFFSET));
    }

    /** @return decoded uint8 venue field */
    int venue() {
        return Byte.toUnsignedInt(buffer.get(EncodingConstants.VENUE_OFFSET));
    }

    /** @return decoded uint8 book depth field */
    int bookDepth() {
        return Byte.toUnsignedInt(buffer.get(EncodingConstants.BOOK_DEPTH_OFFSET));
    }

    /** @return decoded uint32 instrument id field */
    long instrumentId() {
        return Integer.toUnsignedLong(buffer.getInt(EncodingConstants.INSTRUMENT_ID_OFFSET));
    }

    /** @return decoded uint64 gateway message sequence field */
    long gatewayMessageSeq() {
        return buffer.getLong(EncodingConstants.GATEWAY_MESSAGE_SEQ_OFFSET);
    }

    /** @return decoded uint64 first sequence field */
    long seq1() {
        return buffer.getLong(EncodingConstants.SEQ1_OFFSET);
    }

    /** @return decoded uint64 second sequence field */
    long seq2() {
        return buffer.getLong(EncodingConstants.SEQ2_OFFSET);
    }

    /** @return decoded int64 exchange timestamp field */
    long exchangeTimestamp() {
        return buffer.getLong(EncodingConstants.EXCHANGE_TIMESTAMP_OFFSET);
    }

    /** @return decoded int64 ingress timestamp field */
    long ingressTimestamp() {
        return buffer.getLong(EncodingConstants.INGRESS_TIMESTAMP_OFFSET);
    }

    /** @param index BOOK_LEVEL entry index @return decoded side byte */
    int side(int index) {
        return Byte.toUnsignedInt(buffer.get(levelOffset(index)));
    }

    /** @param index BOOK_LEVEL entry index @return decoded action byte */
    int action(int index) {
        return Byte.toUnsignedInt(buffer.get(levelOffset(index) + 1));
    }

    /** @param index BOOK_LEVEL entry index @return decoded price scale byte */
    int priceScale(int index) {
        return Byte.toUnsignedInt(buffer.get(levelOffset(index) + 2));
    }

    /** @param index BOOK_LEVEL entry index @return decoded quantity scale byte */
    int qtyScale(int index) {
        return Byte.toUnsignedInt(buffer.get(levelOffset(index) + 3));
    }

    /** @param index BOOK_LEVEL entry index @return decoded price mantissa */
    long priceMantissa(int index) {
        return buffer.getLong(levelOffset(index) + 4);
    }

    /** @param index BOOK_LEVEL entry index @return decoded quantity mantissa */
    long qtyMantissa(int index) {
        return buffer.getLong(levelOffset(index) + 12);
    }

    /** @param index ORDER_ENTRY entry index @return decoded order id */
    long orderId(int index) {
        return buffer.getLong(orderOffset(index));
    }

    /** @param index ORDER_ENTRY entry index @return decoded side byte */
    int orderSide(int index) {
        return Byte.toUnsignedInt(buffer.get(orderOffset(index) + 8));
    }

    /** @param index ORDER_ENTRY entry index @return decoded action byte */
    int orderAction(int index) {
        return Byte.toUnsignedInt(buffer.get(orderOffset(index) + 9));
    }

    /** @param index ORDER_ENTRY entry index @return decoded order type byte */
    int orderType(int index) {
        return Byte.toUnsignedInt(buffer.get(orderOffset(index) + 10));
    }

    /** @param index ORDER_ENTRY entry index @return decoded price scale byte */
    int orderPriceScale(int index) {
        return Byte.toUnsignedInt(buffer.get(orderOffset(index) + 11));
    }

    /** @param index ORDER_ENTRY entry index @return decoded quantity scale byte */
    int orderQtyScale(int index) {
        return Byte.toUnsignedInt(buffer.get(orderOffset(index) + 12));
    }

    /** @param index ORDER_ENTRY entry index @return decoded price mantissa */
    long orderPriceMantissa(int index) {
        return buffer.getLong(orderOffset(index) + 13);
    }

    /** @param index ORDER_ENTRY entry index @return decoded quantity mantissa */
    long orderQtyMantissa(int index) {
        return buffer.getLong(orderOffset(index) + 21);
    }

    private int levelOffset(int index) {
        return repeatingGroupOffset() + index * EncodingConstants.BOOK_LEVEL_ENTRY_LENGTH;
    }

    private int orderOffset(int index) {
        return repeatingGroupOffset() + index * EncodingConstants.ORDER_ENTRY_LENGTH;
    }

    private int repeatingGroupOffset() {
        return EncodingConstants.HEADER_LENGTH + blockLength();
    }
}
