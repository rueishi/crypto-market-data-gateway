package io.rueishi.marketdata.crypto.core.encoding;

import java.nio.ByteOrder;

/**
 * Binary layout constants for the gateway's SBE-style wire format.
 *
 * <p>{@link SbeEncoder} writes fields using these offsets, widths, and enum
 * values, while test decoders use the same layout contract to verify exact
 * little-endian output. The constants are centralized so later encoder,
 * publisher reset, and decoder work can agree on the fixed Phase 1 message
 * shape.</p>
 */
public final class EncodingConstants {
    /** Little-endian byte order used for every multi-byte numeric field. */
    public static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    /** Fixed protocol marker written at the start of every encoded message. */
    public static final int MAGIC = 0xEB0B;
    /** Initial schema version for Phase 1 encoded messages. */
    public static final int VERSION = 1;
    /** Maximum repeating-group entries allowed by the Phase 1 wire contract. */
    public static final int MAX_SUPPORTED_ENTRIES = 10_000;

    /** BOOK_RESET event type byte. */
    public static final byte EVENT_TYPE_BOOK_RESET = 1;
    /** BOOK_SNAPSHOT event type byte. */
    public static final byte EVENT_TYPE_BOOK_SNAPSHOT = 2;
    /** BOOK_UPDATE event type byte. */
    public static final byte EVENT_TYPE_BOOK_UPDATE = 3;

    /** Header length in bytes. */
    public static final int HEADER_LENGTH = 8;
    /** Body block length in bytes. */
    public static final int BODY_BLOCK_LENGTH = 47;
    /** BOOK_LEVEL repeating-group entry length in bytes. */
    public static final int BOOK_LEVEL_ENTRY_LENGTH = 20;
    /** ORDER_ENTRY repeating-group entry length in bytes. */
    public static final int ORDER_ENTRY_LENGTH = 29;

    /** Header offset for the uint16 magic field. */
    public static final int MAGIC_OFFSET = 0;
    /** Header offset for the uint8 version field. */
    public static final int VERSION_OFFSET = 2;
    /** Header offset for the uint8 template id field. */
    public static final int TEMPLATE_ID_OFFSET = 3;
    /** Header offset for the uint16 fixed body block length field. */
    public static final int BLOCK_LENGTH_OFFSET = 4;
    /** Header offset for the uint16 repeating-group entry count field. */
    public static final int ENTRY_COUNT_OFFSET = 6;

    /** Body offset for the event type field. */
    public static final int EVENT_TYPE_OFFSET = HEADER_LENGTH;
    /** Body offset for the venue byte field. */
    public static final int VENUE_OFFSET = EVENT_TYPE_OFFSET + 1;
    /** Body offset for the book depth byte field. */
    public static final int BOOK_DEPTH_OFFSET = VENUE_OFFSET + 1;
    /** Body offset for the uint32 instrument id field. */
    public static final int INSTRUMENT_ID_OFFSET = BOOK_DEPTH_OFFSET + 1;
    /** Body offset for the uint64 gateway message sequence field. */
    public static final int GATEWAY_MESSAGE_SEQ_OFFSET = INSTRUMENT_ID_OFFSET + Integer.BYTES;
    /** Body offset for the uint64 first sequence field. */
    public static final int SEQ1_OFFSET = GATEWAY_MESSAGE_SEQ_OFFSET + Long.BYTES;
    /** Body offset for the uint64 second sequence field. */
    public static final int SEQ2_OFFSET = SEQ1_OFFSET + Long.BYTES;
    /** Body offset for the int64 exchange timestamp field. */
    public static final int EXCHANGE_TIMESTAMP_OFFSET = SEQ2_OFFSET + Long.BYTES;
    /** Body offset for the int64 ingress timestamp field. */
    public static final int INGRESS_TIMESTAMP_OFFSET = EXCHANGE_TIMESTAMP_OFFSET + Long.BYTES;
    /** Offset where repeating-group entries begin; equal to header length plus body block length. */
    public static final int REPEATING_GROUP_OFFSET = HEADER_LENGTH + BODY_BLOCK_LENGTH;

    /** Header/body size without repeating-group entries. */
    public static final int MESSAGE_PREFIX_LENGTH = REPEATING_GROUP_OFFSET;

    private EncodingConstants() {
    }

    /**
     * Returns the repeating-group entry length for a template id.
     *
     * @param templateIdByte template id byte from {@code TemplateId}
     * @return entry length for the selected template
     * @throws IllegalArgumentException if the template id is not recognized
     */
    public static int entryLength(byte templateIdByte) {
        return switch (templateIdByte) {
            case 1 -> BOOK_LEVEL_ENTRY_LENGTH;
            case 2 -> ORDER_ENTRY_LENGTH;
            default -> throw new IllegalArgumentException("Unknown templateIdByte: " + Byte.toUnsignedInt(templateIdByte));
        };
    }

    /**
     * Computes the maximum encoded message size for one template and capacity.
     *
     * @param templateIdByte template id byte from {@code TemplateId}
     * @param maxEntryCount configured maximum entry count
     * @param headroomBytes additional capacity reserved beyond the maximum encoded size
     * @return required buffer capacity in bytes
     * @throws IllegalArgumentException if the template or capacity inputs are invalid
     */
    public static int requiredCapacity(byte templateIdByte, int maxEntryCount, int headroomBytes) {
        if (maxEntryCount <= 0 || maxEntryCount > MAX_SUPPORTED_ENTRIES) {
            throw new IllegalArgumentException(
                    "maxEntryCount must be between 1 and " + MAX_SUPPORTED_ENTRIES);
        }
        if (headroomBytes < 0) {
            throw new IllegalArgumentException("headroomBytes must not be negative");
        }
        return MESSAGE_PREFIX_LENGTH + Math.multiplyExact(maxEntryCount, entryLength(templateIdByte)) + headroomBytes;
    }
}
