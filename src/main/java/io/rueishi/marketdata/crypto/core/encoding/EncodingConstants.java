package io.rueishi.marketdata.crypto.core.encoding;

import java.nio.ByteOrder;

/**
 * Schema v2 constants for the gateway's SBE-style market-data wire format.
 *
 * <p>{@link SbeEncoder}, publishers, recovery codecs, and test decoders use this class as the shared byte-layout
 * contract for message headers, fixed bodies, repeating-group entries, template ids, event types, and sentinel values.
 * It sits in the core encoding module because every venue parser writes normalized data through the same binary schema
 * before handing the encoded buffer to a publisher.</p>
 *
 * <p>The class is typically referenced statically by encoder and decoder code during connector runtime; it is not
 * instantiated and does not own mutable state. Runtime decisions such as buffer-capacity calculation live in the classes
 * that perform that work.</p>
 */
public final class EncodingConstants {
    /** Little-endian byte order used for every multi-byte numeric field. */
    public static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    // Schema.
    /** Fixed protocol marker written at the start of every encoded message. */
    public static final int MAGIC = 0xEB0B;
    /** Schema v2 version byte written in every encoded message header. */
    public static final int VERSION = 2;
    /** Alias for call sites that name the version by schema rather than header field. */
    public static final byte SCHEMA_VERSION = 2;
    /** Maximum repeating-group entries allowed by the wire contract. */
    public static final int MAX_SUPPORTED_ENTRIES = 10_000;

    // Header (8 bytes, starts at offset 0).
    /** Header offset for the uint16 magic field. */
    public static final int HEADER_MAGIC_OFFSET = 0;
    /** Header offset for the uint8 version field. */
    public static final int HEADER_VERSION_OFFSET = 2;
    /** Header offset for the uint8 template id field. */
    public static final int HEADER_TEMPLATE_ID_OFFSET = 3;
    /** Header offset for the uint16 fixed body block length field. */
    public static final int HEADER_BLOCK_LENGTH_OFFSET = 4;
    /** Header offset for the uint16 repeating-group entry count field. */
    public static final int HEADER_ENTRY_COUNT_OFFSET = 6;
    /** Header length in bytes. */
    public static final int HEADER_LENGTH = 8;

    // Compatibility aliases for pre-v2 call sites; values are schema v2.
    /** @see #HEADER_MAGIC_OFFSET */
    public static final int MAGIC_OFFSET = HEADER_MAGIC_OFFSET;
    /** @see #HEADER_VERSION_OFFSET */
    public static final int VERSION_OFFSET = HEADER_VERSION_OFFSET;
    /** @see #HEADER_TEMPLATE_ID_OFFSET */
    public static final int TEMPLATE_ID_OFFSET = HEADER_TEMPLATE_ID_OFFSET;
    /** @see #HEADER_BLOCK_LENGTH_OFFSET */
    public static final int BLOCK_LENGTH_OFFSET = HEADER_BLOCK_LENGTH_OFFSET;
    /** @see #HEADER_ENTRY_COUNT_OFFSET */
    public static final int ENTRY_COUNT_OFFSET = HEADER_ENTRY_COUNT_OFFSET;

    // Body (51 bytes, starts at absolute offset 8).
    /** Body block length in bytes for schema v2 messages. */
    public static final int BODY_BLOCK_LENGTH = 51;
    /** Body offset for the event type byte. */
    public static final int BODY_EVENT_TYPE_OFFSET = 8;
    /** Body offset for the venue byte. */
    public static final int BODY_VENUE_OFFSET = 9;
    /** Body offset for the book depth byte. */
    public static final int BODY_BOOK_DEPTH_OFFSET = 10;
    /** Body offset for the uint32 instrument id. */
    public static final int BODY_INSTRUMENT_ID_OFFSET = 11;
    /** Body offset for the uint64 gateway message sequence. */
    public static final int BODY_GATEWAY_SEQ_OFFSET = 15;
    /** Body offset for the first uint64 exchange sequence value. */
    public static final int BODY_SEQ1_OFFSET = 23;
    /** Body offset for the second uint64 exchange sequence value. */
    public static final int BODY_SEQ2_OFFSET = 31;
    /** Body offset for the int64 exchange timestamp. */
    public static final int BODY_EXCHANGE_TS_OFFSET = 39;
    /** Body offset for the int64 ingress timestamp. */
    public static final int BODY_INGRESS_TS_OFFSET = 47;
    /** Body offset for the schema v2 uint32 checksum field, written as zero until checksum support lands. */
    public static final int BODY_CHECKSUM_OFFSET = 55;

    // Compatibility aliases for pre-v2 call sites; values are schema v2.
    /** @see #BODY_EVENT_TYPE_OFFSET */
    public static final int EVENT_TYPE_OFFSET = BODY_EVENT_TYPE_OFFSET;
    /** @see #BODY_VENUE_OFFSET */
    public static final int VENUE_OFFSET = BODY_VENUE_OFFSET;
    /** @see #BODY_BOOK_DEPTH_OFFSET */
    public static final int BOOK_DEPTH_OFFSET = BODY_BOOK_DEPTH_OFFSET;
    /** @see #BODY_INSTRUMENT_ID_OFFSET */
    public static final int INSTRUMENT_ID_OFFSET = BODY_INSTRUMENT_ID_OFFSET;
    /** @see #BODY_GATEWAY_SEQ_OFFSET */
    public static final int GATEWAY_MESSAGE_SEQ_OFFSET = BODY_GATEWAY_SEQ_OFFSET;
    /** @see #BODY_SEQ1_OFFSET */
    public static final int SEQ1_OFFSET = BODY_SEQ1_OFFSET;
    /** @see #BODY_SEQ2_OFFSET */
    public static final int SEQ2_OFFSET = BODY_SEQ2_OFFSET;
    /** @see #BODY_EXCHANGE_TS_OFFSET */
    public static final int EXCHANGE_TIMESTAMP_OFFSET = BODY_EXCHANGE_TS_OFFSET;
    /** @see #BODY_INGRESS_TS_OFFSET */
    public static final int INGRESS_TIMESTAMP_OFFSET = BODY_INGRESS_TS_OFFSET;
    /** @see #BODY_CHECKSUM_OFFSET */
    public static final int CHECKSUM_OFFSET = BODY_CHECKSUM_OFFSET;

    // Repeating group start and BOOK_RESET total size.
    /** Offset where repeating-group entries begin; equal to header length plus body block length. */
    public static final int REPEATING_GROUP_OFFSET = 59;
    /** Header/body size without repeating-group entries. */
    public static final int MESSAGE_PREFIX_LENGTH = REPEATING_GROUP_OFFSET;
    /** Total size of a zero-entry BOOK_RESET message. */
    public static final int BOOK_RESET_SIZE = 59;

    // Template IDs.
    /** BOOK_LEVEL template id for aggregated L2 price-level entries. */
    public static final byte TEMPLATE_ID_BOOK_LEVEL = 1;
    /** ORDER_EVENT template id for L3 order lifecycle entries. */
    public static final byte TEMPLATE_ID_ORDER_EVENT = 2;
    /** TRADE_EVENT template id for L3 trade entries. */
    public static final byte TEMPLATE_ID_TRADE_EVENT = 3;

    // Event types.
    /** BOOK_RESET event type byte. */
    public static final byte EVENT_TYPE_BOOK_RESET = 1;
    /** BOOK_SNAPSHOT event type byte. */
    public static final byte EVENT_TYPE_BOOK_SNAPSHOT = 2;
    /** BOOK_UPDATE event type byte. */
    public static final byte EVENT_TYPE_BOOK_UPDATE = 3;
    /** BOOK_TRADE event type byte. */
    public static final byte EVENT_TYPE_BOOK_TRADE = 4;

    // Side.
    /** Bid/buy side byte. */
    public static final byte SIDE_BID = 1;
    /** Ask/sell side byte. */
    public static final byte SIDE_ASK = 2;

    // Action.
    /** Upsert action byte. */
    public static final byte ACTION_UPSERT = 1;
    /** Delete action byte. */
    public static final byte ACTION_DELETE = 2;

    // Order type (ORDER_EVENT only).
    /** Unknown order type byte. */
    public static final byte ORDER_TYPE_UNKNOWN = 0;
    /** Limit order type byte. */
    public static final byte ORDER_TYPE_LIMIT = 1;
    /** Market order type byte. */
    public static final byte ORDER_TYPE_MARKET = 2;
    /** Stop order type byte. */
    public static final byte ORDER_TYPE_STOP = 3;

    // Reason codes (ORDER_EVENT only).
    /** Coinbase received/order-accepted reason byte. */
    public static final byte REASON_RECEIVED = 0;
    /** Open resting-order reason byte. */
    public static final byte REASON_OPEN = 1;
    /** Stop-order triggered reason byte. */
    public static final byte REASON_TRIGGERED = 2;
    /** Order modified reason byte. */
    public static final byte REASON_MODIFIED = 3;
    /** Order filled reason byte. */
    public static final byte REASON_FILLED = 4;
    /** Order cancelled reason byte. */
    public static final byte REASON_CANCELLED = 5;
    /** Order expired reason byte. */
    public static final byte REASON_EXPIRED = 6;
    /** Order rejected reason byte. */
    public static final byte REASON_REJECTED = 7;
    /** Generic delete reason byte for venues that do not expose a more specific cause. */
    public static final byte REASON_DELETE = 8;

    // BOOK_LEVEL entry (templateId=1) - 20 bytes.
    /** BOOK_LEVEL entry offset for side. */
    public static final int BOOK_LEVEL_SIDE_OFFSET = 0;
    /** BOOK_LEVEL entry offset for action. */
    public static final int BOOK_LEVEL_ACTION_OFFSET = 1;
    /** BOOK_LEVEL entry offset for price scale. */
    public static final int BOOK_LEVEL_PRICE_SCALE_OFFSET = 2;
    /** BOOK_LEVEL entry offset for quantity scale. */
    public static final int BOOK_LEVEL_QTY_SCALE_OFFSET = 3;
    /** BOOK_LEVEL entry offset for price mantissa. */
    public static final int BOOK_LEVEL_PRICE_MANTISSA_OFFSET = 4;
    /** BOOK_LEVEL entry offset for quantity mantissa. */
    public static final int BOOK_LEVEL_QTY_MANTISSA_OFFSET = 12;
    /** BOOK_LEVEL repeating-group entry length in bytes. */
    public static final int BOOK_LEVEL_ENTRY_LENGTH = 20;

    // ORDER_EVENT entry (templateId=2) - 55 bytes.
    /** ORDER_EVENT entry offset for order id high bits. */
    public static final int ORDER_EVENT_ORDER_ID_HIGH_OFFSET = 0;
    /** ORDER_EVENT entry offset for order id low bits. */
    public static final int ORDER_EVENT_ORDER_ID_LOW_OFFSET = 8;
    /** ORDER_EVENT entry offset for side. */
    public static final int ORDER_EVENT_SIDE_OFFSET = 16;
    /** ORDER_EVENT entry offset for action. */
    public static final int ORDER_EVENT_ACTION_OFFSET = 17;
    /** ORDER_EVENT entry offset for reason. */
    public static final int ORDER_EVENT_REASON_OFFSET = 18;
    /** ORDER_EVENT entry offset for order type. */
    public static final int ORDER_EVENT_ORDER_TYPE_OFFSET = 19;
    /** ORDER_EVENT entry offset for price scale. */
    public static final int ORDER_EVENT_PRICE_SCALE_OFFSET = 20;
    /** ORDER_EVENT entry offset for quantity scale. */
    public static final int ORDER_EVENT_QTY_SCALE_OFFSET = 21;
    /** ORDER_EVENT entry offset for old quantity scale. */
    public static final int ORDER_EVENT_OLD_QTY_SCALE_OFFSET = 22;
    /** ORDER_EVENT entry offset for order timestamp. */
    public static final int ORDER_EVENT_ORDER_TS_OFFSET = 23;
    /** Alias for the ORDER_EVENT timestamp offset name used by the schema v2 spec. */
    public static final int ORDER_EVENT_TIMESTAMP_OFFSET = ORDER_EVENT_ORDER_TS_OFFSET;
    /** ORDER_EVENT entry offset for price mantissa. */
    public static final int ORDER_EVENT_PRICE_MANTISSA_OFFSET = 31;
    /** ORDER_EVENT entry offset for quantity mantissa. */
    public static final int ORDER_EVENT_QTY_MANTISSA_OFFSET = 39;
    /** ORDER_EVENT entry offset for old quantity mantissa. */
    public static final int ORDER_EVENT_OLD_QTY_MANTISSA_OFFSET = 47;
    /** ORDER_EVENT repeating-group entry length in bytes, including the int64 old quantity at offset 47. */
    public static final int ORDER_EVENT_ENTRY_LENGTH = 55;

    // Compatibility alias for pre-v2 order-entry code; value is schema v2 ORDER_EVENT length.
    /** @see #ORDER_EVENT_ENTRY_LENGTH */
    public static final int ORDER_ENTRY_LENGTH = ORDER_EVENT_ENTRY_LENGTH;

    // TRADE_EVENT entry (templateId=3) - 51 bytes.
    /** TRADE_EVENT entry offset for maker order id high bits. */
    public static final int TRADE_EVENT_MAKER_ID_HIGH_OFFSET = 0;
    /** TRADE_EVENT entry offset for maker order id low bits. */
    public static final int TRADE_EVENT_MAKER_ID_LOW_OFFSET = 8;
    /** TRADE_EVENT entry offset for taker order id high bits. */
    public static final int TRADE_EVENT_TAKER_ID_HIGH_OFFSET = 16;
    /** TRADE_EVENT entry offset for taker order id low bits. */
    public static final int TRADE_EVENT_TAKER_ID_LOW_OFFSET = 24;
    /** TRADE_EVENT entry offset for taker side. */
    public static final int TRADE_EVENT_SIDE_OFFSET = 32;
    /** TRADE_EVENT entry offset for price scale. */
    public static final int TRADE_EVENT_PRICE_SCALE_OFFSET = 33;
    /** TRADE_EVENT entry offset for quantity scale. */
    public static final int TRADE_EVENT_QTY_SCALE_OFFSET = 34;
    /** TRADE_EVENT entry offset for price mantissa. */
    public static final int TRADE_EVENT_PRICE_MANTISSA_OFFSET = 35;
    /** TRADE_EVENT entry offset for quantity mantissa. */
    public static final int TRADE_EVENT_QTY_MANTISSA_OFFSET = 43;
    /** TRADE_EVENT repeating-group entry length in bytes. */
    public static final int TRADE_EVENT_ENTRY_LENGTH = 51;

    // Sentinels.
    /** Timestamp sentinel used when an exchange timestamp is absent. */
    public static final long NO_TIMESTAMP = -1L;
    /** Phase 4 checksum sentinel until checksum computation is introduced. */
    public static final int NO_CHECKSUM = 0;
    /** Sequence sentinel used when a venue sequence is absent. */
    public static final long NO_SEQ = 0L;

    private EncodingConstants() {
    }

}
