package io.rueishi.marketdata.crypto.venue.coinbase.l2;

import io.netty.buffer.ByteBuf;
import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.core.encoding.EncodingConstants;
import io.rueishi.marketdata.crypto.core.parser.ByteBufScanner;
import io.rueishi.marketdata.crypto.core.parser.FeedParser;
import io.rueishi.marketdata.crypto.core.parser.ParseContext;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryReasonCode;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequestType;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Coinbase Exchange Direct Feed parser for the L2 strategy.
 *
 * <p>{@code CoinbaseL2FeedParser} is the venue-specific normalization point
 * between Coinbase JSON WebSocket text frames and the shared binary
 * {@code BOOK_LEVEL} encoder. A connector factory instantiates one parser for a
 * single configured {@link InstrumentConfig}; the connector then calls
 * {@link #onTextFrame(ByteBuf, ParseContext)} from its Netty event-loop frame
 * path. The parser keeps no session state in fields: sequence, snapshot gate,
 * counters, encoder, publisher, and liveness acknowledgement state are all read
 * from the supplied {@link ParseContext} for the current call.</p>
 *
 * <p>The supported Coinbase L2 message types are {@code snapshot},
 * {@code l2update}, {@code heartbeat}, and {@code subscriptions}. Unknown types,
 * malformed frames, product mismatches, bad acknowledgements, and pre-snapshot
 * updates are counted and dropped. A valid first snapshot also opens the
 * parser snapshot gate and signals the subscribe-driven boundary through the
 * parse context. Phase 3 parser-detected integrity failures request recovery
 * through {@link ParseContext} so every reset converges on the connector-owned
 * Coinbase recovery path without storing connector state in the parser.</p>
 */
public final class CoinbaseL2FeedParser implements FeedParser {
    private static final byte[] TYPE_KEY = ascii("type");
    private static final byte[] PRODUCT_ID_KEY = ascii("product_id");
    private static final byte[] TIME_KEY = ascii("time");
    private static final byte[] BIDS_KEY = ascii("bids");
    private static final byte[] ASKS_KEY = ascii("asks");
    private static final byte[] CHANGES_KEY = ascii("changes");
    private static final byte[] CHANNELS_KEY = ascii("channels");
    private static final byte[] NAME_KEY = ascii("name");
    private static final byte[] PRODUCT_IDS_KEY = ascii("product_ids");

    private static final byte[] SNAPSHOT_TYPE = ascii("snapshot");
    private static final byte[] UPDATE_TYPE = ascii("l2update");
    private static final byte[] HEARTBEAT_TYPE = ascii("heartbeat");
    private static final byte[] SUBSCRIPTIONS_TYPE = ascii("subscriptions");
    private static final byte[] LEVEL2_CHANNEL = ascii(CoinbaseL2SubscriptionBuilder.LEVEL2_CHANNEL);
    private static final byte[] HEARTBEAT_CHANNEL = ascii(CoinbaseL2SubscriptionBuilder.HEARTBEAT_CHANNEL);
    private static final byte[] BUY_SIDE = ascii("buy");
    private static final byte[] SELL_SIDE = ascii("sell");

    private static final byte SIDE_BID = 1;
    private static final byte SIDE_ASK = 2;
    private static final byte ACTION_UPSERT = 1;
    private static final byte ACTION_DELETE = 2;

    private final byte[] productId;

    /**
     * Creates a parser for one configured Coinbase L2 instrument.
     *
     * <p>The parser uses the configured {@code exchangeSymbol} as the only
     * accepted Coinbase {@code product_id}. Matching as bytes avoids allocating
     * Java strings on the text-frame path.</p>
     *
     * @param instrument connector instrument configuration
     * @throws NullPointerException if {@code instrument} is null
     * @throws IllegalArgumentException if {@code instrument.exchangeSymbol} is blank
     */
    public CoinbaseL2FeedParser(InstrumentConfig instrument) {
        Objects.requireNonNull(instrument, "instrument");
        if (instrument.exchangeSymbol == null || instrument.exchangeSymbol.isBlank()) {
            throw new IllegalArgumentException("instrument.exchangeSymbol is required");
        }
        this.productId = instrument.exchangeSymbol.getBytes(StandardCharsets.US_ASCII);
    }

    /**
     * Parses one Coinbase L2 JSON text frame and publishes normalized book data when appropriate.
     *
     * <p>The method records frame/byte receipt, routes by top-level
     * {@code type}, validates product id for data and heartbeat messages, and
     * drives {@link io.rueishi.marketdata.crypto.core.encoding.SbeEncoder}
     * directly. Snapshot frames open the session snapshot gate after successful
     * publication. Update frames are dropped until that gate is open.</p>
     *
     * @param frame inbound UTF-8 JSON Coinbase text frame owned by the caller
     * @param ctx parser dependency carrier for this connector session
     * @throws NullPointerException if {@code frame} or {@code ctx} is null
     */
    @Override
    public void onTextFrame(ByteBuf frame, ParseContext ctx) {
        Objects.requireNonNull(frame, "frame");
        Objects.requireNonNull(ctx, "ctx");
        ctx.counters().framesReceived().increment();
        add(ctx.counters().bytesReceived(), frame.readableBytes());
        ctx.counters().lastMessageReceivedNanos().set(ctx.nanoClock().nanoTime());

        int[] type = stringField(frame, TYPE_KEY);
        if (type[0] < 0) {
            rejectMalformed(ctx);
            return;
        }
        if (matches(frame, type, SNAPSHOT_TYPE)) {
            parseSnapshot(frame, ctx);
        } else if (matches(frame, type, UPDATE_TYPE)) {
            parseUpdate(frame, ctx);
        } else if (matches(frame, type, HEARTBEAT_TYPE)) {
            parseHeartbeat(frame, ctx);
        } else if (matches(frame, type, SUBSCRIPTIONS_TYPE)) {
            validateSubscriptions(frame, ctx);
        } else {
            ctx.counters().unknownTypeDrops().increment();
        }
    }

    /**
     * Parses and publishes one full Coinbase L2 book snapshot.
     *
     * <p>Only the first valid snapshot in a parser session is accepted as the
     * boundary. Duplicate snapshots after the gate is open are ignored so they
     * cannot republish a book image or re-signal recovery completion.</p>
     *
     * @param frame JSON frame containing {@code bids} and {@code asks}
     * @param ctx parser dependency carrier
     */
    private void parseSnapshot(ByteBuf frame, ParseContext ctx) {
        if (!productMatches(frame)) {
            rejectProductMismatch(ctx);
            return;
        }
        if (ctx.snapshotGatekeeper().isReady()) {
            ctx.requestRecovery(
                    RecoveryRequestType.RESET,
                    RecoveryReasonCode.OUT_OF_ORDER_OR_INVALID_TRANSITION,
                    "duplicate snapshot after session boundary");
            return;
        }
        int bidCount = scanSnapshotSide(frame, BIDS_KEY, SIDE_BID, null);
        int askCount = scanSnapshotSide(frame, ASKS_KEY, SIDE_ASK, null);
        if (bidCount < 0 || askCount < 0) {
            rejectMalformed(ctx);
            return;
        }

        long seq = ctx.sequenceTracker().next();
        ctx.encoder().beginMessage(
                EncodingConstants.EVENT_TYPE_BOOK_SNAPSHOT,
                seq,
                seq,
                seq,
                timestamp(frame),
                ctx.nanoClock().nanoTime());
        try {
            int encoded = scanSnapshotSide(frame, BIDS_KEY, SIDE_BID, ctx)
                    + scanSnapshotSide(frame, ASKS_KEY, SIDE_ASK, ctx);
            if (ctx.publishEncodedMessage("Coinbase L2 snapshot publish backpressure")) {
                ctx.counters().snapshotMessagesReceived().increment();
                ctx.counters().messagesDecoded().increment();
                ctx.counters().encodeSuccesses().increment();
                add(ctx.counters().levelsEncoded(), encoded);
                add(ctx.counters().encodedBytes(),
                        EncodingConstants.MESSAGE_PREFIX_LENGTH
                                + (long) encoded * EncodingConstants.BOOK_LEVEL_ENTRY_LENGTH);
                ctx.counters().encodeBufferReuseCount().set(ctx.encoder().reuseCount());
                ctx.counters().bookSnapshotPublished().increment();
                ctx.snapshotGatekeeper().accept();
                ctx.onSnapshotBoundaryAccepted();
            }
        } catch (RuntimeException ex) {
            ctx.counters().encodeFailures().increment();
            throw ex;
        }
    }

    /**
     * Parses and publishes one incremental Coinbase L2 update after the snapshot gate opens.
     *
     * @param frame JSON frame containing {@code changes}
     * @param ctx parser dependency carrier
     */
    private void parseUpdate(ByteBuf frame, ParseContext ctx) {
        if (!productMatches(frame)) {
            rejectProductMismatch(ctx);
            return;
        }
        if (!ctx.snapshotGatekeeper().isReady()) {
            ctx.counters().preSnapshotDrops().increment();
            return;
        }
        int updateCount = scanChanges(frame, null);
        if (updateCount < 0) {
            rejectMalformed(ctx);
            return;
        }

        long seq = ctx.sequenceTracker().next();
        ctx.encoder().beginMessage(
                EncodingConstants.EVENT_TYPE_BOOK_UPDATE,
                seq,
                seq,
                seq,
                timestamp(frame),
                ctx.nanoClock().nanoTime());
        try {
            int encoded = scanChanges(frame, ctx);
            if (ctx.publishEncodedMessage("Coinbase L2 update publish backpressure")) {
                ctx.counters().updateMessagesReceived().increment();
                ctx.counters().messagesDecoded().increment();
                ctx.counters().encodeSuccesses().increment();
                add(ctx.counters().levelsEncoded(), encoded);
                add(ctx.counters().encodedBytes(),
                        EncodingConstants.MESSAGE_PREFIX_LENGTH
                                + (long) encoded * EncodingConstants.BOOK_LEVEL_ENTRY_LENGTH);
                ctx.counters().encodeBufferReuseCount().set(ctx.encoder().reuseCount());
            }
        } catch (RuntimeException ex) {
            ctx.counters().encodeFailures().increment();
            throw ex;
        }
    }

    /**
     * Handles one Coinbase heartbeat frame without publishing book data.
     *
     * @param frame JSON heartbeat frame
     * @param ctx parser dependency carrier
     */
    private void parseHeartbeat(ByteBuf frame, ParseContext ctx) {
        if (!productMatches(frame)) {
            rejectProductMismatch(ctx);
            return;
        }
        ctx.counters().heartbeatsReceived().increment();
        ctx.counters().messagesDecoded().increment();
        ctx.counters().lastHeartbeatReceivedNanos().set(ctx.nanoClock().nanoTime());
    }

    /**
     * Validates Coinbase subscriptions acknowledgement for required channels and product id.
     *
     * @param frame JSON subscriptions acknowledgement
     * @param ctx parser dependency carrier
     */
    private void validateSubscriptions(ByteBuf frame, ParseContext ctx) {
        int[] channels = arrayField(frame, CHANNELS_KEY);
        if (channels[0] < 0
                || !containsChannelProduct(frame, channels, LEVEL2_CHANNEL)
                || !containsChannelProduct(frame, channels, HEARTBEAT_CHANNEL)) {
            ctx.counters().subscriptionValidationFailures().increment();
            ctx.requestRecovery(
                    RecoveryRequestType.RESET,
                    RecoveryReasonCode.STREAM_INTEGRITY_FAILURE,
                    "invalid subscriptions acknowledgement");
            return;
        }
        ctx.counters().messagesDecoded().increment();
        ctx.onSubscriptionAckValidated();
    }

    private int scanSnapshotSide(ByteBuf frame, byte[] sideKey, byte side, ParseContext ctx) {
        int[] array = arrayField(frame, sideKey);
        if (array[0] < 0) {
            return -1;
        }

        int encoded = 0;
        int index = array[0];
        long[] price = new long[2];
        long[] qty = new long[2];
        while (index < array[1]) {
            int[] priceRange = nextQuotedRange(frame, index, array[1]);
            if (priceRange[0] < 0) {
                break;
            }
            int[] qtyRange = nextQuotedRange(frame, priceRange[1] + 1, array[1]);
            if (qtyRange[0] < 0) {
                return -1;
            }
            parseDecimal(frame, priceRange, price);
            parseDecimal(frame, qtyRange, qty);
            if (decimalInvalid(price) || decimalInvalid(qty)) {
                return -1;
            }
            if (ctx != null) {
                ctx.encoder().writeLevel(side, ACTION_UPSERT, price[0], (byte) price[1], qty[0], (byte) qty[1]);
            }
            encoded++;
            index = qtyRange[1] + 1;
        }
        return encoded;
    }

    private int scanChanges(ByteBuf frame, ParseContext ctx) {
        int[] array = arrayField(frame, CHANGES_KEY);
        if (array[0] < 0) {
            return -1;
        }

        int encoded = 0;
        int index = array[0];
        long[] price = new long[2];
        long[] qty = new long[2];
        while (index < array[1]) {
            int[] sideRange = nextQuotedRange(frame, index, array[1]);
            if (sideRange[0] < 0) {
                break;
            }
            int[] priceRange = nextQuotedRange(frame, sideRange[1] + 1, array[1]);
            if (priceRange[0] < 0) {
                return -1;
            }
            int[] qtyRange = nextQuotedRange(frame, priceRange[1] + 1, array[1]);
            if (qtyRange[0] < 0) {
                return -1;
            }

            byte side = side(frame, sideRange);
            if (side == 0) {
                return -1;
            }
            parseDecimal(frame, priceRange, price);
            parseDecimal(frame, qtyRange, qty);
            if (decimalInvalid(price) || decimalInvalid(qty) || qty[0] < 0) {
                return -1;
            }
            byte action = qty[0] == 0 ? ACTION_DELETE : ACTION_UPSERT;
            if (ctx != null) {
                ctx.encoder().writeLevel(side, action, price[0], (byte) price[1], qty[0], (byte) qty[1]);
            }
            encoded++;
            index = qtyRange[1] + 1;
        }
        return encoded;
    }

    private boolean containsChannelProduct(ByteBuf frame, int[] channels, byte[] requiredChannel) {
        int index = channels[0];
        while (index < channels[1]) {
            int[] object = nextObjectRange(frame, index, channels[1]);
            if (object[0] < 0) {
                return false;
            }
            int[] channelName = stringField(frame, NAME_KEY, object[0], object[1]);
            if (channelName[0] >= 0 && matches(frame, channelName, requiredChannel)) {
                int[] productIds = arrayField(frame, PRODUCT_IDS_KEY, object[0], object[1]);
                return productIds[0] >= 0 && arrayContainsString(frame, productIds, productId);
            }
            index = object[1] + 1;
        }
        return false;
    }

    private boolean productMatches(ByteBuf frame) {
        int[] product = stringField(frame, PRODUCT_ID_KEY);
        return product[0] >= 0 && matches(frame, product, productId);
    }

    private long timestamp(ByteBuf frame) {
        int[] time = stringField(frame, TIME_KEY);
        if (time[0] < 0) {
            return ByteBufScanner.TIMESTAMP_SENTINEL;
        }
        return ByteBufScanner.parseRfc3339ToEpochNanos(frame, time[0], time[1]);
    }

    private static byte side(ByteBuf frame, int[] range) {
        if (matches(frame, range, BUY_SIDE)) {
            return SIDE_BID;
        }
        if (matches(frame, range, SELL_SIDE)) {
            return SIDE_ASK;
        }
        return 0;
    }

    private static void parseDecimal(ByteBuf frame, int[] range, long[] result) {
        ByteBufScanner.parseDecimal(frame, range[0], range[1], result);
    }

    private static boolean decimalInvalid(long[] decimal) {
        return decimal[0] == ByteBufScanner.DECIMAL_SENTINEL_MANTISSA
                && decimal[1] == ByteBufScanner.DECIMAL_SENTINEL_SCALE;
    }

    private static void rejectMalformed(ParseContext ctx) {
        ctx.counters().parseFailures().increment();
        ctx.counters().malformedRejections().increment();
    }

    private static void rejectProductMismatch(ParseContext ctx) {
        ctx.counters().productIdMismatches().increment();
        ctx.counters().unknownSymbolDrops().increment();
    }

    private static int[] stringField(ByteBuf frame, byte[] key) {
        return stringField(frame, key, frame.readerIndex(), frame.writerIndex());
    }

    private static int[] stringField(ByteBuf frame, byte[] key, int start, int end) {
        int readerIndex = frame.readerIndex();
        frame.readerIndex(start);
        try {
            if (!ByteBufScanner.scanToKey(frame, key)) {
                return missing();
            }
            return quotedValueRange(frame, frame.readerIndex(), end);
        } finally {
            frame.readerIndex(readerIndex);
        }
    }

    private static int[] arrayField(ByteBuf frame, byte[] key) {
        return arrayField(frame, key, frame.readerIndex(), frame.writerIndex());
    }

    private static int[] arrayField(ByteBuf frame, byte[] key, int start, int end) {
        int readerIndex = frame.readerIndex();
        frame.readerIndex(start);
        try {
            if (!ByteBufScanner.scanToKey(frame, key)) {
                return missing();
            }
            int valueStart = skipWhitespace(frame, frame.readerIndex(), end);
            if (valueStart >= end || frame.getByte(valueStart) != '[') {
                return missing();
            }
            int valueEnd = matchingContainerEnd(frame, valueStart, end, (byte) '[', (byte) ']');
            return valueEnd < 0 ? missing() : new int[] {valueStart + 1, valueEnd - 1};
        } finally {
            frame.readerIndex(readerIndex);
        }
    }

    private static boolean arrayContainsString(ByteBuf frame, int[] array, byte[] expected) {
        int index = array[0];
        while (index < array[1]) {
            int[] value = nextQuotedRange(frame, index, array[1]);
            if (value[0] < 0) {
                return false;
            }
            if (matches(frame, value, expected)) {
                return true;
            }
            index = value[1] + 1;
        }
        return false;
    }

    private static int[] nextQuotedRange(ByteBuf frame, int start, int end) {
        int index = start;
        while (index < end) {
            if (frame.getByte(index) == '"') {
                return quotedValueRange(frame, index, end);
            }
            index++;
        }
        return missing();
    }

    private static int[] nextObjectRange(ByteBuf frame, int start, int end) {
        int index = start;
        while (index < end) {
            if (frame.getByte(index) == '{') {
                int objectEnd = matchingContainerEnd(frame, index, end, (byte) '{', (byte) '}');
                return objectEnd < 0 ? missing() : new int[] {index + 1, objectEnd - 1};
            }
            index++;
        }
        return missing();
    }

    private static int[] quotedValueRange(ByteBuf frame, int start, int end) {
        int valueStart = skipWhitespace(frame, start, end);
        if (valueStart >= end || frame.getByte(valueStart) != '"') {
            return missing();
        }
        int index = valueStart + 1;
        while (index < end) {
            byte current = frame.getByte(index);
            if (current == '\\') {
                return missing();
            }
            if (current == '"') {
                return new int[] {valueStart + 1, index};
            }
            index++;
        }
        return missing();
    }

    private static int matchingContainerEnd(ByteBuf frame, int start, int end, byte open, byte close) {
        int depth = 0;
        int index = start;
        while (index < end) {
            byte current = frame.getByte(index);
            if (current == '"') {
                int[] string = quotedValueRange(frame, index, end);
                if (string[0] < 0) {
                    return -1;
                }
                index = string[1] + 1;
                continue;
            }
            if (current == open) {
                depth++;
            } else if (current == close) {
                depth--;
                if (depth == 0) {
                    return index + 1;
                }
            }
            index++;
        }
        return -1;
    }

    private static int skipWhitespace(ByteBuf frame, int start, int end) {
        int index = start;
        while (index < end) {
            byte current = frame.getByte(index);
            if (current != ' ' && current != '\n' && current != '\r' && current != '\t') {
                break;
            }
            index++;
        }
        return index;
    }

    private static boolean matches(ByteBuf frame, int[] range, byte[] expected) {
        if (range[1] - range[0] != expected.length) {
            return false;
        }
        for (int i = 0; i < expected.length; i++) {
            if (frame.getByte(range[0] + i) != expected[i]) {
                return false;
            }
        }
        return true;
    }

    private static void add(org.agrona.concurrent.status.AtomicCounter counter, long delta) {
        counter.set(counter.get() + delta);
    }

    private static int[] missing() {
        return new int[] {-1, -1};
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }
}
