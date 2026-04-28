package io.rueishi.marketdata.crypto.venue.coinbase.l3;

import io.netty.buffer.ByteBuf;
import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.core.encoding.EncodingConstants;
import io.rueishi.marketdata.crypto.core.encoding.SbeEncoder;
import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import io.rueishi.marketdata.crypto.core.parser.ByteBufScanner;
import io.rueishi.marketdata.crypto.core.parser.FeedParser;
import io.rueishi.marketdata.crypto.core.parser.ParseContext;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryReasonCode;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequestType;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * Stateless Coinbase full-feed parser that normalizes L3 WebSocket frames into schema v2 SBE messages.
 *
 * <p>{@code CoinbaseL3FeedParser} sits on the connector hot path between the
 * Coinbase full-channel JSON stream and the shared schema v2 encoders. A
 * connector factory creates one parser for one configured
 * {@link InstrumentConfig}, then the connector calls
 * {@link #onTextFrame(ByteBuf, ParseContext)} for each inbound text frame. The
 * parser keeps only immutable product-id bytes plus one caller-owned
 * {@code long[2]} UUID scratch buffer so the message path can stay
 * allocation-light while using {@link ByteBufScanner#parseUuidHighLow(ByteBuf, long[])}
 * for every venue UUID field.</p>
 *
 * <p>The runtime flow is split across the shared {@link ParseContext}
 * dependencies. The parser validates product id, snapshot readiness,
 * subscriptions acknowledgements, and control-plane error frames; uses
 * {@link ParseContext#encoder()} for {@code ORDER_EVENT} messages and
 * {@link ParseContext#tradeEncoder()} for {@code TRADE_EVENT} match messages;
 * then publishes through the connector-owned publisher and counters. Recovery
 * requests remain connector-owned through
 * {@link ParseContext#requestRecovery(RecoveryRequestType, RecoveryReasonCode, String)},
 * so the parser stays venue-specific but session-stateless.</p>
 */
public final class CoinbaseL3FeedParser implements FeedParser {
    private static final long MISSING_RANGE = -1L;
    private static final int PUBLISH_ATTEMPTS = 3;

    private static final byte[] TYPE_KEY = ascii("type");
    private static final byte[] PRODUCT_ID_KEY = ascii("product_id");
    private static final byte[] TIME_KEY = ascii("time");
    private static final byte[] SEQUENCE_KEY = ascii("sequence");
    private static final byte[] ORDER_ID_KEY = ascii("order_id");
    private static final byte[] MAKER_ORDER_ID_KEY = ascii("maker_order_id");
    private static final byte[] TAKER_ORDER_ID_KEY = ascii("taker_order_id");
    private static final byte[] SIDE_KEY = ascii("side");
    private static final byte[] ORDER_TYPE_KEY = ascii("order_type");
    private static final byte[] PRICE_KEY = ascii("price");
    private static final byte[] SIZE_KEY = ascii("size");
    private static final byte[] REMAINING_SIZE_KEY = ascii("remaining_size");
    private static final byte[] NEW_SIZE_KEY = ascii("new_size");
    private static final byte[] OLD_SIZE_KEY = ascii("old_size");
    private static final byte[] STOP_PRICE_KEY = ascii("stop_price");
    private static final byte[] REASON_KEY = ascii("reason");
    private static final byte[] CANCEL_REASON_KEY = ascii("cancel_reason");
    private static final byte[] CHANNELS_KEY = ascii("channels");
    private static final byte[] NAME_KEY = ascii("name");
    private static final byte[] PRODUCT_IDS_KEY = ascii("product_ids");

    private static final byte[] RECEIVED_TYPE = ascii("received");
    private static final byte[] OPEN_TYPE = ascii("open");
    private static final byte[] DONE_TYPE = ascii("done");
    private static final byte[] ACTIVATE_TYPE = ascii("activate");
    private static final byte[] CHANGE_TYPE = ascii("change");
    private static final byte[] MATCH_TYPE = ascii("match");
    private static final byte[] HEARTBEAT_TYPE = ascii("heartbeat");
    private static final byte[] SUBSCRIPTIONS_TYPE = ascii("subscriptions");
    private static final byte[] ERROR_TYPE = ascii("error");

    private static final byte[] FULL_CHANNEL = ascii("full");
    private static final byte[] HEARTBEAT_CHANNEL = ascii("heartbeat");
    private static final byte[] BUY_SIDE = ascii("buy");
    private static final byte[] SELL_SIDE = ascii("sell");
    private static final byte[] LIMIT_ORDER_TYPE = ascii("limit");
    private static final byte[] MARKET_ORDER_TYPE = ascii("market");
    private static final byte[] STOP_ORDER_TYPE = ascii("stop");
    private static final byte[] FILLED_REASON = ascii("filled");
    private static final byte[] CANCELED_REASON = ascii("canceled");

    private static final String ORDER_EVENT_BACKPRESSURE = "Coinbase L3 order-event publish backpressure";
    private static final String TRADE_EVENT_BACKPRESSURE = "Coinbase L3 trade-event publish backpressure";
    private static final String INVALID_SUBSCRIPTIONS_ACK = "invalid subscriptions acknowledgement";
    private static final String ERROR_FRAME_RECOVERY = "coinbase error frame";

    private final byte[] productId;
    private final long[] uuidResult = new long[2];

    /**
     * Creates a parser bound to one configured Coinbase full-feed product id.
     *
     * <p>The parser compares {@code product_id} as raw ASCII bytes so normal
     * data-path validation does not allocate transient {@link String}
     * instances.</p>
     *
     * @param instrument configured connector instrument
     * @throws NullPointerException if {@code instrument} is null
     * @throws IllegalArgumentException if {@code instrument.exchangeSymbol} is blank
     */
    public CoinbaseL3FeedParser(InstrumentConfig instrument) {
        Objects.requireNonNull(instrument, "instrument");
        if (instrument.exchangeSymbol == null || instrument.exchangeSymbol.isBlank()) {
            throw new IllegalArgumentException("instrument.exchangeSymbol is required");
        }
        this.productId = instrument.exchangeSymbol.getBytes(StandardCharsets.US_ASCII);
    }

    /**
     * Parses one Coinbase L3 JSON text frame and dispatches it to the correct encoder path.
     *
     * <p>The method records frame-level ingress counters, inspects the top-level
     * {@code type}, validates hot-path data messages against product and
     * snapshot state, and either publishes one schema v2 message or records a
     * counted drop. Control-plane frames such as {@code subscriptions},
     * {@code heartbeat}, and {@code error} update connector session state but
     * do not publish book data.</p>
     *
     * @param frame inbound Coinbase JSON text frame
     * @param ctx session-scoped parser dependencies owned by the connector
     * @throws NullPointerException if {@code frame} or {@code ctx} is null
     */
    @Override
    public void onTextFrame(ByteBuf frame, ParseContext ctx) {
        Objects.requireNonNull(frame, "frame");
        Objects.requireNonNull(ctx, "ctx");

        ctx.counters().framesReceived().increment();
        add(ctx.counters().bytesReceived(), frame.readableBytes());
        ctx.counters().lastMessageReceivedNanos().set(ctx.nanoClock().nanoTime());

        try {
            long typeRange = stringField(frame, TYPE_KEY, frame.readerIndex(), frame.writerIndex());
            if (typeRange == MISSING_RANGE) {
                rejectMalformed(ctx);
                return;
            }

            if (matches(frame, typeRange, RECEIVED_TYPE)) {
                handleReceived(frame, ctx);
            } else if (matches(frame, typeRange, OPEN_TYPE)) {
                handleOpen(frame, ctx);
            } else if (matches(frame, typeRange, DONE_TYPE)) {
                handleDone(frame, ctx);
            } else if (matches(frame, typeRange, ACTIVATE_TYPE)) {
                handleActivate(frame, ctx);
            } else if (matches(frame, typeRange, CHANGE_TYPE)) {
                handleChange(frame, ctx);
            } else if (matches(frame, typeRange, MATCH_TYPE)) {
                handleMatch(frame, ctx);
            } else if (matches(frame, typeRange, HEARTBEAT_TYPE)) {
                handleHeartbeat(frame, ctx);
            } else if (matches(frame, typeRange, SUBSCRIPTIONS_TYPE)) {
                handleSubscriptions(frame, ctx);
            } else if (matches(frame, typeRange, ERROR_TYPE)) {
                handleError(ctx);
            } else {
                ctx.counters().unknownTypeDrops().increment();
            }
        } catch (RuntimeException ex) {
            rejectMalformed(ctx);
        }
    }

    /**
     * Parses and publishes a Coinbase {@code received} order event.
     *
     * @param frame inbound Coinbase frame
     * @param ctx parse context for the current connector session
     */
    private void handleReceived(ByteBuf frame, ParseContext ctx) {
        if (!validateProduct(frame, ctx) || !validateSnapshotReady(ctx)) {
            return;
        }
        long sequence = requiredLong(frame, SEQUENCE_KEY);
        long timestamp = timestamp(frame);
        parseUuidField(frame, ORDER_ID_KEY);
        long orderIdHigh = uuidResult[0];
        long orderIdLow = uuidResult[1];
        byte side = requiredSide(frame);
        byte orderType = receivedOrderType(frame);

        long[] price = new long[2];
        long[] qty = new long[2];
        optionalDecimal(frame, PRICE_KEY, price);
        optionalDecimal(frame, SIZE_KEY, qty);

        publishOrderEvent(
                ctx,
                sequence,
                timestamp,
                orderIdHigh,
                orderIdLow,
                side,
                EncodingConstants.REASON_RECEIVED,
                orderType,
                (byte) price[1],
                (byte) qty[1],
                (byte) 0,
                price[0],
                qty[0],
                0L);
    }

    /**
     * Parses and publishes a Coinbase {@code open} order event.
     *
     * @param frame inbound Coinbase frame
     * @param ctx parse context for the current connector session
     */
    private void handleOpen(ByteBuf frame, ParseContext ctx) {
        if (!validateProduct(frame, ctx) || !validateSnapshotReady(ctx)) {
            return;
        }
        long sequence = requiredLong(frame, SEQUENCE_KEY);
        long timestamp = timestamp(frame);
        parseUuidField(frame, ORDER_ID_KEY);
        long orderIdHigh = uuidResult[0];
        long orderIdLow = uuidResult[1];
        byte side = requiredSide(frame);

        long[] price = new long[2];
        long[] qty = new long[2];
        requiredDecimal(frame, PRICE_KEY, price);
        requiredDecimal(frame, REMAINING_SIZE_KEY, qty);

        publishOrderEvent(
                ctx,
                sequence,
                timestamp,
                orderIdHigh,
                orderIdLow,
                side,
                EncodingConstants.REASON_OPEN,
                EncodingConstants.ORDER_TYPE_LIMIT,
                (byte) price[1],
                (byte) qty[1],
                (byte) 0,
                price[0],
                qty[0],
                0L);
    }

    /**
     * Parses and publishes a Coinbase {@code done} order event.
     *
     * <p>The parser maps Coinbase reason values into schema v2 reason codes and
     * treats missing price as an allowed market-order-style neutral value.</p>
     *
     * @param frame inbound Coinbase frame
     * @param ctx parse context for the current connector session
     */
    private void handleDone(ByteBuf frame, ParseContext ctx) {
        if (!validateProduct(frame, ctx) || !validateSnapshotReady(ctx)) {
            return;
        }
        long sequence = requiredLong(frame, SEQUENCE_KEY);
        long timestamp = timestamp(frame);
        parseUuidField(frame, ORDER_ID_KEY);
        long orderIdHigh = uuidResult[0];
        long orderIdLow = uuidResult[1];
        byte side = requiredSide(frame);
        byte reason = doneReason(frame);

        long[] price = new long[2];
        long[] qty = new long[2];
        optionalDecimal(frame, PRICE_KEY, price);
        requiredDecimal(frame, REMAINING_SIZE_KEY, qty);

        publishOrderEvent(
                ctx,
                sequence,
                timestamp,
                orderIdHigh,
                orderIdLow,
                side,
                reason,
                EncodingConstants.ORDER_TYPE_UNKNOWN,
                (byte) price[1],
                (byte) qty[1],
                (byte) 0,
                price[0],
                qty[0],
                0L);
    }

    /**
     * Parses and publishes a Coinbase {@code activate} order event.
     *
     * @param frame inbound Coinbase frame
     * @param ctx parse context for the current connector session
     */
    private void handleActivate(ByteBuf frame, ParseContext ctx) {
        if (!validateProduct(frame, ctx) || !validateSnapshotReady(ctx)) {
            return;
        }
        long sequence = requiredLong(frame, SEQUENCE_KEY);
        long timestamp = timestamp(frame);
        parseUuidField(frame, ORDER_ID_KEY);
        long orderIdHigh = uuidResult[0];
        long orderIdLow = uuidResult[1];
        byte side = requiredSide(frame);

        long[] stopPrice = new long[2];
        requiredDecimal(frame, STOP_PRICE_KEY, stopPrice);

        publishOrderEvent(
                ctx,
                sequence,
                timestamp,
                orderIdHigh,
                orderIdLow,
                side,
                EncodingConstants.REASON_TRIGGERED,
                EncodingConstants.ORDER_TYPE_STOP,
                (byte) stopPrice[1],
                (byte) 0,
                (byte) 0,
                stopPrice[0],
                0L,
                0L);
    }

    /**
     * Parses and publishes a Coinbase {@code change} order event.
     *
     * @param frame inbound Coinbase frame
     * @param ctx parse context for the current connector session
     */
    private void handleChange(ByteBuf frame, ParseContext ctx) {
        if (!validateProduct(frame, ctx) || !validateSnapshotReady(ctx)) {
            return;
        }
        long sequence = requiredLong(frame, SEQUENCE_KEY);
        long timestamp = timestamp(frame);
        parseUuidField(frame, ORDER_ID_KEY);
        long orderIdHigh = uuidResult[0];
        long orderIdLow = uuidResult[1];
        byte side = requiredSide(frame);

        long[] price = new long[2];
        long[] qty = new long[2];
        long[] oldQty = new long[2];
        requiredDecimal(frame, PRICE_KEY, price);
        requiredDecimal(frame, NEW_SIZE_KEY, qty);
        requiredDecimal(frame, OLD_SIZE_KEY, oldQty);

        publishOrderEvent(
                ctx,
                sequence,
                timestamp,
                orderIdHigh,
                orderIdLow,
                side,
                EncodingConstants.REASON_MODIFIED,
                EncodingConstants.ORDER_TYPE_UNKNOWN,
                (byte) price[1],
                (byte) qty[1],
                (byte) oldQty[1],
                price[0],
                qty[0],
                oldQty[0]);
    }

    /**
     * Parses and publishes a Coinbase {@code match} trade event.
     *
     * <p>Match messages are the only L3 frames that use
     * {@link ParseContext#tradeEncoder()} because they emit the schema v2
     * {@code TRADE_EVENT} template instead of {@code ORDER_EVENT}.</p>
     *
     * @param frame inbound Coinbase frame
     * @param ctx parse context for the current connector session
     */
    private void handleMatch(ByteBuf frame, ParseContext ctx) {
        if (!validateProduct(frame, ctx) || !validateSnapshotReady(ctx)) {
            return;
        }
        long sequence = requiredLong(frame, SEQUENCE_KEY);
        long timestamp = timestamp(frame);

        parseUuidField(frame, MAKER_ORDER_ID_KEY);
        long makerOrderIdHigh = uuidResult[0];
        long makerOrderIdLow = uuidResult[1];
        parseUuidField(frame, TAKER_ORDER_ID_KEY);
        long takerOrderIdHigh = uuidResult[0];
        long takerOrderIdLow = uuidResult[1];

        byte takerSide = requiredSide(frame);
        long[] price = new long[2];
        long[] qty = new long[2];
        requiredDecimal(frame, PRICE_KEY, price);
        requiredDecimal(frame, SIZE_KEY, qty);

        long gatewaySequence = ctx.sequenceTracker().next();
        SbeEncoder tradeEncoder = ctx.tradeEncoder();
        tradeEncoder.beginMessage(
                EncodingConstants.EVENT_TYPE_BOOK_TRADE,
                gatewaySequence,
                sequence,
                sequence,
                timestamp,
                ctx.nanoClock().nanoTime());
        tradeEncoder.writeTrade(
                makerOrderIdHigh,
                makerOrderIdLow,
                takerOrderIdHigh,
                takerOrderIdLow,
                takerSide,
                (byte) price[1],
                (byte) qty[1],
                price[0],
                qty[0]);
        publishTradeEvent(ctx, tradeEncoder);
    }

    /**
     * Handles one Coinbase heartbeat frame without publishing market data.
     *
     * @param frame inbound Coinbase heartbeat frame
     * @param ctx parse context for the current connector session
     */
    private void handleHeartbeat(ByteBuf frame, ParseContext ctx) {
        if (!validateProduct(frame, ctx)) {
            return;
        }
        ctx.counters().heartbeatsReceived().increment();
        ctx.counters().messagesDecoded().increment();
        ctx.counters().lastHeartbeatReceivedNanos().set(ctx.nanoClock().nanoTime());
    }

    /**
     * Validates that a subscriptions acknowledgement confirms both required channels for the configured product.
     *
     * @param frame inbound Coinbase subscriptions acknowledgement
     * @param ctx parse context for the current connector session
     */
    private void handleSubscriptions(ByteBuf frame, ParseContext ctx) {
        long channelsRange = arrayField(frame, CHANNELS_KEY, frame.readerIndex(), frame.writerIndex());
        if (channelsRange == MISSING_RANGE
                || !containsChannelProduct(frame, channelsRange, FULL_CHANNEL)
                || !containsChannelProduct(frame, channelsRange, HEARTBEAT_CHANNEL)) {
            ctx.counters().subscriptionValidationFailures().increment();
            ctx.requestRecovery(
                    RecoveryRequestType.RESET,
                    RecoveryReasonCode.STREAM_INTEGRITY_FAILURE,
                    INVALID_SUBSCRIPTIONS_ACK);
            return;
        }
        ctx.counters().messagesDecoded().increment();
        ctx.onSubscriptionAckValidated();
    }

    /**
     * Handles one Coinbase control-plane {@code error} frame by counting it and requesting recovery.
     *
     * @param ctx parse context for the current connector session
     */
    private void handleError(ParseContext ctx) {
        ctx.counters().authenticationErrors().increment();
        ctx.counters().messagesDecoded().increment();
        ctx.requestRecovery(
                RecoveryRequestType.RESET,
                RecoveryReasonCode.STREAM_INTEGRITY_FAILURE,
                ERROR_FRAME_RECOVERY);
    }

    /**
     * Publishes one normalized order lifecycle event through the shared order encoder path.
     *
     * @param ctx parse context for the current connector session
     * @param sequence Coinbase per-product sequence
     * @param timestamp parsed exchange timestamp
     * @param orderIdHigh high 64 bits of the Coinbase order id
     * @param orderIdLow low 64 bits of the Coinbase order id
     * @param side normalized order side
     * @param reason normalized schema v2 reason code
     * @param orderType normalized schema v2 order type
     * @param priceScale price scale
     * @param qtyScale quantity scale
     * @param oldQtyScale old quantity scale
     * @param priceMantissa price mantissa
     * @param qtyMantissa quantity mantissa
     * @param oldQtyMantissa old quantity mantissa
     */
    private void publishOrderEvent(
            ParseContext ctx,
            long sequence,
            long timestamp,
            long orderIdHigh,
            long orderIdLow,
            byte side,
            byte reason,
            byte orderType,
            byte priceScale,
            byte qtyScale,
            byte oldQtyScale,
            long priceMantissa,
            long qtyMantissa,
            long oldQtyMantissa) {
        long gatewaySequence = ctx.sequenceTracker().next();
        ctx.encoder().beginMessage(
                EncodingConstants.EVENT_TYPE_BOOK_UPDATE,
                gatewaySequence,
                sequence,
                sequence,
                timestamp,
                ctx.nanoClock().nanoTime());
        ctx.encoder().writeOrderEvent(
                orderIdHigh,
                orderIdLow,
                side,
                EncodingConstants.ACTION_UPSERT,
                reason,
                orderType,
                priceScale,
                qtyScale,
                oldQtyScale,
                timestamp,
                priceMantissa,
                qtyMantissa,
                oldQtyMantissa);
        try {
            if (ctx.publishEncodedMessage(ORDER_EVENT_BACKPRESSURE)) {
                onPublishSuccess(
                        ctx.counters(),
                        ctx.encoder(),
                        EncodingConstants.MESSAGE_PREFIX_LENGTH + EncodingConstants.ORDER_EVENT_ENTRY_LENGTH);
            }
        } catch (RuntimeException ex) {
            ctx.counters().encodeFailures().increment();
            throw ex;
        }
    }

    /**
     * Publishes one trade event with the same bounded retry and recovery policy used by the default parse context.
     *
     * @param ctx parse context for the current connector session
     * @param tradeEncoder dedicated trade encoder
     */
    private void publishTradeEvent(ParseContext ctx, SbeEncoder tradeEncoder) {
        InstrumentCounters counters = ctx.counters();
        int length = tradeEncoder.finishMessage();
        for (int attempt = 0; attempt < PUBLISH_ATTEMPTS; attempt++) {
            long handoffLatencyNanos = ctx.nanoClock().nanoTime() - tradeEncoder.lastIngressTimestamp();
            try {
                if (ctx.publisher().publish(tradeEncoder.buffer(), 0, length, counters, ctx.nanoClock())) {
                    recordHandoffLatency(counters, handoffLatencyNanos);
                    onPublishSuccess(counters, tradeEncoder, length);
                    return;
                }
            } catch (RuntimeException ex) {
                counters.publishFailures().increment();
                counters.encodeFailures().increment();
                throw ex;
            }
            if (attempt < PUBLISH_ATTEMPTS - 1) {
                Thread.onSpinWait();
            }
        }
        counters.backpressureEvents().increment();
        counters.backpressureDrops().increment();
        ctx.requestRecovery(
                RecoveryRequestType.RESET,
                RecoveryReasonCode.STREAM_INTEGRITY_FAILURE,
                TRADE_EVENT_BACKPRESSURE);
    }

    /**
     * Updates post-publish counters shared by order-event and trade-event success paths.
     *
     * @param counters per-instrument counters
     * @param encoder encoder that produced the message
     * @param encodedLength finalized encoded length in bytes
     */
    private static void onPublishSuccess(InstrumentCounters counters, SbeEncoder encoder, int encodedLength) {
        counters.updateMessagesReceived().increment();
        counters.messagesDecoded().increment();
        counters.encodeSuccesses().increment();
        counters.levelsEncoded().increment();
        add(counters.encodedBytes(), encodedLength);
        counters.encodeBufferReuseCount().set(encoder.reuseCount());
    }

    private static void recordHandoffLatency(InstrumentCounters counters, long latencyNanos) {
        counters.handoffLatencyLastNanos().set(latencyNanos);
        long currentMin = counters.handoffLatencyMinNanos().get();
        counters.handoffLatencyMinNanos().set(currentMin == 0L ? latencyNanos : Math.min(currentMin, latencyNanos));
        counters.handoffLatencyMaxNanos().set(Math.max(counters.handoffLatencyMaxNanos().get(), latencyNanos));
    }

    private boolean validateProduct(ByteBuf frame, ParseContext ctx) {
        long productRange = stringField(frame, PRODUCT_ID_KEY, frame.readerIndex(), frame.writerIndex());
        if (productRange == MISSING_RANGE) {
            rejectMalformed(ctx);
            return false;
        }
        if (!matches(frame, productRange, productId)) {
            ctx.counters().productIdMismatches().increment();
            ctx.counters().unknownSymbolDrops().increment();
            return false;
        }
        return true;
    }

    private static boolean validateSnapshotReady(ParseContext ctx) {
        if (!ctx.snapshotGatekeeper().isReady()) {
            ctx.counters().preSnapshotDrops().increment();
            return false;
        }
        return true;
    }

    private byte requiredSide(ByteBuf frame) {
        long sideRange = stringField(frame, SIDE_KEY, frame.readerIndex(), frame.writerIndex());
        if (sideRange == MISSING_RANGE) {
            throw new IllegalArgumentException("missing side");
        }
        if (matches(frame, sideRange, BUY_SIDE)) {
            return EncodingConstants.SIDE_BID;
        }
        if (matches(frame, sideRange, SELL_SIDE)) {
            return EncodingConstants.SIDE_ASK;
        }
        throw new IllegalArgumentException("unknown side");
    }

    private byte receivedOrderType(ByteBuf frame) {
        long orderTypeRange = stringField(frame, ORDER_TYPE_KEY, frame.readerIndex(), frame.writerIndex());
        if (orderTypeRange == MISSING_RANGE) {
            throw new IllegalArgumentException("missing order_type");
        }
        if (matches(frame, orderTypeRange, LIMIT_ORDER_TYPE)) {
            return EncodingConstants.ORDER_TYPE_LIMIT;
        }
        if (matches(frame, orderTypeRange, MARKET_ORDER_TYPE)) {
            return EncodingConstants.ORDER_TYPE_MARKET;
        }
        if (matches(frame, orderTypeRange, STOP_ORDER_TYPE)) {
            return EncodingConstants.ORDER_TYPE_STOP;
        }
        return EncodingConstants.ORDER_TYPE_UNKNOWN;
    }

    private byte doneReason(ByteBuf frame) {
        long reasonRange = stringField(frame, REASON_KEY, frame.readerIndex(), frame.writerIndex());
        if (reasonRange == MISSING_RANGE) {
            throw new IllegalArgumentException("missing reason");
        }
        if (matches(frame, reasonRange, FILLED_REASON)) {
            return EncodingConstants.REASON_FILLED;
        }
        if (matches(frame, reasonRange, CANCELED_REASON)) {
            long cancelReason = optionalLong(frame, CANCEL_REASON_KEY);
            return cancelReason == 101L ? EncodingConstants.REASON_EXPIRED : EncodingConstants.REASON_CANCELLED;
        }
        throw new IllegalArgumentException("unsupported done reason");
    }

    private void parseUuidField(ByteBuf frame, byte[] key) {
        long range = stringField(frame, key, frame.readerIndex(), frame.writerIndex());
        if (range == MISSING_RANGE) {
            throw new IllegalArgumentException("missing uuid");
        }
        int readerIndex = frame.readerIndex();
        frame.readerIndex(rangeStart(range));
        try {
            if (!ByteBufScanner.parseUuidHighLow(frame, uuidResult) || (uuidResult[0] == 0L && uuidResult[1] == 0L)) {
                throw new IllegalArgumentException("invalid uuid");
            }
        } finally {
            frame.readerIndex(readerIndex);
        }
    }

    private static long requiredLong(ByteBuf frame, byte[] key) {
        long range = rawValueField(frame, key, frame.readerIndex(), frame.writerIndex());
        if (range == MISSING_RANGE) {
            throw new IllegalArgumentException("missing numeric field");
        }
        long value = parseLong(frame, range);
        if (value == Long.MAX_VALUE) {
            throw new IllegalArgumentException("invalid numeric field");
        }
        return value;
    }

    private static long optionalLong(ByteBuf frame, byte[] key) {
        long range = rawValueField(frame, key, frame.readerIndex(), frame.writerIndex());
        return range == MISSING_RANGE ? Long.MIN_VALUE : parseLong(frame, range);
    }

    private static void requiredDecimal(ByteBuf frame, byte[] key, long[] result) {
        long range = rawValueField(frame, key, frame.readerIndex(), frame.writerIndex());
        if (range == MISSING_RANGE) {
            throw new IllegalArgumentException("missing decimal field");
        }
        ByteBufScanner.parseDecimal(frame, rangeStart(range), rangeEnd(range), result);
        if (decimalInvalid(result)) {
            throw new IllegalArgumentException("invalid decimal field");
        }
    }

    private static void optionalDecimal(ByteBuf frame, byte[] key, long[] result) {
        long range = rawValueField(frame, key, frame.readerIndex(), frame.writerIndex());
        if (range == MISSING_RANGE || isBlank(frame, range)) {
            result[0] = 0L;
            result[1] = 0L;
            return;
        }
        ByteBufScanner.parseDecimal(frame, rangeStart(range), rangeEnd(range), result);
        if (decimalInvalid(result)) {
            throw new IllegalArgumentException("invalid decimal field");
        }
    }

    private static long timestamp(ByteBuf frame) {
        long range = stringField(frame, TIME_KEY, frame.readerIndex(), frame.writerIndex());
        return range == MISSING_RANGE
                ? EncodingConstants.NO_TIMESTAMP
                : ByteBufScanner.parseRfc3339ToEpochNanos(frame, rangeStart(range), rangeEnd(range));
    }

    private boolean containsChannelProduct(ByteBuf frame, long channelsRange, byte[] channelName) {
        int index = rangeStart(channelsRange);
        int end = rangeEnd(channelsRange);
        while (index < end) {
            long objectRange = nextObjectRange(frame, index, end);
            if (objectRange == MISSING_RANGE) {
                return false;
            }
            long nameRange = stringField(frame, NAME_KEY, rangeStart(objectRange), rangeEnd(objectRange));
            if (nameRange != MISSING_RANGE && matches(frame, nameRange, channelName)) {
                long productIdsRange = arrayField(frame, PRODUCT_IDS_KEY, rangeStart(objectRange), rangeEnd(objectRange));
                return productIdsRange != MISSING_RANGE && arrayContainsString(frame, productIdsRange, productId);
            }
            index = rangeEnd(objectRange) + 1;
        }
        return false;
    }

    private static boolean arrayContainsString(ByteBuf frame, long arrayRange, byte[] expected) {
        int index = rangeStart(arrayRange);
        int end = rangeEnd(arrayRange);
        while (index < end) {
            long valueRange = nextQuotedRange(frame, index, end);
            if (valueRange == MISSING_RANGE) {
                return false;
            }
            if (matches(frame, valueRange, expected)) {
                return true;
            }
            index = rangeEnd(valueRange) + 1;
        }
        return false;
    }

    private static long stringField(ByteBuf frame, byte[] key, int start, int end) {
        int valueStart = findValueStart(frame, key, start, end);
        return valueStart < 0 ? MISSING_RANGE : quotedValueRange(frame, valueStart, end);
    }

    private static long rawValueField(ByteBuf frame, byte[] key, int start, int end) {
        int valueStart = findValueStart(frame, key, start, end);
        return valueStart < 0 ? MISSING_RANGE : rawValueRange(frame, valueStart, end);
    }

    private static long arrayField(ByteBuf frame, byte[] key, int start, int end) {
        int valueStart = findValueStart(frame, key, start, end);
        if (valueStart < 0) {
            return MISSING_RANGE;
        }
        int arrayStart = skipWhitespace(frame, valueStart, end);
        if (arrayStart >= end || frame.getByte(arrayStart) != '[') {
            return MISSING_RANGE;
        }
        int arrayEnd = matchingContainerEnd(frame, arrayStart, end, (byte) '[', (byte) ']');
        return arrayEnd < 0 ? MISSING_RANGE : packRange(arrayStart + 1, arrayEnd - 1);
    }

    private static int findValueStart(ByteBuf frame, byte[] key, int start, int end) {
        for (int index = start; index < end; index++) {
            if (frame.getByte(index) != '"' || !matchesAt(frame, index + 1, end, key)) {
                continue;
            }
            int afterKey = index + 1 + key.length;
            if (afterKey >= end || frame.getByte(afterKey) != '"') {
                continue;
            }
            int afterQuote = skipWhitespace(frame, afterKey + 1, end);
            if (afterQuote < end && frame.getByte(afterQuote) == ':') {
                return afterQuote + 1;
            }
        }
        return -1;
    }

    private static long quotedValueRange(ByteBuf frame, int start, int end) {
        int valueStart = skipWhitespace(frame, start, end);
        if (valueStart >= end || frame.getByte(valueStart) != '"') {
            return MISSING_RANGE;
        }
        int index = valueStart + 1;
        while (index < end) {
            byte current = frame.getByte(index);
            if (current == '\\') {
                return MISSING_RANGE;
            }
            if (current == '"') {
                return packRange(valueStart + 1, index);
            }
            index++;
        }
        return MISSING_RANGE;
    }

    private static long rawValueRange(ByteBuf frame, int start, int end) {
        int valueStart = skipWhitespace(frame, start, end);
        if (valueStart >= end) {
            return MISSING_RANGE;
        }
        int valueEnd = valueStart;
        boolean quoted = frame.getByte(valueStart) == '"';
        if (quoted) {
            valueStart++;
            valueEnd = valueStart;
            while (valueEnd < end && frame.getByte(valueEnd) != '"') {
                if (frame.getByte(valueEnd) == '\\') {
                    return MISSING_RANGE;
                }
                valueEnd++;
            }
            return valueEnd < end ? packRange(valueStart, valueEnd) : MISSING_RANGE;
        }
        while (valueEnd < end) {
            byte current = frame.getByte(valueEnd);
            if (current == ',' || current == '}' || current == ']' || current == ' ' || current == '\n'
                    || current == '\r' || current == '\t') {
                break;
            }
            valueEnd++;
        }
        return valueEnd == valueStart ? MISSING_RANGE : packRange(valueStart, valueEnd);
    }

    private static long nextQuotedRange(ByteBuf frame, int start, int end) {
        int index = start;
        while (index < end) {
            if (frame.getByte(index) == '"') {
                return quotedValueRange(frame, index, end);
            }
            index++;
        }
        return MISSING_RANGE;
    }

    private static long nextObjectRange(ByteBuf frame, int start, int end) {
        int index = start;
        while (index < end) {
            if (frame.getByte(index) == '{') {
                int objectEnd = matchingContainerEnd(frame, index, end, (byte) '{', (byte) '}');
                return objectEnd < 0 ? MISSING_RANGE : packRange(index + 1, objectEnd - 1);
            }
            index++;
        }
        return MISSING_RANGE;
    }

    private static int matchingContainerEnd(ByteBuf frame, int start, int end, byte open, byte close) {
        int depth = 0;
        int index = start;
        while (index < end) {
            byte current = frame.getByte(index);
            if (current == '"') {
                long stringRange = quotedValueRange(frame, index, end);
                if (stringRange == MISSING_RANGE) {
                    return -1;
                }
                index = rangeEnd(stringRange) + 1;
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

    private static boolean matches(ByteBuf frame, long range, byte[] expected) {
        return matches(frame, rangeStart(range), rangeEnd(range), expected);
    }

    private static boolean matches(ByteBuf frame, int start, int end, byte[] expected) {
        if (end - start != expected.length) {
            return false;
        }
        for (int i = 0; i < expected.length; i++) {
            if (frame.getByte(start + i) != expected[i]) {
                return false;
            }
        }
        return true;
    }

    private static boolean matchesAt(ByteBuf frame, int start, int end, byte[] expected) {
        if (start < 0 || start + expected.length > end) {
            return false;
        }
        for (int i = 0; i < expected.length; i++) {
            if (frame.getByte(start + i) != expected[i]) {
                return false;
            }
        }
        return true;
    }

    private static long parseLong(ByteBuf frame, long range) {
        int start = rangeStart(range);
        int end = rangeEnd(range);
        int index = start;
        if (index < end && frame.getByte(index) == '"') {
            index++;
        }
        long value = 0L;
        boolean sawDigit = false;
        while (index < end) {
            byte current = frame.getByte(index);
            if (current == '"') {
                break;
            }
            if (current < '0' || current > '9') {
                return Long.MAX_VALUE;
            }
            sawDigit = true;
            int digit = current - '0';
            if (value > (Long.MAX_VALUE - digit) / 10L) {
                return Long.MAX_VALUE;
            }
            value = (value * 10L) + digit;
            index++;
        }
        return sawDigit ? value : Long.MAX_VALUE;
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

    private static boolean isBlank(ByteBuf frame, long range) {
        return rangeStart(range) == rangeEnd(range);
    }

    private static boolean decimalInvalid(long[] decimal) {
        return decimal[0] == ByteBufScanner.DECIMAL_SENTINEL_MANTISSA
                && decimal[1] == ByteBufScanner.DECIMAL_SENTINEL_SCALE;
    }

    private static void rejectMalformed(ParseContext ctx) {
        ctx.counters().parseFailures().increment();
        ctx.counters().malformedRejections().increment();
    }

    private static long packRange(int start, int end) {
        return (((long) start) << 32) | (end & 0xFFFF_FFFFL);
    }

    private static int rangeStart(long range) {
        return (int) (range >>> 32);
    }

    private static int rangeEnd(long range) {
        return (int) range;
    }

    private static void add(AtomicCounter counter, long delta) {
        counter.set(counter.get() + delta);
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }
}
