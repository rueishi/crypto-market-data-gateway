package io.rueishi.marketdata.crypto.venue.coinbase.l3;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.core.encoding.EncodingConstants;
import io.rueishi.marketdata.crypto.core.encoding.SbeEncoder;
import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import io.rueishi.marketdata.crypto.core.parser.ByteBufScanner;
import io.rueishi.marketdata.crypto.core.parser.DefaultParseContext;
import io.rueishi.marketdata.crypto.core.parser.ParseContext;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryReasonCode;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequestType;
import io.rueishi.marketdata.crypto.core.sequence.SequenceTracker;
import io.rueishi.marketdata.crypto.core.snapshot.SnapshotContext;
import io.rueishi.marketdata.crypto.core.snapshot.SnapshotStrategy;
import io.rueishi.marketdata.crypto.venue.coinbase.shared.CoinbaseAuthenticator;
import io.rueishi.marketdata.crypto.venue.coinbase.shared.CoinbaseAuthenticator.AuthFields;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Coinbase full-feed L3 snapshot acquisition strategy using the REST_THEN_DELTA algorithm.
 *
 * <p>{@code CoinbaseL3SnapshotStrategy} implements the five-phase REST_THEN_DELTA algorithm
 * defined in spec §5.5. On {@link #triggerSnapshot(InstrumentConfig, SnapshotContext)}:</p>
 * <ol>
 *   <li>Opens a pre-allocated ring buffer for inbound WebSocket delta frames so that every
 *       frame arriving while the REST fetch is in flight is captured without blocking the
 *       event loop.</li>
 *   <li>Issues an authenticated async REST request to the Coinbase
 *       {@code /products/{product_id}/book?level=3} endpoint using a non-hot-path
 *       {@link HttpClient} thread.</li>
 *   <li>Schedules the REST response back onto the connector event loop via the injected
 *       {@link Executor}.</li>
 *   <li>On the event loop: encodes and publishes a {@code BOOK_SNAPSHOT} message covering
 *       all bids and asks from the REST response, using template {@code ORDER_EVENT} and
 *       event type {@code BOOK_SNAPSHOT} per spec §15.6. Each entry uses
 *       {@code reason=OPEN}, {@code action=UPSERT}, {@code orderType=LIMIT}, and
 *       {@code exchangeTimestamp=-1}. {@code seq1} and {@code seq2} equal the REST
 *       response sequence value per §15.7.</li>
 *   <li>Opens the snapshot gate, drains buffered deltas in arrival order applying the
 *       sequence alignment rules from §5.5, replays valid deltas through
 *       {@link CoinbaseL3FeedParser}, and signals boundary completion.</li>
 * </ol>
 *
 * <p>Ring buffer overflow and sequence gaps trigger recovery through the callback extracted
 * from the injected {@link ParseContext} at construction time. Rather than retaining the full
 * {@code ParseContext} or per-session {@link SnapshotContext} as fields—which would violate
 * the venue context-retention constraint enforced by {@code CorePackageArchUnitTest}—this
 * class stores only the narrow-typed lambdas it actually needs: a
 * {@link DefaultParseContext.RecoveryRequestCallback}, a {@link Supplier}{@code <SequenceTracker>},
 * an {@link InstrumentCounters} reference, and a {@link Consumer}{@code <ByteBuf>} for delta
 * replay. The per-session {@link SnapshotContext} is passed solely as a lambda-captured
 * parameter inside the async HTTP callback.</p>
 *
 * <p>The connector factory (P4-B5) creates one instance per instrument during startup,
 * wiring the shared parse context and parser into the constructor. A separate test
 * constructor accepts an explicit ring buffer capacity and {@link HttpClient} for
 * deterministic unit tests.</p>
 */
public final class CoinbaseL3SnapshotStrategy implements SnapshotStrategy {

    /** Default pre-allocated ring buffer capacity for buffering WS delta frames. */
    static final int DEFAULT_RING_BUFFER_CAPACITY = 512;

    private static final String REST_BASE_URL = "https://api.exchange.coinbase.com";
    private static final String BOOK_PATH_FORMAT = "/products/%s/book?level=3";
    private static final int PUBLISH_ATTEMPTS = 3;

    private static final String RING_BUFFER_OVERFLOW = "Coinbase L3 snapshot ring buffer overflow";
    private static final String SEQUENCE_GAP = "Coinbase L3 buffered delta sequence gap";
    private static final String SNAPSHOT_BACKPRESSURE = "Coinbase L3 snapshot publish backpressure";
    private static final String MISSING_SEQUENCE = "missing sequence in Coinbase L3 REST snapshot response";

    private static final byte[] SEQUENCE_KEY = "sequence".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] BIDS_KEY = "bids".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ASKS_KEY = "asks".getBytes(StandardCharsets.US_ASCII);

    private final CoinbaseAuthenticator auth;

    // Narrow-typed lambdas extracted from ParseContext at construction time so that this
    // class does not retain a ParseContext field, satisfying the venue context-retention rule.
    private final DefaultParseContext.RecoveryRequestCallback recoveryCallback;
    private final Supplier<SequenceTracker> sequenceTrackerSupplier;
    private final InstrumentCounters counters;

    // Replays a buffered delta frame through the feed parser with the session-scoped ParseContext
    // that was live when this strategy was constructed (the same object used by the connector).
    private final Consumer<ByteBuf> deltaReplayer;

    private final Executor eventLoop;
    private final int ringBufferCapacity;
    private final HttpClient httpClient;
    private final String restBaseUrl;
    private final ByteBuf[] deltaRing;

    // Per-entry scratch arrays for the off-path snapshot encoding; not thread-safe but
    // processRestResponse runs exclusively on the connector event loop.
    private final long[] decimalScratch = new long[2];
    private final long[] uuidScratch = new long[2];

    // Session state; written before and cleared after one triggerSnapshot cycle.
    private volatile boolean buffering;
    private int writeIdx;
    private int readIdx;

    /**
     * Creates a strategy with the default ring buffer capacity and a new {@link HttpClient}.
     *
     * <p>Connector factories use this constructor. The supplied {@code parseContext} must
     * be the same session-scoped context that the connector passes to the Coinbase L3 feed
     * parser, so that gateway sequence numbers and recovery requests are routed through the
     * same session state. The strategy does not retain the {@code ParseContext} reference as
     * a field; it extracts the three narrow dependencies it needs at construction time.</p>
     *
     * @param auth Coinbase authenticator for REST request signing
     * @param parser feed parser used to replay buffered delta frames after snapshot alignment
     * @param parseContext session-scoped parse context for gateway sequences and recovery
     * @param eventLoop executor that runs tasks on the connector event-loop thread
     * @throws NullPointerException if any argument is null
     */
    public CoinbaseL3SnapshotStrategy(
            CoinbaseAuthenticator auth,
            CoinbaseL3FeedParser parser,
            ParseContext parseContext,
            Executor eventLoop) {
        this(auth,
                Objects.requireNonNull(parseContext, "parseContext")::requestRecovery,
                frame -> parser.onTextFrame(frame, parseContext),
                parseContext::sequenceTracker,
                parseContext.counters(),
                eventLoop,
                DEFAULT_RING_BUFFER_CAPACITY,
                HttpClient.newBuilder().build(),
                REST_BASE_URL);
    }

    /**
     * Creates a strategy with an explicit ring buffer capacity and HTTP client for testing.
     *
     * <p>Tests use this constructor to inject a controlled {@link HttpClient} backed by a
     * local HTTP server and a small ring buffer to trigger overflow with few frames.</p>
     *
     * @param auth Coinbase authenticator for REST request signing
     * @param parser feed parser used to replay buffered delta frames after snapshot alignment
     * @param parseContext session-scoped parse context for gateway sequences and recovery
     * @param eventLoop executor that runs tasks on the connector event-loop thread
     * @param ringBufferCapacity pre-allocated delta ring buffer capacity; must be positive
     * @param httpClient HTTP client used for the async REST snapshot fetch
     * @param restBaseUrl base URL for the Coinbase REST API; injected for tests
     * @throws NullPointerException if any non-primitive argument is null
     * @throws IllegalArgumentException if {@code ringBufferCapacity} is not positive
     */
    CoinbaseL3SnapshotStrategy(
            CoinbaseAuthenticator auth,
            CoinbaseL3FeedParser parser,
            ParseContext parseContext,
            Executor eventLoop,
            int ringBufferCapacity,
            HttpClient httpClient,
            String restBaseUrl) {
        this(auth,
                Objects.requireNonNull(parseContext, "parseContext")::requestRecovery,
                frame -> parser.onTextFrame(frame, parseContext),
                parseContext::sequenceTracker,
                parseContext.counters(),
                eventLoop,
                ringBufferCapacity,
                httpClient,
                restBaseUrl);
    }

    /**
     * Canonical constructor that stores only narrow-typed extracted dependencies.
     */
    private CoinbaseL3SnapshotStrategy(
            CoinbaseAuthenticator auth,
            DefaultParseContext.RecoveryRequestCallback recoveryCallback,
            Consumer<ByteBuf> deltaReplayer,
            Supplier<SequenceTracker> sequenceTrackerSupplier,
            InstrumentCounters counters,
            Executor eventLoop,
            int ringBufferCapacity,
            HttpClient httpClient,
            String restBaseUrl) {
        this.auth = Objects.requireNonNull(auth, "auth");
        this.recoveryCallback = Objects.requireNonNull(recoveryCallback, "recoveryCallback");
        this.deltaReplayer = Objects.requireNonNull(deltaReplayer, "deltaReplayer");
        this.sequenceTrackerSupplier = Objects.requireNonNull(sequenceTrackerSupplier, "sequenceTrackerSupplier");
        this.counters = Objects.requireNonNull(counters, "counters");
        this.eventLoop = Objects.requireNonNull(eventLoop, "eventLoop");
        if (ringBufferCapacity <= 0) {
            throw new IllegalArgumentException("ringBufferCapacity must be positive");
        }
        this.ringBufferCapacity = ringBufferCapacity;
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
        this.restBaseUrl = Objects.requireNonNull(restBaseUrl, "restBaseUrl");
        this.deltaRing = new ByteBuf[ringBufferCapacity];
    }

    /**
     * Returns {@link Mode#REST_THEN_DELTA}, indicating that snapshot acquisition requires a
     * separate REST fetch and delta-alignment phase.
     *
     * @return {@link Mode#REST_THEN_DELTA}
     */
    @Override
    public Mode mode() {
        return Mode.REST_THEN_DELTA;
    }

    /**
     * Starts REST_THEN_DELTA snapshot acquisition for the supplied instrument.
     *
     * <p>This method opens the delta ring buffer <em>before</em> issuing the REST fetch so
     * that any WebSocket frames arriving during the HTTP round trip are captured. The REST
     * request is signed with the Coinbase HMAC-SHA256 algorithm over
     * {@code timestamp + "GET" + "/products/{product_id}/book?level=3"} per spec §15.2. The
     * HTTP response handler is scheduled back onto the connector event loop via the injected
     * {@link Executor} so that snapshot encoding, delta alignment, and gate opening all run
     * on the event-loop thread without additional synchronization.</p>
     *
     * @param instrument configured instrument whose snapshot is required
     * @param ctx session-scoped snapshot context for encoding, publishing, and gate control
     * @throws NullPointerException if {@code instrument} or {@code ctx} is null
     */
    @Override
    public void triggerSnapshot(InstrumentConfig instrument, SnapshotContext ctx) {
        Objects.requireNonNull(instrument, "instrument");
        Objects.requireNonNull(ctx, "ctx");

        // Step 1: Open ring buffer before issuing the REST fetch so no deltas are missed.
        writeIdx = 0;
        readIdx = 0;
        buffering = true;

        // Step 2: Build authenticated REST request.
        String timestamp = Long.toString(Instant.now().getEpochSecond());
        String path = String.format(BOOK_PATH_FORMAT, instrument.exchangeSymbol);
        AuthFields authFields = auth.authFields(timestamp, "GET", path);

        HttpRequest request = HttpRequest.newBuilder(URI.create(restBaseUrl + path))
                .header("CB-ACCESS-KEY", authFields.key())
                .header("CB-ACCESS-SIGN", authFields.signature())
                .header("CB-ACCESS-TIMESTAMP", authFields.timestamp())
                .header("CB-ACCESS-PASSPHRASE", authFields.passphrase())
                .GET()
                .build();

        // Step 3: Issue async fetch; schedule response processing back onto the event loop.
        // ctx is captured in the lambda; no SnapshotContext field is retained.
        httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenAccept(response -> eventLoop.execute(
                        () -> processRestResponse(response.body(), ctx)));
    }

    /**
     * Buffers one inbound WebSocket delta frame during snapshot acquisition.
     *
     * <p>Called from the connector event loop for every inbound text frame while
     * {@link #triggerSnapshot(InstrumentConfig, SnapshotContext)} has opened the ring buffer.
     * The frame's reference count is incremented via {@link ByteBuf#retainedDuplicate()} so
     * the caller may release its own reference. If the ring buffer capacity is exhausted
     * before the REST snapshot arrives, buffering is stopped, the
     * {@link io.rueishi.marketdata.crypto.core.observability.InstrumentCounters#overflowRejections()}
     * counter is incremented, and recovery is requested through the injected callback.</p>
     *
     * <p>Frames arriving after {@code buffering} is {@code false} (either overflow-triggered
     * or after {@code processRestResponse} has run) are silently ignored.</p>
     *
     * @param frame inbound Coinbase WebSocket text frame; caller retains its own reference
     */
    public void bufferDelta(ByteBuf frame) {
        if (!buffering) {
            return;
        }
        int next = (writeIdx + 1) % ringBufferCapacity;
        if (next == readIdx) {
            // Ring buffer full: stop buffering and request recovery.
            buffering = false;
            counters.overflowRejections().increment();
            recoveryCallback.requestRecovery(
                    RecoveryRequestType.RESET,
                    RecoveryReasonCode.STREAM_INTEGRITY_FAILURE,
                    RING_BUFFER_OVERFLOW);
            return;
        }
        deltaRing[writeIdx] = frame.retainedDuplicate();
        writeIdx = next;
    }

    /**
     * Processes the REST snapshot response on the connector event-loop thread.
     *
     * <p>This method stops buffering, parses the REST response, encodes and publishes the
     * {@code BOOK_SNAPSHOT} message, opens the snapshot gate, drains and aligns buffered
     * deltas, and signals {@link SnapshotContext#onSnapshotBoundaryAccepted()}. On any
     * error—missing sequence, publish backpressure, or a sequence gap in buffered deltas—the
     * method requests recovery through the injected callback and returns without opening the
     * gate. The {@code finally} block always releases retained delta frames regardless of
     * success or failure.</p>
     *
     * <p>If the ring buffer overflowed before this response arrived, {@code buffering} will
     * already be {@code false}. In that case recovery was already requested and this method
     * simply releases retained frames and returns without encoding a snapshot.</p>
     *
     * @param json REST response body
     * @param ctx snapshot context for the current session
     */
    private void processRestResponse(String json, SnapshotContext ctx) {
        if (!buffering) {
            // Ring buffer overflowed before the REST response arrived; clean up and bail out.
            releaseBufferedDeltas();
            return;
        }
        buffering = false;
        ByteBuf buf = null;
        try {
            buf = Unpooled.copiedBuffer(json, StandardCharsets.UTF_8);

            // Parse snapshot sequence N.
            if (!ByteBufScanner.scanToKey(buf, SEQUENCE_KEY)) {
                recoveryCallback.requestRecovery(
                        RecoveryRequestType.RESET,
                        RecoveryReasonCode.STREAM_INTEGRITY_FAILURE,
                        MISSING_SEQUENCE);
                return;
            }
            skipWhitespace(buf);
            long snapshotSeq = ByteBufScanner.readLong(buf);

            // Encode BOOK_SNAPSHOT with all bid and ask ORDER_EVENT entries.
            SbeEncoder encoder = ctx.encoder();
            long gatewaySeq = sequenceTrackerSupplier.get().next();
            encoder.beginMessage(
                    EncodingConstants.EVENT_TYPE_BOOK_SNAPSHOT,
                    gatewaySeq,
                    snapshotSeq,
                    snapshotSeq,
                    EncodingConstants.NO_TIMESTAMP,
                    ctx.nanoClock().nanoTime());

            if (ByteBufScanner.scanToKey(buf, BIDS_KEY)) {
                encodeRestSideEntries(buf, encoder, EncodingConstants.SIDE_BID);
            }
            if (ByteBufScanner.scanToKey(buf, ASKS_KEY)) {
                encodeRestSideEntries(buf, encoder, EncodingConstants.SIDE_ASK);
            }

            // Publish snapshot with bounded retry.
            int snapshotLength = encoder.finishMessage();
            boolean published = false;
            for (int attempt = 0; attempt < PUBLISH_ATTEMPTS; attempt++) {
                if (ctx.publisher().publish(
                        encoder.buffer(), 0, snapshotLength, ctx.counters(), ctx.nanoClock())) {
                    ctx.counters().bookSnapshotPublished().increment();
                    ctx.counters().snapshotMessagesReceived().increment();
                    published = true;
                    break;
                }
                if (attempt < PUBLISH_ATTEMPTS - 1) {
                    Thread.onSpinWait();
                }
            }
            if (!published) {
                recoveryCallback.requestRecovery(
                        RecoveryRequestType.RESET,
                        RecoveryReasonCode.STREAM_INTEGRITY_FAILURE,
                        SNAPSHOT_BACKPRESSURE);
                return;
            }

            // Open snapshot gate so that replayed deltas and live frames are not dropped.
            ctx.snapshotGatekeeper().accept();

            // Drain, align, and replay buffered deltas per §5.5 algorithm.
            if (!drainAndAlignDeltas(snapshotSeq)) {
                return;
            }

            // Signal boundary acceptance to connector lifecycle code.
            ctx.onSnapshotBoundaryAccepted();

        } catch (RuntimeException ex) {
            recoveryCallback.requestRecovery(
                    RecoveryRequestType.RESET,
                    RecoveryReasonCode.STREAM_INTEGRITY_FAILURE,
                    "Coinbase L3 REST snapshot processing error");
        } finally {
            if (buf != null) {
                buf.release();
            }
            releaseBufferedDeltas();
        }
    }

    /**
     * Drains the ring buffer, aligns deltas against the snapshot sequence boundary, and
     * replays valid deltas through the feed parser.
     *
     * <p>The algorithm per spec §5.5:</p>
     * <ol>
     *   <li>Discard any buffered delta whose {@code sequence} is at or before the REST
     *       snapshot sequence {@code N}.</li>
     *   <li>Find the first remaining delta whose {@code sequence == N + 1}. If no such
     *       delta exists, discard all remaining buffered deltas and open the gate for the
     *       live stream to continue from wherever it is.</li>
     *   <li>Replay the boundary delta and all subsequent deltas in order, checking for
     *       sequence gaps. A gap ({@code seq != prevSeq + 1}) increments
     *       {@code sequenceGapsDetected} and triggers recovery.</li>
     * </ol>
     *
     * @param snapshotSeq REST snapshot sequence boundary N
     * @return {@code true} when draining completes without error, {@code false} when
     *         recovery was requested due to a sequence gap
     */
    private boolean drainAndAlignDeltas(long snapshotSeq) {
        // Phase 1: advance past stale deltas (seq <= N).
        int idx = readIdx;
        while (idx != writeIdx) {
            long seq = extractSequence(deltaRing[idx]);
            if (seq <= snapshotSeq) {
                idx = (idx + 1) % ringBufferCapacity;
            } else {
                break;
            }
        }

        if (idx == writeIdx) {
            // No post-snapshot deltas in buffer; live stream continues normally.
            return true;
        }

        // Phase 2: find boundary delta (seq == N + 1).
        long firstPostSnapshotSeq = extractSequence(deltaRing[idx]);
        if (firstPostSnapshotSeq != snapshotSeq + 1L) {
            // First post-snapshot delta has seq > N+1 — gap detected.
            counters.sequenceGapsDetected().increment();
            recoveryCallback.requestRecovery(
                    RecoveryRequestType.RESET,
                    RecoveryReasonCode.STREAM_INTEGRITY_FAILURE,
                    SEQUENCE_GAP);
            return false;
        }

        // Phase 3: replay from boundary delta, checking for internal gaps.
        long prevSeq = -1L;
        while (idx != writeIdx) {
            ByteBuf frame = deltaRing[idx];
            long seq = extractSequence(frame);

            if (prevSeq != -1L && seq != prevSeq + 1L) {
                counters.sequenceGapsDetected().increment();
                recoveryCallback.requestRecovery(
                        RecoveryRequestType.RESET,
                        RecoveryReasonCode.STREAM_INTEGRITY_FAILURE,
                        SEQUENCE_GAP);
                return false;
            }

            // Reset reader index so the parser scans from the start of the JSON frame.
            frame.readerIndex(0);
            deltaReplayer.accept(frame);

            prevSeq = seq;
            idx = (idx + 1) % ringBufferCapacity;
        }

        return true;
    }

    /**
     * Encodes one side (bids or asks) of the REST snapshot into ORDER_EVENT repeating-group
     * entries on the supplied encoder.
     *
     * <p>The method expects the buffer reader index to be positioned immediately after the
     * {@code "bids":} or {@code "asks":} colon, as left by
     * {@link ByteBufScanner#scanToKey(ByteBuf, byte[])}. It scans the outer array
     * {@code [[...],[...],...]} and for each inner entry parses the quoted price, size, and
     * order-id strings using allocation-free {@link ByteBufScanner} methods. Each entry is
     * written as an ORDER_EVENT with {@code reason=OPEN}, {@code action=UPSERT},
     * {@code orderType=LIMIT}, and {@code exchangeTimestamp=-1} per spec §15.6.</p>
     *
     * @param buf buffer positioned after the key colon for the current side
     * @param encoder encoder that has already received {@code beginMessage} for the snapshot
     * @param side side byte — {@link EncodingConstants#SIDE_BID} or {@link EncodingConstants#SIDE_ASK}
     */
    private void encodeRestSideEntries(ByteBuf buf, SbeEncoder encoder, byte side) {
        // Advance to the outer array opening bracket.
        skipToNextByte(buf, '[');
        if (buf.readableBytes() == 0 || buf.readByte() != '[') {
            return;
        }

        // Iterate over inner arrays: ["price","size","order_id"]
        while (buf.readableBytes() > 0) {
            // Skip whitespace and commas between inner array elements.
            skipWhitespaceAndCommas(buf);
            if (buf.readableBytes() == 0) {
                break;
            }
            byte peek = buf.getByte(buf.readerIndex());
            if (peek == ']') {
                buf.skipBytes(1); // closing bracket of outer array
                break;
            }
            if (peek != '[') {
                break; // unexpected character; stop gracefully
            }
            buf.skipBytes(1); // skip inner '['

            // Parse quoted price decimal: "333.98"
            // parseDecimal stops AT the closing quote; consume it so nextArrayElement sees ','
            ByteBufScanner.parseDecimal(buf, decimalScratch);
            skipClosingQuote(buf);
            long priceMantissa = decimalScratch[0];
            byte priceScale = (byte) decimalScratch[1];

            // Parse quoted quantity decimal: "5.72036512"
            ByteBufScanner.nextArrayElement(buf);
            ByteBufScanner.parseDecimal(buf, decimalScratch);
            skipClosingQuote(buf);
            long qtyMantissa = decimalScratch[0];
            byte qtyScale = (byte) decimalScratch[1];

            // Parse quoted order-id UUID: "da863862-25f4-4868-ac41-005d11ab0a5f"
            ByteBufScanner.nextArrayElement(buf);
            if (!ByteBufScanner.parseUuidHighLow(buf, uuidScratch)) {
                throw new IllegalArgumentException(
                        "invalid order_id UUID in Coinbase L3 REST snapshot");
            }

            // Advance past inner ']'.
            skipToNextByte(buf, ']');
            if (buf.readableBytes() > 0) {
                buf.skipBytes(1);
            }

            encoder.writeOrderEvent(
                    uuidScratch[0],
                    uuidScratch[1],
                    side,
                    EncodingConstants.ACTION_UPSERT,
                    EncodingConstants.REASON_OPEN,
                    EncodingConstants.ORDER_TYPE_LIMIT,
                    priceScale,
                    qtyScale,
                    (byte) 0,
                    EncodingConstants.NO_TIMESTAMP,
                    priceMantissa,
                    qtyMantissa,
                    0L);
        }
    }

    /**
     * Extracts the {@code sequence} field value from a Coinbase JSON frame without
     * permanently altering the frame's reader index.
     *
     * <p>The method saves the current reader index with {@link ByteBuf#markReaderIndex()},
     * seeks from the start of the buffer, reads the sequence, then restores via
     * {@link ByteBuf#resetReaderIndex()}. Malformed or missing sequence fields return
     * {@code -1}.</p>
     *
     * @param frame buffered WebSocket frame
     * @return parsed sequence value, or {@code -1} if absent or malformed
     */
    private long extractSequence(ByteBuf frame) {
        frame.markReaderIndex();
        try {
            frame.readerIndex(0);
            if (!ByteBufScanner.scanToKey(frame, SEQUENCE_KEY)) {
                return -1L;
            }
            skipWhitespace(frame);
            return ByteBufScanner.readLong(frame);
        } finally {
            frame.resetReaderIndex();
        }
    }

    /**
     * Advances the buffer reader index past all whitespace bytes (space, tab, CR, LF).
     *
     * <p>Used to consume optional whitespace between a JSON colon and its value,
     * e.g. {@code "sequence": 638} where {@link ByteBufScanner#readLong} does not
     * skip leading whitespace itself.</p>
     *
     * @param buf buffer to advance
     */
    private static void skipWhitespace(ByteBuf buf) {
        while (buf.isReadable()) {
            byte b = buf.getByte(buf.readerIndex());
            if (b == ' ' || b == '\n' || b == '\r' || b == '\t') {
                buf.skipBytes(1);
            } else {
                break;
            }
        }
    }

    /**
     * Consumes the closing quote that {@link ByteBufScanner#parseDecimal} leaves unconsumed.
     *
     * <p>{@code parseDecimal} stops its reader index AT the closing {@code "} of a quoted
     * decimal value rather than after it. This means the subsequent
     * {@link ByteBufScanner#nextArrayElement} call sees {@code "} instead of a comma and
     * fails to advance to the next element. Calling this method after {@code parseDecimal}
     * normalises the reader index so that {@code nextArrayElement} behaves as expected.</p>
     *
     * @param buf buffer whose reader index may be positioned at a closing quote
     */
    private static void skipClosingQuote(ByteBuf buf) {
        if (buf.isReadable() && buf.getByte(buf.readerIndex()) == '"') {
            buf.skipBytes(1);
        }
    }

    /**
     * Advances the buffer reader index past all whitespace and comma bytes.
     *
     * @param buf buffer to advance
     */
    private static void skipWhitespaceAndCommas(ByteBuf buf) {
        while (buf.readableBytes() > 0) {
            byte b = buf.getByte(buf.readerIndex());
            if (b == ' ' || b == '\n' || b == '\r' || b == '\t' || b == ',') {
                buf.skipBytes(1);
            } else {
                break;
            }
        }
    }

    /**
     * Advances the buffer reader index until the specified byte is at the reader index.
     *
     * @param buf buffer to advance
     * @param target the byte to stop at
     */
    private static void skipToNextByte(ByteBuf buf, char target) {
        while (buf.readableBytes() > 0 && buf.getByte(buf.readerIndex()) != (byte) target) {
            buf.skipBytes(1);
        }
    }

    /**
     * Releases all retained {@link ByteBuf} references in the ring buffer and resets
     * the write and read indices to zero.
     *
     * <p>Called from the {@code finally} block of {@link #processRestResponse} regardless
     * of success or failure, and also at the start of {@link #processRestResponse} when an
     * overflow was detected before the REST response arrived.</p>
     */
    private void releaseBufferedDeltas() {
        int idx = readIdx;
        while (idx != writeIdx) {
            ByteBuf frame = deltaRing[idx];
            if (frame != null) {
                frame.release();
                deltaRing[idx] = null;
            }
            idx = (idx + 1) % ringBufferCapacity;
        }
        writeIdx = 0;
        readIdx = 0;
    }
}
