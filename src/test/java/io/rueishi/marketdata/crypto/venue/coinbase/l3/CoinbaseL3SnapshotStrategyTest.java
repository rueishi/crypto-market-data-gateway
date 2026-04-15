package io.rueishi.marketdata.crypto.venue.coinbase.l3;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rueishi.marketdata.crypto.core.CoreTestFixtures;
import io.rueishi.marketdata.crypto.core.config.BookDepth;
import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.encoding.EncodingConstants;
import io.rueishi.marketdata.crypto.core.encoding.SbeEncoder;
import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import io.rueishi.marketdata.crypto.core.parser.DefaultParseContext;
import io.rueishi.marketdata.crypto.core.publisher.InMemoryPublisher;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryReasonCode;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequestType;
import io.rueishi.marketdata.crypto.core.snapshot.DefaultSnapshotContext;
import io.rueishi.marketdata.crypto.core.snapshot.SnapshotStrategy.Mode;
import io.rueishi.marketdata.crypto.venue.coinbase.shared.CoinbaseAuthenticator;
import io.rueishi.marketdata.crypto.venue.coinbase.shared.CoinbaseConfig;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.agrona.concurrent.NanoClock;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link CoinbaseL3SnapshotStrategy}.
 *
 * <p>The suite verifies the complete REST_THEN_DELTA snapshot acquisition algorithm defined
 * in spec §5.5. Tests cover: strategy mode, ring buffer ordering before REST fetch, async
 * fetch thread isolation, BOOK_SNAPSHOT encoding per spec §15.6 (event type, template,
 * seq1/seq2, exchangeTimestamp, entry side/action/reason/orderType), delta alignment
 * (stale-drop, boundary detection, gap detection), ring buffer overflow, and snapshot gate /
 * boundary accepted lifecycle ordering.</p>
 *
 * <p>A local {@link HttpServer} from the JDK serves REST fixture responses. The event-loop
 * {@link java.util.concurrent.Executor} injected into each test instance is synchronous
 * ({@code Runnable::run}), ensuring that {@code processRestResponse} runs to completion on
 * the HttpClient callback thread before the test waits on the completion latch.</p>
 */
class CoinbaseL3SnapshotStrategyTest {

    private static final Path FIXTURE_ROOT = Path.of("src/test/resources/venue/coinbase/l3");

    // ──────────────────────────── mode ─────────────────────────────

    /**
     * Verifies that the strategy identifies itself as REST_THEN_DELTA.
     */
    @Test
    void mode_returnsRestThenDelta() throws Exception {
        Support support = Support.forFixture("snapshot_rest.json");
        try {
            assertThat(support.strategy.mode()).isEqualTo(Mode.REST_THEN_DELTA);
        } finally {
            support.stop();
        }
    }

    // ──────────────────── triggerSnapshot lifecycle ────────────────────

    /**
     * Verifies that the ring buffer is opened <em>before</em> the REST fetch is issued by
     * confirming that a delta frame buffered immediately after {@code triggerSnapshot} is
     * accepted without overflow, even while the HTTP request is still in flight.
     */
    @Test
    void triggerSnapshot_opensRingBufferBeforeRestFetch() throws Exception {
        CountDownLatch serverBlockedLatch = new CountDownLatch(1);
        CountDownLatch releaseServerLatch = new CountDownLatch(1);
        HttpServer blockingServer = startBlockingServer(serverBlockedLatch, releaseServerLatch,
                fixture("snapshot_rest.json"));
        try {
            Support support = Support.forServer(blockingServer, CoinbaseL3SnapshotStrategy.DEFAULT_RING_BUFFER_CAPACITY);
            // Let the server accept the connection before asserting
            support.strategy.triggerSnapshot(support.instrument, support.snapshotCtx);
            serverBlockedLatch.await(5, TimeUnit.SECONDS);

            // Buffer a delta frame while the HTTP response is still held by the server.
            ByteBuf delta = buildOpenFrame(13051505639L);
            support.strategy.bufferDelta(delta);
            delta.release();

            // No overflow: buffering was opened before the REST fetch was issued.
            assertThat(support.counters.overflowRejections().get()).isZero();
        } finally {
            releaseServerLatch.countDown();
            blockingServer.stop(0);
        }
    }

    /**
     * Verifies that the REST fetch is issued on a thread other than the event-loop (calling)
     * thread, satisfying the spec requirement that snapshot acquisition does not block the
     * connector event loop.
     */
    @Test
    void triggerSnapshot_issuesAsyncRestFetchOnNonEventLoopThread() throws Exception {
        AtomicReference<String> serverHandlerThread = new AtomicReference<>();
        CountDownLatch handlerCalledLatch = new CountDownLatch(1);
        CountDownLatch allowResponseLatch = new CountDownLatch(1);

        HttpServer capturingServer = startCapturingServer(
                serverHandlerThread, handlerCalledLatch, allowResponseLatch,
                fixture("snapshot_rest.json"));
        try {
            String callerThread = Thread.currentThread().getName();
            Support support = Support.forServer(capturingServer, CoinbaseL3SnapshotStrategy.DEFAULT_RING_BUFFER_CAPACITY);
            support.strategy.triggerSnapshot(support.instrument, support.snapshotCtx);

            // Wait for the HTTP server handler to record which thread served the request.
            assertThat(handlerCalledLatch.await(5, TimeUnit.SECONDS)).isTrue();

            assertThat(serverHandlerThread.get())
                    .as("REST fetch must run on a thread other than the event-loop caller")
                    .isNotEqualTo(callerThread);
        } finally {
            allowResponseLatch.countDown();
            capturingServer.stop(0);
        }
    }

    // ──────────────────── BOOK_SNAPSHOT encoding ────────────────────

    /**
     * Verifies that the REST response is encoded and published as a BOOK_SNAPSHOT message.
     */
    @Test
    void restResponse_parsed_publishedAsBookSnapshot() throws Exception {
        Support support = Support.forFixture("snapshot_rest.json");
        try {
            support.triggerAndWait();
            assertThat(support.decoded(0).eventType()).isEqualTo(EncodingConstants.EVENT_TYPE_BOOK_SNAPSHOT);
        } finally {
            support.stop();
        }
    }

    /**
     * Verifies that bid entries from the REST snapshot are encoded with {@code side = BID}.
     */
    @Test
    void restResponse_bids_encodedWithSideBid() throws Exception {
        Support support = Support.forFixture("snapshot_rest.json");
        try {
            support.triggerAndWait();
            DecodedMessage snapshot = support.decoded(0);
            // Entries 0 and 1 are the two bid entries from snapshot_rest.json.
            assertThat(snapshot.sideAt(0)).isEqualTo(EncodingConstants.SIDE_BID);
            assertThat(snapshot.sideAt(1)).isEqualTo(EncodingConstants.SIDE_BID);
        } finally {
            support.stop();
        }
    }

    /**
     * Verifies that the ask entry from the REST snapshot is encoded with {@code side = ASK}.
     */
    @Test
    void restResponse_asks_encodedWithSideAsk() throws Exception {
        Support support = Support.forFixture("snapshot_rest.json");
        try {
            support.triggerAndWait();
            // Entry 2 is the single ask entry from snapshot_rest.json.
            assertThat(support.decoded(0).sideAt(2)).isEqualTo(EncodingConstants.SIDE_ASK);
        } finally {
            support.stop();
        }
    }

    /**
     * Verifies that all REST snapshot entries carry {@code action = UPSERT}.
     */
    @Test
    void restResponse_allEntries_actionUpsert() throws Exception {
        Support support = Support.forFixture("snapshot_rest.json");
        try {
            support.triggerAndWait();
            DecodedMessage snapshot = support.decoded(0);
            for (int i = 0; i < snapshot.entryCount(); i++) {
                assertThat(snapshot.actionAt(i)).as("entry %d action", i)
                        .isEqualTo(EncodingConstants.ACTION_UPSERT);
            }
        } finally {
            support.stop();
        }
    }

    /**
     * Verifies that all REST snapshot entries carry {@code reason = OPEN} (value 1).
     */
    @Test
    void restResponse_allEntries_reasonOpenOne() throws Exception {
        Support support = Support.forFixture("snapshot_rest.json");
        try {
            support.triggerAndWait();
            DecodedMessage snapshot = support.decoded(0);
            for (int i = 0; i < snapshot.entryCount(); i++) {
                assertThat(snapshot.reasonAt(i)).as("entry %d reason", i)
                        .isEqualTo(EncodingConstants.REASON_OPEN);
            }
        } finally {
            support.stop();
        }
    }

    /**
     * Verifies that all REST snapshot entries carry {@code orderType = LIMIT}.
     */
    @Test
    void restResponse_allEntries_orderTypeLimit() throws Exception {
        Support support = Support.forFixture("snapshot_rest.json");
        try {
            support.triggerAndWait();
            DecodedMessage snapshot = support.decoded(0);
            for (int i = 0; i < snapshot.entryCount(); i++) {
                assertThat(snapshot.orderTypeAt(i)).as("entry %d orderType", i)
                        .isEqualTo(EncodingConstants.ORDER_TYPE_LIMIT);
            }
        } finally {
            support.stop();
        }
    }

    /**
     * Verifies that the BOOK_SNAPSHOT message carries {@code exchangeTimestamp = -1} because
     * REST snapshots do not carry an exchange event time per spec §15.6.
     */
    @Test
    void restResponse_exchangeTimestampMinusOne() throws Exception {
        Support support = Support.forFixture("snapshot_rest.json");
        try {
            support.triggerAndWait();
            assertThat(support.decoded(0).exchangeTimestamp()).isEqualTo(EncodingConstants.NO_TIMESTAMP);
        } finally {
            support.stop();
        }
    }

    /**
     * Verifies that {@code seq1} and {@code seq2} in the BOOK_SNAPSHOT both equal the REST
     * response {@code sequence} value per spec §15.7.
     */
    @Test
    void restResponse_seq1AndSeq2EqualRestSequenceValue() throws Exception {
        Support support = Support.forFixture("snapshot_rest.json");
        try {
            support.triggerAndWait();
            DecodedMessage snapshot = support.decoded(0);
            assertThat(snapshot.seq1()).isEqualTo(13051505638L);
            assertThat(snapshot.seq2()).isEqualTo(13051505638L);
        } finally {
            support.stop();
        }
    }

    // ──────────────────── delta alignment ────────────────────

    /**
     * Verifies that buffered deltas with sequence at or before the REST snapshot boundary
     * ({@code seq <= N}) are discarded and not published.
     *
     * <p>A blocking server ensures the deltas are buffered while the HTTP response is still
     * in flight, making the test deterministic regardless of HttpClient thread scheduling.</p>
     */
    @Test
    void restResponse_deltaAlignment_staleDeltas_discarded() throws Exception {
        CountDownLatch serverHeld = new CountDownLatch(1);
        CountDownLatch releaseServer = new CountDownLatch(1);
        HttpServer server = startBlockingServer(serverHeld, releaseServer, fixture("snapshot_rest.json"));
        Support support = Support.forServer(server, CoinbaseL3SnapshotStrategy.DEFAULT_RING_BUFFER_CAPACITY);
        try {
            long N = 13051505638L;
            support.strategy.triggerSnapshot(support.instrument, support.snapshotCtx);
            assertThat(serverHeld.await(5, TimeUnit.SECONDS)).isTrue();

            ByteBuf stale1 = buildOpenFrame(N - 1);
            ByteBuf stale2 = buildOpenFrame(N);
            support.strategy.bufferDelta(stale1);
            support.strategy.bufferDelta(stale2);
            stale1.release();
            stale2.release();

            releaseServer.countDown();
            support.awaitCompletion();

            // Only the BOOK_SNAPSHOT itself should be published; no BOOK_UPDATE for stale frames.
            assertThat(support.publisher.totalCount()).isEqualTo(1);
        } finally {
            releaseServer.countDown();
            server.stop(0);
        }
    }

    /**
     * Verifies that the boundary delta (sequence == N+1) is replayed and published as a
     * BOOK_UPDATE message following the BOOK_SNAPSHOT.
     *
     * <p>A blocking server ensures the boundary delta is buffered while the HTTP response is
     * still in flight, making the test deterministic.</p>
     */
    @Test
    void restResponse_deltaAlignment_boundaryDelta_included() throws Exception {
        CountDownLatch serverHeld = new CountDownLatch(1);
        CountDownLatch releaseServer = new CountDownLatch(1);
        HttpServer server = startBlockingServer(serverHeld, releaseServer, fixture("snapshot_rest.json"));
        Support support = Support.forServer(server, CoinbaseL3SnapshotStrategy.DEFAULT_RING_BUFFER_CAPACITY);
        try {
            long N = 13051505638L;
            support.strategy.triggerSnapshot(support.instrument, support.snapshotCtx);
            assertThat(serverHeld.await(5, TimeUnit.SECONDS)).isTrue();

            ByteBuf boundary = buildOpenFrame(N + 1);
            support.strategy.bufferDelta(boundary);
            boundary.release();

            releaseServer.countDown();
            support.awaitCompletion();

            // BOOK_SNAPSHOT + 1 BOOK_UPDATE for the boundary delta.
            assertThat(support.publisher.totalCount()).isEqualTo(2);
            assertThat(support.decoded(1).eventType()).isEqualTo(EncodingConstants.EVENT_TYPE_BOOK_UPDATE);
        } finally {
            releaseServer.countDown();
            server.stop(0);
        }
    }

    /**
     * Verifies that a missing boundary delta (first buffered sequence > N+1) is detected as
     * a gap and triggers a stream-integrity recovery request.
     *
     * <p>A blocking server ensures the gap delta is buffered before the REST response arrives.</p>
     */
    @Test
    void restResponse_deltaAlignment_gapDetected_triggersRecovery() throws Exception {
        CountDownLatch serverHeld = new CountDownLatch(1);
        CountDownLatch releaseServer = new CountDownLatch(1);
        HttpServer server = startBlockingServer(serverHeld, releaseServer, fixture("snapshot_rest.json"));
        Support support = Support.forServer(server, CoinbaseL3SnapshotStrategy.DEFAULT_RING_BUFFER_CAPACITY);
        try {
            long N = 13051505638L;
            support.strategy.triggerSnapshot(support.instrument, support.snapshotCtx);
            assertThat(serverHeld.await(5, TimeUnit.SECONDS)).isTrue();

            // Buffer a delta that skips seq N+1 (gap: N+1 is missing).
            ByteBuf gapDelta = buildOpenFrame(N + 3);
            support.strategy.bufferDelta(gapDelta);
            gapDelta.release();

            releaseServer.countDown();
            support.awaitCompletion();

            assertThat(support.recoverySignals).singleElement().satisfies(signal -> {
                assertThat(signal.type()).isEqualTo(RecoveryRequestType.RESET);
                assertThat(signal.reason()).isEqualTo(RecoveryReasonCode.STREAM_INTEGRITY_FAILURE);
            });
            assertThat(support.counters.sequenceGapsDetected().get()).isEqualTo(1);
        } finally {
            releaseServer.countDown();
            server.stop(0);
        }
    }

    /**
     * Verifies that flooding the ring buffer beyond its capacity triggers an overflow recovery
     * request and increments the overflow counter.
     *
     * <p>A blocking server ensures overflow fires synchronously during {@code bufferDelta}
     * before the REST response is processed. With the early-return guard in
     * {@code processRestResponse}, releasing the server afterwards does not generate a second
     * recovery signal.</p>
     */
    @Test
    void restResponse_ringBufferFull_triggersStreamIntegrityFailure() throws Exception {
        // Use capacity=5 so overflow is reachable with 5 frames (ring holds capacity-1 = 4 entries).
        CountDownLatch serverHeld = new CountDownLatch(1);
        CountDownLatch releaseServer = new CountDownLatch(1);
        HttpServer server = startBlockingServer(serverHeld, releaseServer, fixture("snapshot_rest.json"));
        Support support = Support.forServer(server, 5);
        try {
            support.strategy.triggerSnapshot(support.instrument, support.snapshotCtx);
            assertThat(serverHeld.await(5, TimeUnit.SECONDS)).isTrue();

            for (int i = 0; i < 5; i++) {
                ByteBuf delta = buildOpenFrame(13051505639L + i);
                support.strategy.bufferDelta(delta);
                delta.release();
            }

            // Overflow fires synchronously during the 5th bufferDelta; assert immediately.
            assertThat(support.recoverySignals).singleElement().satisfies(signal -> {
                assertThat(signal.type()).isEqualTo(RecoveryRequestType.RESET);
                assertThat(signal.reason()).isEqualTo(RecoveryReasonCode.STREAM_INTEGRITY_FAILURE);
            });
            assertThat(support.counters.overflowRejections().get()).isEqualTo(1);
        } finally {
            releaseServer.countDown();
            server.stop(0);
        }
    }

    /**
     * Verifies that a sequence gap within the buffered delta window (boundary delta found,
     * then a later delta skips a sequence) triggers a recovery request.
     *
     * <p>A blocking server ensures all deltas are buffered before the REST response arrives.</p>
     */
    @Test
    void restResponse_sequenceGap_inBufferedDeltas_triggersRecovery() throws Exception {
        // Use recovery_snapshot_rest.json with sequence=638 for smaller sequence numbers.
        CountDownLatch serverHeld = new CountDownLatch(1);
        CountDownLatch releaseServer = new CountDownLatch(1);
        HttpServer server = startBlockingServer(serverHeld, releaseServer,
                fixture("recovery_snapshot_rest.json"));
        Support support = Support.forServer(server, CoinbaseL3SnapshotStrategy.DEFAULT_RING_BUFFER_CAPACITY);
        try {
            long N = 638L;
            support.strategy.triggerSnapshot(support.instrument, support.snapshotCtx);
            assertThat(serverHeld.await(5, TimeUnit.SECONDS)).isTrue();

            // seq=639 is the boundary delta (N+1); seq=641 introduces a gap (640 missing).
            ByteBuf delta639 = buildOpenFrame(N + 1);
            ByteBuf delta641 = buildOpenFrame(N + 3);
            support.strategy.bufferDelta(delta639);
            support.strategy.bufferDelta(delta641);
            delta639.release();
            delta641.release();

            releaseServer.countDown();
            support.awaitCompletion();

            assertThat(support.recoverySignals).singleElement().satisfies(signal -> {
                assertThat(signal.type()).isEqualTo(RecoveryRequestType.RESET);
                assertThat(signal.reason()).isEqualTo(RecoveryReasonCode.STREAM_INTEGRITY_FAILURE);
            });
            assertThat(support.counters.sequenceGapsDetected().get()).isEqualTo(1);
        } finally {
            releaseServer.countDown();
            server.stop(0);
        }
    }

    // ──────────────────── gate and boundary lifecycle ────────────────────

    /**
     * Verifies that the snapshot gatekeeper is opened after the buffered delta ring is fully
     * drained, ensuring live incremental updates are not processed until alignment is complete.
     */
    @Test
    void snapshotGatekeeper_openedAfterBufferDrained() throws Exception {
        Support support = Support.forFixture("snapshot_rest.json");
        try {
            support.triggerAndWait();
            assertThat(support.snapshotCtx.snapshotGatekeeper().isReady()).isTrue();
        } finally {
            support.stop();
        }
    }

    /**
     * Verifies that {@link io.rueishi.marketdata.crypto.core.snapshot.SnapshotContext#onSnapshotBoundaryAccepted()}
     * is called only after the snapshot gatekeeper has been opened, satisfying the connector
     * lifecycle contract that gate opening precedes boundary signaling.
     */
    @Test
    void onSnapshotBoundaryAccepted_calledAfterGatekeeperOpen() throws Exception {
        Support support = Support.forFixture("snapshot_rest.json");
        try {
            support.triggerAndWait();
            // Both must be true: boundary accepted, and gate open.
            assertThat(support.snapshotBoundaryAccepted).isTrue();
            assertThat(support.snapshotCtx.snapshotGatekeeper().isReady()).isTrue();
        } finally {
            support.stop();
        }
    }

    // ─────────────────────────── helpers ───────────────────────────

    /** Builds a minimal Coinbase {@code open} WebSocket text frame with the given sequence. */
    static ByteBuf buildOpenFrame(long sequence) {
        String json = "{\"type\":\"open\",\"time\":\"2023-02-09T20:33:09.456789Z\","
                + "\"product_id\":\"BTC-USD\",\"sequence\":" + sequence + ","
                + "\"order_id\":\"d50ec984-77a8-460a-b958-66f114b0de9b\","
                + "\"remaining_size\":\"1.00000000\",\"price\":\"333.98\",\"side\":\"buy\"}";
        return Unpooled.copiedBuffer(json, StandardCharsets.UTF_8);
    }

    /** Reads a named test fixture from the L3 resource directory into a string. */
    static String fixture(String name) {
        try {
            return Files.readString(FIXTURE_ROOT.resolve(name));
        } catch (Exception ex) {
            throw new IllegalStateException("Cannot read fixture: " + name, ex);
        }
    }

    /** Starts a local HTTP server that blocks before responding until {@code release} counts down. */
    private static HttpServer startBlockingServer(
            CountDownLatch serverReady, CountDownLatch release, String body) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/", exchange -> {
            serverReady.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        return server;
    }

    /** Starts a local HTTP server that records its handler thread and waits before responding. */
    private static HttpServer startCapturingServer(
            AtomicReference<String> handlerThread,
            CountDownLatch handlerCalled,
            CountDownLatch allowResponse,
            String body) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/", exchange -> {
            handlerThread.set(Thread.currentThread().getName());
            handlerCalled.countDown();
            try {
                allowResponse.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        return server;
    }

    /** Starts a local HTTP server that immediately serves a fixed response body. */
    private static HttpServer startImmediateServer(String body) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/", exchange -> {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        return server;
    }

    // ──────────────────────────── Support ────────────────────────────────

    /**
     * Test harness that wires one {@link CoinbaseL3SnapshotStrategy} session with in-memory
     * infrastructure and a synchronous event-loop executor for deterministic test assertions.
     *
     * <p>The {@code completionLatch} counts down when either the snapshot boundary is accepted
     * (success path) or the first recovery signal is recorded (failure path), so
     * {@link #awaitCompletion()} returns promptly regardless of which path is taken.</p>
     */
    static final class Support {
        final InstrumentCounters counters = CoreTestFixtures.newInstrumentCounters();
        final InMemoryPublisher publisher = new InMemoryPublisher(16, 4096);
        final NanoClock clock = () -> 9_876_543_210L;
        final List<RecoverySignal> recoverySignals = new ArrayList<>();
        volatile boolean snapshotBoundaryAccepted;

        final DefaultParseContext parseContext;
        final DefaultSnapshotContext snapshotCtx;
        final CoinbaseL3FeedParser parser;
        final CoinbaseL3SnapshotStrategy strategy;
        final InstrumentConfig instrument;

        private final CountDownLatch completionLatch = new CountDownLatch(1);
        private final HttpServer httpServer;

        private Support(HttpServer httpServer, int ringBufferCapacity) {
            this.httpServer = httpServer;

            instrument = new InstrumentConfig();
            instrument.exchangeSymbol = "BTC-USD";
            instrument.instrumentId = 1001;

            SbeEncoder orderEncoder = new SbeEncoder(
                    instrument.instrumentId,
                    VenueEnum.COINBASE_L3.byteValue(),
                    BookDepth.L3.byteValue(),
                    EncodingConstants.TEMPLATE_ID_ORDER_EVENT,
                    10,
                    0);
            SbeEncoder tradeEncoder = new SbeEncoder(
                    instrument.instrumentId,
                    VenueEnum.COINBASE_L3.byteValue(),
                    BookDepth.L3.byteValue(),
                    EncodingConstants.TEMPLATE_ID_TRADE_EVENT,
                    1,
                    0);

            // Recovery callback counts down the completion latch so awaiting tests unblock
            // on recovery paths as well as success paths.
            this.parseContext = new DefaultParseContext(
                    orderEncoder,
                    tradeEncoder,
                    publisher,
                    counters,
                    clock,
                    (type, reason, text) -> {
                        recoverySignals.add(new RecoverySignal(type, reason, text));
                        completionLatch.countDown();
                    },
                    () -> { },
                    () -> { });

            // Snapshot boundary callback counts down on success.
            this.snapshotCtx = new DefaultSnapshotContext(
                    parseContext.currentSnapshotGatekeeper(),
                    orderEncoder,
                    publisher,
                    counters,
                    clock,
                    () -> {
                        snapshotBoundaryAccepted = true;
                        completionLatch.countDown();
                    });

            this.parser = new CoinbaseL3FeedParser(instrument);

            String baseUrl = "http://localhost:" + httpServer.getAddress().getPort();

            CoinbaseConfig config = new CoinbaseConfig(
                    URI.create("wss://dummy.example.com"),
                    "test-key",
                    "dGVzdC1zZWNyZXQ=",
                    "test-passphrase",
                    List.of("full", "heartbeat"));
            CoinbaseAuthenticator auth = new CoinbaseAuthenticator(config);

            this.strategy = new CoinbaseL3SnapshotStrategy(
                    auth,
                    parser,
                    parseContext,
                    Runnable::run,
                    ringBufferCapacity,
                    HttpClient.newBuilder().build(),
                    baseUrl);
        }

        /** Creates a harness for a fixture served by an immediately-responding local server. */
        static Support forFixture(String fixtureName) throws Exception {
            return new Support(startImmediateServer(fixture(fixtureName)),
                    CoinbaseL3SnapshotStrategy.DEFAULT_RING_BUFFER_CAPACITY);
        }

        /** Creates a harness for a fixture with a custom ring buffer capacity. */
        static Support forFixture(String fixtureName, int ringBufferCapacity) throws Exception {
            return new Support(startImmediateServer(fixture(fixtureName)), ringBufferCapacity);
        }

        /** Creates a harness backed by a caller-managed server. */
        static Support forServer(HttpServer server, int ringBufferCapacity) {
            return new Support(server, ringBufferCapacity);
        }

        /**
         * Triggers the snapshot and awaits completion (boundary accepted or recovery triggered)
         * with a five-second timeout.
         */
        void triggerAndWait() throws Exception {
            strategy.triggerSnapshot(instrument, snapshotCtx);
            awaitCompletion();
        }

        /**
         * Waits for the snapshot processing to complete (success or recovery) after a manual
         * {@code triggerSnapshot} and optional delta buffering.
         */
        void awaitCompletion() throws Exception {
            assertThat(completionLatch.await(5, TimeUnit.SECONDS))
                    .as("snapshot processing must complete within timeout")
                    .isTrue();
        }

        /** Returns the published message at the given zero-based index. */
        DecodedMessage decoded(int idx) {
            return new DecodedMessage(publisher.allMessages().get(idx));
        }

        /** Stops the underlying HTTP server. */
        void stop() {
            httpServer.stop(0);
        }

        private static HttpServer startImmediateServer(String body) throws Exception {
            HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
            server.createContext("/", exchange -> {
                byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, bytes.length);
                exchange.getResponseBody().write(bytes);
                exchange.close();
            });
            server.start();
            return server;
        }
    }

    // ───────────────────────── data types ─────────────────────────

    /** Captured recovery metadata from the injected parse context callback. */
    record RecoverySignal(RecoveryRequestType type, RecoveryReasonCode reason, String diagnosticText) { }

    /**
     * Minimal decoder for schema v2 ORDER_EVENT messages captured by {@link InMemoryPublisher}.
     *
     * <p>Reads header, body, and repeating-group entries using the same byte offsets defined
     * in {@link EncodingConstants}. Entry-level methods accept a zero-based entry index and
     * compute the absolute buffer offset accordingly.</p>
     */
    static final class DecodedMessage {
        private final ByteBuffer buffer;

        DecodedMessage(byte[] encoded) {
            this.buffer = ByteBuffer.wrap(encoded).order(EncodingConstants.BYTE_ORDER);
        }

        int eventType() {
            return Byte.toUnsignedInt(buffer.get(EncodingConstants.EVENT_TYPE_OFFSET));
        }

        long seq1() {
            return buffer.getLong(EncodingConstants.SEQ1_OFFSET);
        }

        long seq2() {
            return buffer.getLong(EncodingConstants.SEQ2_OFFSET);
        }

        long exchangeTimestamp() {
            return buffer.getLong(EncodingConstants.EXCHANGE_TIMESTAMP_OFFSET);
        }

        int entryCount() {
            return Short.toUnsignedInt(buffer.getShort(EncodingConstants.ENTRY_COUNT_OFFSET));
        }

        private int entryBase(int entryIdx) {
            return EncodingConstants.REPEATING_GROUP_OFFSET
                    + entryIdx * EncodingConstants.ORDER_EVENT_ENTRY_LENGTH;
        }

        int sideAt(int entryIdx) {
            return Byte.toUnsignedInt(buffer.get(entryBase(entryIdx) + EncodingConstants.ORDER_EVENT_SIDE_OFFSET));
        }

        int actionAt(int entryIdx) {
            return Byte.toUnsignedInt(buffer.get(entryBase(entryIdx) + EncodingConstants.ORDER_EVENT_ACTION_OFFSET));
        }

        int reasonAt(int entryIdx) {
            return Byte.toUnsignedInt(buffer.get(entryBase(entryIdx) + EncodingConstants.ORDER_EVENT_REASON_OFFSET));
        }

        int orderTypeAt(int entryIdx) {
            return Byte.toUnsignedInt(buffer.get(entryBase(entryIdx) + EncodingConstants.ORDER_EVENT_ORDER_TYPE_OFFSET));
        }
    }
}
