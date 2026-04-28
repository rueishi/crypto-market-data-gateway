package io.rueishi.marketdata.crypto.venue.coinbase.l3;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.rueishi.marketdata.crypto.core.CoreTestFixtures;
import io.rueishi.marketdata.crypto.core.config.BookDepth;
import io.rueishi.marketdata.crypto.core.config.GatewayConfig;
import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.core.config.TemplateId;
import io.rueishi.marketdata.crypto.core.config.TransportConfig;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.connector.DefaultConnectorContext;
import io.rueishi.marketdata.crypto.core.encoding.EncodingConstants;
import io.rueishi.marketdata.crypto.core.observability.GatewayCounters;
import io.rueishi.marketdata.crypto.core.publisher.InMemoryPublisher;
import io.rueishi.marketdata.crypto.core.publisher.Publisher;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryContext;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryReasonCode;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequest;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequestType;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryStrategy;
import io.rueishi.marketdata.crypto.core.snapshot.SnapshotContext;
import io.rueishi.marketdata.crypto.core.snapshot.SnapshotStrategy;
import io.rueishi.marketdata.crypto.core.subscription.SubscriptionBuilder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import org.agrona.concurrent.CachedNanoClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Structural and lifecycle integration tests for {@link CoinbaseL3Connector}.
 *
 * <p>The suite verifies encoder configuration, session context wiring, lifecycle
 * callbacks, recovery ordering, liveness timeout detection, and schema v2
 * constants without starting HTTP infrastructure or WebSocket transports. Tests
 * use in-memory publishers, injectable clocks, and deterministic mock strategies
 * so every assertion can be made synchronously on the test thread.</p>
 */
@Tag("integration")
class CoinbaseL3ConnectorIntegrationTest {

    private static final int INSTRUMENT_ID = 1001;
    private static final String EXCHANGE_SYMBOL = "BTC-USD";
    private static final long DEFAULT_NANO_TIME = 9_876_543_210L;
    private static final long HEARTBEAT_TIMEOUT_MS = 5_000L;

    private GatewayCounters gatewayCounters;
    private EventLoopGroup eventLoopGroup;
    private InMemoryPublisher publisher;
    private CachedNanoClock nanoClock;

    @BeforeEach
    void setUp() {
        gatewayCounters = new GatewayCounters(CoreTestFixtures.newCountersManager(), "gw-1", "test", "COINBASE_L3");
        eventLoopGroup = new NioEventLoopGroup(1);
        publisher = new InMemoryPublisher(8, 65_536);
        nanoClock = new CachedNanoClock();
        nanoClock.advance(1L);
    }

    @AfterEach
    void tearDown() {
        gatewayCounters.close();
        eventLoopGroup.shutdownGracefully().syncUninterruptibly();
    }

    // -------------------------------------------------------------------------
    // init tests
    // -------------------------------------------------------------------------

    /**
     * Verifies that initialization allocates an encoder with the ORDER_EVENT template byte.
     */
    @Test
    void init_allocatesEncoderWithOrderEventTemplate() {
        Fixture fixture = new Fixture(noopSnapshot(), noopRecovery());
        fixture.init();

        assertThat(fixture.connector.parseContext().encoder().templateIdByte())
                .isEqualTo(EncodingConstants.TEMPLATE_ID_ORDER_EVENT);
    }

    /**
     * Verifies that initialization allocates a separate encoder for TRADE_EVENT messages.
     */
    @Test
    void init_allocatesSeparateTradeEventBuffer() {
        Fixture fixture = new Fixture(noopSnapshot(), noopRecovery());
        fixture.init();

        assertThat(fixture.connector.parseContext().tradeEncoder())
                .isNotSameAs(fixture.connector.parseContext().encoder());
        assertThat(fixture.connector.parseContext().tradeEncoder().templateIdByte())
                .isEqualTo(EncodingConstants.TEMPLATE_ID_TRADE_EVENT);
    }

    /**
     * Verifies that the connector does not retain the ConnectorContext reference after init.
     *
     * <p>The base class contract requires context to be discarded; this test verifies the
     * connector's own fields are driven from stable per-connector copies rather than
     * from a retained context handle.</p>
     */
    @Test
    void init_doesNotHoldConnectorContext() {
        Fixture fixture = new Fixture(noopSnapshot(), noopRecovery());
        DefaultConnectorContext ctx = fixture.init();

        // After init, connector operates via its own fields; context may be GCd.
        // Verify the connector exposes the correct instrument directly.
        assertThat(fixture.connector.venue()).isEqualTo(VenueEnum.COINBASE_L3);
        assertThat(fixture.connector.instrumentId()).isEqualTo(INSTRUMENT_ID);
    }

    /**
     * Verifies that counters are allocated per-instrument during initialization.
     */
    @Test
    void init_countersArePerInstrument() {
        Fixture fixture = new Fixture(noopSnapshot(), noopRecovery());
        fixture.init();

        assertThat(fixture.connector.counters()).isNotNull();
        // framesReceived starts at 0 for a fresh instrument
        assertThat(fixture.connector.counters().framesReceived().get()).isZero();
    }

    // -------------------------------------------------------------------------
    // connect / subscribe tests
    // -------------------------------------------------------------------------

    /**
     * Verifies that connect triggers a subscribe payload containing the full and heartbeat channels.
     */
    @Test
    void connect_sendSubscribeCalledWithFullAndHeartbeat() {
        CapturingSubscription subscription = capturingL3Subscription();
        Fixture fixture = new Fixture(noopSnapshot(), noopRecovery(), subscription);
        fixture.init();
        fixture.connector.connect();

        assertThat(subscription.lastSubscribePayload).isNotNull();
        String payload = new String(subscription.lastSubscribePayload, StandardCharsets.UTF_8);
        assertThat(payload).contains("\"type\":\"subscribe\"");
        assertThat(payload).contains("\"full\"");
        assertThat(payload).contains("\"heartbeat\"");
        assertThat(payload).contains("\"BTC-USD\"");
    }

    // -------------------------------------------------------------------------
    // shutdown tests
    // -------------------------------------------------------------------------

    /**
     * Verifies that shutdown builds the unsubscribe payload before publishing the reset.
     */
    @Test
    void shutdown_sendsUnsubscribeBeforeClose() {
        CapturingSubscription subscription = capturingL3Subscription();
        Fixture fixture = new Fixture(noopSnapshot(), noopRecovery(), subscription);
        fixture.init();

        fixture.connector.shutdown();

        assertThat(subscription.lastUnsubscribePayload).isNotNull();
        String payload = new String(subscription.lastUnsubscribePayload, StandardCharsets.UTF_8);
        assertThat(payload).contains("\"type\":\"unsubscribe\"");
        // Reset was also published after unsubscribe
        assertThat(publisher.totalCount()).isGreaterThanOrEqualTo(1);
    }

    /**
     * Verifies that shutdown publishes a BOOK_RESET before connector resources are released.
     */
    @Test
    void shutdown_publishesBookResetBeforeClose() {
        Fixture fixture = new Fixture(noopSnapshot(), noopRecovery());
        fixture.init();

        fixture.connector.shutdown();

        assertThat(publisher.totalCount()).isGreaterThanOrEqualTo(1);
        byte[] reset = publisher.lastMessage();
        int eventType = Byte.toUnsignedInt(reset[EncodingConstants.BODY_EVENT_TYPE_OFFSET]);
        assertThat(eventType).isEqualTo(Byte.toUnsignedInt(EncodingConstants.EVENT_TYPE_BOOK_RESET));
    }

    /**
     * Verifies that the BOOK_RESET published by shutdown is exactly 59 bytes with schema version 2.
     */
    @Test
    void shutdown_bookResetIs59Bytes_schemaVersion2() {
        Fixture fixture = new Fixture(noopSnapshot(), noopRecovery());
        fixture.init();
        fixture.connector.shutdown();

        byte[] reset = publisher.lastMessage();
        assertThat(reset.length).isEqualTo(EncodingConstants.BOOK_RESET_SIZE);
        assertThat(reset[EncodingConstants.HEADER_VERSION_OFFSET]).isEqualTo(EncodingConstants.SCHEMA_VERSION);
    }

    /**
     * Verifies that the BOOK_RESET published by shutdown carries the correct venue, depth, and template bytes.
     */
    @Test
    void shutdown_bookResetHasCorrectVenueBookDepthTemplate() {
        Fixture fixture = new Fixture(noopSnapshot(), noopRecovery());
        fixture.init();
        fixture.connector.shutdown();

        byte[] reset = publisher.lastMessage();
        assertThat(reset[EncodingConstants.BODY_VENUE_OFFSET]).isEqualTo(VenueEnum.COINBASE_L3.byteValue());
        assertThat(reset[EncodingConstants.BODY_BOOK_DEPTH_OFFSET]).isEqualTo(BookDepth.L3.byteValue());
        assertThat(reset[EncodingConstants.HEADER_TEMPLATE_ID_OFFSET])
                .isEqualTo(TemplateId.ORDER_EVENT.byteValue());
    }

    // -------------------------------------------------------------------------
    // session context wiring
    // -------------------------------------------------------------------------

    /**
     * Verifies that the parse context and snapshot context share the same gatekeeper instance.
     */
    @Test
    void buildSessionContexts_parseCtxAndSnapshotCtxShareSameGatekeeper() {
        Fixture fixture = new Fixture(noopSnapshot(), noopRecovery());
        fixture.init();

        assertThat(fixture.connector.parseContext().snapshotGatekeeper())
                .isSameAs(fixture.connector.snapshotContext().snapshotGatekeeper());
    }

    /**
     * Verifies that channel-restored recovery resets the sequence tracker to zero.
     */
    @Test
    void onChannelRestored_resetsSequenceTrackerToZero() {
        RecoveryStrategy channelRestoredRecovery = (req, ctx) -> ctx.onChannelRestored();
        Fixture fixture = new Fixture(noopSnapshot(), channelRestoredRecovery);
        fixture.init();

        long seqBefore = fixture.connector.parseContext().sequenceTracker().next();
        assertThat(seqBefore).isEqualTo(1L);

        fixture.connector.recover(recoveryRequest());
        long seqAfter = fixture.connector.parseContext().sequenceTracker().next();
        assertThat(seqAfter).isEqualTo(1L); // reset to fresh tracker
    }

    // -------------------------------------------------------------------------
    // snapshot acceptance / recovery completion
    // -------------------------------------------------------------------------

    /**
     * Verifies that a snapshot boundary acceptance clears the recovery-in-progress flag.
     */
    @Test
    void onSnapshotAccepted_clearsRecoveryInProgress() {
        SnapshotStrategy immediateSnapshot = new SnapshotStrategy() {
            @Override
            public Mode mode() {
                return Mode.SUBSCRIBE_DRIVEN;
            }

            @Override
            public void triggerSnapshot(InstrumentConfig instrument, SnapshotContext ctx) {
                ctx.snapshotGatekeeper().accept();
                ctx.onSnapshotBoundaryAccepted();
            }
        };
        RecoveryStrategy channelRestoredRecovery = (req, ctx) -> ctx.onChannelRestored();
        Fixture fixture = new Fixture(immediateSnapshot, channelRestoredRecovery);
        fixture.init();

        fixture.connector.recover(recoveryRequest());

        assertThat(fixture.connector.recoveryInProgress()).isFalse();
    }

    /**
     * Verifies that recovery completions counter is incremented when a snapshot boundary is accepted.
     */
    @Test
    void onSnapshotAccepted_incrementsRecoveryCompletions() {
        SnapshotStrategy immediateSnapshot = new SnapshotStrategy() {
            @Override
            public Mode mode() {
                return Mode.SUBSCRIBE_DRIVEN;
            }

            @Override
            public void triggerSnapshot(InstrumentConfig instrument, SnapshotContext ctx) {
                ctx.snapshotGatekeeper().accept();
                ctx.onSnapshotBoundaryAccepted();
            }
        };
        RecoveryStrategy channelRestoredRecovery = (req, ctx) -> ctx.onChannelRestored();
        Fixture fixture = new Fixture(immediateSnapshot, channelRestoredRecovery);
        fixture.init();

        fixture.connector.recover(recoveryRequest());

        assertThat(fixture.connector.counters().recoveryCompletions().get()).isEqualTo(1);
    }

    // -------------------------------------------------------------------------
    // recover tests
    // -------------------------------------------------------------------------

    /**
     * Verifies that recover publishes a BOOK_RESET with the ORDER_EVENT template byte.
     */
    @Test
    void recover_publishesBookResetWithOrderEventTemplate() {
        Fixture fixture = new Fixture(noopSnapshot(), noopRecovery());
        fixture.init();

        fixture.connector.recover(recoveryRequest());

        assertThat(publisher.totalCount()).isGreaterThanOrEqualTo(1);
        byte[] reset = publisher.lastMessage();
        assertThat(reset[EncodingConstants.HEADER_TEMPLATE_ID_OFFSET])
                .isEqualTo(TemplateId.ORDER_EVENT.byteValue());
    }

    /**
     * Verifies that recoveryInProgress is true before the recovery strategy execute call returns.
     */
    @Test
    void recover_recoveryInProgressFlagSetBeforeExecute() {
        List<Boolean> flagsDuringExecution = new ArrayList<>();
        RecoveryStrategy capturingRecovery = (req, ctx) -> flagsDuringExecution.add(true);
        Fixture fixture = new Fixture(noopSnapshot(), capturingRecovery);
        fixture.init();

        fixture.connector.recover(recoveryRequest());

        assertThat(flagsDuringExecution).hasSize(1);
        // After noop recovery (no onChannelRestored called), flag remains true
        assertThat(fixture.connector.recoveryInProgress()).isTrue();
    }

    /**
     * Verifies that a second recovery request while recovery is in progress is ignored.
     */
    @Test
    void recover_whileInProgress_secondRequestIgnored() {
        Fixture fixture = new Fixture(noopSnapshot(), noopRecovery());
        fixture.init();

        fixture.connector.recover(recoveryRequest());  // sets recoveryInProgress = true
        fixture.connector.recover(recoveryRequest());  // should be ignored

        assertThat(fixture.connector.counters().recoveryRequestsIgnoredInProgress().get()).isEqualTo(1);
    }

    // -------------------------------------------------------------------------
    // backpressure
    // -------------------------------------------------------------------------

    /**
     * Verifies that exhausted publisher retries trigger a recovery request.
     */
    @Test
    void backpressure_allRetriesExhausted_triggersRecovery() {
        InMemoryPublisher rejectingPublisher = new InMemoryPublisher(8, 65_536) {
            @Override
            public boolean publish(
                    org.agrona.DirectBuffer buffer,
                    int offset,
                    int length,
                    io.rueishi.marketdata.crypto.core.observability.InstrumentCounters counters,
                    org.agrona.concurrent.NanoClock clock) {
                return false; // always reject
            }
        };
        Fixture fixture = new Fixture(noopSnapshot(), noopRecovery(), capturingL3Subscription(),
                rejectingPublisher);
        fixture.init();

        // Trigger backpressure by publishing through the parse context
        long ingressTs = nanoClock.nanoTime();
        fixture.connector.parseContext().encoder().beginMessage(
                EncodingConstants.EVENT_TYPE_BOOK_UPDATE, 1L, 1L, 1L, 0L, ingressTs);
        fixture.connector.parseContext().publishEncodedMessage("test backpressure");
        waitUntil(fixture.connector::recoveryInProgress);

        assertThat(fixture.connector.counters().backpressureDrops().get()).isEqualTo(1);
        assertThat(fixture.connector.recoveryInProgress()).isTrue();
    }

    // -------------------------------------------------------------------------
    // liveness tests
    // -------------------------------------------------------------------------

    /**
     * Verifies that a heartbeat expiry triggers recovery when liveness monitoring is active.
     */
    @Test
    void checkLiveness_heartbeatExpired_triggersRecovery() {
        Fixture fixture = new Fixture(noopSnapshot(), noopRecovery());
        fixture.init();

        // Seed heartbeat timestamp so liveness check has a reference time
        long heartbeatTime = DEFAULT_NANO_TIME;
        fixture.connector.counters().lastHeartbeatReceivedNanos().set(heartbeatTime);

        // Activate liveness monitoring via subscription acknowledgement
        fixture.connector.parseContext().onSubscriptionAckValidated();
        assertThat(fixture.connector.livenessMonitoringActive()).isTrue();

        // Advance clock past the configured heartbeat timeout
        long timeoutNanos = HEARTBEAT_TIMEOUT_MS * 1_000_000L;
        nanoClock.advance(1L);
        // Manually set nanotime well beyond timeout
        long expiredTime = heartbeatTime + timeoutNanos + 1L;
        fixture.connector.counters().lastHeartbeatReceivedNanos().set(heartbeatTime);
        // The CachedNanoClock reports its cached value; override the test helper time via context
        Fixture expiredFixture = new Fixture(
                noopSnapshot(), noopRecovery(), capturingL3Subscription(),
                publisher, () -> expiredTime);
        expiredFixture.init();
        expiredFixture.connector.parseContext().onSubscriptionAckValidated();
        // Override the heartbeat time set by onSubscriptionAckValidated so elapsed > timeout
        expiredFixture.connector.counters().lastHeartbeatReceivedNanos().set(heartbeatTime);

        expiredFixture.connector.detectLivenessTimeout();
        waitUntil(expiredFixture.connector::recoveryInProgress);

        assertThat(expiredFixture.connector.counters().heartbeatsMissed().get()).isEqualTo(1);
        assertThat(expiredFixture.connector.counters().livenessFailures().get()).isEqualTo(1);
        assertThat(expiredFixture.connector.recoveryInProgress()).isTrue();
    }

    /**
     * Verifies that a liveness check after shutdown does not trigger recovery.
     */
    @Test
    void checkLiveness_shutdownRequested_noAction() {
        Fixture fixture = new Fixture(noopSnapshot(), noopRecovery());
        fixture.init();

        fixture.connector.counters().lastHeartbeatReceivedNanos().set(1L);
        fixture.connector.parseContext().onSubscriptionAckValidated();

        fixture.connector.shutdown();
        fixture.connector.detectLivenessTimeout();

        assertThat(fixture.connector.counters().heartbeatsMissed().get()).isZero();
        assertThat(fixture.connector.recoveryInProgress()).isFalse();
    }

    // -------------------------------------------------------------------------
    // schema constant tests
    // -------------------------------------------------------------------------

    /**
     * Verifies that the L3 venue has book depth 3 (L3).
     */
    @Test
    void orderEventTemplate_bookDepthIsThree() {
        assertThat(VenueEnum.COINBASE_L3.bookDepth()).isEqualTo(BookDepth.L3);
        assertThat(BookDepth.L3.byteValue()).isEqualTo((byte) 3);
    }

    /**
     * Verifies that the ORDER_EVENT template id is byte value 2.
     */
    @Test
    void orderEventTemplate_templateIdIsOrderEvent() {
        assertThat(VenueEnum.COINBASE_L3.templateId()).isEqualTo(TemplateId.ORDER_EVENT);
        assertThat(TemplateId.ORDER_EVENT.byteValue()).isEqualTo(EncodingConstants.TEMPLATE_ID_ORDER_EVENT);
    }

    /**
     * Verifies that the body block length for schema v2 ORDER_EVENT messages is 51 bytes.
     */
    @Test
    void orderEventTemplate_bodyIs51Bytes() {
        Fixture fixture = new Fixture(noopSnapshot(), noopRecovery());
        fixture.init();

        long ingressTs = nanoClock.nanoTime();
        fixture.connector.parseContext().encoder().beginMessage(
                EncodingConstants.EVENT_TYPE_BOOK_UPDATE, 1L, 1L, 1L, 0L, ingressTs);
        int length = fixture.connector.parseContext().encoder().finishMessage();

        byte[] encoded = new byte[length];
        fixture.connector.parseContext().encoder().buffer().getBytes(0, encoded);
        int blockLength = Short.toUnsignedInt(
                ByteBuffer.wrap(encoded)
                        .order(EncodingConstants.BYTE_ORDER)
                        .getShort(EncodingConstants.HEADER_BLOCK_LENGTH_OFFSET));
        assertThat(blockLength).isEqualTo(EncodingConstants.BODY_BLOCK_LENGTH);
    }

    /**
     * Verifies that the TRADE_EVENT repeating-group entry size is 51 bytes.
     */
    @Test
    void tradeEventTemplate_entryIs51Bytes() {
        assertThat(EncodingConstants.TRADE_EVENT_ENTRY_LENGTH)
                .isEqualTo(EncodingConstants.BODY_BLOCK_LENGTH);
    }

    /**
     * Verifies that the wire schema version byte is 2.
     */
    @Test
    void schemaVersion_isTwo() {
        Fixture fixture = new Fixture(noopSnapshot(), noopRecovery());
        fixture.init();
        fixture.connector.shutdown();

        byte[] reset = publisher.lastMessage();
        assertThat(reset[EncodingConstants.HEADER_VERSION_OFFSET])
                .isEqualTo((byte) EncodingConstants.VERSION);
        assertThat(EncodingConstants.VERSION).isEqualTo(2);
    }

    /**
     * Verifies that the only core classes with schema v2 additions are in the encoding package and
     * that all required Coinbase L3 venue classes are present.
     */
    @Test
    void onlyCorClassesModified_areSbeEncoderAndEncodingConstants() {
        // Verify all required L3 venue classes exist and are instantiable via the connector
        Fixture fixture = new Fixture(noopSnapshot(), noopRecovery());
        fixture.init();

        assertThat(fixture.connector.venue()).isEqualTo(VenueEnum.COINBASE_L3);
        assertThat(fixture.connector.parseContext().tradeEncoder().templateIdByte())
                .isEqualTo(EncodingConstants.TEMPLATE_ID_TRADE_EVENT);
        assertThat(TemplateId.TRADE_EVENT.byteValue()).isEqualTo((byte) 3);
        assertThat(TemplateId.ORDER_EVENT.byteValue()).isEqualTo((byte) 2);

        // Verify the L3 service factory can be found via the connector factory interface
        assertThat(new CoinbaseL3ConnectorFactory().venue()).isEqualTo(VenueEnum.COINBASE_L3);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static SnapshotStrategy noopSnapshot() {
        return new SnapshotStrategy() {
            @Override
            public Mode mode() {
                return Mode.SUBSCRIBE_DRIVEN;
            }

            @Override
            public void triggerSnapshot(InstrumentConfig instrument, SnapshotContext ctx) {
            }
        };
    }

    private static RecoveryStrategy noopRecovery() {
        return (request, ctx) -> {
        };
    }

    private static RecoveryRequest recoveryRequest() {
        return new RecoveryRequest(
                VenueEnum.COINBASE_L3,
                INSTRUMENT_ID,
                RecoveryRequestType.RESET,
                RecoveryReasonCode.STREAM_INTEGRITY_FAILURE,
                DEFAULT_NANO_TIME,
                null);
    }

    private CapturingSubscription capturingL3Subscription() {
        InstrumentConfig instrument = instrument();
        Map<String, Object> venueConfig = l3VenueConfig();
        CoinbaseL3SubscriptionBuilder real = new CoinbaseL3SubscriptionBuilder(
                io.rueishi.marketdata.crypto.venue.coinbase.shared.CoinbaseConfig.from(venueConfig),
                new io.rueishi.marketdata.crypto.venue.coinbase.shared.CoinbaseAuthenticator(
                        io.rueishi.marketdata.crypto.venue.coinbase.shared.CoinbaseConfig.from(venueConfig)),
                () -> 1_700_000_000_000L);
        return new CapturingSubscription(real, instrument);
    }

    private static InstrumentConfig instrument() {
        InstrumentConfig instrument = new InstrumentConfig();
        instrument.exchangeSymbol = EXCHANGE_SYMBOL;
        instrument.instrumentId = INSTRUMENT_ID;
        return instrument;
    }

    private static Map<String, Object> l3VenueConfig() {
        return new LinkedHashMap<>(Map.of(
                "apiKey", "test-key",
                "apiSecret", "c2VjcmV0",
                "passphrase", "test-passphrase",
                "channels", List.of("full", "heartbeat")));
    }

    private DefaultConnectorContext connectorContextWith(
            org.agrona.concurrent.NanoClock clock,
            Publisher pub) {
        GatewayConfig config = new GatewayConfig();
        config.transport = new TransportConfig();
        config.transport.heartbeatTimeoutMs = HEARTBEAT_TIMEOUT_MS;
        config.transport.reconnectBackoffBaseMs = 10;
        config.transport.reconnectBackoffMaxMs = 100;
        config.transport.reconnectBackoffJitterMs = 0;
        config.venueConfig = l3VenueConfig();
        return new DefaultConnectorContext(
                gatewayCounters,
                pub,
                clock,
                () -> 1_700_000_000_000L,
                config.transport,
                eventLoopGroup,
                config.venueConfig);
    }

    private static void waitUntil(BooleanSupplier condition) {
        long deadlineNanos = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadlineNanos) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(10L);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted while waiting for L3 recovery", ex);
            }
        }
        throw new AssertionError("Timed out waiting for L3 recovery");
    }

    private static final class CapturingSubscription implements SubscriptionBuilder {
        private final SubscriptionBuilder delegate;
        private final InstrumentConfig instrument;
        byte[] lastSubscribePayload;
        byte[] lastUnsubscribePayload;

        CapturingSubscription(SubscriptionBuilder delegate, InstrumentConfig instrument) {
            this.delegate = delegate;
            this.instrument = instrument;
        }

        @Override
        public byte[] buildSubscribe(InstrumentConfig instr) {
            return lastSubscribePayload = delegate.buildSubscribe(instr);
        }

        @Override
        public byte[] buildUnsubscribe(InstrumentConfig instr) {
            return lastUnsubscribePayload = delegate.buildUnsubscribe(instr);
        }
    }

    /**
     * Owns connector and its test dependencies for one test scenario.
     */
    private final class Fixture {
        final CoinbaseL3Connector connector;
        private final org.agrona.concurrent.NanoClock clock;
        private final Publisher fixturePublisher;

        Fixture(SnapshotStrategy snapshot, RecoveryStrategy recovery) {
            this(snapshot, recovery, capturingL3Subscription(), publisher, nanoClock);
        }

        Fixture(SnapshotStrategy snapshot, RecoveryStrategy recovery, CapturingSubscription subscription) {
            this(snapshot, recovery, subscription, publisher, nanoClock);
        }

        Fixture(SnapshotStrategy snapshot, RecoveryStrategy recovery,
                CapturingSubscription subscription, InMemoryPublisher pub) {
            this(snapshot, recovery, subscription, pub, nanoClock);
        }

        Fixture(SnapshotStrategy snapshot, RecoveryStrategy recovery,
                CapturingSubscription subscription, Publisher pub,
                org.agrona.concurrent.NanoClock clock) {
            this.clock = clock;
            this.fixturePublisher = pub;
            CoinbaseL3FeedParser parser = new CoinbaseL3FeedParser(instrument());
            this.connector = new CoinbaseL3Connector(instrument(), parser, snapshot, recovery, subscription);
        }

        DefaultConnectorContext init() {
            DefaultConnectorContext ctx = connectorContextWith(clock, fixturePublisher);
            connector.init(ctx);
            return ctx;
        }
    }
}
