package io.rueishi.marketdata.crypto.venue.coinbase.l2;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.rueishi.marketdata.crypto.core.config.BookDepth;
import io.rueishi.marketdata.crypto.core.config.GatewayConfig;
import io.rueishi.marketdata.crypto.core.config.TemplateId;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.encoding.EncodingConstants;
import io.rueishi.marketdata.crypto.core.observability.GatewayCounters;
import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import io.rueishi.marketdata.crypto.core.publisher.InMemoryPublisher;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryReasonCode;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequest;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequestType;
import java.time.Duration;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

/**
 * End-to-end integration tests for the Coinbase L2 connector path.
 *
 * <p>The suite uses the test-only {@link CoinbaseExchangeL2Server} to emulate
 * Coinbase L2 control-plane and market-data frames while the production
 * {@link CoinbaseL2ConnectorFactory}, {@link CoinbaseL2Connector}, Netty
 * WebSocket transport, parser, recovery strategy, counters, and
 * {@link InMemoryPublisher} stay in the runtime flow. It covers the happy path,
 * Coinbase-shaped protocol violations, recovery ordering, and shutdown without
 * contacting external Coinbase infrastructure.</p>
 */
class CoinbaseL2ConnectorIntegrationTest {
    private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(5);

    /**
     * Verifies connect, authenticated subscribe, valid ack, snapshot/update
     * publication, heartbeat accounting, recovery ordering, and shutdown
     * against the Coinbase L2 simulator.
     *
     * @throws Exception if the simulator or connector exchange fails
     */
    @Test
    void coinbaseSimulatorConnectSubscribeParseRecoverAndShutdownFlow() throws Exception {
        try (CoinbaseExchangeL2Server server = new CoinbaseExchangeL2Server()) {
            Fixture fixture = new Fixture(server);
            try {
                fixture.connect();

                String subscribe = server.awaitSubscribe();
                assertThat(subscribe).contains("\"type\":\"subscribe\"");
                assertThat(subscribe).contains("\"product_ids\":[\"BTC-USD\"]");
                assertThat(subscribe).contains("\"channels\":[\"level2\",\"heartbeat\"]");
                assertThat(subscribe).contains("\"signature\"");
                assertThat(fixture.gatewayCounters.connectionAttempts().get()).isEqualTo(1);
                assertThat(fixture.gatewayCounters.connectionSuccesses().get()).isEqualTo(1);
                assertThat(fixture.gatewayCounters.activeConnections().get()).isEqualTo(1);

                server.sendValidSubscriptionsAck();
                waitUntil(fixture.connector::livenessMonitoringActive);
                assertThat(fixture.instrumentCounters().lastHeartbeatReceivedNanos().get())
                        .isEqualTo(CoinbaseL2ConnectorTestSupport.DEFAULT_NANO_TIME);
                assertThat(fixture.publisher.totalCount()).isZero();

                server.sendSnapshot();
                waitUntil(() -> fixture.publisher.totalCount() == 1);
                CoinbaseL2ParserTestSupport.DecodedMessage snapshot =
                        new CoinbaseL2ParserTestSupport.DecodedMessage(fixture.publisher.lastMessage());
                assertThat(snapshot.eventType()).isEqualTo(EncodingConstants.EVENT_TYPE_BOOK_SNAPSHOT);
                assertCommonHeader(snapshot, fixture.publisher.lastMessage(), 1);
                assertThat(snapshot.exchangeTimestamp()).isEqualTo(1_675_974_789_999_999_000L);
                assertThat(snapshot.entryCount()).isEqualTo(2);
                assertThat(snapshot.side(0)).isEqualTo(1);
                assertThat(snapshot.action(0)).isEqualTo(1);
                assertThat(snapshot.priceMantissa(0)).isEqualTo(2_192_173L);
                assertThat(snapshot.qtyMantissa(0)).isEqualTo(6_317_902L);

                server.sendL2UpdateMultiChange();
                waitUntil(() -> fixture.publisher.totalCount() == 2);
                CoinbaseL2ParserTestSupport.DecodedMessage update =
                        new CoinbaseL2ParserTestSupport.DecodedMessage(fixture.publisher.lastMessage());
                assertThat(update.eventType()).isEqualTo(EncodingConstants.EVENT_TYPE_BOOK_UPDATE);
                assertCommonHeader(update, fixture.publisher.lastMessage(), 2);
                assertThat(update.entryCount()).isEqualTo(3);
                assertThat(update.action(0)).isEqualTo(1);
                assertThat(update.action(1)).isEqualTo(2);
                assertThat(update.qtyMantissa(1)).isZero();
                assertThat(update.side(2)).isEqualTo(2);

                server.sendHeartbeat();
                waitUntil(() -> fixture.instrumentCounters().heartbeatsReceived().get() == 1);
                assertThat(fixture.publisher.totalCount()).isEqualTo(2);
                assertThat(fixture.instrumentCounters().messagesPublished().get()).isEqualTo(2);
                assertThat(fixture.instrumentCounters().bookSnapshotPublished().get()).isEqualTo(1);
                assertThat(fixture.instrumentCounters().updateMessagesReceived().get()).isEqualTo(1);
                assertThat(fixture.instrumentCounters().messagesDecoded().get()).isEqualTo(4);

                fixture.connector.recover(new RecoveryRequest(
                        VenueEnum.COINBASE_L2,
                        1001,
                        RecoveryRequestType.RESYNC,
                        RecoveryReasonCode.MANUAL_RESET,
                        CoinbaseL2ConnectorTestSupport.DEFAULT_NANO_TIME));
                assertThat(fixture.publisher.totalCount()).isEqualTo(3);
                CoinbaseL2ParserTestSupport.DecodedMessage reset =
                        new CoinbaseL2ParserTestSupport.DecodedMessage(fixture.publisher.lastMessage());
                assertThat(reset.eventType()).isEqualTo(EncodingConstants.EVENT_TYPE_BOOK_RESET);
                assertThat(server.awaitUnsubscribe()).contains("\"product_ids\":[\"BTC-USD\"]");
                assertThat(server.awaitSubscribe()).contains("\"type\":\"subscribe\"");
                assertThat(fixture.instrumentCounters().recoveryExecutions().get()).isEqualTo(1);
                assertThat(fixture.gatewayCounters.reconnectAttempts().get()).isEqualTo(1);
                assertThat(fixture.instrumentCounters().recoveryCompletions().get()).isZero();

                server.sendL2UpdateBeforeSnapshot();
                Thread.sleep(100L);
                assertThat(fixture.publisher.totalCount()).isEqualTo(3);
                assertThat(fixture.instrumentCounters().preSnapshotDrops().get()).isEqualTo(1);

                server.sendRecoverySnapshot();
                waitUntil(() -> fixture.publisher.totalCount() == 4);
                CoinbaseL2ParserTestSupport.DecodedMessage recoverySnapshot =
                        new CoinbaseL2ParserTestSupport.DecodedMessage(fixture.publisher.lastMessage());
                assertThat(recoverySnapshot.eventType()).isEqualTo(EncodingConstants.EVENT_TYPE_BOOK_SNAPSHOT);
                assertThat(fixture.instrumentCounters().recoveryCompletions().get()).isEqualTo(1);

                server.sendRecoveryL2Update();
                waitUntil(() -> fixture.publisher.totalCount() == 5);
                CoinbaseL2ParserTestSupport.DecodedMessage recoveryUpdate =
                        new CoinbaseL2ParserTestSupport.DecodedMessage(fixture.publisher.lastMessage());
                assertThat(recoveryUpdate.eventType()).isEqualTo(EncodingConstants.EVENT_TYPE_BOOK_UPDATE);

                fixture.connector.shutdown();
                waitUntil(() -> fixture.gatewayCounters.activeConnections().get() == 0);
            } finally {
                fixture.close();
            }
        }
    }

    /**
     * Verifies Coinbase-shaped reject/drop fixtures are counted without
     * publishing book messages, and a later valid snapshot without time still
     * opens the session with the timestamp sentinel.
     *
     * @throws Exception if the simulator or connector exchange fails
     */
    @Test
    void coinbaseSimulatorProtocolViolationsAreCountedWithoutPublishing() throws Exception {
        try (CoinbaseExchangeL2Server server = new CoinbaseExchangeL2Server()) {
            Fixture fixture = new Fixture(server);
            try {
                fixture.connect();
                server.awaitSubscribe();
                server.sendSubscriptionsAckWithExtraChannel();
                waitUntil(fixture.connector::livenessMonitoringActive);

                server.sendL2UpdateBeforeSnapshot();
                server.sendSnapshotWrongProduct();
                server.sendHeartbeatWrongProduct();
                server.sendUnknownType();
                server.sendMalformed();
                server.sendBadDecimal();
                Thread.sleep(100L);

                InstrumentCounters counters = fixture.instrumentCounters();
                assertThat(fixture.publisher.totalCount()).isZero();
                assertThat(counters.preSnapshotDrops().get()).isEqualTo(1);
                assertThat(counters.productIdMismatches().get()).isEqualTo(2);
                assertThat(counters.unknownSymbolDrops().get()).isEqualTo(2);
                assertThat(counters.unknownTypeDrops().get()).isEqualTo(1);
                assertThat(counters.malformedRejections().get()).isEqualTo(2);

                server.sendSnapshotWithoutTime();
                waitUntil(() -> fixture.publisher.totalCount() == 1);
                CoinbaseL2ParserTestSupport.DecodedMessage decoded =
                        new CoinbaseL2ParserTestSupport.DecodedMessage(fixture.publisher.lastMessage());
                assertThat(decoded.eventType()).isEqualTo(EncodingConstants.EVENT_TYPE_BOOK_SNAPSHOT);
                assertThat(decoded.exchangeTimestamp()).isEqualTo(-1L);
            } finally {
                fixture.close();
            }
        }
    }

    private static void assertCommonHeader(
            CoinbaseL2ParserTestSupport.DecodedMessage decoded,
            byte[] message,
            long expectedSequence) {
        assertThat(headerByte(message, EncodingConstants.TEMPLATE_ID_OFFSET))
                .isEqualTo(TemplateId.BOOK_LEVEL.byteValue());
        assertThat(headerShort(message, EncodingConstants.BLOCK_LENGTH_OFFSET))
                .isEqualTo(EncodingConstants.BODY_BLOCK_LENGTH);
        assertThat(decoded.venue()).isEqualTo(VenueEnum.COINBASE_L2.byteValue());
        assertThat(decoded.bookDepth()).isEqualTo(BookDepth.L2.byteValue());
        assertThat(decoded.instrumentId()).isEqualTo(1001);
        assertThat(decoded.gatewayMessageSeq()).isEqualTo(expectedSequence);
        assertThat(decoded.seq1()).isEqualTo(expectedSequence);
        assertThat(decoded.seq2()).isEqualTo(expectedSequence);
        assertThat(decoded.ingressTimestamp()).isEqualTo(CoinbaseL2ConnectorTestSupport.DEFAULT_NANO_TIME);
    }

    private static int headerByte(byte[] message, int offset) {
        return Byte.toUnsignedInt(message[offset]);
    }

    private static int headerShort(byte[] message, int offset) {
        return Short.toUnsignedInt(java.nio.ByteBuffer.wrap(message)
                .order(EncodingConstants.BYTE_ORDER)
                .getShort(offset));
    }

    private static void waitUntil(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + WAIT_TIMEOUT.toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }

    /**
     * Owns connector runtime dependencies for a single simulator-backed test.
     */
    private static final class Fixture implements AutoCloseable {
        private final EventLoopGroup clientGroup = new NioEventLoopGroup(1);
        private final InMemoryPublisher publisher = new InMemoryPublisher(8, 4096);
        private final GatewayCounters gatewayCounters = CoinbaseL2ConnectorTestSupport.gatewayCounters();
        private final CoinbaseL2Connector connector;

        private Fixture(CoinbaseExchangeL2Server server) {
            GatewayConfig config = CoinbaseL2ConnectorFactoryTest.validConfig(server.uri().toString());
            connector = (CoinbaseL2Connector) new CoinbaseL2ConnectorFactory()
                    .create(CoinbaseL2ConnectorFactoryTest.instrument("BTC-USD", 1001), config);
            connector.init(CoinbaseL2ConnectorTestSupport.context(gatewayCounters, publisher, clientGroup, config));
        }

        private void connect() {
            connector.connect();
        }

        private InstrumentCounters instrumentCounters() {
            return connector.instrumentCounters();
        }

        /**
         * Closes connector, counters, and event loop resources for the test.
         */
        @Override
        public void close() {
            connector.shutdown();
            gatewayCounters.close();
            clientGroup.shutdownGracefully().syncUninterruptibly();
        }
    }
}
