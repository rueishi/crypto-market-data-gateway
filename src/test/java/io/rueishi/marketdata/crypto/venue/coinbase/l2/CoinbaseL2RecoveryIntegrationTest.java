package io.rueishi.marketdata.crypto.venue.coinbase.l2;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.rueishi.marketdata.crypto.core.config.GatewayConfig;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.encoding.EncodingConstants;
import io.rueishi.marketdata.crypto.core.observability.GatewayCounters;
import io.rueishi.marketdata.crypto.core.observability.InstrumentCounters;
import io.rueishi.marketdata.crypto.core.publisher.InMemoryPublisher;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryReasonCode;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequest;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequestType;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the Coinbase L2 recovery path.
 *
 * <p>The suite drives a real {@link CoinbaseL2Connector} through
 * {@link CoinbaseExchangeL2Server} and captures output with
 * {@link InMemoryPublisher}. It verifies the system-level recovery ordering
 * contract that spans transport reconnect, Coinbase unsubscribe/subscribe
 * control messages, parser snapshot gating, encoded reset publication, and
 * recovery counters. External Coinbase infrastructure is replaced only by the
 * deterministic test simulator.</p>
 */
class CoinbaseL2RecoveryIntegrationTest {
    private static final Path FIXTURE_DIR = Path.of("src/test/resources/fixtures/coinbase/l2");
    private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(5);

    /**
     * Verifies recovery emits reset, then fresh snapshot, then update without
     * allowing an update received before the recovery snapshot to interleave.
     *
     * @throws Exception if the simulator or fixture exchange fails
     */
    @Test
    void recoveryPublishesResetSnapshotThenUpdateWithoutInterleaving() throws Exception {
        try (CoinbaseExchangeL2Server server = new CoinbaseExchangeL2Server()) {
            Fixture fixture = new Fixture(server);
            try {
                fixture.connect();
                assertThat(server.awaitSubscribe()).contains("\"type\":\"subscribe\"");
                server.sendValidSubscriptionsAck();
                waitUntil(fixture.connector::livenessMonitoringActive);

                server.sendSnapshot();
                waitUntil(() -> fixture.publisher.totalCount() == 1);
                server.sendL2UpdateMultiChange();
                waitUntil(() -> fixture.publisher.totalCount() == 2);

                fixture.connector.recover(new RecoveryRequest(
                        VenueEnum.COINBASE_L2,
                        1001,
                        RecoveryRequestType.RESYNC,
                        RecoveryReasonCode.MANUAL_RESET,
                        CoinbaseL2ConnectorTestSupport.DEFAULT_NANO_TIME));
                waitUntil(() -> fixture.publisher.totalCount() == 3);
                assertThat(server.awaitUnsubscribe()).contains("\"type\":\"unsubscribe\"");
                assertThat(server.awaitSubscribe()).contains("\"type\":\"subscribe\"");

                server.sendRecoveryL2Update();
                Thread.sleep(100L);
                assertThat(fixture.publisher.totalCount()).isEqualTo(3);

                server.sendRecoverySnapshot();
                waitUntil(() -> fixture.publisher.totalCount() == 4);
                server.sendRecoveryL2Update();
                waitUntil(() -> fixture.publisher.totalCount() == 5);

                List<byte[]> publishedMessages = fixture.publisher.allMessages();
                CoinbaseL2ParserTestSupport.DecodedMessage reset =
                        new CoinbaseL2ParserTestSupport.DecodedMessage(publishedMessages.get(2));
                CoinbaseL2ParserTestSupport.DecodedMessage snapshot =
                        new CoinbaseL2ParserTestSupport.DecodedMessage(publishedMessages.get(3));
                CoinbaseL2ParserTestSupport.DecodedMessage update =
                        new CoinbaseL2ParserTestSupport.DecodedMessage(publishedMessages.get(4));
                assertThat(reset.eventType()).isEqualTo(EncodingConstants.EVENT_TYPE_BOOK_RESET);
                assertThat(snapshot.eventType()).isEqualTo(EncodingConstants.EVENT_TYPE_BOOK_SNAPSHOT);
                assertThat(update.eventType()).isEqualTo(EncodingConstants.EVENT_TYPE_BOOK_UPDATE);

                InstrumentCounters instrumentCounters = fixture.connector.instrumentCounters();
                assertThat(instrumentCounters.preSnapshotDrops().get()).isEqualTo(1);
                assertThat(instrumentCounters.recoveryExecutions().get()).isEqualTo(1);
                assertThat(instrumentCounters.recoveryCompletions().get()).isEqualTo(1);
            } finally {
                fixture.close();
            }
        }
    }

    /**
     * Verifies a bad Coinbase subscription acknowledgement enters the same
     * reset recovery request path used by other stream-integrity failures.
     *
     * @throws Exception if the fixture cannot be loaded
     */
    @Test
    void badSubscriptionsAcknowledgementRequestsResetRecovery() throws Exception {
        CoinbaseL2ParserTestSupport support = new CoinbaseL2ParserTestSupport();

        support.parse(fixture("subscriptions_ack_missing_heartbeat.json"));

        assertThat(support.context.subscriptionAckValidated()).isFalse();
        assertThat(support.counters.subscriptionValidationFailures().get()).isEqualTo(1);
        assertThat(support.recoverySignals)
                .singleElement()
                .satisfies(signal -> {
                    assertThat(signal.type()).isEqualTo(RecoveryRequestType.RESET);
                    assertThat(signal.reason()).isEqualTo(RecoveryReasonCode.STREAM_INTEGRITY_FAILURE);
                    assertThat(signal.diagnosticText()).contains("subscriptions");
                });
    }

    /**
     * Verifies an invalid simulator acknowledgement enters connector-owned
     * reset recovery and emits the best-effort unsubscribe control message.
     *
     * @throws Exception if the simulator exchange fails
     */
    @Test
    void badSimulatorAcknowledgementTriggersConnectorResetRecovery() throws Exception {
        try (CoinbaseExchangeL2Server server = new CoinbaseExchangeL2Server()) {
            Fixture fixture = new Fixture(server);
            try {
                fixture.connect();
                server.awaitSubscribe();

                server.sendBadSubscriptionsAckMissingHeartbeat();

                waitUntil(() -> fixture.publisher.totalCount() == 1);
                assertThat(new CoinbaseL2ParserTestSupport.DecodedMessage(fixture.publisher.lastMessage()).eventType())
                        .isEqualTo(EncodingConstants.EVENT_TYPE_BOOK_RESET);
                assertThat(server.awaitUnsubscribe()).contains("\"type\":\"unsubscribe\"");
                assertThat(fixture.connector.instrumentCounters().subscriptionValidationFailures().get()).isEqualTo(1);
                assertThat(fixture.connector.instrumentCounters().recoveryExecutions().get()).isEqualTo(1);
            } finally {
                fixture.close();
            }
        }
    }

    private static String fixture(String name) throws java.io.IOException {
        return Files.readString(FIXTURE_DIR.resolve(name), StandardCharsets.UTF_8);
    }

    private static void waitUntil(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + WAIT_TIMEOUT.toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }

    /**
     * Owns connector runtime dependencies for a single recovery integration test.
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
