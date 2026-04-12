package io.rueishi.marketdata.crypto.venue.coinbase.l2;

import static org.assertj.core.api.Assertions.assertThat;

import io.rueishi.marketdata.crypto.core.bootstrap.GatewayBootstrap;
import io.rueishi.marketdata.crypto.core.bootstrap.GatewayRuntime;
import io.rueishi.marketdata.crypto.core.config.EncodingConfig;
import io.rueishi.marketdata.crypto.core.config.GatewayConfig;
import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.core.config.ObservabilityConfig;
import io.rueishi.marketdata.crypto.core.config.PublisherConfig;
import io.rueishi.marketdata.crypto.core.config.TransportConfig;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.connector.Connector;
import io.rueishi.marketdata.crypto.core.encoding.EncodingConstants;
import io.rueishi.marketdata.crypto.core.publisher.InMemoryPublisher;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Bootstrap-level venue end-to-end test for Coinbase L2.
 *
 * <p>The test initializes a real {@link GatewayBootstrap} with the production
 * {@link CoinbaseL2ConnectorFactory}, temporary mapped observability files, an
 * {@link InMemoryPublisher}, and a {@link CoinbaseExchangeL2Server} endpoint.
 * It then starts the returned connector and verifies that config-to-runtime
 * wiring, WebSocket subscribe, subscription acknowledgement, parser encoding,
 * publisher capture, counters, and graceful shutdown cooperate as one system
 * path. The only external dependency is the in-process simulator.</p>
 */
class CoinbaseL2GatewayEndToEndIntegrationTest {
    private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(5);

    /**
     * Verifies bootstrap-created Coinbase L2 runtime connects to the simulator
     * and publishes decoded snapshot/update output through the configured
     * in-memory publisher.
     *
     * @param tempDir temporary directory for mapped counter and error-log files
     * @throws Exception if the simulator or runtime exchange fails
     */
    @Test
    void bootstrapRuntimeConnectsToSimulatorAndPublishesDecodedMessages(@TempDir Path tempDir) throws Exception {
        try (CoinbaseExchangeL2Server server = new CoinbaseExchangeL2Server()) {
            GatewayConfig config = validConfig(tempDir, server.uri().toString());
            GatewayRuntime runtime = new GatewayBootstrap().initialize(config);
            try {
                assertThat(runtime.publisher()).isInstanceOf(InMemoryPublisher.class);
                assertThat(runtime.connectors()).hasSize(1);
                assertThat(runtime.metricsEndpoint().boundPort()).isGreaterThan(0);

                Connector connector = runtime.connectors().get(0);
                connector.connect();
                assertThat(server.awaitSubscribe()).contains("\"product_ids\":[\"BTC-USD\"]");
                CoinbaseL2Connector coinbaseConnector = (CoinbaseL2Connector) connector;

                server.sendValidSubscriptionsAck();
                waitUntil(coinbaseConnector::livenessMonitoringActive);
                server.sendSnapshotEpochZeroTime();

                InMemoryPublisher publisher = (InMemoryPublisher) runtime.publisher();
                waitUntil(() -> publisher.totalCount() == 1);
                CoinbaseL2ParserTestSupport.DecodedMessage snapshot =
                        new CoinbaseL2ParserTestSupport.DecodedMessage(publisher.lastMessage());
                assertThat(snapshot.eventType()).isEqualTo(EncodingConstants.EVENT_TYPE_BOOK_SNAPSHOT);
                assertThat(snapshot.exchangeTimestamp()).isEqualTo(-1L);
                assertThat(snapshot.entryCount()).isEqualTo(2);

                server.sendL2UpdateBuyUpsert();
                waitUntil(() -> publisher.totalCount() == 2);
                CoinbaseL2ParserTestSupport.DecodedMessage update =
                        new CoinbaseL2ParserTestSupport.DecodedMessage(publisher.lastMessage());
                assertThat(update.eventType()).isEqualTo(EncodingConstants.EVENT_TYPE_BOOK_UPDATE);
                assertThat(update.entryCount()).isEqualTo(1);
                assertThat(update.side(0)).isEqualTo(1);
                assertThat(update.action(0)).isEqualTo(1);

                server.sendHeartbeat();
                waitUntil(() -> coinbaseConnector.instrumentCounters().heartbeatsReceived().get() == 1);
                assertThat(publisher.totalCount()).isEqualTo(2);
                assertThat(runtime.counters().activeConnections().get()).isEqualTo(1);

                runtime.shutdownGracefully();

                assertThat(runtime.shutdownDeadlineExceeded()).isFalse();
                assertThat(runtime.metricsEndpoint().boundPort()).isZero();
                assertThat(Files.exists(Path.of(config.observability.countersSharedMemoryPath + ".metadata")))
                        .isTrue();
                assertThat(Files.exists(Path.of(config.observability.countersSharedMemoryPath + ".values")))
                        .isTrue();
                assertThat(Files.exists(Path.of(config.observability.errorLogPath))).isTrue();
            } finally {
                runtime.shutdownGracefully();
            }
        }
    }

    private static GatewayConfig validConfig(Path tempDir, String endpoint) {
        GatewayConfig config = new GatewayConfig();
        config.instanceId = "gateway-coinbase-l2-e2e";
        config.environment = "test";
        config.venue = VenueEnum.COINBASE_L2;
        config.instruments = List.of(instrument("BTC-USD", 1001));
        config.transport = new TransportConfig();
        config.transport.connectTimeoutMs = 1_000;
        config.transport.reconnectBackoffBaseMs = 10;
        config.transport.reconnectBackoffMaxMs = 40;
        config.transport.reconnectBackoffJitterMs = 0;
        config.transport.heartbeatTimeoutMs = 1_000;
        config.transport.frameSizeLimitBytes = 4096;
        config.transport.shutdownDeadlineMs = 5_000;
        config.encoding = new EncodingConfig();
        config.encoding.maxLevelsPerMessage = 16;
        config.encoding.bufferHeadroomBytes = 0;
        config.observability = new ObservabilityConfig();
        config.observability.countersSharedMemoryPath = tempDir.resolve("counters").toString();
        config.observability.errorLogPath = tempDir.resolve("errors").toString();
        config.observability.metricsHttpPort = 0;
        config.publisher = new PublisherConfig();
        config.publisher.type = "IN_MEMORY";
        config.venueConfig = new LinkedHashMap<>(Map.of(
                "endpoint", endpoint,
                "apiKey", "test-key",
                "apiSecret", "c2VjcmV0",
                "passphrase", "test-passphrase"));
        return config;
    }

    private static InstrumentConfig instrument(String exchangeSymbol, int instrumentId) {
        InstrumentConfig instrument = new InstrumentConfig();
        instrument.exchangeSymbol = exchangeSymbol;
        instrument.instrumentId = instrumentId;
        return instrument;
    }

    private static void waitUntil(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + WAIT_TIMEOUT.toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }
}
