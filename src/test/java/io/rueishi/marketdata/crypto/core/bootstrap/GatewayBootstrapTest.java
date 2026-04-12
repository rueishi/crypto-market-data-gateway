package io.rueishi.marketdata.crypto.core.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.rueishi.marketdata.crypto.core.config.EncodingConfig;
import io.rueishi.marketdata.crypto.core.config.GatewayConfig;
import io.rueishi.marketdata.crypto.core.config.InstrumentConfig;
import io.rueishi.marketdata.crypto.core.config.LoggingPublisherConfig;
import io.rueishi.marketdata.crypto.core.config.ObservabilityConfig;
import io.rueishi.marketdata.crypto.core.config.PublisherConfig;
import io.rueishi.marketdata.crypto.core.config.TransportConfig;
import io.rueishi.marketdata.crypto.core.config.VenueEnum;
import io.rueishi.marketdata.crypto.core.connector.Connector;
import io.rueishi.marketdata.crypto.core.connector.ConnectorContext;
import io.rueishi.marketdata.crypto.core.connector.ConnectorFactory;
import io.rueishi.marketdata.crypto.core.publisher.InMemoryPublisher;
import io.rueishi.marketdata.crypto.core.publisher.LoggingPublisher;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryReasonCode;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequest;
import io.rueishi.marketdata.crypto.core.recovery.RecoveryRequestType;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests for {@link GatewayBootstrap}.
 *
 * <p>The tests use real core bootstrap code, the ServiceLoader-discovered
 * Coinbase L2 venue factory, in-memory counters, and the Phase 1
 * {@link InMemoryPublisher}. Phase 3 maps observability resources into
 * per-test temporary files and starts the metrics endpoint on an OS-assigned
 * port so bootstrap exercises the same counter, error-log, and metrics runtime
 * used by production startup. No transport connection is opened by bootstrap.
 * The covered scenarios prove one connector is created and initialized per
 * instrument, invalid config fails before venue factory resolution, and missing
 * registry entries abort startup before any connector is created. Phase 2 also
 * verifies the bootstrap-owned event-loop selection that feeds the Netty
 * transport: epoll is preferred when the native provider is available and NIO
 * remains the portable fallback.</p>
 */
class GatewayBootstrapTest {

    /**
     * Verifies that bootstrap creates and initializes exactly one Coinbase L2 connector per instrument.
     */
    @Test
    void createsOneInitializedCoinbaseL2ConnectorPerInstrument(@TempDir Path tempDir) {
        try (GatewayBootstrap.Runtime runtime = new GatewayBootstrap().initialize(validConfig(tempDir))) {
            assertThat(runtime.config().venue).isEqualTo(VenueEnum.COINBASE_L2);
            assertThat(runtime.publisher()).isInstanceOf(InMemoryPublisher.class);
            assertThat(runtime.connectors()).hasSize(2);
            assertThat(runtime.eventLoopGroups()).hasSize(2);
            assertThat(runtime.observabilityRuntime().countersReader())
                    .isSameAs(runtime.observabilityRuntime().countersManager());
            assertThat(runtime.metricsEndpoint().boundPort()).isGreaterThan(0);
            assertThat(runtime.connectors())
                    .extracting(connector -> connector.getClass().getName())
                    .containsOnly("io.rueishi.marketdata.crypto.venue.coinbase.l2.CoinbaseL2Connector");
            assertThat(runtime.connectors())
                    .extracting(connector -> connector.instrumentId())
                    .containsExactly(1001, 1002);
        }
    }

    /**
     * Verifies bootstrap event-loop selection satisfies the AC-48 epoll preference with NIO fallback.
     */
    @Test
    void createsPlatformAppropriateConnectorEventLoopGroups(@TempDir Path tempDir) {
        try (GatewayBootstrap.Runtime runtime = new GatewayBootstrap().initialize(validConfig(tempDir))) {
            Class<? extends EventLoopGroup> expectedClass =
                    Epoll.isAvailable() ? EpollEventLoopGroup.class : NioEventLoopGroup.class;

            assertThat(runtime.eventLoopGroups()).hasSize(2);
            assertThat(runtime.eventLoopGroups())
                    .allSatisfy(group -> assertThat(group).isInstanceOf(expectedClass));
        }
    }

    /**
     * Verifies that config validation runs before registry resolution or connector creation.
     */
    @Test
    void invalidConfigFailsBeforeFactoryResolution(@TempDir Path tempDir) {
        GatewayConfig config = validConfig(tempDir);
        config.instruments = List.of();

        assertThatThrownBy(() -> new GatewayBootstrap().initialize(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("instruments must not be empty");
    }

    /**
     * Verifies that an unregistered configured venue aborts startup before connector creation.
     */
    @Test
    void missingVenueProviderFailsBeforeConnectorCreation(@TempDir Path tempDir) {
        GatewayConfig config = validConfig(tempDir);
        config.venue = VenueEnum.BINANCE_L2;

        assertThatThrownBy(() -> new GatewayBootstrap().initialize(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No ConnectorFactory registered for venue: BINANCE_L2");
    }

    /**
     * Verifies that the runtime exposes the downstream recovery routing entry
     * point and ignores requests that do not belong to this gateway instance.
     */
    @Test
    void runtimeRoutesRecoveryRequestsByVenueAndInstrument(@TempDir Path tempDir) {
        try (GatewayBootstrap.Runtime runtime = new GatewayBootstrap().initialize(validConfig(tempDir))) {
            assertThat(runtime.requestRecovery(recoveryRequest(VenueEnum.COINBASE_L2, 1001))).isTrue();
            assertThat(runtime.requestRecovery(recoveryRequest(VenueEnum.BINANCE_L2, 1001))).isFalse();
            assertThat(runtime.requestRecovery(recoveryRequest(VenueEnum.COINBASE_L2, 9999))).isFalse();
        }
    }

    /**
     * Verifies that bootstrap can instantiate the Log4j 2 async logging publisher.
     */
    @Test
    void createsLoggingPublisherWhenConfigured(@TempDir Path tempDir) {
        GatewayConfig config = validConfig(tempDir);
        config.publisher.type = "LOGGING";
        config.publisher.logging = new LoggingPublisherConfig();
        config.publisher.logging.outputPath = tempDir.resolve("messages.log").toString();
        config.publisher.logging.rollSizeMb = 1;

        try (GatewayBootstrap.Runtime runtime = new GatewayBootstrap().initialize(config)) {
            assertThat(runtime.publisher()).isInstanceOf(LoggingPublisher.class);
        }
    }

    /**
     * Verifies that observability mappings are released when connector
     * initialization fails, so startup leaves no mapped files pinned after a
     * partial allocation.
     *
     * @throws Exception if the test cannot delete mapped-file fixtures after failed startup
     */
    @Test
    void closesObservabilityRuntimeWhenConnectorInitializationFails(@TempDir Path tempDir) throws Exception {
        GatewayConfig config = validConfig(tempDir);
        GatewayBootstrap bootstrap = new GatewayBootstrap(new VenueRegistry(List.of(new FailingConnectorFactory())));

        assertThatThrownBy(() -> bootstrap.initialize(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("init failed");

        Files.deleteIfExists(Path.of(config.observability.countersSharedMemoryPath + ".metadata"));
        Files.deleteIfExists(Path.of(config.observability.countersSharedMemoryPath + ".values"));
        Files.deleteIfExists(Path.of(config.observability.errorLogPath));
    }

    private static GatewayConfig validConfig(Path tempDir) {
        GatewayConfig config = new GatewayConfig();
        config.instanceId = "gateway-stub-1";
        config.environment = "test";
        config.venue = VenueEnum.COINBASE_L2;
        config.instruments = List.of(instrument("BTC-USD", 1001), instrument("ETH-USD", 1002));
        config.transport = new TransportConfig();
        config.transport.connectTimeoutMs = 5000;
        config.transport.reconnectBackoffBaseMs = 500;
        config.transport.reconnectBackoffMaxMs = 30_000;
        config.transport.reconnectBackoffJitterMs = 250;
        config.transport.heartbeatTimeoutMs = 10_000;
        config.transport.frameSizeLimitBytes = 4_194_304;
        config.transport.busyWaitEnabled = false;
        config.transport.shutdownDeadlineMs = 5000;
        config.encoding = new EncodingConfig();
        config.encoding.maxLevelsPerMessage = 10_000;
        config.encoding.bufferHeadroomBytes = 8192;
        config.observability = new ObservabilityConfig();
        config.observability.countersSharedMemoryPath = tempDir.resolve("counters").toString();
        config.observability.errorLogPath = tempDir.resolve("errors").toString();
        config.observability.metricsHttpPort = 0;
        config.publisher = new PublisherConfig();
        config.publisher.type = "IN_MEMORY";
        config.venueConfig = new LinkedHashMap<>(Map.of(
                "endpoint", "ws://example.test",
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

    private static RecoveryRequest recoveryRequest(VenueEnum venue, int instrumentId) {
        return new RecoveryRequest(
                venue,
                instrumentId,
                RecoveryRequestType.RESET,
                RecoveryReasonCode.MANUAL_RESET,
                123L);
    }

    /**
     * Connector factory that lets bootstrap allocate observability resources
     * and then fails during connector initialization.
     */
    private static final class FailingConnectorFactory implements ConnectorFactory {
        @Override
        public VenueEnum venue() {
            return VenueEnum.COINBASE_L2;
        }

        @Override
        public Connector create(InstrumentConfig instrument, GatewayConfig config) {
            return new FailingConnector(instrument.instrumentId);
        }
    }

    /**
     * Minimal connector used to verify failed-startup cleanup after init errors.
     */
    private static final class FailingConnector implements Connector {
        private final int instrumentId;

        private FailingConnector(int instrumentId) {
            this.instrumentId = instrumentId;
        }

        @Override
        public void init(ConnectorContext ctx) {
            throw new IllegalStateException("init failed");
        }

        @Override
        public void connect() {
        }

        @Override
        public void recover(RecoveryRequest request) {
        }

        @Override
        public void shutdown() {
        }

        @Override
        public VenueEnum venue() {
            return VenueEnum.COINBASE_L2;
        }

        @Override
        public int instrumentId() {
            return instrumentId;
        }
    }
}
